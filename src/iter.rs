//! Merging and MVCC/GC filtering over sorted streams of [`Entry`](crate::memtable::Entry).
//!
//! ## Ordering
//!
//! Every stream in this module — memtable `iter_from`, SST `iter_from`, and every function
//! here — yields entries ordered by `user_key ASC, seqno DESC`: all versions of one key are
//! contiguous, newest first. [`merge`] combines several such streams into one that keeps that
//! same order (k-way merge, tie-broken by source index so lower-indexed sources — the newer
//! ones in the read path: active memtable, then frozen, then L0 newest-first, then L1+ — win
//! duplicate `(key, seq)` pairs). [`visible`] and [`compaction_filter`] both rely on this
//! ordering: they can decide a key's fate by looking at one contiguous run, without buffering
//! more than the current key's group.
//!
//! ## The GC rule
//!
//! A snapshot at `seq = s` must see, for every key, the newest version with `seqno <= s`
//! (or nothing, if that version is a tombstone or the key doesn't exist yet). That's what
//! [`visible`] computes directly.
//!
//! [`compaction_filter`] is the rule compaction uses to drop data without breaking that
//! invariant for any snapshot that might still read the result. Given `oldest_snapshot` (the
//! smallest seqno among all currently-open snapshots, or `u64::MAX` if none are open), it keeps:
//!
//! * every version with `seqno > oldest_snapshot` — some open snapshot might depend on it, and
//!   we don't know which one, so all of them stay;
//! * the newest version with `seqno <= oldest_snapshot` — this is the version every snapshot
//!   `s <= oldest_snapshot`, and every snapshot in between with no version of its own, would
//!   read;
//! * ... unless that kept version is a tombstone and `drop_tombstones` is set (only true for a
//!   compaction whose output is the bottom-most level for this key range, i.e. there is no
//!   older data anywhere it could be un-deleting).
//!
//! Everything else — older versions below that boundary — is invisible to every snapshot
//! `s >= oldest_snapshot` by definition (a newer version `<= s` shadows them), so dropping them
//! changes nothing any live or future (`>= oldest_snapshot`) snapshot can observe. Concretely:
//! `visible(compaction_filter(x, oldest_snapshot, true), s) == visible(x, s)` for every
//! `s >= oldest_snapshot`, which is the property the proptest below checks. Tombstones with
//! `seqno > oldest_snapshot` are always kept because dropping them would resurrect the key for
//! a snapshot that hasn't opened yet but will land above `oldest_snapshot`.

use crate::memtable::{Entry, Value};
use crate::{Error, Result};
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, VecDeque};

/// A boxed, thread-safe stream of entries — the common currency between the memtable, SSTable
/// readers, and this module.
pub type BoxIter<'a> = Box<dyn Iterator<Item = Result<Entry>> + Send + 'a>;

// ---------------------------------------------------------------------------------------------
// merge
// ---------------------------------------------------------------------------------------------

/// One entry sitting in the merge heap, tagged with which source it came from so ties break by
/// source index (ascending — lower-indexed sources win).
#[derive(Eq, PartialEq)]
struct HeapEntry {
    entry: Entry,
    source: usize,
}

impl HeapEntry {
    /// Sort key for `Ord`. `BinaryHeap` is a max-heap, so this is built such that the entry
    /// that belongs *first* in the merged output — smallest user_key, then largest seqno, then
    /// smallest source index — compares as the maximum.
    fn sort_key(&self) -> (Reverse<&[u8]>, u64, Reverse<usize>) {
        (
            Reverse(self.entry.0.as_slice()),
            self.entry.1,
            Reverse(self.source),
        )
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sort_key().cmp(&other.sort_key())
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct Merge<'a> {
    sources: Vec<BoxIter<'a>>,
    heap: BinaryHeap<HeapEntry>,
    /// An error already pulled from a source, waiting to be surfaced by the *next* call to
    /// `next()` (the entry popped just before it was found is returned first).
    pending_err: Option<Error>,
    done: bool,
}

impl<'a> Merge<'a> {
    fn new(mut sources: Vec<BoxIter<'a>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        let mut pending_err = None;
        for (idx, src) in sources.iter_mut().enumerate() {
            match src.next() {
                Some(Ok(entry)) => heap.push(HeapEntry { entry, source: idx }),
                Some(Err(e)) => {
                    pending_err = Some(e);
                    break;
                }
                None => {}
            }
        }
        Merge {
            sources,
            heap,
            pending_err,
            done: false,
        }
    }

    /// Pull the next entry from `source` into the heap; stash an error rather than losing it.
    fn pull(&mut self, source: usize) {
        match self.sources[source].next() {
            Some(Ok(entry)) => self.heap.push(HeapEntry { entry, source }),
            Some(Err(e)) => self.pending_err = Some(e),
            None => {}
        }
    }
}

impl<'a> Iterator for Merge<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let HeapEntry { entry, source } = match self.heap.pop() {
            Some(e) => e,
            None => {
                if let Some(e) = self.pending_err.take() {
                    self.done = true;
                    return Some(Err(e));
                }
                return None;
            }
        };
        self.pull(source);
        // Drop duplicate (key, seq) pairs from higher-indexed sources.
        while let Some(top) = self.heap.peek() {
            if top.entry.0 == entry.0 && top.entry.1 == entry.1 {
                let dup = self.heap.pop().expect("peeked");
                self.pull(dup.source);
            } else {
                break;
            }
        }
        Some(Ok(entry))
    }
}

/// K-way merge of sorted sources into one sorted stream (see module docs for ordering).
/// Sources earlier in `sources` win ties on identical `(key, seq)` — the duplicate from a
/// later source is dropped. An `Err` from any source is yielded once and ends the stream.
pub fn merge<'a>(sources: Vec<BoxIter<'a>>) -> BoxIter<'a> {
    Box::new(Merge::new(sources))
}

// ---------------------------------------------------------------------------------------------
// visible
// ---------------------------------------------------------------------------------------------

struct VisibleIter<'a> {
    inner: BoxIter<'a>,
    snapshot_seq: u64,
    lookahead: Option<Entry>,
    done: bool,
}

impl<'a> Iterator for VisibleIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            let first = match self.lookahead.take() {
                Some(e) => e,
                None => match self.inner.next() {
                    Some(Ok(e)) => e,
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                },
            };
            let (key, seq, val) = first;
            let mut chosen = if seq <= self.snapshot_seq {
                Some(val)
            } else {
                None
            };
            loop {
                match self.inner.next() {
                    Some(Ok((k2, s2, v2))) => {
                        if k2 != key {
                            self.lookahead = Some((k2, s2, v2));
                            break;
                        }
                        if chosen.is_none() && s2 <= self.snapshot_seq {
                            chosen = Some(v2);
                        }
                        // Older duplicate of a version we've already resolved: drop.
                    }
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                    None => {
                        self.done = true;
                        break;
                    }
                }
            }
            match chosen {
                Some(Value::Put(v)) => return Some(Ok((key, v))),
                Some(Value::Delete) | None => continue,
            }
        }
    }
}

/// User-visible view at `snapshot_seq`: per user key, the newest version with
/// `seq <= snapshot_seq`; tombstones (and keys with no such version) are suppressed. Yields
/// `(key, value)`. An `Err` from `inner` is yielded once and ends the stream.
pub fn visible<'a>(
    inner: BoxIter<'a>,
    snapshot_seq: u64,
) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a {
    VisibleIter {
        inner,
        snapshot_seq,
        lookahead: None,
        done: false,
    }
}

// ---------------------------------------------------------------------------------------------
// compaction_filter
// ---------------------------------------------------------------------------------------------

struct CompactionFilterIter<'a> {
    inner: BoxIter<'a>,
    oldest_snapshot: u64,
    drop_tombstones: bool,
    lookahead: Option<Entry>,
    queue: VecDeque<Entry>,
    pending_err: Option<Error>,
    finished: bool,
}

impl<'a> CompactionFilterIter<'a> {
    /// Decide whether to keep `e`, given whether it is the boundary version (newest with
    /// `seq <= oldest_snapshot`) for its key.
    fn push_filtered(&mut self, e: Entry, is_boundary: bool) {
        if is_boundary && self.drop_tombstones && e.2 == Value::Delete {
            return;
        }
        self.queue.push_back(e);
    }
}

impl<'a> Iterator for CompactionFilterIter<'a> {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(e) = self.queue.pop_front() {
                return Some(Ok(e));
            }
            if let Some(e) = self.pending_err.take() {
                return Some(Err(e));
            }
            if self.finished {
                return None;
            }
            let first = match self.lookahead.take() {
                Some(e) => e,
                None => match self.inner.next() {
                    Some(Ok(e)) => e,
                    Some(Err(e)) => {
                        self.finished = true;
                        return Some(Err(e));
                    }
                    None => {
                        self.finished = true;
                        return None;
                    }
                },
            };
            let key = first.0.clone();
            let mut boundary_hit = first.1 <= self.oldest_snapshot;
            self.push_filtered(first, boundary_hit);
            loop {
                match self.inner.next() {
                    Some(Ok(e)) => {
                        if e.0 != key {
                            self.lookahead = Some(e);
                            break;
                        }
                        if boundary_hit {
                            // Older than the kept boundary version: always dropped.
                            continue;
                        }
                        let is_boundary = e.1 <= self.oldest_snapshot;
                        self.push_filtered(e, is_boundary);
                        boundary_hit = is_boundary;
                    }
                    Some(Err(e)) => {
                        self.pending_err = Some(e);
                        self.finished = true;
                        break;
                    }
                    None => {
                        self.finished = true;
                        break;
                    }
                }
            }
        }
    }
}

/// Compaction GC. Per user key, keeps every version with `seq > oldest_snapshot`, plus the
/// newest version with `seq <= oldest_snapshot`; older versions are dropped. That kept boundary
/// version is also dropped if it is a tombstone and `drop_tombstones` is set (pass `true` only
/// when the output is the bottom-most level for this key range — see module docs). An `Err`
/// from `inner` is yielded once (after any entries already resolved) and ends the stream.
pub fn compaction_filter<'a>(
    inner: BoxIter<'a>,
    oldest_snapshot: u64,
    drop_tombstones: bool,
) -> BoxIter<'a> {
    Box::new(CompactionFilterIter {
        inner,
        oldest_snapshot,
        drop_tombstones,
        lookahead: None,
        queue: VecDeque::new(),
        pending_err: None,
        finished: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use std::collections::BTreeMap;

    fn ok_iter<'a>(entries: Vec<Entry>) -> BoxIter<'a> {
        Box::new(entries.into_iter().map(Ok))
    }

    fn put(k: &str, s: u64, v: &str) -> Entry {
        (k.as_bytes().to_vec(), s, Value::Put(v.as_bytes().to_vec()))
    }

    fn del(k: &str, s: u64) -> Entry {
        (k.as_bytes().to_vec(), s, Value::Delete)
    }

    // -- memtable (get / tombstone / snapshot / iter_from) is covered in memtable.rs; a smoke
    // test here just checks it plugs into iter_from cleanly.
    #[test]
    fn memtable_iter_from_feeds_merge() {
        let m = Memtable::new();
        m.insert(b"a".to_vec(), 1, Value::Put(b"1".to_vec()));
        m.insert(b"b".to_vec(), 1, Value::Put(b"1".to_vec()));
        let entries: Vec<Entry> = m.iter_from(b"").collect();
        let merged: Vec<Entry> = merge(vec![ok_iter(entries)])
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_of_sorted_sources_equals_sorted_concat() {
        let a = vec![put("a", 3, "a3"), put("b", 1, "b1")];
        let b = vec![put("a", 1, "a1"), put("c", 2, "c2")];
        let merged: Vec<Entry> = merge(vec![ok_iter(a), ok_iter(b)])
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            merged,
            vec![
                put("a", 3, "a3"),
                put("a", 1, "a1"),
                put("b", 1, "b1"),
                put("c", 2, "c2"),
            ]
        );
    }

    #[test]
    fn merge_duplicate_key_seq_keeps_lowest_source_index() {
        let a = vec![put("a", 1, "from-a")];
        let b = vec![put("a", 1, "from-b")];
        let merged: Vec<Entry> = merge(vec![ok_iter(a), ok_iter(b)])
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(merged, vec![put("a", 1, "from-a")]);
    }

    #[test]
    fn merge_error_propagates_then_ends() {
        let good = vec![Ok(put("a", 1, "a1"))];
        let bad: Vec<Result<Entry>> = vec![Err(Error::WalCorrupt("boom".into()))];
        let src_a: BoxIter<'_> = Box::new(good.into_iter());
        let src_b: BoxIter<'_> = Box::new(bad.into_iter());
        let mut it = merge(vec![src_a, src_b]);
        assert_eq!(it.next().unwrap().unwrap(), put("a", 1, "a1"));
        assert!(it.next().unwrap().is_err());
        assert!(it.next().is_none());
    }

    fn model_visible(x: &[Entry], snapshot_seq: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut latest: BTreeMap<Vec<u8>, (u64, Value)> = BTreeMap::new();
        for (k, s, v) in x {
            if *s > snapshot_seq {
                continue;
            }
            latest
                .entry(k.clone())
                .and_modify(|cur| {
                    if *s > cur.0 {
                        *cur = (*s, v.clone());
                    }
                })
                .or_insert((*s, v.clone()));
        }
        latest
            .into_iter()
            .filter_map(|(k, (_, v))| match v {
                Value::Put(val) => Some((k, val)),
                Value::Delete => None,
            })
            .collect()
    }

    #[test]
    fn visible_matches_btreemap_model() {
        let x = vec![
            put("a", 5, "a5"),
            put("a", 2, "a2"),
            del("b", 3),
            put("b", 1, "b1"),
            put("c", 4, "c4"),
        ];
        for snap in [0u64, 1, 2, 3, 4, 5, 6] {
            let got: BTreeMap<Vec<u8>, Vec<u8>> = visible(ok_iter(x.clone()), snap)
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(got, model_visible(&x, snap), "snapshot {snap}");
        }
    }

    #[test]
    fn compaction_filter_keeps_versions_above_oldest_snapshot() {
        // Multiple snapshots: everything above oldest_snapshot=2 survives untouched, plus the
        // newest version <= 2 (seq 2 itself).
        let x = vec![
            put("a", 5, "a5"),
            put("a", 3, "a3"),
            put("a", 2, "a2"),
            put("a", 1, "a1"),
        ];
        let out: Vec<Entry> = compaction_filter(ok_iter(x), 2, false)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            out,
            vec![put("a", 5, "a5"), put("a", 3, "a3"), put("a", 2, "a2")]
        );
    }

    #[test]
    fn compaction_filter_tombstone_at_bottom_dropped_when_requested() {
        let x = vec![put("a", 5, "a5"), del("a", 2), put("a", 1, "a1")];
        // Not bottom level: tombstone at the boundary is kept.
        let out: Vec<Entry> = compaction_filter(ok_iter(x.clone()), 2, false)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(out, vec![put("a", 5, "a5"), del("a", 2)]);

        // Bottom level: tombstone at the boundary is dropped entirely.
        let out: Vec<Entry> = compaction_filter(ok_iter(x), 2, true)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(out, vec![put("a", 5, "a5")]);
    }

    #[test]
    fn compaction_filter_tombstone_above_oldest_snapshot_always_kept() {
        // The tombstone itself is above oldest_snapshot, so it's always kept even with
        // drop_tombstones=true. It is *not* the boundary version here (seq 1 is), so the
        // older put underneath it is independently kept too — collapsing "a tombstone hides
        // what's under it" is `visible()`'s job, not this function's.
        let x = vec![del("a", 5), put("a", 1, "a1")];
        let out: Vec<Entry> = compaction_filter(ok_iter(x), 2, true)
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(out, vec![del("a", 5), put("a", 1, "a1")]);
    }

    #[test]
    fn compaction_filter_error_propagates_after_resolved_entries() {
        let mut items: Vec<Result<Entry>> = vec![Ok(put("a", 5, "a5")), Ok(put("a", 1, "a1"))];
        items.push(Err(Error::WalCorrupt("boom".into())));
        let src: BoxIter<'_> = Box::new(items.into_iter());
        let mut it = compaction_filter(src, 10, false);
        // seq 5 and seq 1 are both > nothing... oldest_snapshot=10 means both <=10; first (seq
        // 5) is the boundary version, seq 1 is older and dropped, then the error surfaces.
        assert_eq!(it.next().unwrap().unwrap(), put("a", 5, "a5"));
        assert!(it.next().unwrap().is_err());
        assert!(it.next().is_none());
    }

    // -- proptest: random entries split across N sources -----------------------------------

    use proptest::collection::vec as pvec;
    use proptest::prelude::*;

    fn arb_entry() -> impl Strategy<Value = Entry> {
        (
            "[a-e]{1,2}",
            1u64..50,
            prop_oneof![
                "[a-z]{0,4}".prop_map(|s| Value::Put(s.into_bytes())),
                Just(Value::Delete),
            ],
        )
            .prop_map(|(k, s, v)| (k.into_bytes(), s, v))
    }

    fn sort_entries(mut v: Vec<Entry>) -> Vec<Entry> {
        v.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
        v.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        v
    }

    proptest! {
        #[test]
        fn merge_is_sorted_and_complete(
            entries in pvec(arb_entry(), 0..60),
            n_sources in 1usize..5,
        ) {
            // Distinct (key, seq) pairs only, since duplicate resolution across sources is a
            // separate, already-tested concern.
            let expected = sort_entries(entries.clone());

            // Round-robin split into n_sources, each individually sorted (required input
            // contract of `merge`).
            let mut buckets: Vec<Vec<Entry>> = vec![Vec::new(); n_sources];
            for (i, e) in expected.iter().enumerate() {
                buckets[i % n_sources].push(e.clone());
            }
            for b in &mut buckets {
                b.sort_by(|a, c| a.0.cmp(&c.0).then_with(|| c.1.cmp(&a.1)));
            }

            let sources: Vec<BoxIter<'_>> = buckets.into_iter().map(ok_iter).collect();
            let got: Vec<Entry> = merge(sources).collect::<Result<Vec<_>>>().unwrap();
            prop_assert_eq!(got, expected);
        }

        /// THE key invariant: compacting away everything below `oldest_snapshot` (dropping
        /// tombstones at the boundary too) must not change what any snapshot `s >= oldest_snapshot`
        /// observes.
        #[test]
        fn compaction_filter_preserves_visibility_for_all_live_snapshots(
            entries in pvec(arb_entry(), 0..80),
            oldest_snapshot in 1u64..50,
            extra_snap in 0u64..20,
        ) {
            let x = sort_entries(entries);
            let s = oldest_snapshot + extra_snap; // s >= oldest_snapshot

            let filtered: Vec<Entry> = compaction_filter(ok_iter(x.clone()), oldest_snapshot, true)
                .collect::<Result<Vec<_>>>()
                .unwrap();

            let before: BTreeMap<Vec<u8>, Vec<u8>> = visible(ok_iter(x), s)
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .collect();
            let after: BTreeMap<Vec<u8>, Vec<u8>> = visible(ok_iter(filtered), s)
                .collect::<Result<Vec<_>>>()
                .unwrap()
                .into_iter()
                .collect();
            prop_assert_eq!(before, after);
        }
    }
}

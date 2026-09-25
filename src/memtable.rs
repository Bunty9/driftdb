//! In-memory sorted map of `(user_key, seqno) -> Value`.
//!
//! Backed by `crossbeam-skiplist` for lock-free concurrent inserts + range scans. MVCC
//! ordering: user keys ascend, but multiple versions of the same key sort by seqno DESC so
//! the first range hit on a `get` is the newest version at or below the snapshot.

use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// `(user_key, seqno)` key shape used inside the skipmap. Ordering rule documented in `Ord`.
#[derive(Clone, Debug)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seqno: u64,
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // user_key ASC, then seqno DESC so newest version of a key iterates first.
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seqno.cmp(&self.seqno))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, o: &Self) -> bool {
        self.user_key == o.user_key && self.seqno == o.seqno
    }
}

impl Eq for InternalKey {}

/// MVCC value variant — `Delete` is a tombstone the compactor eventually drops once it falls
/// below the oldest live snapshot watermark.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Put(Vec<u8>),
    Delete,
}

/// One versioned record: `(user_key, seqno, value)`. Streams of entries are always ordered by
/// user_key ASC, then seqno DESC (newest version of a key first) — see `iter.rs`.
pub type Entry = (Vec<u8>, u64, Value);

/// Concurrent memtable. `Db` holds one of these as the active write target plus a stack of
/// frozen ones waiting on the flush thread.
#[derive(Debug, Default)]
pub struct Memtable {
    map: SkipMap<InternalKey, Value>,
    approx_bytes: AtomicUsize,
}

impl Memtable {
    /// Empty memtable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a versioned record. Bumps the approximate-byte counter so the freeze threshold
    /// can be cheap.
    pub fn insert(&self, key: Vec<u8>, seqno: u64, val: Value) {
        let bytes = key.len()
            + match &val {
                Value::Put(v) => v.len(),
                Value::Delete => 0,
            };
        self.approx_bytes.fetch_add(bytes + 16, Ordering::Relaxed);
        self.map.insert(
            InternalKey {
                user_key: key,
                seqno,
            },
            val,
        );
    }

    /// Snapshot read: newest version `<= snapshot_seq` for `user_key`. `Some(Value::Delete)`
    /// means a tombstone was found (the caller must stop searching older sources — L0, L1, ...
    /// — for this key); `None` means the key is absent from this memtable at or below the
    /// snapshot.
    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Option<Value> {
        // Probe key allocated once. Range from (user_key, MAX) onwards; ordering means the
        // first matching user_key entry is the newest version, but we still skip any version
        // newer than the snapshot.
        let probe = InternalKey {
            user_key: user_key.to_vec(),
            seqno: u64::MAX,
        };
        for entry in self.map.range(probe..) {
            if entry.key().user_key != user_key {
                return None;
            }
            if entry.key().seqno > snapshot_seq {
                continue;
            }
            return Some(entry.value().clone());
        }
        None
    }

    /// All versions of every key with `user_key >= start`, in `Entry` order (user_key ASC,
    /// seqno DESC).
    pub fn iter_from(&self, start: &[u8]) -> impl Iterator<Item = Entry> + '_ {
        let probe = InternalKey {
            user_key: start.to_vec(),
            seqno: u64::MAX,
        };
        self.map.range(probe..).map(|entry| {
            let key = entry.key();
            (key.user_key.clone(), key.seqno, entry.value().clone())
        })
    }

    /// Approximate byte size — used to trigger freeze + flush.
    pub fn size(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// Number of versioned records held (not distinct keys).
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if no records have been inserted.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_put_value() {
        let m = Memtable::new();
        m.insert(b"a".to_vec(), 1, Value::Put(b"v1".to_vec()));
        assert_eq!(m.get(b"a", 1), Some(Value::Put(b"v1".to_vec())));
    }

    #[test]
    fn get_missing_key_is_none() {
        let m = Memtable::new();
        m.insert(b"a".to_vec(), 1, Value::Put(b"v1".to_vec()));
        assert_eq!(m.get(b"z", 10), None);
    }

    #[test]
    fn get_tombstone_is_some_delete() {
        let m = Memtable::new();
        m.insert(b"a".to_vec(), 1, Value::Put(b"v1".to_vec()));
        m.insert(b"a".to_vec(), 2, Value::Delete);
        assert_eq!(m.get(b"a", 2), Some(Value::Delete));
    }

    #[test]
    fn get_respects_snapshot_seq() {
        let m = Memtable::new();
        m.insert(b"a".to_vec(), 1, Value::Put(b"v1".to_vec()));
        m.insert(b"a".to_vec(), 5, Value::Put(b"v5".to_vec()));
        // Snapshot before the second write only sees the first.
        assert_eq!(m.get(b"a", 3), Some(Value::Put(b"v1".to_vec())));
        // Snapshot at/after the second write sees the newest.
        assert_eq!(m.get(b"a", 5), Some(Value::Put(b"v5".to_vec())));
        // Snapshot before any write sees nothing.
        assert_eq!(m.get(b"a", 0), None);
    }

    #[test]
    fn iter_from_orders_and_filters_by_start() {
        let m = Memtable::new();
        m.insert(b"b".to_vec(), 1, Value::Put(b"b1".to_vec()));
        m.insert(b"a".to_vec(), 1, Value::Put(b"a1".to_vec()));
        m.insert(b"b".to_vec(), 2, Value::Put(b"b2".to_vec()));
        m.insert(b"c".to_vec(), 1, Value::Delete);

        // From the very start: all keys, user_key ASC, seqno DESC within a key.
        let all: Vec<Entry> = m.iter_from(b"").collect();
        assert_eq!(
            all,
            vec![
                (b"a".to_vec(), 1, Value::Put(b"a1".to_vec())),
                (b"b".to_vec(), 2, Value::Put(b"b2".to_vec())),
                (b"b".to_vec(), 1, Value::Put(b"b1".to_vec())),
                (b"c".to_vec(), 1, Value::Delete),
            ]
        );

        // From "b": skips "a" entirely, keeps all versions of "b" and beyond.
        let from_b: Vec<Entry> = m.iter_from(b"b").collect();
        assert_eq!(
            from_b,
            vec![
                (b"b".to_vec(), 2, Value::Put(b"b2".to_vec())),
                (b"b".to_vec(), 1, Value::Put(b"b1".to_vec())),
                (b"c".to_vec(), 1, Value::Delete),
            ]
        );
    }

    #[test]
    fn len_counts_versions_not_distinct_keys() {
        let m = Memtable::new();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        m.insert(b"a".to_vec(), 1, Value::Put(b"v1".to_vec()));
        m.insert(b"a".to_vec(), 2, Value::Put(b"v2".to_vec()));
        m.insert(b"b".to_vec(), 1, Value::Put(b"v1".to_vec()));
        assert_eq!(m.len(), 3);
        assert!(!m.is_empty());
    }
}

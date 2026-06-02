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
#[derive(Clone, Debug)]
pub enum Value {
    Put(Vec<u8>),
    Delete,
}

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
        self.map.insert(InternalKey { user_key: key, seqno }, val);
    }

    /// Snapshot read: find the first version `<= snapshot_seq` for `user_key`. Returns `None`
    /// on tombstone or absence.
    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Option<Vec<u8>> {
        // Range from (user_key, MAX) onwards; ordering means the first matching user_key entry
        // is the newest version, but we still skip any version newer than the snapshot.
        for entry in self.map.range(
            InternalKey {
                user_key: user_key.to_vec(),
                seqno: u64::MAX,
            }..,
        ) {
            if entry.key().user_key != user_key {
                return None;
            }
            if entry.key().seqno > snapshot_seq {
                continue;
            }
            return match entry.value() {
                Value::Put(v) => Some(v.clone()),
                Value::Delete => None,
            };
        }
        None
    }

    /// Approximate byte size — used to trigger freeze + flush.
    pub fn size(&self) -> usize {
        self.approx_bytes.load(Ordering::Relaxed)
    }

    /// True if no records have been inserted.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

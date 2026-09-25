//! Top-level `Db` handle.
//!
//! Holds:
//!   - the current WAL file (`wal.rs`'s `WalFile`),
//!   - the active memtable + a stack of frozen memtables waiting on flush,
//!   - the manifest log + per-level SST metadata,
//!   - the snapshot watermark used to gate MVCC reads + tombstone GC.
//!
//! **Transitional status:** the memtable-only path (`put`/`delete` → WAL sync → memtable, `get`
//! → memtable) is wired so the quickstart example exercises real code, but every write
//! synchronously locks the WAL and fsyncs inline — no group commit, no WAL rotation, no replay
//! on open. The dedicated writer thread + freeze/flush cycle described in
//! `docs/plans/2026-09-25-driftdb-phase-2.md`'s "Durability model" lands with the full `db.rs`
//! rewrite; flush / compaction / SST reads are stubbed out in their dedicated modules until then.

use crate::compaction::CompactionState;
use crate::manifest::Manifest;
use crate::memtable::{Memtable, Value};
use crate::wal::WalFile;
use crate::Result;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A point-in-time snapshot. Holds a seqno watermark — reads through the snapshot ignore
/// records with `seqno > snap.seq`. While at least one snapshot is live, the compactor cannot
/// drop tombstones whose seqno is above the oldest live snapshot.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub seq: u64,
}

/// Per-level metadata. One vector of SST paths per level. Phase 1 leaves this empty — flush
/// is not yet wired.
#[derive(Debug, Default)]
struct Levels {
    /// `levels[0]` = L0, `levels[1]` = L1, ... Up to ~7 levels is typical.
    levels: Vec<Vec<PathBuf>>,
}

/// Shared mutable state. Wrapped in `Arc` so the compactor task can hold a handle.
#[derive(Debug)]
pub(crate) struct DbState {
    pub(crate) levels: Mutex<Levels>,
    pub(crate) manifest: Mutex<Manifest>,
    /// Monotonically increasing — bumped on every `put`/`delete`.
    pub(crate) last_seq: AtomicU64,
    /// Oldest live snapshot seqno; tombstones below this are safe to GC.
    pub(crate) oldest_snapshot: AtomicU64,
}

impl CompactionState for DbState {
    fn pick_compaction(&self) -> Option<crate::compaction::CompactionPlan> {
        // Phase 2: inspect `self.levels` against size thresholds.
        None
    }
}

/// The public handle. Cheap to clone — internally holds an `Arc` of the shared state plus the
/// WAL file.
#[derive(Debug, Clone)]
pub struct Db {
    state: Arc<DbState>,
    active: Arc<Memtable>,
    /// The current WAL file. `db.rs` gets a full group-commit rewrite in a later phase; for now
    /// every write takes the lock and syncs synchronously.
    wal: Arc<Mutex<WalFile>>,
}

impl Db {
    /// Open (or create) a driftdb instance rooted at `path`.
    ///
    /// Layout under `path`:
    /// ```text
    ///   path/
    ///     MANIFEST
    ///     wal-000001.log
    ///     L0/<uuid>.sst
    ///     L1/<uuid>.sst
    ///     ...
    /// ```
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let manifest = Manifest::open(path)?;

        // Phase 1/2-transition: always start a fresh WAL at generation 1 and skip replay. Real
        // WAL-file enumeration, replay, and the group-commit writer thread land with the full
        // `db.rs` rewrite (see `docs/plans/2026-09-25-driftdb-phase-2.md`); this is just enough
        // to keep the crate compiling on the new `wal.rs` contract.
        let wal = WalFile::create(path, 1)?;

        let state = Arc::new(DbState {
            levels: Mutex::new(Levels::default()),
            manifest: Mutex::new(manifest),
            last_seq: AtomicU64::new(0),
            oldest_snapshot: AtomicU64::new(0),
        });

        // Phase 2: spawn the compactor.
        //   tokio::spawn(crate::compaction::compactor(state.clone()));

        Ok(Self {
            state,
            active: Arc::new(Memtable::new()),
            wal: Arc::new(Mutex::new(wal)),
        })
    }

    /// Durable put. Returns once the record is in the WAL + memtable.
    pub async fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.write(key, Value::Put(val.to_vec()))
    }

    /// Latest-version read. Equivalent to `self.snapshot().get(...)`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let seq = self.state.last_seq.load(Ordering::Acquire);
        // Phase 1: memtable-only path. Phase 2 falls through to frozen memtables + L0 + L1+.
        Ok(self.active.get(key, seq))
    }

    /// Tombstone write. Same durability contract as `put`.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.write(key, Value::Delete)
    }

    /// Assign the next seqno, append + sync the WAL record, then apply it to the memtable.
    /// Blocking (WAL sync is a syscall); the transitional single-writer path in `db.rs` doesn't
    /// yet hand this off to a dedicated group-commit thread — see the `open` doc comment.
    fn write(&self, key: &[u8], val: Value) -> Result<()> {
        let seq = self.state.last_seq.fetch_add(1, Ordering::AcqRel) + 1;
        {
            let mut wal = self.wal.lock();
            wal.append(seq, key, &val);
            wal.sync()?;
        }
        self.active.insert(key.to_vec(), seq, val);
        Ok(())
    }

    /// Open a snapshot at the current `last_seq`. Reads through the snapshot are isolated
    /// from later writes. While at least one snapshot is alive, the compactor will not GC
    /// tombstones whose seqno is above the oldest snapshot watermark.
    pub fn snapshot(&self) -> Snapshot {
        let seq = self.state.last_seq.load(Ordering::Acquire);
        // Phase 2: track the snapshot in a watermark tracker so `oldest_snapshot` updates.
        Snapshot { seq }
    }
}

//! Top-level `Db` handle.
//!
//! Holds:
//!   - a channel into the WAL writer task (group-commit fsync),
//!   - the active memtable + a stack of frozen memtables waiting on flush,
//!   - the manifest log + per-level SST metadata,
//!   - the snapshot watermark used to gate MVCC reads + tombstone GC.
//!
//! **Phase 1 status:** the memtable-only path (`put` → memtable, `get` → memtable) is wired
//! so the quickstart example exercises real code. The WAL writer task is spawned, but flush /
//! compaction / SST reads are stubbed out and surface as `todo!()` in their dedicated
//! modules. This is the same shape the other P1–P4 scaffolds ship in.

use crate::compaction::CompactionState;
use crate::manifest::Manifest;
use crate::memtable::{Memtable, Value};
use crate::wal::{WalMsg, WalWriter, DEFAULT_COMMIT_WINDOW_MS};
use crate::Result;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
/// WAL sender.
#[derive(Debug, Clone)]
pub struct Db {
    state: Arc<DbState>,
    active: Arc<Memtable>,
    /// Send a record to the group-commit WAL writer.
    wal_tx: mpsc::Sender<WalMsg>,
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

        // Phase 1: a single fresh WAL file. Phase 2 enumerates + replays.
        let wal_path = path.join("wal-000001.log");
        let wal_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;
        let writer = WalWriter::new(wal_file, 1);

        let (wal_tx, wal_rx) = mpsc::channel::<WalMsg>(1024);
        let commit_window = std::time::Duration::from_millis(DEFAULT_COMMIT_WINDOW_MS);
        tokio::spawn(writer.run(wal_rx, commit_window));

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
            wal_tx,
        })
    }

    /// Durable put. Returns once the record is in the WAL + memtable.
    pub async fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.wal_tx
            .send(WalMsg::Write {
                key: key.to_vec(),
                val: val.to_vec(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "wal writer task gone",
                ))
            })?;
        let seq = ack_rx.await.map_err(|_| {
            crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "wal writer dropped ack",
            ))
        })?;
        self.state.last_seq.store(seq, Ordering::Release);
        self.active
            .insert(key.to_vec(), seq, Value::Put(val.to_vec()));
        Ok(())
    }

    /// Latest-version read. Equivalent to `self.snapshot().get(...)`.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let seq = self.state.last_seq.load(Ordering::Acquire);
        // Phase 1: memtable-only path. Phase 2 falls through to frozen memtables + L0 + L1+.
        Ok(self.active.get(key, seq.max(u64::MAX / 2)))
    }

    /// Tombstone write. Same durability contract as `put`.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        // Mirrors put but with an empty value + a tombstone marker in the memtable.
        let (ack_tx, ack_rx) = oneshot::channel();
        self.wal_tx
            .send(WalMsg::Write {
                key: key.to_vec(),
                val: Vec::new(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "wal writer task gone",
                ))
            })?;
        let seq = ack_rx.await.map_err(|_| {
            crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "wal writer dropped ack",
            ))
        })?;
        self.state.last_seq.store(seq, Ordering::Release);
        self.active.insert(key.to_vec(), seq, Value::Delete);
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

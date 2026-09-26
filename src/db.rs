//! Top-level `Db` handle: group-commit writer thread, background flush/compaction thread,
//! MVCC snapshot reads, and crash recovery.
//!
//! Write path: `put/delete/write_batch` -> channel -> writer thread -> WAL append + one
//! `fdatasync` per drained batch -> memtable insert -> `visible_seq` published -> ack.
//! When the active memtable crosses `Options::memtable_size` the writer thread freezes it,
//! opens a new WAL, and wakes the background thread, which flushes the oldest frozen memtable
//! to an L0 SST and (independently) runs leveled compaction. All mutations to the manifest,
//! the live `Version`, and the memtable list happen on exactly one of these two threads, so
//! there is never a race to install a new `Version` or append a manifest edit.
//!
//! Read path: active memtable -> frozen memtables (newest first) -> L0 (newest first) -> L1+
//! (binary search into the non-overlapping sorted run). See `Inner::get_at` for the locking
//! order that keeps a concurrent flush from ever hiding data mid-flight.

use crate::compaction::{self, CompactionPlan, Table, Version};
use crate::error::{Error, Result};
use crate::iter::{self, BoxIter};
use crate::manifest::{Manifest, ManifestRecord, SstMeta};
use crate::memtable::{Entry, Memtable, Value};
use crate::sstable::{sst_path, SstReader, SstWriter};
use crate::wal::{self, WalFile};
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::BTreeMap;
use std::io::BufWriter;
use std::ops::{Bound, RangeBounds};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

/// Engine tuning knobs. See each field for the default (matches the phase-2 plan).
#[derive(Clone, Debug)]
pub struct Options {
    /// Freeze the active memtable and roll to a new WAL once its approximate size reaches this.
    ///
    /// This also bounds recovery time: replay on reopen never reads more than roughly
    /// `memtable_size * (1 + MAX_IMMUTABLE_MEMTABLES)` bytes of WAL (the active memtable plus
    /// however many frozen-but-unflushed ones `rotate` allows to queue -- see
    /// `MAX_IMMUTABLE_MEMTABLES` in `db.rs`), since anything older has already been flushed to
    /// an SST and its WAL deleted. Each memtable can overshoot that budget by at most one
    /// request's bytes (`write_batch`'s ops are never split across a group-commit boundary), since
    /// the writer thread stops draining a group commit once it's already queued that many bytes
    /// -- see `writer_thread`'s `budget` in `db.rs`.
    pub memtable_size: usize,
    /// Compact all of L0 (+ overlapping L1) once L0 holds at least this many files.
    pub l0_compaction_trigger: usize,
    /// L1's byte budget; `L_n`'s budget is `l1_max_bytes * level_multiplier^(n-1)`.
    pub l1_max_bytes: u64,
    /// Per-level growth factor for the byte budget above.
    pub level_multiplier: u32,
    /// Roll to a new output SST once a compaction/flush output reaches this size (only at a
    /// user-key boundary).
    pub target_file_size: u64,
    /// Number of levels, L0..L(max_levels-1). The bottom level is never a compaction source.
    pub max_levels: usize,
    /// How long the writer thread waits for more writes to batch after draining what's already
    /// queued, once at least one request has arrived. `Duration::ZERO` disables the wait.
    pub commit_window: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_size: 4 * 1024 * 1024,
            l0_compaction_trigger: 4,
            l1_max_bytes: 10 * 1024 * 1024,
            level_multiplier: 10,
            target_file_size: 2 * 1024 * 1024,
            max_levels: 7,
            commit_window: Duration::ZERO,
        }
    }
}

/// A batch of puts/deletes applied atomically: one writer-thread request, consecutive seqnos,
/// one ack.
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    ops: Vec<(Vec<u8>, Value)>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(mut self, key: impl Into<Vec<u8>>, val: impl Into<Vec<u8>>) -> Self {
        self.ops.push((key.into(), Value::Put(val.into())));
        self
    }

    pub fn delete(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.ops.push((key.into(), Value::Delete));
        self
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }
}

/// Per-level file counts + byte totals, plus write-amplification inputs.
#[derive(Debug, Clone, Default)]
pub struct Stats {
    pub level_files: Vec<usize>,
    pub level_bytes: Vec<u64>,
    pub memtable_bytes: u64,
    pub user_bytes_written: u64,
    /// Bytes written to SST files by flushes and compactions combined (not the WAL -- see
    /// [`Stats::wal_bytes_written`]).
    pub disk_bytes_written: u64,
    /// Bytes written to the WAL (post-fsync, i.e. only what actually reached disk).
    pub wal_bytes_written: u64,
}

impl Stats {
    /// Write amplification, defined the way RocksDB defines it: `(WAL bytes + flush bytes +
    /// compaction bytes) / user bytes`. `disk_bytes_written` already sums flush + compaction
    /// bytes (see `Db::stats`), so this is `(wal_bytes_written + disk_bytes_written) /
    /// user_bytes_written`, or `0.0` before anything has been written.
    pub fn write_amplification(&self) -> f64 {
        if self.user_bytes_written == 0 {
            0.0
        } else {
            (self.wal_bytes_written + self.disk_bytes_written) as f64
                / self.user_bytes_written as f64
        }
    }
}

/// A point-in-time read view. Registers its seqno in the snapshot table on creation and
/// unregisters on drop; while at least one snapshot is registered at or below a given seqno,
/// the compactor keeps every version a read at that seqno (or lower) might need.
pub struct Snapshot {
    inner: Arc<Inner>,
    seq: u64,
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").field("seq", &self.seq).finish()
    }
}

impl Snapshot {
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get_at(key, self.seq)
    }

    pub fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.scan_at(range, self.seq)
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.inner.unregister_snapshot(self.seq);
    }
}

/// Active + frozen memtables. `immutables` is oldest-first; each is tagged with the number of
/// the WAL file that holds exactly its records (see module docs).
struct MemState {
    active: Arc<Memtable>,
    immutables: Vec<(u64, Arc<Memtable>)>,
}

#[derive(Default)]
struct StatsInner {
    user_bytes: AtomicU64,
    wal_bytes: AtomicU64,
    bytes_flushed: AtomicU64,
    bytes_compacted: AtomicU64,
}

/// One request sent to the writer thread.
enum Req {
    Write {
        batch: Vec<(Vec<u8>, Value)>,
        ack: tokio::sync::oneshot::Sender<Result<u64>>,
    },
    /// Force-rotate the active memtable (used by `flush()`), even if it's under the size
    /// threshold. `ack` carries the WAL number of the memtable that was frozen, or `None` if
    /// the active memtable was empty (nothing to flush).
    Rotate {
        ack: tokio::sync::oneshot::Sender<Result<Option<u64>>>,
    },
}

/// Shared engine state. Every mutation to `manifest`, `version`, or `mem.immutables` (removal)
/// happens on the background thread; every mutation to `mem.active`/`mem.immutables` (push)
/// happens on the writer thread. `fatal` is the one-way poison switch: once set, the writer
/// thread fails every request and the background thread stops picking new work.
struct Inner {
    dir: PathBuf,
    options: Options,
    version: RwLock<Arc<Version>>,
    mem: RwLock<MemState>,
    manifest: Mutex<Manifest>,
    visible_seq: AtomicU64,
    next_file_number: AtomicU64,
    /// seqno -> count of live `Snapshot`s registered at that seqno.
    snapshots: Mutex<BTreeMap<u64, usize>>,
    fatal: Mutex<Option<String>>,
    shutdown: AtomicBool,
    /// Set by `Db::compact()` while a forced full compaction is in flight; the background
    /// thread's picker consults it to bypass the normal trigger thresholds.
    force_compact: AtomicBool,
    work_mu: Mutex<()>,
    work_cv: Condvar,
    stall_mu: Mutex<()>,
    stall_cv: Condvar,
    flush_mu: Mutex<()>,
    flush_cv: Condvar,
    stats: StatsInner,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner").field("dir", &self.dir).finish()
    }
}

/// Error returned to a caller whose request can't reach the writer thread: normal shutdown (the
/// sender was dropped by `Guard::shutdown_and_join`) or the writer thread having gone away
/// unexpectedly (e.g. a panic, which drops its ack sender without replying). Both look the same
/// from here -- the channel is just closed -- so the message says so plainly rather than
/// guessing which one happened.
fn closed_err() -> Error {
    Error::Io(std::io::Error::other(
        "driftdb: engine closed (writer thread is no longer running -- either close()/Drop \
         already ran, or the writer thread exited unexpectedly)",
    ))
}

impl Inner {
    fn notify_bg(&self) {
        self.work_cv.notify_all();
    }

    fn notify_all_waiters(&self) {
        self.stall_cv.notify_all();
        self.flush_cv.notify_all();
    }

    fn check_fatal(&self) -> Result<()> {
        if let Some(msg) = self.fatal.lock().clone() {
            return Err(Error::Io(std::io::Error::other(msg)));
        }
        Ok(())
    }

    /// Register a new read view at the current `visible_seq` and return it. Registration and the
    /// `visible_seq` read happen under the same `snapshots` lock that [`Inner::oldest_snapshot`]
    /// uses, so a concurrent compaction can never observe a `visible_seq` that's newer than what
    /// this call is about to register (which would let it GC a version this read still needs).
    ///
    /// ponytail: one mutex lock/unlock pair per registration is the whole cost of correctness
    /// here; a lock-free epoch scheme would remove the mutex from the read path entirely if this
    /// ever shows up in a profile.
    fn register_snapshot(&self) -> u64 {
        let mut snapshots = self.snapshots.lock();
        let seq = self.visible_seq.load(Ordering::Acquire);
        *snapshots.entry(seq).or_insert(0) += 1;
        seq
    }

    /// Unregister a read view previously returned by [`Inner::register_snapshot`].
    ///
    /// No `notify_all_waiters()` here: `stall_cv` is only waited on by `rotate` (for
    /// `mem.immutables` shrinking, which only `do_flush` changes) and `flush_cv` only by
    /// `wait_flushed`/`compact_blocking` (for a flush/compaction actually finishing, which
    /// `do_flush`/`do_compact` already notify on). Nothing ever blocks on a condvar waiting for
    /// the *snapshot table* to shrink -- `do_compact` just reads `oldest_snapshot()` once, it
    /// doesn't wait for it to advance -- so broadcasting on every `get`/`scan` unregister (i.e.
    /// on every read) was pure overhead.
    fn unregister_snapshot(&self, seq: u64) {
        let mut snapshots = self.snapshots.lock();
        if let Some(count) = snapshots.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                snapshots.remove(&seq);
            }
        }
    }

    fn oldest_snapshot(&self) -> u64 {
        let snapshots = self.snapshots.lock();
        let floor = snapshots.keys().next().copied().unwrap_or(u64::MAX);
        let visible = self.visible_seq.load(Ordering::Acquire);
        drop(snapshots);
        floor.min(visible)
    }

    fn alloc_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::SeqCst)
    }

    /// Read at `seq`: active -> frozen (newest first) -> L0 (newest first) -> L1+ (binary
    /// search). Memtables are snapshotted *before* the version so a flush that's mid-flight can
    /// never hide data: the background thread installs the new version before it drops the
    /// frozen memtable, so a reader either still sees the memtable or already sees the new SST.
    fn get_at(&self, key: &[u8], seq: u64) -> Result<Option<Vec<u8>>> {
        self.check_fatal()?;
        let (active, immutables) = {
            let mem = self.mem.read();
            (
                mem.active.clone(),
                mem.immutables
                    .iter()
                    .rev()
                    .map(|(_, m)| m.clone())
                    .collect::<Vec<_>>(),
            )
        };
        let version = self.version.read().clone();

        if let Some(v) = active.get(key, seq) {
            return Ok(as_option(v));
        }
        for imm in &immutables {
            if let Some(v) = imm.get(key, seq) {
                return Ok(as_option(v));
            }
        }
        if let Some(l0) = version.levels.first() {
            for table in l0 {
                if key < table.meta.smallest.as_slice() || key > table.meta.largest.as_slice() {
                    continue;
                }
                if let Some(v) = table.reader.get(key, seq)? {
                    return Ok(as_option(v));
                }
            }
        }
        for level in version.levels.iter().skip(1) {
            let idx = level.partition_point(|t| t.meta.largest.as_slice() < key);
            if let Some(table) = level.get(idx) {
                if key >= table.meta.smallest.as_slice() && key <= table.meta.largest.as_slice() {
                    if let Some(v) = table.reader.get(key, seq)? {
                        return Ok(as_option(v));
                    }
                }
            }
        }
        Ok(None)
    }

    fn scan_at<R: RangeBounds<Vec<u8>>>(
        &self,
        range: R,
        seq: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_fatal()?;
        let start_bound = range.start_bound().cloned();
        let start: Vec<u8> = match &start_bound {
            Bound::Included(k) | Bound::Excluded(k) => k.clone(),
            Bound::Unbounded => Vec::new(),
        };
        let end_bound = range.end_bound().cloned();

        let (active, immutables) = {
            let mem = self.mem.read();
            (
                mem.active.clone(),
                mem.immutables
                    .iter()
                    .rev()
                    .map(|(_, m)| m.clone())
                    .collect::<Vec<_>>(),
            )
        };
        let version = self.version.read().clone();

        // ponytail: memtables are collected eagerly (rather than streamed via a borrowing
        // iterator) to sidestep a `BoxIter<'a>: Send` lifetime fight with the skiplist's range
        // iterator. Fine while memtables are capped at a few MiB by `Options::memtable_size`;
        // revisit if scans ever need to run against a much larger active memtable.
        let mut sources: Vec<BoxIter<'_>> = Vec::new();
        let active_entries: Vec<Entry> = active.iter_from(&start).collect();
        sources.push(Box::new(active_entries.into_iter().map(Ok)));
        for imm in &immutables {
            let entries: Vec<Entry> = imm.iter_from(&start).collect();
            sources.push(Box::new(entries.into_iter().map(Ok)));
        }
        for level in &version.levels {
            for t in level {
                if t.meta.largest.as_slice() < start.as_slice() {
                    continue;
                }
                sources.push(Box::new(t.reader.iter_from(&start)));
            }
        }

        let merged = iter::merge(sources);
        let visible = iter::visible(merged, seq);
        let mut out = Vec::new();
        for item in visible {
            let (k, v) = item?;
            // `iter_from`/the SST readers seek to `start` inclusively, so an `Excluded` start
            // bound needs its own filter here to drop the boundary key itself.
            if let Bound::Excluded(s) = &start_bound {
                if k.as_slice() == s.as_slice() {
                    continue;
                }
            }
            let past_end = match &end_bound {
                Bound::Included(e) => k.as_slice() > e.as_slice(),
                Bound::Excluded(e) => k.as_slice() >= e.as_slice(),
                Bound::Unbounded => false,
            };
            if past_end {
                break;
            }
            out.push((k, v));
        }
        Ok(out)
    }

    /// Flush the oldest frozen memtable (tagged with `wal_number`) to an L0 SST, append the
    /// manifest edit, install the new version, then drop the memtable and delete its WAL file.
    /// Only ever called from the background thread.
    fn do_flush(&self, wal_number: u64, memtable: &Arc<Memtable>) -> Result<()> {
        let mut edit = Vec::new();
        let mut new_table: Option<Arc<Table>> = None;
        let last_seq = if memtable.is_empty() {
            self.visible_seq.load(Ordering::Acquire)
        } else {
            let number = self.alloc_file_number();
            let path = sst_path(&self.dir, number);
            let file = std::fs::File::create(&path)?;
            let mut w = SstWriter::new(BufWriter::new(file));
            for (k, seq, v) in memtable.iter_from(&[]) {
                w.add(&k, seq, &v)?;
            }
            let (bufw, summary) = w.finish()?;
            bufw.into_inner().map_err(|e| e.into_error())?.sync_all()?;
            wal::sync_dir(&self.dir)?;
            let meta = SstMeta {
                number,
                smallest: summary.smallest,
                largest: summary.largest,
                size: summary.file_size,
                max_seq: summary.max_seq,
            };
            let reader = Arc::new(SstReader::open(&path)?);
            new_table = Some(Arc::new(Table {
                meta: meta.clone(),
                reader,
            }));
            edit.push(ManifestRecord::SstAdded { level: 0, meta });
            self.stats
                .bytes_flushed
                .fetch_add(summary.file_size, Ordering::Relaxed);
            summary.max_seq
        };
        edit.push(ManifestRecord::WalFlushed {
            number: wal_number,
            last_seq,
        });
        edit.push(ManifestRecord::NextFileNumber(
            self.next_file_number.load(Ordering::SeqCst),
        ));
        self.manifest.lock().append(&edit)?;

        if let Some(table) = new_table {
            let mut v = self.version.write();
            let mut nv = (**v).clone();
            if nv.levels.is_empty() {
                nv.levels.push(Vec::new());
            }
            nv.levels[0].insert(0, table);
            *v = Arc::new(nv);
        }

        {
            let mut mem = self.mem.write();
            mem.immutables.retain(|(n, _)| *n != wal_number);
        }
        let _ = std::fs::remove_file(wal::wal_path(&self.dir, wal_number));
        self.notify_all_waiters();
        Ok(())
    }

    /// Run one compaction plan and install its result. Only ever called from the background
    /// thread.
    fn do_compact(&self, plan: &CompactionPlan) -> Result<()> {
        let version = self.version.read().clone();
        let result = compaction::run(
            &self.dir,
            &self.options,
            plan,
            &version,
            self.oldest_snapshot(),
            &self.next_file_number,
        )?;

        let mut edit = Vec::new();
        for meta in &result.added {
            edit.push(ManifestRecord::SstAdded {
                level: result.output_level,
                meta: meta.clone(),
            });
        }
        for (level, number) in &result.deleted {
            edit.push(ManifestRecord::SstDeleted {
                level: *level,
                number: *number,
            });
        }
        edit.push(ManifestRecord::NextFileNumber(
            self.next_file_number.load(Ordering::SeqCst),
        ));
        self.manifest.lock().append(&edit)?;

        let bytes_out: u64 = result.added.iter().map(|m| m.size).sum();
        self.stats
            .bytes_compacted
            .fetch_add(bytes_out, Ordering::Relaxed);

        {
            let mut v = self.version.write();
            let mut nv = (**v).clone();
            for (level, number) in &result.deleted {
                if let Some(l) = nv.levels.get_mut(*level as usize) {
                    l.retain(|t| t.meta.number != *number);
                }
            }
            let out = result.output_level as usize;
            if nv.levels.len() <= out {
                nv.levels.resize(out + 1, Vec::new());
            }
            nv.levels[out].extend(result.new_tables.iter().cloned());
            nv.levels[out].sort_by(|a, b| a.meta.smallest.cmp(&b.meta.smallest));
            *v = Arc::new(nv);
        }

        for (_, number) in &result.deleted {
            let _ = std::fs::remove_file(sst_path(&self.dir, *number));
        }
        self.notify_all_waiters();
        Ok(())
    }

    /// Block (synchronously) until the frozen memtable tagged `wal_number` has been flushed
    /// (or, if `fatal` gets set first, return that error). Called from a `spawn_blocking` task.
    fn wait_flushed(&self, wal_number: u64) -> Result<()> {
        loop {
            self.check_fatal()?;
            if !self
                .mem
                .read()
                .immutables
                .iter()
                .any(|(n, _)| *n == wal_number)
            {
                return Ok(());
            }
            let mut g = self.flush_mu.lock();
            self.flush_cv.wait_for(&mut g, Duration::from_millis(50));
        }
    }

    /// Force a full compaction: flag it, wake the background thread, then block until
    /// `pick_forced` has nothing left. Called from a `spawn_blocking` task.
    fn compact_blocking(&self) -> Result<()> {
        self.force_compact.store(true, Ordering::SeqCst);
        self.notify_bg();
        let result = loop {
            if let Err(e) = self.check_fatal() {
                break Err(e);
            }
            let metas = self.version.read().level_metas();
            if compaction::pick_forced(&metas, &self.options).is_none() {
                break Ok(());
            }
            let mut g = self.flush_mu.lock();
            self.flush_cv.wait_for(&mut g, Duration::from_millis(50));
        };
        self.force_compact.store(false, Ordering::SeqCst);
        result
    }
}

fn as_option(v: Value) -> Option<Vec<u8>> {
    match v {
        Value::Put(v) => Some(v),
        Value::Delete => None,
    }
}

/// How many frozen memtables `rotate` lets pile up (waiting on the background flush thread)
/// before it stalls new writes. This is also the recovery-time bound: on crash, the WAL holds at
/// most the active memtable plus this many frozen-but-unflushed ones, so replay on reopen never
/// has more than roughly `Options::memtable_size * (1 + MAX_IMMUTABLE_MEMTABLES)` bytes of WAL to
/// read, plus at most one request's bytes per memtable -- see `Options::memtable_size`'s doc
/// comment.
const MAX_IMMUTABLE_MEMTABLES: usize = 2;

/// Freeze the active memtable and open a fresh WAL, stalling (respecting shutdown/fatal) while
/// `MAX_IMMUTABLE_MEMTABLES` frozen memtables are already waiting on the background thread.
/// Returns the WAL number the just-frozen memtable is tagged with, or `None` if the active
/// memtable was empty. Only ever called from the writer thread.
fn rotate(inner: &Inner, wal: &mut WalFile) -> Result<Option<u64>> {
    {
        let mem = inner.mem.read();
        if mem.active.is_empty() {
            // Nothing new to freeze, but a flush caller still needs to wait for whatever's
            // already frozen. Flushes drain `immutables` oldest-first, so the newest entry's
            // wal number is a valid wait target: once it's gone, every older one is too.
            return Ok(mem.immutables.last().map(|(n, _)| *n));
        }
    }
    loop {
        if inner.mem.read().immutables.len() < MAX_IMMUTABLE_MEMTABLES {
            break;
        }
        inner.check_fatal()?;
        let mut g = inner.stall_mu.lock();
        if inner.mem.read().immutables.len() >= MAX_IMMUTABLE_MEMTABLES {
            inner.stall_cv.wait_for(&mut g, Duration::from_millis(50));
        }
    }
    let old_wal_number = wal.number();
    let new_number = inner.alloc_file_number();
    let new_wal = WalFile::create(&inner.dir, new_number)?;
    let old_wal = std::mem::replace(wal, new_wal);
    drop(old_wal);

    let mut mem = inner.mem.write();
    let old_active = std::mem::replace(&mut mem.active, Arc::new(Memtable::new()));
    mem.immutables.push((old_wal_number, old_active));
    drop(mem);
    Ok(Some(old_wal_number))
}

fn fail_write(ack: tokio::sync::oneshot::Sender<Result<u64>>, msg: &str) {
    let _ = ack.send(Err(Error::Io(std::io::Error::other(msg.to_string()))));
}

fn fail_rotate(ack: tokio::sync::oneshot::Sender<Result<Option<u64>>>, msg: &str) {
    let _ = ack.send(Err(Error::Io(std::io::Error::other(msg.to_string()))));
}

/// Total key+value bytes a request would add to the memtable (`0` for `Req::Rotate`, which adds
/// nothing). Used by `writer_thread` to bound how many bytes one group commit drains.
fn write_req_bytes(req: &Req) -> u64 {
    match req {
        Req::Write { batch, .. } => batch
            .iter()
            .map(|(k, v)| {
                k.len() as u64
                    + match v {
                        Value::Put(v) => v.len() as u64,
                        Value::Delete => 0,
                    }
            })
            .sum(),
        Req::Rotate { .. } => 0,
    }
}

/// Cap on requests drained into one group-commit batch. Higher pays off under heavy concurrency
/// (more puts amortized over one `fdatasync`); it costs nothing at low concurrency since
/// `rx.try_recv()` simply returns empty once the queue is drained, so this is sized for the
/// high end (hundreds to low thousands of concurrent callers) rather than split into a separate
/// low-concurrency tier.
const MAX_BATCH_REQUESTS: usize = 1024;

/// The single writer thread: owns the current WAL file, batches requests via group commit, and
/// is the only place that ever appends to `mem.active` or freezes it into `mem.immutables`.
fn writer_thread(inner: Arc<Inner>, rx: mpsc::Receiver<Req>, mut wal: WalFile) {
    loop {
        let first = match rx.recv() {
            Ok(r) => r,
            Err(_) => break, // every Sender dropped -- shut down.
        };
        // Bound how many bytes this group commit can add to the active memtable: once the
        // batch already holds at least `budget` bytes of ops, stop draining, even if
        // `MAX_BATCH_REQUESTS` hasn't been reached yet. Without this, draining purely by request
        // count let one group overshoot `memtable_size` by however much a burst of large
        // requests added up to (up to `MAX_BATCH_REQUESTS` of them) before `rotate` ever got a
        // chance to look at the size -- see `Options::memtable_size`'s doc comment. The first
        // request is always taken regardless of `budget` so a single request larger than the
        // whole budget still makes progress.
        let budget = (inner.options.memtable_size as u64)
            .saturating_sub(inner.mem.read().active.size() as u64);
        let mut batch_bytes = write_req_bytes(&first);
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH_REQUESTS && batch_bytes < budget {
            match rx.try_recv() {
                Ok(r) => {
                    batch_bytes += write_req_bytes(&r);
                    batch.push(r);
                }
                Err(_) => break,
            }
        }
        if !inner.options.commit_window.is_zero() {
            let deadline = std::time::Instant::now() + inner.options.commit_window;
            while batch.len() < MAX_BATCH_REQUESTS && batch_bytes < budget {
                let now = std::time::Instant::now();
                if now >= deadline {
                    break;
                }
                match rx.recv_timeout(deadline - now) {
                    Ok(r) => {
                        batch_bytes += write_req_bytes(&r);
                        batch.push(r);
                    }
                    Err(_) => break,
                }
            }
        }

        if let Some(msg) = inner.fatal.lock().clone() {
            for req in batch {
                match req {
                    Req::Write { ack, .. } => fail_write(ack, &msg),
                    Req::Rotate { ack } => fail_rotate(ack, &msg),
                }
            }
            continue;
        }

        let mut writes = Vec::new();
        let mut rotates = Vec::new();
        for req in batch {
            match req {
                Req::Write { batch, ack } => writes.push((batch, ack)),
                Req::Rotate { ack } => rotates.push(ack),
            }
        }

        let mut sync_failed = false;
        if !writes.is_empty() {
            let mut flat: Vec<(u64, Vec<u8>, Value)> = Vec::new();
            let mut acks: Vec<(tokio::sync::oneshot::Sender<Result<u64>>, u64)> = Vec::new();
            let mut seq = inner.visible_seq.load(Ordering::Relaxed);
            let mut user_bytes = 0u64;
            for (ops, ack) in writes {
                for (k, v) in ops {
                    seq += 1;
                    wal.append(seq, &k, &v);
                    user_bytes += k.len() as u64
                        + match &v {
                            Value::Put(v) => v.len() as u64,
                            Value::Delete => 0,
                        };
                    flat.push((seq, k, v));
                }
                acks.push((ack, seq));
            }

            if flat.is_empty() {
                for (ack, s) in acks {
                    let _ = ack.send(Ok(s));
                }
            } else {
                let wal_bytes = wal.pending_len();
                match wal.sync() {
                    Ok(()) => {
                        let active = inner.mem.read().active.clone();
                        for (s, k, v) in flat {
                            active.insert(k, s, v);
                        }
                        inner.visible_seq.store(seq, Ordering::Release);
                        inner
                            .stats
                            .user_bytes
                            .fetch_add(user_bytes, Ordering::Relaxed);
                        inner
                            .stats
                            .wal_bytes
                            .fetch_add(wal_bytes, Ordering::Relaxed);
                        for (ack, s) in acks {
                            let _ = ack.send(Ok(s));
                        }
                    }
                    Err(e) => {
                        sync_failed = true;
                        let msg = format!("engine poisoned after fsync failure: {e}");
                        *inner.fatal.lock() = Some(msg.clone());
                        inner.notify_all_waiters();
                        inner.notify_bg();
                        for (ack, _) in acks {
                            fail_write(ack, &msg);
                        }
                    }
                }
            }
        }

        if sync_failed {
            let msg = inner.fatal.lock().clone().unwrap_or_default();
            for ack in rotates {
                fail_rotate(ack, &msg);
            }
            continue;
        }

        let active_big = inner.mem.read().active.size() >= inner.options.memtable_size;
        if active_big || !rotates.is_empty() {
            match rotate(&inner, &mut wal) {
                Ok(rotated) => {
                    if rotated.is_some() {
                        inner.notify_bg();
                    }
                    for ack in rotates {
                        let _ = ack.send(Ok(rotated));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    *inner.fatal.lock() = Some(msg.clone());
                    inner.notify_all_waiters();
                    inner.notify_bg();
                    for ack in rotates {
                        fail_rotate(ack, &msg);
                    }
                }
            }
        }
    }
    let _ = wal.sync();
}

/// The single background thread: flushes frozen memtables (oldest first) then runs compaction,
/// repeating until there's nothing left, then sleeps until woken. Exits once shutdown has been
/// requested and every frozen memtable has been drained.
fn bg_thread(inner: Arc<Inner>) {
    loop {
        loop {
            if inner.fatal.lock().is_some() {
                break;
            }
            let oldest = inner.mem.read().immutables.first().cloned();
            if let Some((wal_n, mt)) = oldest {
                if let Err(e) = inner.do_flush(wal_n, &mt) {
                    tracing::error!("driftdb: flush of wal {wal_n} failed: {e}");
                    *inner.fatal.lock() = Some(format!("flush failed: {e}"));
                    inner.notify_all_waiters();
                }
                continue;
            }
            let version = inner.version.read().clone();
            let metas = version.level_metas();
            let plan = if inner.force_compact.load(Ordering::SeqCst) {
                compaction::pick_forced(&metas, &inner.options)
            } else {
                compaction::pick(&metas, &inner.options)
            };
            if let Some(plan) = plan {
                if let Err(e) = inner.do_compact(&plan) {
                    tracing::error!("driftdb: compaction failed: {e}");
                    *inner.fatal.lock() = Some(format!("compaction failed: {e}"));
                    inner.notify_all_waiters();
                }
                continue;
            }
            break;
        }
        // Keep running past `shutdown` while a forced `compact()` is still in flight -- it
        // polls `pick_forced` on its own and would otherwise spin forever once this thread,
        // the only thing that can actually run a plan, stops picking up work.
        //
        // Once `fatal` is set, though, nothing here will ever make progress again (every flush
        // and every compaction attempt bails out at the top of the inner loop), so `shutdown`
        // must be able to tear this thread down immediately regardless of leftover `immutables`
        // or an in-flight forced compaction -- otherwise a flush/fsync failure leaves a frozen
        // memtable stuck forever and `Guard::shutdown_and_join` (close()/Drop) never returns.
        let fatal = inner.fatal.lock().is_some();
        if inner.shutdown.load(Ordering::SeqCst)
            && (fatal
                || (inner.mem.read().immutables.is_empty()
                    && !inner.force_compact.load(Ordering::SeqCst)))
        {
            break;
        }
        let mut g = inner.work_mu.lock();
        inner.work_cv.wait_for(&mut g, Duration::from_millis(200));
    }
}

/// Take an exclusive, non-blocking `flock` on `dir/LOCK` (creating it if needed) so at most one
/// `Db` -- in this process or another -- has `dir` open at a time. Two writers sharing a
/// WAL/manifest would corrupt each other's state, and mmap-based WAL replay racing a live writer
/// could SIGBUS, so this is checked before recovery touches anything else in `dir`.
///
/// The returned `File` must be kept open for as long as the lock should be held; the kernel
/// releases the lock when the fd is closed (including on process exit, e.g. `kill -9`, which is
/// exactly what lets `tests/crash_kill.rs`'s killed child not wedge the parent's reopen).
fn acquire_dir_lock(dir: &Path) -> Result<std::fs::File> {
    let path = dir.join("LOCK");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    // Safety: `flock` on an fd this call owns exclusively until it returns; LOCK_NB makes the
    // call return immediately (EWOULDBLOCK) instead of blocking if another process holds it.
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            return Err(Error::Locked(dir.to_path_buf()));
        }
        return Err(err.into());
    }
    Ok(file)
}

/// Owns the writer-thread `Sender`, both thread `JoinHandle`s, and the directory lock. Reachable
/// only through `Db::guard` (an `Arc<Guard>`) -- never cloned into the threads themselves -- so
/// when the last `Db` handle drops, this drops too, which is what actually tells the threads to
/// stop.
struct Guard {
    inner: Arc<Inner>,
    sender: Mutex<Option<mpsc::Sender<Req>>>,
    handles: Mutex<Option<(std::thread::JoinHandle<()>, std::thread::JoinHandle<()>)>>,
    /// The `dir/LOCK` file from [`acquire_dir_lock`]. Cleared (closing the fd, releasing the
    /// flock) only *after* both threads have joined in `shutdown_and_join`, so a concurrent
    /// `Db::open` on the same directory can never race the writer/background threads while they
    /// still have the WAL or manifest open.
    lock: Mutex<Option<std::fs::File>>,
}

impl Guard {
    fn shutdown_and_join(&self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        *self.sender.lock() = None;
        self.inner.notify_bg();
        self.inner.notify_all_waiters();
        if let Some((writer, bg)) = self.handles.lock().take() {
            let _ = writer.join();
            let _ = bg.join();
        }
        *self.lock.lock() = None;
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.shutdown_and_join();
    }
}

/// The public handle. Cheap to clone (`Arc` under the hood). Every write blocks on the writer
/// thread's group commit; every read is lock-free past a couple of short `RwLock` reads.
#[derive(Clone)]
pub struct Db {
    guard: Arc<Guard>,
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("dir", &self.guard.inner.dir)
            .finish()
    }
}

impl Db {
    /// Open (or create) a driftdb instance rooted at `path` with default [`Options`].
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Options::default()).await
    }

    /// Open (or create) a driftdb instance rooted at `path` with custom [`Options`]. Runs
    /// recovery (manifest replay, orphan cleanup, WAL replay) synchronously on a blocking
    /// thread before returning.
    pub async fn open_with(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || open_sync(&dir, options))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
    }

    fn send_req(&self, req: Req) -> Result<()> {
        let sender = self.guard.sender.lock();
        match sender.as_ref() {
            Some(s) => s.send(req).map_err(|_| closed_err()),
            None => Err(closed_err()),
        }
    }

    /// Durable, atomic write of one or more ops. Returns the seqno of the batch's last op.
    ///
    /// Every key/value is checked against [`wal::MAX_KEY_LEN`]/[`wal::MAX_VALUE_LEN`] before the
    /// batch is sent to the writer thread; an oversized op fails the whole batch with
    /// [`Error::InvalidArgument`] and nothing is written. Empty keys and empty values are
    /// allowed.
    pub async fn write_batch(&self, batch: WriteBatch) -> Result<u64> {
        for (key, val) in &batch.ops {
            if key.len() > wal::MAX_KEY_LEN {
                return Err(Error::InvalidArgument(format!(
                    "key length {} exceeds MAX_KEY_LEN ({})",
                    key.len(),
                    wal::MAX_KEY_LEN
                )));
            }
            if let Value::Put(v) = val {
                if v.len() > wal::MAX_VALUE_LEN {
                    return Err(Error::InvalidArgument(format!(
                        "value length {} exceeds MAX_VALUE_LEN ({})",
                        v.len(),
                        wal::MAX_VALUE_LEN
                    )));
                }
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_req(Req::Write {
            batch: batch.ops,
            ack: tx,
        })?;
        rx.await.map_err(|_| closed_err())?
    }

    /// Durable put. Returns once the record is in the WAL + memtable.
    pub async fn put(&self, key: &[u8], val: &[u8]) -> Result<()> {
        self.write_batch(WriteBatch::new().put(key, val)).await?;
        Ok(())
    }

    /// Tombstone write. Same durability contract as `put`.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(WriteBatch::new().delete(key)).await?;
        Ok(())
    }

    /// Latest-version read. Registers a short-lived snapshot for the duration of the read so a
    /// concurrent compaction can't drop the version this call is in the middle of reading (see
    /// `Inner::register_snapshot`).
    ///
    /// The registration itself is kept (only the redundant condvar notify on unregister was
    /// removed -- see `Inner::unregister_snapshot`): once `get_at` has cloned the active/frozen
    /// memtable `Arc`s and the `Version` `Arc`, a *later* compaction genuinely can't take data
    /// away from this call (SSTs are immutable and `do_compact`/`do_flush` only ever install a
    /// new `Version`, never mutate an existing `Table` in place). But between this call
    /// registering `seq` and `get_at` actually taking those clones, an *already in-flight*
    /// compaction could otherwise install a version that already dropped a pre-`seq` entry this
    /// read needs (it computes `oldest_snapshot()` -- the floor below which it's safe to GC old
    /// versions/tombstones -- from the same `snapshots` map). Registering first closes that
    /// window: `oldest_snapshot()` can never be computed as newer than a `seq` that's already
    /// registered. Dropping registration entirely would reopen it, so it stays.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = &self.guard.inner;
        let seq = inner.register_snapshot();
        let result = inner.get_at(key, seq);
        inner.unregister_snapshot(seq);
        result
    }

    /// Full range scan at the current seqno (see [`Snapshot::scan`] for a fixed-seqno version).
    /// Same short-lived-snapshot protection as [`Db::get`].
    pub async fn scan<R: RangeBounds<Vec<u8>>>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let inner = &self.guard.inner;
        let seq = inner.register_snapshot();
        let result = inner.scan_at(range, seq);
        inner.unregister_snapshot(seq);
        result
    }

    /// Open a read snapshot at the current seqno. Compaction won't drop any version a live
    /// snapshot could still observe.
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.guard.inner.clone();
        let seq = inner.register_snapshot();
        Snapshot { inner, seq }
    }

    /// Force-rotate the active memtable (even if under the size threshold) and wait until it's
    /// been flushed to L0.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.send_req(Req::Rotate { ack: tx })?;
        let target: Option<u64> = rx.await.map_err(|_| closed_err())??;
        if let Some(wal_n) = target {
            let inner = self.guard.inner.clone();
            tokio::task::spawn_blocking(move || inner.wait_flushed(wal_n))
                .await
                .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))??;
        }
        Ok(())
    }

    /// Flush, then force compaction until every level is under budget (or, in the case of L0
    /// and any level over its byte budget, fully drained one file at a time down to the bottom).
    pub async fn compact(&self) -> Result<()> {
        self.flush().await?;
        let inner = self.guard.inner.clone();
        tokio::task::spawn_blocking(move || inner.compact_blocking())
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
    }

    /// Snapshot of current file/byte counts and write-amplification inputs.
    pub fn stats(&self) -> Stats {
        let inner = &self.guard.inner;
        let version = inner.version.read().clone();
        let mut level_files = Vec::with_capacity(version.levels.len());
        let mut level_bytes = Vec::with_capacity(version.levels.len());
        for level in &version.levels {
            level_files.push(level.len());
            level_bytes.push(level.iter().map(|t| t.meta.size).sum());
        }
        let memtable_bytes = {
            let mem = inner.mem.read();
            mem.active.size() as u64
                + mem
                    .immutables
                    .iter()
                    .map(|(_, m)| m.size() as u64)
                    .sum::<u64>()
        };
        Stats {
            level_files,
            level_bytes,
            memtable_bytes,
            user_bytes_written: inner.stats.user_bytes.load(Ordering::Relaxed),
            disk_bytes_written: inner.stats.bytes_flushed.load(Ordering::Relaxed)
                + inner.stats.bytes_compacted.load(Ordering::Relaxed),
            wal_bytes_written: inner.stats.wal_bytes.load(Ordering::Relaxed),
        }
    }

    /// Drain the writer thread and join both background threads. Idempotent; also runs (with a
    /// blocking join) if the last `Db` handle is simply dropped, so WAL-acked data always
    /// survives even without calling this explicitly.
    pub async fn close(&self) -> Result<()> {
        let guard = self.guard.clone();
        tokio::task::spawn_blocking(move || guard.shutdown_and_join())
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))
    }
}

/// Recovery + startup, run synchronously on a blocking thread. See the module docs and
/// `docs/plans/2026-09-25-driftdb-phase-2.md`'s "Recovery" section for the sequence.
fn open_sync(dir: &Path, options: Options) -> Result<Db> {
    std::fs::create_dir_all(dir)?;
    let lock_file = acquire_dir_lock(dir)?;
    let (mut manifest, mstate) = Manifest::open(dir)?;

    let live_ssts: std::collections::HashSet<u64> =
        mstate.levels.iter().flatten().map(|m| m.number).collect();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if let Some(num_str) = name.strip_suffix(".sst") {
                if let Ok(num) = num_str.parse::<u64>() {
                    if !live_ssts.contains(&num) {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }
        }
    }

    let wal_files = wal::list_wal_files(dir)?;
    for (n, p) in &wal_files {
        if *n <= mstate.last_flushed_wal {
            let _ = std::fs::remove_file(p);
        }
    }
    let remaining: Vec<(u64, PathBuf)> = wal_files
        .into_iter()
        .filter(|(n, _)| *n > mstate.last_flushed_wal)
        .collect();

    let replay_mem = Memtable::new();
    let mut max_replayed_seq = 0u64;
    for (_n, p) in &remaining {
        let seq = wal::replay(p, |seq, key, val| {
            replay_mem.insert(key, seq, val);
        })?;
        max_replayed_seq = max_replayed_seq.max(seq);
    }

    let mut next_file_number = mstate.next_file_number.max(1);
    let mut levels: Vec<Vec<SstMeta>> = mstate.levels.clone();
    let mut last_flushed_wal = mstate.last_flushed_wal;

    if !replay_mem.is_empty() {
        let number = next_file_number;
        next_file_number += 1;
        let path = sst_path(dir, number);
        let file = std::fs::File::create(&path)?;
        let mut w = SstWriter::new(BufWriter::new(file));
        for (k, seq, v) in replay_mem.iter_from(&[]) {
            w.add(&k, seq, &v)?;
        }
        let (bufw, summary) = w.finish()?;
        bufw.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        wal::sync_dir(dir)?;
        let meta = SstMeta {
            number,
            smallest: summary.smallest,
            largest: summary.largest,
            size: summary.file_size,
            max_seq: summary.max_seq,
        };
        if levels.is_empty() {
            levels.push(Vec::new());
        }
        levels[0].push(meta.clone());
        let max_wal_number = remaining.iter().map(|(n, _)| *n).max().unwrap();
        last_flushed_wal = last_flushed_wal.max(max_wal_number);
        manifest.append(&[
            ManifestRecord::SstAdded { level: 0, meta },
            ManifestRecord::WalFlushed {
                number: max_wal_number,
                last_seq: summary.max_seq.max(max_replayed_seq),
            },
            ManifestRecord::NextFileNumber(next_file_number),
        ])?;
        for (_n, p) in &remaining {
            let _ = std::fs::remove_file(p);
        }
    } else if !remaining.is_empty() {
        // Every `remaining` WAL replayed to zero records -- either genuinely empty (no writes
        // landed before the last close) or entirely torn (already truncated to nothing by
        // `wal::replay`). There's nothing to flush, but leaving these files on disk means one
        // extra empty `wal-*.log` piles up per open/close cycle forever, so clean them up here
        // too instead of only in the "had data" branch above.
        for (_n, p) in &remaining {
            let _ = std::fs::remove_file(p);
        }
    }

    let last_seq = mstate.last_seq.max(max_replayed_seq);

    let mut version_levels: Vec<Vec<Arc<Table>>> = Vec::with_capacity(levels.len());
    for (li, metas) in levels.iter().enumerate() {
        let mut tabs = Vec::with_capacity(metas.len());
        for m in metas {
            let reader = Arc::new(SstReader::open(&sst_path(dir, m.number))?);
            tabs.push(Arc::new(Table {
                meta: m.clone(),
                reader,
            }));
        }
        if li == 0 {
            tabs.sort_by_key(|a| std::cmp::Reverse(a.meta.number));
        } else {
            tabs.sort_by(|a, b| a.meta.smallest.cmp(&b.meta.smallest));
        }
        version_levels.push(tabs);
    }

    let new_wal_number = next_file_number
        .max(remaining.iter().map(|(n, _)| n + 1).max().unwrap_or(0))
        .max(1);
    next_file_number = next_file_number.max(new_wal_number + 1);
    let wal_file = WalFile::create(dir, new_wal_number)?;
    manifest.append(&[ManifestRecord::NextFileNumber(next_file_number)])?;

    let inner = Arc::new(Inner {
        dir: dir.to_path_buf(),
        options,
        version: RwLock::new(Arc::new(Version {
            levels: version_levels,
        })),
        mem: RwLock::new(MemState {
            active: Arc::new(Memtable::new()),
            immutables: Vec::new(),
        }),
        manifest: Mutex::new(manifest),
        visible_seq: AtomicU64::new(last_seq),
        next_file_number: AtomicU64::new(next_file_number),
        snapshots: Mutex::new(BTreeMap::new()),
        fatal: Mutex::new(None),
        shutdown: AtomicBool::new(false),
        force_compact: AtomicBool::new(false),
        work_mu: Mutex::new(()),
        work_cv: Condvar::new(),
        stall_mu: Mutex::new(()),
        stall_cv: Condvar::new(),
        flush_mu: Mutex::new(()),
        flush_cv: Condvar::new(),
        stats: StatsInner::default(),
    });
    let _ = last_flushed_wal; // only used to compute `remaining` above; kept for clarity.

    let (tx, rx) = mpsc::channel::<Req>();
    let writer_inner = inner.clone();
    let writer_handle = std::thread::Builder::new()
        .name("driftdb-writer".into())
        .spawn(move || writer_thread(writer_inner, rx, wal_file))
        .expect("spawn driftdb writer thread");
    let bg_inner = inner.clone();
    let bg_handle = std::thread::Builder::new()
        .name("driftdb-bg".into())
        .spawn(move || bg_thread(bg_inner))
        .expect("spawn driftdb background thread");

    let guard = Arc::new(Guard {
        inner,
        sender: Mutex::new(Some(tx)),
        handles: Mutex::new(Some((writer_handle, bg_handle))),
        lock: Mutex::new(Some(lock_file)),
    });
    Ok(Db { guard })
}

# driftdb — Phase 2 plan: a working, crash-safe LSM

Phase 1 shipped a memtable-only scaffold. Phase 2 turns it into a real engine:
WAL replay, flush to L0, SST reads, manifest, leveled compaction, snapshots,
range scans. This document is the **interface contract** every module is
built against. Modules are developed in parallel; do not change a signature
here without updating this file.

## Shared types

```rust
// memtable.rs
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value { Put(Vec<u8>), Delete }

/// One versioned record: (user_key, seqno, value). Streams of entries are always
/// ordered by user_key ASC, then seqno DESC (newest version of a key first).
pub type Entry = (Vec<u8>, u64, Value);
```

Error type (`error.rs`) gains `SstCorrupt(String)`. All fallible module APIs
return `crate::Result<T>` unless noted.

## Durability model (single writer thread)

* One dedicated **writer thread** (std thread, not a tokio task — it does
  blocking `write` + `fdatasync`) owns the current WAL file and the active
  memtable. Callers send `WriteReq { ops, ack: tokio::oneshot }` over a
  `std::sync::mpsc` channel.
* Group commit = natural batching: block on the first request, drain every
  request already queued (up to 128), optionally wait up to
  `Options::commit_window` (default 0) for more, then append all records,
  one `fdatasync`, insert into the memtable, publish `visible_seq`, ack.
* Reads use `visible_seq` as their snapshot, so they never observe a write
  before it is durable.
* If `fdatasync` fails the engine is **poisoned**: every pending and future
  write fails with an error (fsyncgate — a failed fsync can't be retried).
* **One WAL file per memtable.** When the active memtable exceeds
  `Options::memtable_size`, the writer thread (between batches) freezes it,
  pushes it onto the immutable list, opens `wal-{n+1}.log`, and signals the
  background thread. So memtable k contains exactly the records of WAL k.
* The background thread flushes the oldest immutable memtable to an L0 SST,
  appends one manifest edit `[SstAdded{..}, WalFlushed{number: k, last_seq}]`,
  drops the immutable memtable, deletes `wal-k.log`.
* Backpressure: the writer thread stalls while 2+ immutable memtables are
  waiting for flush.

## Recovery (`Db::open`)

1. Replay MANIFEST → levels, `last_flushed_wal`, `last_seq`, `next_file_number`.
2. Delete `*.sst` files not referenced by the manifest (orphans from a crashed
   flush/compaction) and `wal-*.log` with number `<= last_flushed_wal`.
3. Replay remaining WAL files in order into one memtable (torn tail of any file
   is truncated). `last_seq = max(manifest.last_seq, max wal seq)`.
4. If that memtable is non-empty, flush it synchronously to L0, manifest edit,
   delete the replayed WALs.
5. Open a fresh WAL with number `max(existing)+1`, start threads.

## Module contracts

### `wal.rs` — record codec, file, replay

Record: `[u32 crc][u64 seq][u8 kind][u32 klen][u32 vlen][key][val]` (21-byte
header, big-endian, crc32 over everything after the crc). `kind`: 1 = Put,
0 = Delete (vlen = 0).

```rust
pub fn wal_path(dir: &Path, number: u64) -> PathBuf;          // dir/wal-000042.log
pub fn list_wal_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>>; // sorted by number
pub fn sync_dir(dir: &Path) -> std::io::Result<()>;           // fsync a directory

pub struct WalFile { .. }
impl WalFile {
    pub fn create(dir: &Path, number: u64) -> Result<Self>;   // create_new + sync_dir
    pub fn number(&self) -> u64;
    pub fn append(&mut self, seq: u64, key: &[u8], val: &Value); // buffers in memory
    pub fn sync(&mut self) -> std::io::Result<()>;            // write_all buffer + fdatasync, checks errors
    pub fn size(&self) -> u64;
}
/// Stream every valid record to `f`; truncate (set_len + fsync) at the first
/// torn/corrupt record. Returns the max seq seen (0 if none).
pub fn replay(path: &Path, f: impl FnMut(u64, Vec<u8>, Value)) -> Result<u64>;
```

### `memtable.rs`

```rust
impl Memtable {
    pub fn insert(&self, key: Vec<u8>, seqno: u64, val: Value);
    /// Newest version <= snapshot_seq. Some(Value::Delete) = tombstone found
    /// (caller must stop searching older sources); None = key absent here.
    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Option<Value>;
    /// All entries from `start` (inclusive, all versions), in Entry order.
    pub fn iter_from(&self, start: &[u8]) -> impl Iterator<Item = Entry> + '_;
    pub fn size(&self) -> usize; pub fn len(&self) -> usize; pub fn is_empty(&self) -> bool;
}
```

### `iter.rs` — merging + filtering (new module)

```rust
pub type BoxIter<'a> = Box<dyn Iterator<Item = Result<Entry>> + Send + 'a>;
/// k-way merge of sorted sources into one sorted stream. Sources earlier in the
/// vec win ties on identical (key, seq) (the duplicate is dropped). An Err from
/// any source is yielded once and ends the stream.
pub fn merge<'a>(sources: Vec<BoxIter<'a>>) -> BoxIter<'a>;
/// User-visible view at `snapshot_seq`: per user key, the newest version with
/// seq <= snapshot_seq; tombstones suppressed. Yields (key, value).
pub fn visible<'a>(inner: BoxIter<'a>, snapshot_seq: u64)
    -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a;
/// Compaction GC. Per user key keep every version with seq > oldest_snapshot,
/// plus the newest version with seq <= oldest_snapshot; drop older ones. That
/// kept version is also dropped if it is a tombstone and `drop_tombstones`
/// (output is the bottom-most level for this key range).
pub fn compaction_filter<'a>(inner: BoxIter<'a>, oldest_snapshot: u64, drop_tombstones: bool)
    -> BoxIter<'a>;
```

### `sstable.rs`

Data block on disk: `[u32 BE compressed_len][u32 BE crc32(compressed)][zstd bytes]`.
Index/bloom/footer as in Phase 1.

```rust
pub struct SstSummary { pub smallest: Vec<u8>, pub largest: Vec<u8>,
                        pub entries: u64, pub file_size: u64, pub max_seq: u64 }
impl<W: Write> SstWriter<W> {
    pub fn new(w: W) -> Self;
    pub fn add(&mut self, key: &[u8], seqno: u64, val: &Value) -> std::io::Result<()>;
    pub fn estimated_size(&self) -> u64;   // bytes written + pending block
    pub fn is_empty(&self) -> bool;
    pub fn finish(self) -> std::io::Result<(W, SstSummary)>; // flushes W
}
pub fn sst_path(dir: &Path, number: u64) -> PathBuf;          // dir/000042.sst

pub struct SstReader { mmap, index, bloom, .. }   // Send + Sync
impl SstReader {
    pub fn open(path: &Path) -> Result<Self>;       // validates magic, parses index + bloom
    /// Newest version <= snapshot_seq; Some(Delete) for tombstone. Versions of
    /// one key may span blocks. Bloom-negative → Ok(None) without touching blocks.
    pub fn get(&self, user_key: &[u8], snapshot_seq: u64) -> Result<Option<Value>>;
    /// Owning iterator (holds the Arc) over entries with user_key >= start.
    pub fn iter_from(self: &Arc<Self>, start: &[u8]) -> SstIter; // Item = Result<Entry>
}
```

### `manifest.rs`

Log of **edits**; each edit is one frame `[u32 BE len][u32 BE crc32][bincode(Vec<ManifestRecord>)]`
so a compaction's adds + deletes apply atomically. A torn/corrupt final frame is
truncated on open.

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SstMeta { pub number: u64, pub smallest: Vec<u8>, pub largest: Vec<u8>,
                     pub size: u64, pub max_seq: u64 }
pub enum ManifestRecord {
    SstAdded { level: u8, meta: SstMeta },
    SstDeleted { level: u8, number: u64 },
    WalFlushed { number: u64, last_seq: u64 },
    /// Persists `next_file_number` across a manifest rewrite. Appended at the END of the enum
    /// (bincode is position-indexed) so existing frames keep decoding. A file number can be
    /// allocated and then deleted without ever appearing in a live `SstAdded` (e.g. a
    /// compaction output later superseded), so `next_file_number` can't always be recovered
    /// from the live SST set alone — the snapshot rewrite emits one of these to carry it
    /// forward.
    NextFileNumber(u64),
}
#[derive(Clone, Debug, Default)]
pub struct ManifestState {
    pub levels: Vec<Vec<SstMeta>>,   // L0 sorted by number ASC; L1+ by smallest ASC
    pub last_flushed_wal: u64,
    pub last_seq: u64,
    pub next_file_number: u64,       // > every sst number seen, >= 1
}
impl ManifestState {
    pub fn apply(&mut self, rec: &ManifestRecord);
    /// One edit that, replayed from a fresh state, reproduces this state exactly: an
    /// `SstAdded` for every live file, then `WalFlushed{last_flushed_wal, last_seq}` and
    /// `NextFileNumber(next_file_number)`. Used by `Manifest::open` to compact the log.
    pub fn snapshot_edit(&self) -> Vec<ManifestRecord>;
}
impl Manifest {
    /// Open/create dir/MANIFEST, replay, then rewrite it as a single snapshot
    /// edit (MANIFEST.tmp → fsync → rename → sync_dir) so it never grows unbounded.
    pub fn open(dir: &Path) -> Result<(Manifest, ManifestState)>;
    pub fn append(&mut self, edit: &[ManifestRecord]) -> Result<()>; // write + fdatasync
}
```

### `compaction.rs` + `db.rs` (integration)

* `Options { memtable_size: 4 MiB, l0_compaction_trigger: 4, l1_max_bytes: 10 MiB,
  level_multiplier: 10, target_file_size: 2 MiB, max_levels: 7, commit_window: 0 }`.
* Current file set is an immutable `Arc<Version>` swapped under a lock
  (copy-on-write); readers clone the Arc and read without locks.
* Pick: L0 file count >= trigger → all L0 + overlapping L1. Else first level n>=1
  with bytes > l1_max_bytes * mult^(n-1) → its oldest-picked file (round-robin
  by key) + overlapping L_{n+1}. Output split at `target_file_size`, never
  splitting versions of one user key across files.
* `Db` API: `open`, `open_with`, `put`, `delete`, `write_batch`, `get`,
  `snapshot` (RAII; registers seq so compaction keeps its versions),
  `Snapshot::get`, `scan(range)`, `flush`, `compact` (force full compaction),
  `stats`, `close`.

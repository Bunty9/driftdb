# driftdb

> Embeddable LSM-tree key-value engine in Rust. WAL group-commit, MVCC
> snapshot reads, leveled compaction, mmap-backed SSTable reads.

[![ci](https://github.com/Bunty9/driftdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Bunty9/driftdb/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/driftdb-lsm.svg)](https://crates.io/crates/driftdb-lsm)
[![docs.rs](https://img.shields.io/docsrs/driftdb-lsm)](https://docs.rs/driftdb-lsm)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## The problem

Most app-tier code reaches for Postgres + JSON files for state. **driftdb**
is a real embedded engine instead — WAL durability, MVCC snapshot reads,
leveled compaction — sized to embed into job-queue metadata, agent state,
or edge-runtime checkpoints, and shipped as a public crate with
reproducible YCSB-style benchmarks.

## Architecture

```
        Write path:                           Read path:
        ----------                            ---------
        put(k,v) --> WAL.append --> fsync     get(k) --> memtable.get(k)
                                                            | miss
                                                            v
                       (durable now)                     l0.get(k)  (newest SST first)
                                                            | miss
        --> memtable.insert(k,v)                            v
                |                                       l1.get(k) --> bloom filter --> block read
                | size > threshold                      ...
                v
        freeze memtable (new WAL file)
                |
                v
        new memtable starts taking writes
                |
                v
        flush thread: write SST in L0
                |
                v
        compactor: merge L0 --> L1 --> L2 (size-based)

  All keys in SST carry (key, seqno) -- MVCC. Snapshot reads ignore seqno > snapshot_seq.
```

## On-disk formats

### WAL record layout

```
  offset  size  field
  ------  ----  ----------------------------------------
    0      4    crc32 (big-endian) over everything after
    4      8    seqno          (u64 BE)
   12      1    kind           (1 = Put, 0 = Delete)
   13      4    key_len        (u32 BE)
   17      4    val_len        (u32 BE, always 0 for Delete)
   21      K    key bytes
 21+K      V    value bytes
```

Files are named `wal-NNNNNN.log` — one WAL file per memtable generation, not rotated
by file size. When the active memtable crosses the size threshold, the writer thread
freezes it, opens a new WAL, and signals the background thread to flush the frozen
memtable. Recovery streams records until EOF, a length prefix that overruns the file,
an invalid kind byte, or a CRC mismatch — anything past that point is treated as
torn-tail garbage and truncated on replay.

### SSTable file layout

```
  +-----------------+
  | data block 0    |   4 KiB target (uncompressed), zstd-compressed
  | data block 1    |   [u32 BE compressed_len][u32 BE crc32(compressed)][zstd bytes]
  | ...             |
  | data block N    |
  +-----------------+
  | index block     |   bincode: Vec<(last_key, block_offset)>
  +-----------------+
  | bloom block     |   bincode: GrowableBloom
  +-----------------+
  | footer (32 B)   |   [u64 BE index_off][u64 BE bloom_off][u32 BE crc32(index..bloom)]
  |                 |   [u32 BE format_version][u64 BE magic=0xDEADBEEF]
  +-----------------+
```

`format_version` is the SST format version (currently `1`). Opening an SST rejects any
other value with `Error::UnsupportedFormat` rather than guessing at a layout it doesn't know.

Each data-block entry (inside the decompressed block):

```
  [u32 BE klen][u32 BE vlen][u64 BE seq][u8 kind][key][val]
```

`kind` is `1` for `Put` (value bytes follow) and `0` for `Delete`
(value bytes are absent). Inside a block, entries sort by `(user_key
ASC, seqno DESC)` so the newest version of any key is the first hit
on a linear scan.

The footer's crc32 covers the index block and bloom block together (from `index_off`
to the start of the footer). It is validated before either block is bincode-deserialized,
preventing a bit flip in the index from silently skipping blocks or a bit flip in the
bloom filter from being handed to the deserializer unchecked. The magic acts as a
torn-write detector — a truncated flush won't carry the complete trailer.

### Manifest log layout

The manifest is an append-only log of edits, each frame containing a batch of records
that must apply atomically (e.g., a compaction's adds and deletes):

```
  [u32 BE len][u32 BE crc32(payload)][bincode(Vec<ManifestRecord>) — `payload`, `len` bytes]
```

Each `ManifestRecord` is either `SstAdded { level, meta }`, `SstDeleted { level, number }`,
`WalFlushed { number, last_seq }`, `NextFileNumber(u64)`, or `FormatVersion(u32)`. Records
within a frame are applied atomically to the in-memory `ManifestState`.

A torn or CRC-mismatched frame at the tail of the file is silently dropped on replay
(not an error). The same goes for an all-zero `[len=0][crc=0]` frame. On open, the
manifest is immediately rewritten as a single snapshot edit (atomically via tmp+rename)
to prevent unbounded growth — only the live SST set plus the file-number allocator and
`FormatVersion` are carried forward.

`FormatVersion(u32)` records the on-disk format version (currently `1`, shared with the
SST footer's `format_version`). `Db::open` checks it as each manifest frame replays — before
reading any later frame, doing the snapshot rewrite, or touching any WAL/SST — and refuses
with `Error::UnsupportedFormat` on a mismatch, leaving the directory untouched. A manifest
with no `FormatVersion` record at all (written before this field existed) is treated as
version 1.

## Design tradeoffs

The reasoning behind each load-bearing engine choice.

### Leveled vs tiered compaction

driftdb runs **leveled** compaction. L0 holds the N most recent
flushes (may overlap on key range); L1+ are non-overlapping sorted
runs. Trigger: L0 compacts once it holds `l0_compaction_trigger`
files; `L_n` (n >= 1) compacts once its total bytes exceed
`l1_max_bytes * level_multiplier^(n-1)`. Pick: on the L0 path, all of
L0 + any overlapping L1 files; otherwise the `L_n` file with the
smallest `smallest` key (a simple round-robin proxy, not recency) +
all overlapping `L_{n+1}` files → merge → write to `L_{n+1}`.

- **Leveled** gives `O(log N)` reads + low space amp; cost is write
  amplification roughly 5–10×.
- **Tiered** (size-tiered) gives cheap writes (~2× write amp) but
  higher read amp (multiple sorted runs per level) and worse space
  amp (a key's old versions linger across runs).
- driftdb's intended embed targets — job-queue metadata, agent state,
  edge-runtime checkpoints — are read-dominated. Leveled wins.
  Write-dominated workloads (log shipping, ingest) would pick tiered.

### fdatasync vs fsync

The WAL group-commit path issues `fdatasync(2)`, not `fsync(2)`.

- `fsync` also flushes the inode's metadata (mtime, size). On ext4
  with `data=ordered` that's an extra journal write.
- `fdatasync` flushes only the data + the metadata strictly required
  to make the data findable (notably, file size when it grows).
- The WAL is append-only and we never make decisions based on mtime,
  so the inode-metadata flush is pure overhead. `fdatasync` wins by
  ~30–50% on ext4 and is the documented LevelDB / RocksDB default.

### MVCC GC strategy (oldest-snapshot watermark)

Tombstones and superseded versions are dropped during compaction
only if their seqno is `< oldest_snapshot`. `Db::snapshot()` returns
a `Snapshot { seq }`; while at least one snapshot is alive, the
compactor freezes the watermark at the oldest live `seq`.

- The alternative — a per-key MVCC TTL or background scavenger —
  forces extra index passes and gets the policy wrong under
  long-running scans.
- The watermark approach piggy-backs on the compactor we already run
  and has a single, easily-reasoned-about invariant: "no
  reachable-from-a-live-snapshot data is ever GC'd."

### bincode for index serialization

The SSTable index, bloom filter, and manifest records are
bincode-encoded.

- bincode 1.x is fast (memcpy-ish, no schema discovery), stable, and
  produces compact output for the `Vec<(Vec<u8>, u64)>` shape the
  index uses.
- The cost is that bincode lacks forward/backward compatibility — a
  field reorder in `ManifestRecord` is a breaking change for on-disk
  files. driftdb owns its formats; while the crate is pre-1.0, a minor
  version bump (`0.1` to `0.2`) may change the on-disk format instead
  of waiting for a major bump — see the Changelog. The format-version
  marker on the manifest and SST footer means an old build refuses a
  newer on-disk format (and vice versa) rather than misreading it.
- A future direction is to replace the index with a hand-rolled binary
  format once profiling shows the bincode decode is on the read-path
  hot path.

## Benchmarks

Measured with `cargo bench --bench report` (release, default sizes) on an
8-thread Intel i5-9300H laptop with a consumer NVMe SSD (ext4), on a shared,
not-idle machine. Values are 100 B incompressible random bytes. Treat these
as order-of-magnitude numbers, not a lab result.

| Metric                                         | Target            | Measured |
| ---------------------------------------------- | ----------------- | -------- |
| Sustained write throughput (group commit)      | > 50,000 writes/s | 93,800 writes/s at 1,024 concurrent writers |
| Write amplification (leveled, steady state)    | 5–10×             | 5.2× (WAL + flush + compaction bytes / user bytes, ~200 MB loaded, 4 levels populated) |
| p99 read latency during compaction storm       | < 10 ms           | 14 µs |
| YCSB-C get latency (Zipfian, 20k records)      | —                 | p50 15 µs, p99 169 µs, p999 704 µs |
| WAL replay throughput                          | —                 | ~2.4 GB/s (mmap replay, 180k records) |
| Recovery after `kill -9`                       | < 5 s             | 9 ms (WAL replay is bounded, see below) |

Write throughput against concurrency shows group commit at work. Every ack
waits for an `fdatasync`, so throughput is about batch size divided by fsync
latency:

| concurrent writers | 1   | 16    | 64     | 256    | 1,024  |
| ------------------ | --- | ----- | ------ | ------ | ------ |
| writes/s           | 399 | 2,886 | 11,590 | 41,140 | 93,828 |

**Recovery is bounded by design, not by replay speed.** Each memtable owns
one WAL file, and the writer stalls while two frozen memtables are waiting to
flush. So a crash leaves at most about `memtable_size × 3` of WAL to replay,
plus one group-commit batch per memtable. That is about 12 MiB with the
default options, and the WAL can never grow to gigabytes.

No RocksDB comparison has been run yet; see `PROGRESS.md`.

Run the benchmarks:

```bash
cargo bench --bench report       # one-shot markdown table (the numbers above)
cargo bench --bench throughput   # criterion: concurrent puts, write_batch, sequential puts
cargo bench --bench ycsb         # criterion: YCSB A/B/C/F with a Zipfian key chooser
```

`benches/report.rs` reads `DRIFTDB_BENCH_*` environment variables to scale
the run up or down; they are listed in the file header.

## Durability & recovery

**Single writer thread.** One dedicated worker thread owns the WAL file and active
memtable. User calls (put, delete, write_batch) send requests over a channel; the
writer drains them into one batch, appends all records to the WAL, issues one
`fdatasync`, inserts into the memtable, then publishes the visible seqno and acks.
This is **group commit**: multiple writes block together, amortizing the fsync cost.

**Ack == durable.** Reads use the published `visible_seq` snapshot, so they never
observe a write before it survives a crash.

**Fsync failure poisons the engine.** If `fdatasync` fails, every pending and future
write fails with an error (fsyncgate — a failed fsync cannot be safely retried, as the
kernel gives no guarantee the dirty pages are still queued).

**One WAL per memtable.** When the active memtable crosses the size threshold, the
writer freezes it, opens a fresh WAL, and signals the background thread
to flush the frozen one. So WAL generation k contains exactly the records of memtable k.

**Recovery steps** (on `Db::open`):
1. Replay MANIFEST to load the SSTable set and find the last flushed WAL generation.
2. Delete orphaned SST files (not referenced by manifest) and old WAL files.
3. Replay remaining WAL files in order into a single memtable (truncate torn tails).
4. If the replayed memtable is non-empty, flush it synchronously to L0 and update the manifest.
5. Open a fresh WAL and start background threads.

**Torn-tail policy.** WAL replay stops at EOF, an invalid kind byte, a header/body
that runs off the end, or a CRC mismatch, and truncates away everything from that point on.
Manifest replay only drops a torn/CRC-invalid frame the same way when it's the very tail of
the file; the same corruption occurring mid-log (more bytes follow it) is not treated as a
crash artifact — it's a hard `Error::ManifestCorrupt`. Either way, `Manifest::open` rewrites
the manifest as a single snapshot edit (tmp file + fsync + rename) rather than truncating the
existing file in place.

**Crash test.** `tests/crash_kill.rs` spawns a child process, crashes it with `SIGKILL`
mid-batch, and verifies recovery replays all committed writes.

## Usage

Embed into a downstream crate:

```toml
[dependencies]
driftdb-lsm = "0.1"
```

The package is named `driftdb-lsm` on crates.io, because `driftdb` belongs to an
unrelated project. The library is still imported as `driftdb`. It requires Rust
1.85+ and runs on Linux only: the WAL uses `fdatasync(2)` and the directory lock
uses `flock(2)`.

```rust
use driftdb::{Db, Options, WriteBatch};

#[tokio::main]
async fn main() -> driftdb::Result<()> {
    // `Db::open(path)` uses `Options::default()` (4 MiB memtable, 10 MiB L1, ...).
    // Only one `Db` may hold a directory at a time (flock on `LOCK`).
    let opts = Options {
        memtable_size: 8 * 1024 * 1024,  // 8 MiB
        l0_compaction_trigger: 4,
        target_file_size: 2 * 1024 * 1024,
        ..Default::default()
    };
    let db = Db::open_with("/var/lib/myapp/driftdb", opts).await?;

    // Single point writes
    db.put(b"hello", b"world").await?;
    let val = db.get(b"hello").await?;
    assert_eq!(val.as_deref(), Some(&b"world"[..]));

    // Delete a key
    db.delete(b"hello").await?;
    assert_eq!(db.get(b"hello").await?, None);

    // Atomic batch
    let batch = WriteBatch::new()
        .put(b"k1", b"v1")
        .put(b"k2", b"v2")
        .delete(b"k3");
    db.write_batch(batch).await?;

    // Snapshot: point-in-time read view (prevents GC of older versions)
    let snap = db.snapshot();
    let old_k1 = snap.get(b"k1")?;
    let old_range = snap.scan(b"k".to_vec()..b"l".to_vec())?;
    println!("{old_k1:?} {}", old_range.len());
    drop(snap);  // unregisters and allows GC

    // Range scan at the current visible seqno
    let range = db.scan(b"k".to_vec()..b"l".to_vec()).await?;
    for (k, v) in range {
        println!("{:?} -> {:?}", k, v);
    }

    // Force flush of the active memtable to L0
    db.flush().await?;

    // Force full compaction (all levels → bottom level)
    db.compact().await?;

    // Engine stats
    let stats = db.stats();
    println!("Levels: {:?}", stats.level_files);
    println!("Write amp: {:.2}x", stats.write_amplification());

    // Graceful shutdown
    db.close().await?;
    Ok(())
}
```

Quick start:

```bash
cargo run --example quickstart
# Opens /tmp/driftdb-demo, writes 1000 keys, reads them back, prints stats.
```

## Examples

`examples/jobqueue` is a reference integration: a durable job queue (typed store,
secondary index, snapshot reports, backup, HTTP API, SIGKILL crash demo) built on
`driftdb-lsm`. Its [README](examples/jobqueue/README.md) maps each engine feature to the
code that uses it and lists the pitfalls. It is not published to crates.io.

```bash
cargo run -p jobqueue -- demo --jobs 300
cargo run -p jobqueue -- crash-demo
cargo run -p jobqueue -- serve --dir ./jobqueue-data --addr 127.0.0.1:3000
```

## Repository layout

```
driftdb/
  src/
    lib.rs          public re-exports + crate docs
    db.rs           Db handle: writer thread (group commit), background flush/compaction
                    thread, recovery, MVCC reads, snapshots, scans, stats
    wal.rs          WAL record codec, WalFile (buffered append + fdatasync), torn-tail replay
    memtable.rs     crossbeam-skiplist memtable keyed by (user_key, seqno DESC)
    sstable.rs      SST writer + mmap reader (bloom, index, per-table block cache)
    manifest.rs     append-only manifest of atomic edits, rewritten on open
    compaction.rs   leveled compaction picker + executor
    iter.rs         k-way merge, MVCC visibility filter, compaction GC filter
    error.rs        thiserror error type
  tests/
    engine.rs          model-based tests vs BTreeMap, snapshots, concurrency, compaction
    crash_recovery.rs  reopen, torn WAL, orphan SSTs, flush failure, dir lock
    crash_kill.rs      SIGKILL a writer child process, verify every acked write
  benches/          report.rs, throughput.rs, ycsb.rs
  examples/         quickstart.rs, jobqueue/ (reference job queue crate, see its README)
  docs/             ARCHITECTURE.md (internals guide), design spec, phase plans
```

## Roadmap

Phase 1 (scaffold + memtable-only round-trip) and Phase 2 (working crash-safe engine:
WAL replay, flush to L0, SST reads, manifest, leveled compaction, snapshots, range scans)
are complete. Ongoing and future work is tracked in
[`PROGRESS.md`](./PROGRESS.md).

## License <a id="license"></a>

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

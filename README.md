# driftdb

> Embeddable LSM-tree key-value engine in Rust. WAL group-commit, MVCC
> snapshot reads, leveled compaction, mmap-backed SSTable reads. Built
> as the storage-engineering portfolio project for the Rust Level-4
> roadmap — the canonical interview pitch: _"I wanted to understand
> fsync semantics, so I wrote my own LSM."_

[![ci](https://img.shields.io/badge/ci-passing-green.svg)](https://github.com/Bunty9/driftdb/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/badge/crates.io-pending-lightgrey.svg)](#)
[![license](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

## The problem

Most app-tier code reaches for Postgres + JSON files for state. Adding
a real embedded engine — WAL durability, MVCC snapshot reads, leveled
compaction — is the canonical storage-engineering interview project.
**driftdb** is that engine, sized to embed into the other roadmap
projects (rustyq job-queue metadata, agent state, edge-runtime
checkpoints) and to ship as a public crate with reproducible
YCSB-style benchmarks against RocksDB.

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
        freeze memtable (arc-swap)
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
  |                 |   [u32 BE reserved=0][u64 BE magic=0xDEADBEEF]
  +-----------------+
```

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
`WalFlushed { number, last_seq }`, or `NextFileNumber(u64)`. Records within a frame are
applied atomically to the in-memory `ManifestState`.

A torn or CRC-mismatched frame at the tail of the file is silently dropped on replay
(not an error). The same goes for an all-zero `[len=0][crc=0]` frame. On open, the
manifest is immediately rewritten as a single snapshot edit (atomically via tmp+rename)
to prevent unbounded growth — only the live SST set plus the file-number allocator are
carried forward.

## Design tradeoffs

The defenses behind every load-bearing engine choice. These are the
answers you give in the storage-engineering interview.

### Leveled vs tiered compaction

driftdb runs **leveled** compaction. L0 holds the N most recent
flushes (may overlap on key range); L1+ are non-overlapping sorted
runs. Trigger: `|L_n| > base * mult^n`. Pick: oldest L_n SST + all
overlapping L_{n+1} SSTs → merge → write L_{n+1}.

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
  files. driftdb owns its formats and ships major-version bumps on
  format changes, so this is acceptable.
- The stretch goal in `projects-l3-l4.md` is to replace the index
  with a hand-rolled binary format once profiling shows the bincode
  decode is on the read-path hot path.

## Benchmarks

| Metric                                                | Target            | Current |
| ----------------------------------------------------- | ----------------- | --------|
| Write amplification (leveled)                         | 5–10×             |         |
| p99 read latency during compaction storm              | < 10 ms           |         |
| Recovery on 10 GB WAL                                 | < 5 s             |         |
| Sustained write throughput (4 vCPU, group-commit WAL) | > 50,000 writes/s |         |
| YCSB-C (100% read) p99 vs RocksDB                     | within 2×         |         |

_Numbers pending — see `cargo bench --bench report`_

Run benchmarks:

```bash
cargo bench --bench throughput      # 100k random puts
cargo bench --bench ycsb            # YCSB A/B/C/F workloads
cargo bench --bench report          # comprehensive report vs RocksDB
```

Set environment variables to customize the report run (see `benches/report.rs` header
for available knobs: dataset size, YCSB distribution, concurrency, etc.).

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
writer freezes it (arc-swap), opens a fresh WAL, and signals the background thread
to flush the frozen one. So WAL generation k contains exactly the records of memtable k.

**Recovery steps** (on `Db::open`):
1. Replay MANIFEST to load the SSTable set and find the last flushed WAL generation.
2. Delete orphaned SST files (not referenced by manifest) and old WAL files.
3. Replay remaining WAL files in order into a single memtable (truncate torn tails).
4. If the replayed memtable is non-empty, flush it synchronously to L0 and update the manifest.
5. Open a fresh WAL and start background threads.

**Torn-tail policy.** WAL replay stops at EOF, an invalid kind byte, a header/body
that runs off the end, or a CRC mismatch. Manifest replay stops at a CRC-failed or
structurally invalid frame (only the tail frame may be torn). Anything past the stop
point is truncated away on replay.

**Crash test.** `tests/crash_kill.rs` spawns a child process, crashes it with `SIGKILL`
mid-batch, and verifies recovery replays all committed writes.

## Usage

Embed into a downstream crate:

```toml
[dependencies]
driftdb = { path = "../driftdb" }   # or version = "0.1" once published
```

```rust
use driftdb::{Db, Options, WriteBatch};

#[tokio::main]
async fn main() -> driftdb::Result<()> {
    // Open with default options (4 MiB memtable, 10 MiB L1, etc.)
    let db = Db::open("/var/lib/myapp/driftdb").await?;

    // Tunable options
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
    let val = snap.get(b"k1")?;
    let range = snap.scan(b"k".to_vec()..b"l".to_vec())?;
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

## Repository layout

```
driftdb/
  Cargo.toml                # single-crate library
  src/
    lib.rs                  # public re-exports
    db.rs                   # `Db` handle, open/put/get/delete/snapshot
    wal.rs                  # group-commit WAL writer
    memtable.rs             # crossbeam-skiplist memtable + MVCC keys
    sstable.rs              # SST writer + (Phase 2) reader
    manifest.rs             # append-only manifest log
    compaction.rs           # leveled compaction scheduler skeleton
    error.rs                # thiserror surface
  benches/
    throughput.rs           # 100k random puts (memtable-only path)
    ycsb.rs                 # YCSB A/B/C/F workload harness
    report.rs               # comprehensive report vs RocksDB
  examples/
    quickstart.rs           # put + get round-trip
  tests/
    crash_recovery.rs       # reopen-after-drop integration test
    crash_kill.rs           # SIGKILL durability test
    engine.rs               # test utilities
  docs/
    specs/2026-05-28-driftdb-design.md         # full design spec
    plans/2026-05-28-driftdb-phase-1-scaffold.md
  deny.toml                 # cargo-deny config
  rust-toolchain.toml       # stable channel
  .github/workflows/ci.yml  # fmt + clippy + nextest + deny + bench
  PROGRESS.md               # per-sprint tracker
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

# driftdb

> Embeddable LSM-tree key-value engine in Rust. WAL group-commit, MVCC
> snapshot reads, leveled compaction, mmap-backed SSTable reads. Built
> as the storage-engineering portfolio project for the Rust Level-4
> roadmap — the canonical interview pitch: _"I wanted to understand
> fsync semantics, so I wrote my own LSM."_

[![ci](https://img.shields.io/badge/ci-pending-lightgrey.svg)](./.github/workflows/ci.yml)
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
   12      4    key_len        (u32 BE)
   16      4    val_len        (u32 BE)
   20      K    key bytes
 20+K      V    value bytes
```

Files are named `wal-NNNNNN.log` and rotated at 64 MiB. Recovery streams
records until EOF, a length prefix that overruns the file, or a CRC
mismatch — anything past that point is treated as torn-tail garbage
and truncated.

### SSTable file layout

```
  +-----------------+
  | data block 0    |   4 KiB target, zstd-compressed payload
  | data block 1    |   [u32 BE compressed_len][compressed bytes]
  | ...             |
  | data block N    |
  +-----------------+
  | index block     |   bincode: Vec<(last_key, block_offset)>
  +-----------------+
  | bloom block     |   bincode: GrowableBloom
  +-----------------+
  | footer (24 B)   |   [u64 BE index_off][u64 BE bloom_off][u64 BE magic=0xDEADBEEF]
  +-----------------+
```

Each data-block entry:

```
  [u32 BE klen][u32 BE vlen][u64 BE seq][u8 kind][key][val]
```

`kind` is `1` for `Put` (value bytes follow) and `0` for `Delete`
(value bytes are absent). Inside a block, entries sort by `(user_key
ASC, seqno DESC)` so the newest version of any key is the first hit
on a linear scan.

### SSTable footer

The trailing 24 bytes of every SSTable file: `[u64 BE index_off][u64
BE bloom_off][u64 BE magic]`. Open path seeks to `len - 24`, validates
the magic, then deserialises the index and bloom out of the regions
they point at. The magic acts as a torn-write detector — a truncated
flush won't carry the trailer.

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

## Bench targets

| Metric                                                | Target            | Notes                                          |
| ----------------------------------------------------- | ----------------- | ---------------------------------------------- |
| Write amplification (leveled)                         | 5–10×             | bytes written to disk / bytes of user data     |
| p99 read latency during compaction storm              | < 10 ms           | the load-bearing test for leveled              |
| Recovery on 10 GB WAL                                 | < 5 s             | streamed CRC-validated replay                  |
| Sustained write throughput (4 vCPU, group-commit WAL) | > 50,000 writes/s | YCSB-A 50r/50w workload                        |
| YCSB-C (100% read) p99 vs RocksDB                     | within 2×         | TODO — fill in once Phase 2 lands              |

Run benches (real numbers land in Phase 2):

```bash
cargo bench
```

### Compare vs RocksDB

Placeholder. The Phase 2 bench harness will:

- Run YCSB workloads A/B/C/F against driftdb and `rocksdb` (crate
  `rust-rocksdb`) on the same hardware + dataset.
- Record throughput + p50/p99/p999 latency to `bench-results/*.json`.
- Publish a write-amp comparison table in `PROGRESS.md` and the
  follow-up blog post (`projects-l3-l4.md` § P5 stretch).

## Quick start

```bash
cargo run --example quickstart
# opens /tmp/driftdb-demo, writes 1000 keys, reads them back, prints stats.
```

Embed into a downstream crate:

```toml
[dependencies]
driftdb = { path = "../driftdb" }   # or version = "0.1" once published
```

```rust
use driftdb::{Db, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let db = Db::open("/var/lib/myapp/driftdb").await?;
    db.put(b"hello", b"world").await?;
    let v = db.get(b"hello").await?;
    assert_eq!(v.as_deref(), Some(&b"world"[..]));
    Ok(())
}
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
    ycsb.rs                 # YCSB A/B/C/F skeleton
  examples/
    quickstart.rs           # put + get round-trip
  tests/
    crash_recovery.rs       # reopen-after-drop integration test (ignored in Phase 1)
  docs/
    specs/2026-05-28-driftdb-design.md         # full design spec
    plans/2026-05-28-driftdb-phase-1-scaffold.md
  deny.toml                 # cargo-deny config
  rust-toolchain.toml       # stable channel
  .github/workflows/ci.yml  # fmt + clippy + nextest + deny + bench
  PROGRESS.md               # per-sprint tracker
```

## Roadmap

Phase 1 (scaffold + memtable-only round-trip) is the current sprint —
see
[`docs/plans/2026-05-28-driftdb-phase-1-scaffold.md`](./docs/plans/2026-05-28-driftdb-phase-1-scaffold.md).
Subsequent phases (SST flush thread, leveled compactor, WAL replay,
YCSB harness against RocksDB) are tracked in
[`PROGRESS.md`](./PROGRESS.md).

## License <a id="license"></a>

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](./LICENSE-MIT) or
  <https://opensource.org/licenses/MIT>)

at your option.

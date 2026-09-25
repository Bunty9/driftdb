# PROGRESS — driftdb

> Per-sprint tracker. Template adapted from `project-plan.md` § 7,
> customised for P5 (driftdb) bench targets and the Phase B sequencing
> in `backend-cloud-roadmap.md` § 2 (weeks 31–38).

## Done — Phase 1 scaffold

- [x] `Cargo.toml` — single-crate library, pinned stack deps
- [x] `src/lib.rs` — module decls + public re-exports
- [x] `src/error.rs` — `thiserror` surface
- [x] `src/wal.rs` — group-commit WAL writer
- [x] `src/memtable.rs` — crossbeam-skiplist memtable + MVCC keys
- [x] `src/sstable.rs` — SST writer (Phase-1 complete); reader stubbed
- [x] `src/manifest.rs` — manifest record + log (Phase-1 stubs)
- [x] `src/compaction.rs` — `CompactionState` trait + compactor skeleton
- [x] `src/db.rs` — `Db` handle wiring WAL writer task + memtable
- [x] `benches/throughput.rs` — 100k random puts harness
- [x] `benches/ycsb.rs` — YCSB A/B/C/F skeleton
- [x] `examples/quickstart.rs` — 1k put + get round-trip
- [x] `tests/crash_recovery.rs` — `#[ignore]` reopen integration stub
- [x] `.github/workflows/ci.yml` — fmt + clippy + nextest + deny + bench
- [x] `deny.toml`, `rust-toolchain.toml`, `.gitignore`
- [x] `README.md`, design spec, phase plan
- [x] `cargo check` passes locally (verified at end of scaffold)
- [x] `cargo run --example quickstart` round-trips 1k keys

## Done — Phase 2: working crash-safe engine

- [x] Freeze-and-flush path: `Db::put` swaps the active memtable on a
      size threshold, hands the frozen one to a flush task that calls
      `SstWriter` and appends `ManifestRecord::SstAdded`.
- [x] `SstReader::open` — mmap the file, parse footer + index + bloom.
- [x] `SstReader::get` — bloom check → binary-search index → decompress
      block → linear scan.
- [x] WAL replay on `Db::open` — stream records, validate CRC, truncate
      torn-tail, rebuild memtable up to `last_seq`.
- [x] Manifest log open + append + replay bodies; orphan SST GC on open.
- [x] Compactor body: leveled pick + merge iterator + write L_{n+1}
      SST + append manifest + unlink old files.
- [x] `Db::snapshot` registers in a watermark tracker so the compactor
      knows `oldest_snapshot`.
- [x] Enabled integration tests: `crash_recovery.rs` and added
      `crash_kill.rs` (SIGKILL durability verification).
- [x] YCSB harness: pre-populate keys, Zipfian sampler, p50/p99/p999
      latency histograms, comparison runner vs RocksDB.

## Next

- **Block cache**: LRU cache by (file, block_offset) to reduce decompression under
  compaction/read storms. (ponytail: currently relies on OS page cache over mmap;
  add when profiling shows decompression is a bottleneck — see `sstable.rs:34`.)
- **Per-level compaction round-robin cursor**: track the rightmost key in each level
  to avoid starving rightmost files. (ponytail: comment at `compaction.rs:137`.)
- **Streaming memtable scan iterator**: avoid collecting all entries into a Vec for
  large `scan()` results. (ponytail: currently eager; upgrade once latency profiles
  show large scans are a hot path — see `db.rs:361`.)
- **Replication stretch (openraft)**: add multi-node consensus so driftdb can be
  embedded as a replicated state machine. (Out of scope for Phase 2.)
- **Blog post**: "I built a tiny LSM and benchmarked it against RocksDB" — document
  design tradeoffs, crash semantics, and performance profiles. (Phase 2 stretch,
  per `projects-l3-l4.md` § P5.)

## Bench numbers (targets per `projects-l3-l4.md` § P5; updated weekly)

| metric                                              | target            | current | as-of      |
|-----------------------------------------------------|-------------------|---------|------------|
| Write amplification (leveled)                       | 5–10×             |         |            |
| p99 read latency during compaction storm            | < 10 ms           |         |            |
| Recovery on 10 GB WAL                               | < 5 s             |         |            |
| Sustained write throughput (4 vCPU, group-commit)   | > 50,000 writes/s |         |            |
| YCSB-C (100R) p99 vs RocksDB                        | within 2×         |         |            |

## Blog topics surfacing

- **"I built a tiny LSM and benchmarked it against RocksDB"** — Phase 2 completed;
  ready for write-up once bench numbers land. Cover design tradeoffs, fsync semantics,
  group commit, leveled compaction, MVCC snapshot reads, crash recovery.

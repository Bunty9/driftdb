# PROGRESS — driftdb

> Development tracker: what is done, what is next, and the benchmark targets.

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

- **Publish 0.1.0 as `driftdb-lsm`**. Metadata is done; the blockers are listed in
  `docs/plans/2026-09-28-publishing.md` (shrink the public API, missing docs, on-disk
  format version).
- **Shared block cache**: today each SST keeps a tiny 8-block LRU for point reads.
  A global, size-bounded cache would help large working sets.
- **RocksDB comparison**: run the same YCSB mix against RocksDB on the same box.
- **Per-level compaction round-robin cursor**: track the rightmost key in each level
  to avoid starving rightmost files. (`ponytail:` note in `compaction.rs`.)
- **Streaming memtable scan iterator**: avoid collecting all entries into a Vec for
  large `scan()` results. (ponytail: currently eager; upgrade once latency profiles
  show large scans are a hot path; `ponytail:` note in `db.rs`.)
- **Replication stretch (openraft)**: add multi-node consensus so driftdb can be
  embedded as a replicated state machine. (Out of scope for Phase 2.)
- **Blog post**: "I built a tiny LSM and benchmarked it against RocksDB" — document
  design tradeoffs, crash semantics, and performance profiles. (Phase 2 stretch.)

## Bench numbers (`cargo bench --bench report`, i5-9300H + NVMe, shared box)

| metric                                              | target            | current                          | as-of      |
|-----------------------------------------------------|-------------------|----------------------------------|------------|
| Write amplification (leveled)                       | 5–10×             | 5.2× (incl. WAL)                 | 2026-09-26 |
| p99 read latency during compaction storm            | < 10 ms           | 14 µs                            | 2026-09-26 |
| Recovery on 10 GB WAL                               | < 5 s             | n/a: WAL bounded to ~3× memtable; kill -9 recovery 9 ms | 2026-09-26 |
| Sustained write throughput (4 vCPU, group-commit)   | > 50,000 writes/s | 93.8k/s @ 1024 writers           | 2026-09-26 |
| YCSB-C (100R) p99 vs RocksDB                        | within 2×         | 169 µs p99; RocksDB run pending  | 2026-09-26 |

## Blog topics surfacing

- **"I built a tiny LSM and benchmarked it against RocksDB"** — Phase 2 completed;
  bench numbers landed; RocksDB comparison still to run. Cover design tradeoffs, fsync semantics,
  group commit, leveled compaction, MVCC snapshot reads, crash recovery.

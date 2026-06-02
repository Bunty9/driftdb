---
title: driftdb Phase 1 — Scaffold + Compile
status: draft
date: 2026-05-28
related:
    - ../specs/2026-05-28-driftdb-design.md
    - ../../../projects-l3-l4.md
    - ../../../backend-cloud-roadmap.md
---

# driftdb Phase 1 — Scaffold + Compile

> **Goal:** lay down the single-crate library skeleton — every module
> in the canonical LSM split (WAL, memtable, SSTable, manifest,
> compaction, top-level `Db`), the bench + example + integration-test
> harnesses, and CI — so that `cargo check` is green, `cargo run
> --example quickstart` round-trips 1,000 keys through the
> memtable-only path, and the library is ready to be embedded as a
> dependency by downstream P1 (rustyq) experiments. The SST flush
> thread, compactor body, WAL replay, and SST reader path land in
> Phase 2.

**Spec source:** [`../specs/2026-05-28-driftdb-design.md`](../specs/2026-05-28-driftdb-design.md).

## File inventory (checklist)

- [x] `Cargo.toml` — single-crate library, pinned stack from
      `backend-cloud-roadmap.md` § 3 (already existed).
- [x] `rust-toolchain.toml` — `channel = "stable"` + clippy + rustfmt.
- [x] `deny.toml` — minimal `cargo-deny` config (advisories deny,
      license allowlist for MIT/Apache/BSD/ISC/MPL/Unicode/CC0).
- [x] `.gitignore` — Rust + `.env` + `target/` + `*.cwasm` + `dist/`
      + `.venv/`.
- [x] `src/lib.rs` — module declarations + public re-exports
      (already existed).
- [x] `src/error.rs` — `thiserror` surface: `Io`, `WalCorrupt`,
      `ManifestCorrupt`, `Bincode` (already existed).
- [x] `src/wal.rs` — `WalRecord`, `WalMsg`, `WalWriter` with
      group-commit driver (already existed).
- [x] `src/memtable.rs` — `InternalKey`, `Value`, `Memtable` with
      MVCC ordering (already existed).
- [x] `src/sstable.rs` — `SstWriter` (complete), `SstReader` /
      `SstIter` (Phase-2 stubs) (already existed).
- [x] `src/manifest.rs` — `ManifestRecord`, `Manifest` open / append
      / replay (Phase-2 bodies stubbed) (already existed).
- [x] `src/compaction.rs` — `CompactionPlan`, `CompactionState`
      trait, `compactor` task + `run_compaction` stubs (already
      existed; Phase-1 trait-bound bug on `Arc<S>` fixed in this
      scaffold).
- [x] `src/db.rs` — `Db` handle: `open`, `put`, `get`, `delete`,
      `snapshot`. Wires the WAL writer task; memtable-only read
      path (already existed).
- [x] `benches/throughput.rs` — 100k random puts against a fresh
      `Db` rooted in a tempdir (already existed).
- [x] `benches/ycsb.rs` — criterion harness skeleton for YCSB A / B
      / C / F. Inner loops marked `// TODO(phase-2): real YCSB
      workload`.
- [x] `examples/quickstart.rs` — `Db::open("/tmp/driftdb-demo")`,
      put 1k keys, get them back, assert equal, print stats.
- [x] `tests/crash_recovery.rs` — `#[tokio::test] #[ignore]` reopen
      integration stub.
- [x] `.github/workflows/ci.yml` — matrix on stable + beta; runs
      `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo
      nextest run`, `cargo deny check`, and `cargo bench --no-run`
      (non-blocking).
- [x] `README.md` — problem, ASCII architecture, format diagrams
      (WAL record, SSTable layout, footer), design-tradeoffs
      section (leveled vs tiered, fdatasync vs fsync, MVCC GC
      watermark, bincode index), bench targets table, license.
- [x] `docs/specs/2026-05-28-driftdb-design.md` — full P5 design
      spec lifted from `projects-l3-l4.md`.
- [x] `docs/plans/2026-05-28-driftdb-phase-1-scaffold.md` — this
      plan.
- [x] `PROGRESS.md` — per-sprint tracker, P5 bench targets
      recorded.

## Exit criteria

1. **`cargo check` passes** from the project root with no environment
   prerequisites (offline check; the crate has no external service
   dependencies in Phase 1).
2. **`cargo run --example quickstart` runs the put+get loop in
   memtable-only mode** — opens a fresh `Db`, writes 1,000 keys,
   reads every one back, asserts equality, and prints throughput
   stats. No SST flush is required because `Db::get` consults only
   the active memtable in Phase 1.
3. **`cargo test --lib` passes** — the library has no unit tests yet,
   so this is an "exits clean" check; integration tests live in
   `tests/` and the only one shipped (`crash_recovery`) is
   `#[ignore]`d until WAL replay lands.

## Out of scope (deferred to later phases)

- SST flush thread (freeze memtable → write L0 SST → append
  `SstAdded` to the manifest).
- Compactor body (`pick_compaction` + `run_compaction` + merge
  iterator).
- SST reader path (`SstReader::open`, `SstReader::get`,
  `SstReader::iter`).
- WAL replay on `Db::open` (stream + CRC-validate + truncate
  torn-tail + rebuild memtable up to `last_seq`).
- Manifest log open + append + replay bodies.
- YCSB harness real workloads (Zipfian key sampling, RocksDB
  comparison run, per-op latency histograms).
- `Db::snapshot` watermark tracker so the compactor knows the
  oldest live snapshot.
- crates.io publish.

## Verification recipe

```bash
cd driftdb
cargo check                          # exit-criterion 1
cargo run --example quickstart       # exit-criterion 2 — "ok" at end
cargo test --lib                     # exit-criterion 3
```

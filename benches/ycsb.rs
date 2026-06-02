//! YCSB-style workload skeleton.
//!
//! Mirrors the standard Yahoo! Cloud Serving Benchmark mix so driftdb numbers
//! can be compared apples-to-apples against RocksDB / SlateDB:
//!
//! | Workload | Mix              | Phase-2 use                                    |
//! | -------- | ---------------- | ---------------------------------------------- |
//! | A        | 50% read / 50% update | "update heavy" — exercises memtable + WAL  |
//! | B        | 95% read / 5%  update | "read mostly" — exercises bloom + SST cache |
//! | C        | 100% read             | pure point-read latency                    |
//! | F        | 50% read / 50% read-modify-write | snapshot read + put         |
//!
//! Phase-1 status: every workload is an empty harness — the inner loop is
//! marked `// TODO(phase-2): real YCSB workload` so the bench binary
//! compiles and `cargo bench --no-run` is green in CI. The
//! `criterion_group!` registers the four functions but each
//! `bench_function` body is a single iteration of a noop closure so
//! criterion does not panic on a zero-sample group.
//!
//! See `projects-l3-l4.md` § P5 ("Eval / benchmarks") for the target numbers
//! that land alongside the Phase-2 implementation.

#![allow(unused)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use driftdb::Db;
use tempfile::TempDir;

/// Total operations per workload sample.
const OPS: usize = 10_000;

/// Boot a fresh `Db` in a tempdir. Shared by every workload so the harness
/// reuses one runtime + one driftdb open per Criterion iteration.
fn boot(rt: &tokio::runtime::Runtime) -> (TempDir, Db) {
    let dir = TempDir::new().expect("tempdir");
    let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");
    (dir, db)
}

/// Workload A — 50% read / 50% update. Update-heavy.
fn ycsb_a(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("ycsb_a");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("50r/50u", |b| {
        b.iter(|| {
            let (_dir, _db) = boot(&rt);
            // TODO(phase-2): real YCSB workload — Zipfian key sampling,
            // 50/50 read/update mix, record per-op latency for p99.
        });
    });
    group.finish();
}

/// Workload B — 95% read / 5% update. Read-mostly.
fn ycsb_b(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("ycsb_b");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("95r/5u", |b| {
        b.iter(|| {
            let (_dir, _db) = boot(&rt);
            // TODO(phase-2): real YCSB workload — bloom hit-rate matters here,
            // so the bench should warm a population of N keys before the read
            // burst to populate L0 SSTs.
        });
    });
    group.finish();
}

/// Workload C — 100% read. Pure point-read latency.
fn ycsb_c(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("ycsb_c");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("100r", |b| {
        b.iter(|| {
            let (_dir, _db) = boot(&rt);
            // TODO(phase-2): real YCSB workload — pre-populate keys, then
            // measure `get` latency. Compare against RocksDB on identical
            // hardware (see README "Compare vs RocksDB").
        });
    });
    group.finish();
}

/// Workload F — 50% read / 50% read-modify-write. Exercises the snapshot path.
fn ycsb_f(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let mut group = c.benchmark_group("ycsb_f");
    group.throughput(Throughput::Elements(OPS as u64));
    group.sample_size(10);
    group.bench_function("50r/50rmw", |b| {
        b.iter(|| {
            let (_dir, _db) = boot(&rt);
            // TODO(phase-2): real YCSB workload — RMW path validates that
            // `db.snapshot().get(...)` followed by `db.put(...)` sees the
            // pre-write value (MVCC isolation).
        });
    });
    group.finish();
}

criterion_group!(benches, ycsb_a, ycsb_b, ycsb_c, ycsb_f);
criterion_main!(benches);

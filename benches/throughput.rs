//! Throughput benchmark: 100k random puts against a fresh `Db` rooted in a tempdir.
//!
//! Phase 1 status: the harness compiles + boots but the inner loop short-circuits because
//! the SST flush + compactor are stubbed. Once Phase 2 lands the bench measures sustained
//! `put` throughput end-to-end (WAL group-commit included).
//!
//! Target (per `projects-l3-l4.md` § P5): write amp 5–10× for leveled, p99 read < 10 ms
//! during compaction, recovery < 5 s on a 10 GB WAL.

#![allow(unused)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use driftdb::Db;
use tempfile::TempDir;

const N: usize = 100_000;

fn bench_puts(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let mut group = c.benchmark_group("driftdb_puts");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);

    group.bench_function("100k random puts (memtable-only path)", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().expect("tempdir");
                let db = rt
                    .block_on(Db::open(dir.path()))
                    .expect("open driftdb");
                let start = std::time::Instant::now();
                for i in 0..N {
                    let k = format!("k{:08}", pseudo_random(i));
                    let v = format!("v{}", i);
                    // Phase 2 will surface a real error here; for now `put` is infallible past
                    // the channel send.
                    let _ = rt.block_on(db.put(k.as_bytes(), v.as_bytes()));
                }
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// Tiny xorshift to avoid pulling in `rand` just for the bench.
fn pseudo_random(seed: usize) -> u64 {
    let mut x = seed as u64 ^ 0x9E37_79B9_7F4A_7C15;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

criterion_group!(benches, bench_puts);
criterion_main!(benches);

//! Write-throughput benchmarks.
//!
//! Every `put` is durable (one `fdatasync` per group-commit batch), so a single sequential
//! writer is fsync-bound at roughly one op per fsync round-trip. The point of these benches is
//! to show the difference between that worst case and the two ways driftdb amortizes the fsync
//! cost: many concurrent callers sharing one group-commit batch, and an explicit `write_batch`.
//!
//! Target (per `PROGRESS.md` bench table): sustained write throughput (4 vCPU, group-commit)
//! > 50,000 writes/s.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use driftdb::{Db, WriteBatch};
use tempfile::TempDir;

/// 100-byte value, matching the YCSB default record size.
const VALUE: [u8; 100] = [b'v'; 100];

const CONCURRENT_TASKS: usize = 64;
const PUTS_PER_TASK: usize = 200;

const BATCH_OPS: usize = 100;

const SEQUENTIAL_N: usize = 500;

fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// (a) 64 concurrent tasks each doing `PUTS_PER_TASK` puts against one `Db` -- exercises
/// group commit: many callers, one `fdatasync` per drained batch.
fn bench_concurrent_puts(c: &mut Criterion) {
    let rt = multi_thread_rt();
    let total_ops = (CONCURRENT_TASKS * PUTS_PER_TASK) as u64;

    let mut group = c.benchmark_group("driftdb_puts");
    group.throughput(Throughput::Elements(total_ops));
    group.sample_size(10);

    group.bench_function("64 tasks x 200 puts (group commit)", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().expect("tempdir");
                let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");
                let start = std::time::Instant::now();
                rt.block_on(async {
                    let mut set = tokio::task::JoinSet::new();
                    for t in 0..CONCURRENT_TASKS {
                        let db = db.clone();
                        set.spawn(async move {
                            for i in 0..PUTS_PER_TASK {
                                let seed = t * PUTS_PER_TASK + i;
                                let k = format!("k{:016x}", pseudo_random(seed));
                                db.put(k.as_bytes(), &VALUE).await.expect("put");
                            }
                        });
                    }
                    while set.join_next().await.is_some() {}
                });
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// (b) One `write_batch` of 100 ops per iteration -- one fsync amortized over 100 ops.
fn bench_write_batch(c: &mut Criterion) {
    let rt = multi_thread_rt();

    let mut group = c.benchmark_group("driftdb_puts");
    group.throughput(Throughput::Elements(BATCH_OPS as u64));
    group.sample_size(10);

    group.bench_function("write_batch(100 ops)", |b| {
        let dir = TempDir::new().expect("tempdir");
        let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");
        let mut counter = 0usize;
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let mut batch = WriteBatch::new();
                for _ in 0..BATCH_OPS {
                    let k = format!("k{:016x}", pseudo_random(counter));
                    counter += 1;
                    batch = batch.put(k.into_bytes(), VALUE.to_vec());
                }
                let start = std::time::Instant::now();
                rt.block_on(db.write_batch(batch)).expect("write_batch");
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// (c) Sequential single-writer puts, small N -- no concurrency to amortize the fsync, so this
/// is close to one fsync round-trip per op. Documents the fsync-bound floor.
fn bench_sequential_puts(c: &mut Criterion) {
    let rt = multi_thread_rt();

    let mut group = c.benchmark_group("driftdb_puts");
    group.throughput(Throughput::Elements(SEQUENTIAL_N as u64));
    group.sample_size(10);

    group.bench_function("sequential (fsync-bound)", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let dir = TempDir::new().expect("tempdir");
                let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");
                let start = std::time::Instant::now();
                for i in 0..SEQUENTIAL_N {
                    let k = format!("k{:016x}", pseudo_random(i));
                    rt.block_on(db.put(k.as_bytes(), &VALUE)).expect("put");
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

criterion_group!(
    benches,
    bench_concurrent_puts,
    bench_write_batch,
    bench_sequential_puts
);
criterion_main!(benches);

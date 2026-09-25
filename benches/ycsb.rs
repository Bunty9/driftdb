//! YCSB-style workload benchmarks.
//!
//! Mirrors the standard Yahoo! Cloud Serving Benchmark mixes so driftdb numbers can be
//! compared apples-to-apples against RocksDB / SlateDB:
//!
//! | Workload | Mix                              | Exercises                          |
//! | -------- | --------------------------------- | ----------------------------------- |
//! | A        | 50% read / 50% update             | memtable + WAL (update heavy)       |
//! | B        | 95% read / 5%  update             | bloom + SST cache (read mostly)     |
//! | C        | 100% read                         | pure point-read latency             |
//! | F        | 50% read / 50% read-modify-write  | `snapshot().get()` + `put` isolation |
//!
//! Keys are drawn from a Zipfian distribution (theta = 0.99, the YCSB default -- a small "hot"
//! subset of keys gets most of the traffic) via the standard YCSB Zipfian generator. Each
//! workload preloads `RECORDS` 100-byte-value keys via `write_batch` + `flush()` *before* the
//! timed section (`iter_custom`, population outside the loop), so reads hit real SSTables
//! rather than the memtable. `Options::memtable_size` is set small so compaction runs during
//! the load, same as a real ingest.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use driftdb::{Db, Options, WriteBatch};
use tempfile::TempDir;

/// Keys preloaded before every workload.
const RECORDS: usize = 50_000;
/// Records per `write_batch` call during preload.
const LOAD_BATCH: usize = 500;
/// 100-byte value, matching the YCSB default record size.
const VALUE: [u8; 100] = [b'v'; 100];
/// Zipfian skew -- YCSB's default.
const ZIPF_THETA: f64 = 0.99;

/// Concurrent client tasks issuing ops against the shared `Db`.
const TASKS: usize = 8;
/// Ops per task per benchmark iteration.
const OPS_PER_TASK: usize = 512;
const TOTAL_OPS: usize = TASKS * OPS_PER_TASK;

fn key_at(i: usize) -> String {
    format!("key{:010}", i)
}

fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

/// Boot a fresh `Db` with a small memtable (so flush/compaction runs during the load) and
/// preload `RECORDS` keys, then flush so subsequent reads hit SSTables. Runs once per bench
/// group, outside the timed section.
fn preload(rt: &tokio::runtime::Runtime) -> (TempDir, Db) {
    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        memtable_size: 1024 * 1024,
        ..Options::default()
    };
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("open driftdb");
    rt.block_on(async {
        let mut i = 0;
        while i < RECORDS {
            let end = (i + LOAD_BATCH).min(RECORDS);
            let mut batch = WriteBatch::new();
            for j in i..end {
                batch = batch.put(key_at(j).into_bytes(), VALUE.to_vec());
            }
            db.write_batch(batch).await.expect("write_batch");
            i = end;
        }
        db.flush().await.expect("flush");
    });
    (dir, db)
}

/// What a client task does with each key it draws.
#[derive(Clone, Copy)]
enum Mix {
    /// `read_pct`% plain reads, the rest plain updates (workloads A/B/C).
    ReadUpdate { read_pct: u32 },
    /// 50% plain reads, 50% read-modify-write (`snapshot().get()` then `put`) (workload F).
    ReadModifyWrite,
}

async fn run_mix(db: &Db, mix: Mix, seed_base: u64) {
    let mut set = tokio::task::JoinSet::new();
    for t in 0..TASKS {
        let db = db.clone();
        let seed = seed_base ^ (t as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        set.spawn(async move {
            let mut rng = Xorshift64::new(seed);
            let zipf = Zipfian::new(RECORDS as u64, ZIPF_THETA);
            for _ in 0..OPS_PER_TASK {
                let idx = zipf.next(rng.next_f64()) as usize;
                let key = key_at(idx);
                let roll = (rng.next_u64() % 100) as u32;
                match mix {
                    Mix::ReadUpdate { read_pct } => {
                        if roll < read_pct {
                            db.get(key.as_bytes()).await.expect("get");
                        } else {
                            db.put(key.as_bytes(), &VALUE).await.expect("put");
                        }
                    }
                    Mix::ReadModifyWrite => {
                        if roll < 50 {
                            db.get(key.as_bytes()).await.expect("get");
                        } else {
                            let snap = db.snapshot();
                            snap.get(key.as_bytes()).expect("snapshot get");
                            db.put(key.as_bytes(), &VALUE).await.expect("put");
                        }
                    }
                }
            }
        });
    }
    while set.join_next().await.is_some() {}
}

fn bench_workload(c: &mut Criterion, group_name: &str, bench_name: &str, mix: Mix) {
    let rt = multi_thread_rt();
    let (_dir, db) = preload(&rt);

    let mut group = c.benchmark_group(group_name);
    group.throughput(Throughput::Elements(TOTAL_OPS as u64));
    group.sample_size(10);

    let mut seed = 0u64;
    group.bench_function(bench_name, |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                seed = seed.wrapping_add(1);
                let start = std::time::Instant::now();
                rt.block_on(run_mix(&db, mix, seed));
                total += start.elapsed();
            }
            total
        });
    });

    group.finish();
}

/// Workload A -- 50% read / 50% update. Update-heavy.
fn ycsb_a(c: &mut Criterion) {
    bench_workload(c, "ycsb_a", "50r/50u", Mix::ReadUpdate { read_pct: 50 });
}

/// Workload B -- 95% read / 5% update. Read-mostly.
fn ycsb_b(c: &mut Criterion) {
    bench_workload(c, "ycsb_b", "95r/5u", Mix::ReadUpdate { read_pct: 95 });
}

/// Workload C -- 100% read. Pure point-read latency.
fn ycsb_c(c: &mut Criterion) {
    bench_workload(c, "ycsb_c", "100r", Mix::ReadUpdate { read_pct: 100 });
}

/// Workload F -- 50% read / 50% read-modify-write. Exercises the snapshot path.
fn ycsb_f(c: &mut Criterion) {
    bench_workload(c, "ycsb_f", "50r/50rmw", Mix::ReadModifyWrite);
}

/// Tiny xorshift64 PRNG -- no `rand` dependency.
struct Xorshift64(u64);

impl Xorshift64 {
    fn new(seed: u64) -> Self {
        Xorshift64(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Standard YCSB Zipfian generator (Gray et al. 1994, as implemented in YCSB's
/// `ZipfianGenerator`). Draws indices into `[0, items)` skewed so a small prefix of items gets
/// most of the density; `theta` close to 1.0 is a strong skew (YCSB default: 0.99).
struct Zipfian {
    items: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}

impl Zipfian {
    fn new(items: u64, theta: f64) -> Self {
        let zeta2theta = zeta(2, theta);
        let zetan = zeta(items, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - zeta2theta / zetan);
        Zipfian {
            items,
            theta,
            zetan,
            alpha,
            eta,
        }
    }

    /// Map a uniform `u` in `[0, 1)` to an index in `[0, items)`.
    fn next(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let arg = (self.eta * u - self.eta + 1.0).max(0.0);
        let v = (self.items as f64 * arg.powf(self.alpha)) as u64;
        v.min(self.items - 1)
    }
}

fn zeta(n: u64, theta: f64) -> f64 {
    (0..n).map(|i| 1.0 / ((i + 1) as f64).powf(theta)).sum()
}

criterion_group!(benches, ycsb_a, ycsb_b, ycsb_c, ycsb_f);
criterion_main!(benches);

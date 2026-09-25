//! `cargo bench --bench report` -- a plain (non-criterion) binary that runs a handful of
//! one-shot measurements and prints a markdown table. Criterion's statistical rigor is
//! overkill for "what's write amplification after loading 100MB" or "how long does recovery
//! take" -- these are single wall-clock numbers, so this just times them directly.
//!
//! All sizes are overridable via env vars so this stays fast in CI and can be scaled up for a
//! real measurement run:
//!
//! - `DRIFTDB_BENCH_VALUE_SIZE`          (default 100)   -- value size in bytes, everywhere.
//! - `DRIFTDB_BENCH_WRITE_TASKS`         (default 64)    -- concurrent writer tasks.
//! - `DRIFTDB_BENCH_WRITE_OPS_PER_TASK`  (default 300)   -- puts per writer task.
//! - `DRIFTDB_BENCH_RECORDS`             (default 20000) -- preload size for the latency benches.
//! - `DRIFTDB_BENCH_READ_TASKS`          (default 16)    -- concurrent reader tasks (YCSB-C).
//! - `DRIFTDB_BENCH_READ_OPS_PER_TASK`   (default 500)   -- gets per reader task (YCSB-C).
//! - `DRIFTDB_BENCH_COMPACTION_READS`    (default 2000)  -- gets issued during the compaction storm.
//! - `DRIFTDB_BENCH_LOAD_MB`             (default 100)   -- data volume for the write-amp measurement.
//! - `DRIFTDB_BENCH_RECOVERY_MB`         (default 20)    -- WAL volume for the recovery-time measurement.

use driftdb::{Db, Options, WriteBatch};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn multi_thread_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn key_at(i: usize) -> String {
    format!("key{:010}", i)
}

/// Approximate on-disk bytes per record (key + value), used to size loads from a target MB.
fn approx_record_bytes(value_size: usize) -> usize {
    value_size + 16
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((p * (sorted.len() - 1) as f64).round() as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Load `records` `key_at(i) -> value` pairs into `db` via `write_batch`, `batch_size` ops at a
/// time.
async fn load(db: &Db, records: usize, value: &[u8], batch_size: usize) {
    let mut i = 0;
    while i < records {
        let end = (i + batch_size).min(records);
        let mut batch = WriteBatch::new();
        for j in i..end {
            batch = batch.put(key_at(j).into_bytes(), value.to_vec());
        }
        db.write_batch(batch).await.expect("write_batch");
        i = end;
    }
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

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// Standard YCSB Zipfian generator (theta = 0.99 skew). See `benches/ycsb.rs` for the derivation.
struct Zipfian {
    items: u64,
    theta: f64,
    zetan: f64,
    alpha: f64,
    eta: f64,
}

impl Zipfian {
    fn new(items: u64, theta: f64) -> Self {
        let zeta = |n: u64| -> f64 { (0..n).map(|i| 1.0 / ((i + 1) as f64).powf(theta)).sum() };
        let zeta2theta = zeta(2);
        let zetan = zeta(items);
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

    fn next(&self, u: f64) -> u64 {
        let uz = u * self.zetan;
        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return 1;
        }
        let arg = (self.eta * u - self.eta + 1.0).max(0.0);
        ((self.items as f64 * arg.powf(self.alpha)) as u64).min(self.items - 1)
    }
}

/// Sustained concurrent write throughput -- many tasks sharing group commit.
fn bench_write_throughput(rt: &tokio::runtime::Runtime) -> String {
    let tasks = env_usize("DRIFTDB_BENCH_WRITE_TASKS", 64);
    let ops_per_task = env_usize("DRIFTDB_BENCH_WRITE_OPS_PER_TASK", 300);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let value = vec![b'v'; value_size];

    let dir = TempDir::new().expect("tempdir");
    let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");

    eprintln!("[write-throughput] {tasks} tasks x {ops_per_task} puts...");
    let start = Instant::now();
    rt.block_on(async {
        let mut set = tokio::task::JoinSet::new();
        for t in 0..tasks {
            let db = db.clone();
            let value = value.clone();
            set.spawn(async move {
                for i in 0..ops_per_task {
                    let k = key_at(t * ops_per_task + i);
                    db.put(k.as_bytes(), &value).await.expect("put");
                }
            });
        }
        while set.join_next().await.is_some() {}
    });
    let elapsed = start.elapsed();
    rt.block_on(db.close()).ok();

    let total_ops = tasks * ops_per_task;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    format!(
        "| sustained write throughput ({tasks} concurrent tasks) | {ops_per_sec:.0} ops/s ({total_ops} ops in {elapsed:.2?}) |"
    )
}

/// YCSB-C (100% read) get-latency percentiles against SSTable-resident data.
fn bench_ycsb_c_latency(rt: &tokio::runtime::Runtime) -> String {
    let records = env_usize("DRIFTDB_BENCH_RECORDS", 20_000);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let read_tasks = env_usize("DRIFTDB_BENCH_READ_TASKS", 16);
    let reads_per_task = env_usize("DRIFTDB_BENCH_READ_OPS_PER_TASK", 500);
    let value = vec![b'v'; value_size];

    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        memtable_size: 1024 * 1024,
        ..Options::default()
    };
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("open driftdb");

    eprintln!("[ycsb-c] preloading {records} records...");
    rt.block_on(async {
        load(&db, records, &value, 500).await;
        db.flush().await.expect("flush");
    });

    eprintln!("[ycsb-c] {read_tasks} tasks x {reads_per_task} gets...");
    let mut latencies = rt.block_on(async {
        let mut set = tokio::task::JoinSet::new();
        for t in 0..read_tasks {
            let db = db.clone();
            set.spawn(async move {
                let mut rng = Xorshift64::new(0xC0FF_EE00 ^ t as u64);
                let zipf = Zipfian::new(records as u64, 0.99);
                let mut lat = Vec::with_capacity(reads_per_task);
                for _ in 0..reads_per_task {
                    let idx = zipf.next(rng.next_f64()) as usize;
                    let key = key_at(idx);
                    let start = Instant::now();
                    db.get(key.as_bytes()).await.expect("get");
                    lat.push(start.elapsed());
                }
                lat
            });
        }
        let mut all = Vec::with_capacity(read_tasks * reads_per_task);
        while let Some(res) = set.join_next().await {
            all.extend(res.expect("join"));
        }
        all
    });
    rt.block_on(db.close()).ok();

    latencies.sort();
    let p50 = percentile(&latencies, 0.50);
    let p99 = percentile(&latencies, 0.99);
    let p999 = percentile(&latencies, 0.999);
    format!(
        "| YCSB-C get latency ({records} records, {} reads) | p50 {p50:.2?}, p99 {p99:.2?}, p999 {p999:.2?} |",
        latencies.len()
    )
}

/// p99 read latency while a background writer + `compact()` loop is running concurrently.
fn bench_compaction_storm(rt: &tokio::runtime::Runtime) -> String {
    let records = env_usize("DRIFTDB_BENCH_RECORDS", 20_000);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let reads = env_usize("DRIFTDB_BENCH_COMPACTION_READS", 2_000);
    let value = vec![b'v'; value_size];

    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        memtable_size: 256 * 1024,
        ..Options::default()
    };
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("open driftdb");

    eprintln!("[compaction-storm] preloading {records} records...");
    rt.block_on(async {
        load(&db, records, &value, 500).await;
        db.flush().await.expect("flush");
    });

    eprintln!("[compaction-storm] background writer + compact(), {reads} reads...");
    let stop = Arc::new(AtomicBool::new(false));
    let mut latencies = rt.block_on(async {
        let bg_db = db.clone();
        let bg_stop = stop.clone();
        let bg_value = value.clone();
        let writer = tokio::spawn(async move {
            let mut i = 0usize;
            while !bg_stop.load(Ordering::Relaxed) {
                let mut batch = WriteBatch::new();
                for _ in 0..200 {
                    batch = batch.put(key_at(i % records).into_bytes(), bg_value.clone());
                    i += 1;
                }
                bg_db.write_batch(batch).await.expect("write_batch");
                let _ = bg_db.compact().await;
            }
        });

        let mut rng = Xorshift64::new(0xBEEF_0000);
        let zipf = Zipfian::new(records as u64, 0.99);
        let mut lat = Vec::with_capacity(reads);
        for _ in 0..reads {
            let idx = zipf.next(rng.next_f64()) as usize;
            let key = key_at(idx);
            let start = Instant::now();
            db.get(key.as_bytes()).await.expect("get");
            lat.push(start.elapsed());
        }
        stop.store(true, Ordering::Relaxed);
        writer.await.expect("writer join");
        lat
    });
    rt.block_on(db.close()).ok();

    latencies.sort();
    let p99 = percentile(&latencies, 0.99);
    format!(
        "| p99 read latency during compaction storm | {p99:.2?} ({} reads) |",
        latencies.len()
    )
}

/// Write amplification (`disk_bytes_written / user_bytes_written`) after loading ~`DRIFTDB_BENCH_LOAD_MB`
/// of data and fully compacting it.
fn bench_write_amplification(rt: &tokio::runtime::Runtime) -> String {
    let load_mb = env_usize("DRIFTDB_BENCH_LOAD_MB", 100);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let records = (load_mb * 1024 * 1024) / approx_record_bytes(value_size);
    let value = vec![b'v'; value_size];

    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        memtable_size: 2 * 1024 * 1024,
        ..Options::default()
    };
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("open driftdb");

    eprintln!("[write-amp] loading ~{load_mb}MB ({records} records)...");
    rt.block_on(async {
        load(&db, records, &value, 1000).await;
    });
    eprintln!("[write-amp] compacting...");
    rt.block_on(db.compact()).expect("compact");

    let stats = db.stats();
    rt.block_on(db.close()).ok();
    format!(
        "| write amplification (~{load_mb}MB loaded, fully compacted) | {:.2}x (user {}B, disk {}B) |",
        stats.write_amplification(),
        stats.user_bytes_written,
        stats.disk_bytes_written
    )
}

/// Recovery time: write `DRIFTDB_BENCH_RECOVERY_MB` of data to the WAL only (memtable large
/// enough that nothing flushes), drop the `Db`, then time reopening (WAL replay).
fn bench_recovery_time(rt: &tokio::runtime::Runtime) -> String {
    let recovery_mb = env_usize("DRIFTDB_BENCH_RECOVERY_MB", 20);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let records = (recovery_mb * 1024 * 1024) / approx_record_bytes(value_size);
    let value = vec![b'v'; value_size];

    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        // Large enough that `records` worth of writes never crosses the flush threshold --
        // everything stays in the WAL for recovery to replay.
        memtable_size: 1024 * 1024 * 1024,
        ..Options::default()
    };

    eprintln!("[recovery] writing ~{recovery_mb}MB to WAL (no flush)...");
    {
        let db = rt
            .block_on(Db::open_with(dir.path(), options.clone()))
            .expect("open driftdb");
        rt.block_on(async {
            load(&db, records, &value, 1000).await;
        });
        // Dropped without an explicit close(): each `put`/`write_batch` already waited for its
        // WAL fsync ack, so the data is durable -- this is exactly the crash-recovery path.
    }

    eprintln!("[recovery] reopening...");
    let start = Instant::now();
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("reopen driftdb");
    let elapsed = start.elapsed();
    rt.block_on(db.close()).ok();

    format!("| recovery time (~{recovery_mb}MB WAL, {records} records) | {elapsed:.2?} |")
}

fn main() {
    let rt = multi_thread_rt();

    let rows = vec![
        bench_write_throughput(&rt),
        bench_ycsb_c_latency(&rt),
        bench_compaction_storm(&rt),
        bench_write_amplification(&rt),
        bench_recovery_time(&rt),
    ];

    println!("\n# driftdb bench report\n");
    println!("| metric | value |");
    println!("|---|---|");
    for row in rows {
        println!("{row}");
    }
}

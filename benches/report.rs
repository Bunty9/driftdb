//! `cargo bench --bench report` -- a plain (non-criterion) binary that runs a handful of
//! one-shot measurements and prints a markdown table. Criterion's statistical rigor is
//! overkill for "what's write amplification after loading 100MB" or "how long does recovery
//! take" -- these are single wall-clock numbers, so this just times them directly.
//!
//! All sizes are overridable via env vars so this stays fast in CI and can be scaled up for a
//! real measurement run:
//!
//! - `DRIFTDB_BENCH_VALUE_SIZE`          (default 100)   -- value size in bytes, everywhere.
//! - `DRIFTDB_BENCH_WRITE_OPS_PER_TASK`  (default 300)   -- puts per writer task, per concurrency level.
//! - `DRIFTDB_BENCH_RECORDS`             (default 20000) -- preload size for the latency benches.
//! - `DRIFTDB_BENCH_READ_TASKS`          (default 16)    -- concurrent reader tasks (YCSB-C).
//! - `DRIFTDB_BENCH_READ_OPS_PER_TASK`   (default 500)   -- gets per reader task (YCSB-C).
//! - `DRIFTDB_BENCH_COMPACTION_READS`    (default 2000)  -- gets issued during the compaction storm.
//! - `DRIFTDB_BENCH_LOAD_MB`             (default 200)   -- data volume for the write-amp measurement.
//! - `DRIFTDB_BENCH_RECOVERY_MB`         (default 20)    -- WAL volume for the raw-replay measurement.
//!
//! Write throughput is reported at several concurrency levels (1, 16, 64, 256, 1024 tasks) since
//! group-commit throughput scales with how many concurrent callers share one `fdatasync`.

use driftdb::{Db, Options, WriteBatch};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Concurrency levels the sustained-write-throughput bench reports.
const WRITE_CONCURRENCY_LEVELS: &[usize] = &[1, 16, 64, 256, 1024];

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

    /// Fill `buf` with pseudo-random bytes -- incompressible, unlike a constant-byte value
    /// (zstd crushes `vec![b'v'; n]` to almost nothing, which understates both throughput cost
    /// and write amplification).
    fn fill(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i < buf.len() {
            let bytes = self.next_u64().to_le_bytes();
            let n = (buf.len() - i).min(8);
            buf[i..i + n].copy_from_slice(&bytes[..n]);
            i += n;
        }
    }
}

/// Load `records` `key_at(i) -> <incompressible random bytes>` pairs into `db` via
/// `write_batch`, `batch_size` ops at a time. Each task/call uses its own `rng` so concurrent
/// callers don't contend on it.
async fn load_random(db: &Db, records: usize, value_size: usize, batch_size: usize, seed: u64) {
    let mut rng = Xorshift64::new(seed);
    let mut i = 0;
    while i < records {
        let end = (i + batch_size).min(records);
        let mut batch = WriteBatch::new();
        for j in i..end {
            let mut value = vec![0u8; value_size];
            rng.fill(&mut value);
            batch = batch.put(key_at(j).into_bytes(), value);
        }
        db.write_batch(batch).await.expect("write_batch");
        i = end;
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

/// Sustained write throughput at one concurrency level -- `tasks` tasks sharing group commit,
/// each writing incompressible random values (see [`Xorshift64::fill`]) so the fsync path isn't
/// getting an unrealistic assist from zstd crushing constant bytes.
fn bench_write_throughput_at(rt: &tokio::runtime::Runtime, tasks: usize) -> String {
    let ops_per_task = env_usize("DRIFTDB_BENCH_WRITE_OPS_PER_TASK", 300);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);

    let dir = TempDir::new().expect("tempdir");
    let db = rt.block_on(Db::open(dir.path())).expect("open driftdb");

    eprintln!("[write-throughput] {tasks} tasks x {ops_per_task} puts...");
    let start = Instant::now();
    rt.block_on(async {
        let mut set = tokio::task::JoinSet::new();
        for t in 0..tasks {
            let db = db.clone();
            set.spawn(async move {
                let mut rng = Xorshift64::new(0xD00D_0000 ^ t as u64);
                let mut value = vec![0u8; value_size];
                for i in 0..ops_per_task {
                    rng.fill(&mut value);
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

/// Poll `db.stats()` until per-level byte totals stop changing for a few consecutive polls (or a
/// generous timeout elapses) -- i.e. wait for the background flush/compaction thread to reach
/// steady state. Deliberately does *not* force a final `compact()`: that would drive every level
/// down to the bottom one, which inflates the write-amp number past what a real steady-state
/// workload (that never runs a manual full compaction) would ever see.
async fn wait_for_compaction_to_settle(db: &Db) {
    let mut stable_polls = 0u32;
    let mut last = db.stats().level_bytes;
    let deadline = Instant::now() + Duration::from_secs(120);
    while stable_polls < 5 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let cur = db.stats().level_bytes;
        if cur == last {
            stable_polls += 1;
        } else {
            stable_polls = 0;
            last = cur;
        }
    }
}

/// Write amplification, RocksDB-style: `(WAL + flush + compaction bytes) / user bytes` (see
/// `Stats::write_amplification`). Uses a small memtable/L1 budget relative to the load size so
/// leveled compaction actually pushes data into L2+ (reported via per-level file counts) instead
/// of everything sitting in L0/L1, loads incompressible random values (constant bytes would let
/// zstd erase most of the cost), and measures steady state after the load settles -- see
/// `wait_for_compaction_to_settle`.
fn bench_write_amplification(rt: &tokio::runtime::Runtime) -> String {
    let load_mb = env_usize("DRIFTDB_BENCH_LOAD_MB", 200);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let records = (load_mb * 1024 * 1024) / approx_record_bytes(value_size);

    let dir = TempDir::new().expect("tempdir");
    let options = Options {
        memtable_size: 1024 * 1024,
        l1_max_bytes: 4 * 1024 * 1024,
        target_file_size: 1024 * 1024,
        ..Options::default()
    };
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("open driftdb");

    eprintln!("[write-amp] loading ~{load_mb}MB ({records} records, incompressible values)...");
    rt.block_on(async {
        load_random(&db, records, value_size, 1000, 0xA5A5_0000).await;
        db.flush().await.expect("flush");
        wait_for_compaction_to_settle(&db).await;
    });

    let stats = db.stats();
    rt.block_on(db.close()).ok();
    format!(
        "| write amplification (~{load_mb}MB loaded, steady state, no forced compact) | {:.2}x (wal {}B, flush+compact {}B, user {}B) -- level files {:?} |",
        stats.write_amplification(),
        stats.wal_bytes_written,
        stats.disk_bytes_written,
        stats.user_bytes_written,
        stats.level_files,
    )
}

/// Raw WAL replay throughput: write `DRIFTDB_BENCH_RECOVERY_MB` of incompressible records
/// straight to one WAL file (bypassing `Db` entirely -- no manifest, no SST flush), then time
/// `driftdb::wal::replay` decoding them back out. Isolates the replay decode path from the
/// SST-flush-on-recovery cost `bench_recovery_time_default` also pays.
fn bench_raw_replay_throughput(_rt: &tokio::runtime::Runtime) -> String {
    use driftdb::memtable::Value;
    use driftdb::wal::{wal_path, WalFile};

    let recovery_mb = env_usize("DRIFTDB_BENCH_RECOVERY_MB", 20);
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let records = (recovery_mb * 1024 * 1024) / approx_record_bytes(value_size);

    let dir = TempDir::new().expect("tempdir");
    let mut rng = Xorshift64::new(0x5EED_5EED);
    let mut wal = WalFile::create(dir.path(), 1).expect("create wal");
    for i in 0..records {
        let mut v = vec![0u8; value_size];
        rng.fill(&mut v);
        wal.append((i + 1) as u64, key_at(i).as_bytes(), &Value::Put(v));
    }
    wal.sync().expect("sync");
    let path = wal_path(dir.path(), 1);
    let bytes_written = std::fs::metadata(&path).expect("metadata").len();

    eprintln!("[raw-replay] replaying {bytes_written} bytes ({records} records)...");
    let start = Instant::now();
    let mut count = 0u64;
    driftdb::wal::replay(&path, |_seq, _key, _val| count += 1).expect("replay");
    let elapsed = start.elapsed();
    let mb_per_s = (bytes_written as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64();
    format!(
        "| raw WAL replay throughput ({records} records, {bytes_written}B) | {mb_per_s:.1} MB/s ({elapsed:.2?}, {count} records replayed) |"
    )
}

/// Recovery time at default `Options` -- the realistic worst case, per the bound documented on
/// `Options::memtable_size`: replay never reads more than `memtable_size * 3` bytes of WAL (the
/// active memtable plus up to `MAX_IMMUTABLE_MEMTABLES` = 2 queued-but-unflushed frozen ones).
/// Writes fast enough that the background flush thread can't fully drain ahead of it, so this
/// approximates (rather than guarantees) actually landing on that bound.
fn bench_recovery_time_default(rt: &tokio::runtime::Runtime) -> String {
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let options = Options::default();
    let worst_case_wal_bytes = options.memtable_size * 3;
    let records = worst_case_wal_bytes / approx_record_bytes(value_size);

    let dir = TempDir::new().expect("tempdir");
    eprintln!(
        "[recovery-default] writing ~{}MB at default Options (memtable_size={}B)...",
        worst_case_wal_bytes / (1024 * 1024),
        options.memtable_size
    );
    {
        let db = rt
            .block_on(Db::open_with(dir.path(), options.clone()))
            .expect("open driftdb");
        rt.block_on(async {
            load_random(&db, records, value_size, 500, 0xF00D_0000).await;
        });
        // Dropped without an explicit close(): each write_batch already waited for its WAL
        // fsync ack, so the data is durable -- this is exactly the crash-recovery path.
    }

    eprintln!("[recovery-default] reopening...");
    let start = Instant::now();
    let db = rt
        .block_on(Db::open_with(dir.path(), options))
        .expect("reopen driftdb");
    let elapsed = start.elapsed();
    rt.block_on(db.close()).ok();

    format!(
        "| recovery time at default Options (~{}MB worst-case WAL, {records} records) | {elapsed:.2?} |",
        worst_case_wal_bytes / (1024 * 1024)
    )
}

fn main() {
    let rt = multi_thread_rt();

    let mut rows: Vec<String> = WRITE_CONCURRENCY_LEVELS
        .iter()
        .map(|&tasks| bench_write_throughput_at(&rt, tasks))
        .collect();
    rows.push(bench_ycsb_c_latency(&rt));
    rows.push(bench_compaction_storm(&rt));
    rows.push(bench_write_amplification(&rt));
    rows.push(bench_raw_replay_throughput(&rt));
    rows.push(bench_recovery_time_default(&rt));

    println!("\n# driftdb bench report\n");
    println!("| metric | value |");
    println!("|---|---|");
    for row in rows {
        println!("{row}");
    }
}

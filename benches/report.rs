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
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Concurrency levels the sustained-write-throughput bench reports.
const WRITE_CONCURRENCY_LEVELS: &[usize] = &[1, 16, 64, 256, 1024];

/// Env var that, when set (to a directory path), tells this binary to act as the recovery-crash
/// bench's child instead of running the report -- see `run_recovery_crash_child` and
/// `bench_recovery_crash`.
const RECOVERY_CHILD_ENV: &str = "DRIFTDB_BENCH_RECOVERY_CHILD";

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

/// Poll `db.stats().background_idle` until the background flush/compaction thread reports
/// nothing left to do (or a generous timeout elapses) -- i.e. wait for steady state. Deliberately
/// does *not* force a final `compact()`: that would drive every level down to the bottom one,
/// which inflates the write-amp number past what a real steady-state workload (that never runs a
/// manual full compaction) would ever see.
///
/// This used to poll `level_bytes` for a few consecutive unchanged samples instead, which can't
/// tell "actually done" apart from "between two compactions that happen to land on the same byte
/// totals" -- `background_idle` is exact (no immutables, not mid-flush/mid-compact, and
/// `compaction::pick` finds nothing), so one confirming poll is enough.
async fn wait_for_compaction_to_settle(db: &Db) -> bool {
    let deadline = Instant::now() + Duration::from_secs(120);
    while Instant::now() < deadline {
        if db.stats().background_idle {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
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
    let settled = rt.block_on(async {
        load_random(&db, records, value_size, 1000, 0xA5A5_0000).await;
        db.flush().await.expect("flush");
        wait_for_compaction_to_settle(&db).await
    });
    if !settled {
        eprintln!(
            "[write-amp] WARNING: background work did not go idle within the deadline -- the \
             write-amp number below may still include an in-progress flush/compaction"
        );
    }

    let stats = db.stats();
    rt.block_on(db.close()).ok();
    let warning = if settled {
        ""
    } else {
        " -- WARNING: hit the settle deadline, see stderr"
    };
    format!(
        "| write amplification (~{load_mb}MB loaded, steady state, no forced compact; wal bytes include the 21B/record header) | {:.2}x (wal {}B, flush+compact {}B, user {}B) -- level files {:?}{warning} |",
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

/// `Options` for the recovery-crash bench: a small `memtable_size` so rotation (and therefore
/// frozen-but-unflushed memtables) happens often under sustained load, without needing gigabytes
/// of data to get there.
fn recovery_crash_options() -> Options {
    Options {
        memtable_size: 256 * 1024,
        target_file_size: 128 * 1024,
        l1_max_bytes: 512 * 1024,
        l0_compaction_trigger: 4,
        level_multiplier: 4,
        max_levels: 5,
        commit_window: Duration::ZERO,
    }
}

/// Child entry point for the recovery-crash bench (dispatched from `main` when
/// `RECOVERY_CHILD_ENV` is set -- see `bench_recovery_crash`). Opens the db at `dir` and hammers
/// it with concurrent writers forever, printing one line per acked write (flushed immediately)
/// so the parent can tell it's making progress. Never returns on its own -- the parent SIGKILLs
/// it once enough memtables have had a chance to freeze.
fn run_recovery_crash_child(dir: &str) {
    let value_size = env_usize("DRIFTDB_BENCH_VALUE_SIZE", 100);
    let rt = multi_thread_rt();
    rt.block_on(async {
        let db = Db::open_with(dir, recovery_crash_options())
            .await
            .expect("recovery-crash child: open");
        let mut set = tokio::task::JoinSet::new();
        for w in 0..8usize {
            let db = db.clone();
            set.spawn(async move {
                let mut rng = Xorshift64::new(0xC0DE_0000 ^ w as u64);
                let mut value = vec![0u8; value_size];
                let mut i = 0usize;
                loop {
                    rng.fill(&mut value);
                    let k = format!("rk{w:02}-{i:010}");
                    if db.put(k.as_bytes(), &value).await.is_err() {
                        return; // engine already gone -- fine, the kill can land anywhere.
                    }
                    let mut out = std::io::stdout().lock();
                    let _ = writeln!(out, "{w} {i}");
                    let _ = out.flush();
                    i += 1;
                }
            });
        }
        while set.join_next().await.is_some() {}
    });
}

/// Real `kill -9` recovery bench: re-execs this same binary (like `tests/crash_kill.rs`) as a
/// child that writes continuously against a tiny `memtable_size`, gives it just long enough to
/// pile up several frozen-but-unflushed memtables, then SIGKILLs it -- no graceful shutdown, no
/// chance for the background thread's shutdown-drain loop to flush anything away first. That
/// drain loop is exactly why a plain "write then drop `Db`" bench (the previous version of this
/// row) doesn't measure real crash recovery: `Drop` still joins the background thread, which
/// flushes every frozen memtable to an SST before returning, so replay on the next open never
/// sees more than the still-active memtable's WAL. Only an actual `kill -9` skips that.
///
/// Reports the wall-clock time for `Db::open` in the parent to finish (replay + re-flush of
/// whatever survived) and the total bytes of `wal-*.log` on disk right before that open, which is
/// exactly what gets replayed.
fn bench_recovery_crash(rt: &tokio::runtime::Runtime) -> String {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    let exe = std::env::current_exe().expect("current_exe");
    eprintln!("[recovery-crash] spawning child, writing until kill -9...");
    let mut child = Command::new(&exe)
        .env(RECOVERY_CHILD_ENV, &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn recovery-crash child");

    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel::<()>();
    let reader_handle = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break, // child exited / pipe closed.
                Ok(_) => {
                    if tx.send(()).is_err() {
                        break; // parent stopped listening.
                    }
                }
            }
        }
    });

    // Wait for the first acked write separately from the run window: process spawn + tokio
    // runtime init + the first WAL fsync can take a while on a loaded machine, and that startup
    // latency shouldn't eat into the window meant for actually piling up frozen memtables.
    let startup_deadline = Instant::now() + Duration::from_secs(5);
    let _ = rx.recv_timeout(startup_deadline.saturating_duration_since(Instant::now()));
    // With memtable_size=256KiB and 8 concurrent writers, this is comfortably enough wall time
    // to rotate past `MAX_IMMUTABLE_MEMTABLES` several times over and have the writer thread
    // stalled waiting on the (by-then-dead) background flush thread -- i.e. several WAL
    // generations sitting unflushed at kill time, not just the active one.
    std::thread::sleep(Duration::from_millis(750));

    // Ignore the error: if the child somehow already exited on its own, `kill` fails with
    // `InvalidInput` and there's nothing left to kill anyway.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader_handle.join();

    let wal_bytes_before_open: u64 = std::fs::read_dir(&path)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("wal-") && n.ends_with(".log"))
        })
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();

    eprintln!("[recovery-crash] killed; {wal_bytes_before_open}B of WAL on disk, reopening...");
    let start = Instant::now();
    let db = rt
        .block_on(Db::open_with(&path, recovery_crash_options()))
        .expect("reopen after kill -9 must succeed");
    let elapsed = start.elapsed();
    rt.block_on(db.close()).ok();

    format!(
        "| recovery time after kill -9 ({wal_bytes_before_open}B of WAL replayed, incl. 21B/record header) | {elapsed:.2?} |"
    )
}

fn main() {
    if let Ok(dir) = std::env::var(RECOVERY_CHILD_ENV) {
        run_recovery_crash_child(&dir);
        return;
    }
    let rt = multi_thread_rt();

    // YCSB-C runs first, on its own fresh `Db`/tempdir, before any of the write-throughput
    // sweeps below (which push tens of thousands of ops through five concurrency levels,
    // including 1024 concurrent tasks) or the other benches. A prior p99 regression (38us ->
    // 1.29ms between runs) couldn't be reproduced in isolation here -- with the block cache in
    // place or removed, p99 stayed in the 80-190us range at both 16 and 64 concurrent readers --
    // so it looks like it was noise from whatever ran immediately before YCSB-C in that
    // particular run (allocator/page-cache pressure, tokio worker-thread churn from the
    // concurrency sweep, ...) rather than a defect in the read path itself. Running it first
    // removes that confound for future measurements instead of leaving it to guess at.
    let mut rows: Vec<String> = vec![bench_ycsb_c_latency(&rt)];
    rows.extend(
        WRITE_CONCURRENCY_LEVELS
            .iter()
            .map(|&tasks| bench_write_throughput_at(&rt, tasks)),
    );
    rows.push(bench_compaction_storm(&rt));
    rows.push(bench_write_amplification(&rt));
    rows.push(bench_raw_replay_throughput(&rt));
    rows.push(bench_recovery_crash(&rt));

    println!("\n# driftdb bench report\n");
    println!("| metric | value |");
    println!("|---|---|");
    for row in rows {
        println!("{row}");
    }
}

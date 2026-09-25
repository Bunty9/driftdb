//! Real `kill -9` crash test.
//!
//! This binary re-execs itself (`std::env::current_exe()`) as a child process that opens the db
//! and hammers it with many concurrent writers, printing each acked write's `(worker, index)` to
//! stdout right after the ack (flushed immediately). The parent reads those lines for a fixed
//! window, then sends `SIGKILL` to the child -- no graceful shutdown, no chance for the writer
//! thread to finish anything in flight -- reopens the db, and asserts every acked write survived
//! with the right value (and that open itself succeeds: no torn state).
//!
//! `crash_child_worker` is a no-op under a plain `cargo test` run (it checks the env var the
//! parent uses to signal "you are the child" and returns immediately if it's unset), so this
//! file behaves like any other test file unless `kill_minus_9_preserves_every_acked_write`
//! deliberately spawns it as a child.

use driftdb::{Db, Options};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

const CHILD_ENV: &str = "DRIFTDB_CRASH_CHILD";
const WORKERS: usize = 8;

fn small_options() -> Options {
    Options {
        memtable_size: 16 * 1024,
        target_file_size: 8 * 1024,
        l1_max_bytes: 32 * 1024,
        l0_compaction_trigger: 2,
        level_multiplier: 4,
        max_levels: 5,
        commit_window: Duration::ZERO,
    }
}

fn key(worker: usize, i: usize) -> String {
    format!("w{worker}-k{i:07}")
}
fn val(worker: usize, i: usize) -> String {
    format!("v-{worker}-{i}")
}

/// Child entry point. No-op unless `DRIFTDB_CRASH_CHILD` is set (to the db directory to open),
/// in which case it never returns on its own -- it writes until the parent kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn crash_child_worker() {
    let Ok(dir) = std::env::var(CHILD_ENV) else {
        return; // plain `cargo test` run of this file -- nothing to do.
    };
    let db = Db::open_with(&dir, small_options())
        .await
        .expect("child open");

    let mut tasks = Vec::new();
    for worker in 0..WORKERS {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0usize.. {
                let k = key(worker, i);
                let v = val(worker, i);
                if db.put(k.as_bytes(), v.as_bytes()).await.is_err() {
                    return; // engine already gone -- fine, the kill can land anywhere.
                }
                let mut out = std::io::stdout().lock();
                let _ = writeln!(out, "{worker} {i}");
                let _ = out.flush();
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
}

/// Parses one `"{worker} {index}"` line; ignores anything else (libtest's own banner/result
/// lines end up interleaved on the same stdout since the child runs under `--nocapture`).
fn parse_acked_line(line: &str) -> Option<(usize, usize)> {
    let mut parts = line.trim().split(' ');
    let w = parts.next()?.parse().ok()?;
    let i = parts.next()?.parse().ok()?;
    Some((w, i))
}

fn run_one_iteration(iteration: usize) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(&exe)
        .arg("crash_child_worker")
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_ENV, &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn child");

    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel::<(usize, usize)>();
    let reader_handle = std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) | Err(_) => break, // child exited / pipe closed.
                Ok(_) => {
                    if let Some(item) = parse_acked_line(&line) {
                        if tx.send(item).is_err() {
                            break; // parent stopped listening.
                        }
                    }
                }
            }
        }
    });

    // Wait for the first acked write separately from the collection window below: process spawn
    // + tokio runtime init + the first WAL fsync can take a while under a loaded machine (e.g.
    // `cargo test` running every other test binary concurrently), and we don't want that startup
    // latency to eat the whole window we actually want to spend killing the child mid-write.
    let mut acked = Vec::new();
    let startup_deadline = std::time::Instant::now() + Duration::from_secs(5);
    if let Ok(item) =
        rx.recv_timeout(startup_deadline.saturating_duration_since(std::time::Instant::now()))
    {
        acked.push(item);
    }

    // Let the child run for a bit, with small/aggressive flush+compaction settings so flushes
    // and compactions are actively happening when we kill it -- then SIGKILL, no chance for any
    // graceful shutdown.
    if !acked.is_empty() {
        let deadline = std::time::Instant::now() + Duration::from_millis(1_000);
        loop {
            let now = std::time::Instant::now();
            if now >= deadline || acked.len() >= 3_000 {
                break;
            }
            match rx.recv_timeout(deadline - now) {
                Ok(item) => acked.push(item),
                Err(_) => break, // timed out or child's stdout closed early.
            }
        }
    }

    // Ignore the error: if the child somehow already exited on its own, `kill` fails with
    // `InvalidInput` and there's nothing left to kill anyway.
    let _ = child.kill();
    let _ = child.wait();
    let _ = reader_handle.join();

    eprintln!(
        "iteration {iteration}: acked {} writes before kill",
        acked.len()
    );
    assert!(
        !acked.is_empty(),
        "iteration {iteration}: child acked nothing before being killed"
    );

    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let db = Db::open_with(&path, small_options())
            .await
            .expect("reopen after kill -9 must succeed -- no torn state");
        for (worker, i) in &acked {
            let got = db.get(key(*worker, *i).as_bytes()).await.expect("get");
            assert_eq!(
                got.as_deref(),
                Some(val(*worker, *i).as_bytes()),
                "iteration {iteration}: acked key w{worker}-k{i:07} missing or wrong \
                 after kill -9"
            );
        }
        db.close().await.expect("close");
    });
}

#[test]
fn kill_minus_9_preserves_every_acked_write() {
    for iteration in 0..3 {
        run_one_iteration(iteration);
    }
}

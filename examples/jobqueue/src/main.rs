use anyhow::{bail, Context};
use clap::{Parser, Subcommand};
use driftdb::Options;
use jobqueue::model::{JobStatus, NewJob};
use jobqueue::store::JobStore;
use jobqueue::{api, now_ms};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Parser)]
#[command(about = "Durable job queue on driftdb-lsm — a reference integration")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Scripted end-to-end run: producers, workers, failures, snapshot report, backup,
    /// purge + compaction, close and reopen.
    Demo {
        /// Must be empty or not exist; defaults to a temp dir that is removed afterwards.
        #[arg(long)]
        dir: Option<PathBuf>,
        #[arg(long, default_value_t = 2_000)]
        jobs: u64,
    },
    /// SIGKILL a writer mid-flight, reopen, and verify every acknowledged job survived.
    CrashDemo {
        /// Must be empty or not exist; defaults to a temp dir that is removed afterwards.
        #[arg(long)]
        dir: Option<PathBuf>,
    },
    #[command(hide = true)]
    CrashChild {
        #[arg(long)]
        dir: PathBuf,
    },
    /// Run the HTTP API.
    Serve {
        #[arg(long, default_value = "./jobqueue-data")]
        dir: PathBuf,
        #[arg(long, default_value = "127.0.0.1:3000")]
        addr: SocketAddr,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Cli::parse().cmd {
        Cmd::Demo { dir, jobs } => demo(prepare_scratch_dir(dir, "demo")?, jobs).await,
        Cmd::CrashDemo { dir } => crash_demo(prepare_scratch_dir(dir, "crash")?).await,
        Cmd::CrashChild { dir } => crash_child(dir).await,
        Cmd::Serve { dir, addr } => serve(dir, addr).await,
    }
}

fn temp_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("jobqueue-{tag}-{}", std::process::id()))
}

/// Resolve the demo directory. `owned` means we made up the path, so we may delete it afterwards.
/// A user-supplied directory must be empty or new and is never deleted.
fn prepare_scratch_dir(dir: Option<PathBuf>, tag: &str) -> anyhow::Result<(PathBuf, bool)> {
    match dir {
        None => {
            let d = temp_dir(tag);
            let _ = std::fs::remove_dir_all(&d); // stale leftover of this exact temp path
            Ok((d, true))
        }
        Some(d) => {
            if d.exists() && std::fs::read_dir(&d)?.next().is_some() {
                bail!(
                    "{} is not empty; demo commands need an empty or new directory and never delete existing data",
                    d.display()
                );
            }
            Ok((d, false))
        }
    }
}

/// Small sizes so a short demo actually flushes and compacts.
fn demo_options() -> Options {
    Options {
        memtable_size: 256 * 1024,
        l1_max_bytes: 1024 * 1024,
        target_file_size: 256 * 1024,
        ..Default::default()
    }
}

fn step(n: u32, text: &str) {
    println!("\n[{n}] {text}");
}

async fn demo((dir, owned): (PathBuf, bool), jobs: u64) -> anyhow::Result<()> {
    step(
        1,
        &format!(
            "open {} with small Options so flushes/compactions happen",
            dir.display()
        ),
    );
    let store = JobStore::open(&dir, demo_options()).await?;

    step(
        2,
        "a worker claims one job with a 1s lease and then 'crashes' (never finishes it)",
    );
    store
        .enqueue(
            NewJob {
                kind: "report".into(),
                payload: serde_json::json!({ "orphan": true }),
                max_attempts: None,
            },
            now_ms(),
        )
        .await?;
    let orphan = store
        .claim(1_000, now_ms())
        .await?
        .context("orphan claim")?;
    println!("    job {} is running with nobody working on it", orphan.id);

    step(
        3,
        &format!("4 producers enqueue {jobs} jobs while 8 workers drain the queue"),
    );
    let producers_done = Arc::new(AtomicBool::new(false));
    let mut producers = Vec::new();
    for p in 0..4u64 {
        let store = store.clone();
        producers.push(tokio::spawn(async move {
            let mut n = p;
            while n < jobs {
                // Every 10th job always fails: it retries max_attempts (3) times, then goes dead.
                let new = NewJob {
                    kind: "email".into(),
                    payload: serde_json::json!({ "n": n, "fail": n % 10 == 0 }),
                    max_attempts: Some(3),
                };
                store.enqueue(new, now_ms()).await?;
                n += 4;
            }
            anyhow::Ok(())
        }));
    }
    let mut workers = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let producers_done = producers_done.clone();
        workers.push(tokio::spawn(async move {
            let mut processed = 0u64;
            loop {
                match store.claim(30_000, now_ms()).await? {
                    Some(job) if job.payload["fail"] == true => {
                        store
                            .fail(
                                job.id,
                                job.claim_token,
                                "simulated failure".into(),
                                now_ms(),
                            )
                            .await?;
                    }
                    Some(job) => {
                        store.complete(job.id, job.claim_token, now_ms()).await?;
                        processed += 1;
                    }
                    None if producers_done.load(Ordering::Acquire) => break,
                    None => tokio::time::sleep(Duration::from_millis(2)).await,
                }
            }
            anyhow::Ok(processed)
        }));
    }
    for p in producers {
        p.await??;
    }
    producers_done.store(true, Ordering::Release);

    step(
        4,
        "snapshot report while the workers are still writing (consistent point-in-time view)",
    );
    let r = store.report().await?;
    println!("    at seq {}: {:?}", r.snapshot_seq, r.counts);

    let mut completed = 0;
    for w in workers {
        completed += w.await??;
    }
    println!("    workers completed {completed} jobs");

    step(
        5,
        "the crashed worker's lease expires; requeue_expired puts its job back",
    );
    let requeued = store.requeue_expired(now_ms() + 2_000).await?;
    let job = store
        .claim(30_000, now_ms())
        .await?
        .context("requeued job")?;
    store.complete(job.id, job.claim_token, now_ms()).await?;
    // The crashed worker wakes up and tries to finish the job it lost: fenced off.
    let stale = store
        .complete(orphan.id, orphan.claim_token, now_ms())
        .await;
    if !matches!(stale, Err(jobqueue::store::StoreError::LeaseLost { .. })) {
        bail!("stale worker was not fenced off: {stale:?}");
    }
    println!("    the crashed worker's late complete() was rejected: LeaseLost (fencing token)");
    println!(
        "    requeued {requeued} job(s); job {} finished by another worker",
        job.id
    );

    let report = store.report().await?;
    println!("    final: {:?}", report.counts);
    let expected_dead = jobs.div_ceil(10);
    if report.counts["dead"] != expected_dead || report.counts["done"] != jobs + 1 - expected_dead {
        bail!("unexpected final counts {:?}", report.counts);
    }

    step(
        6,
        "online backup: export every job from a snapshot to JSONL",
    );
    let backup = dir.with_extension("jsonl");
    let lines = store.export(&backup).await?;
    println!("    wrote {lines} jobs to {}", backup.display());

    step(
        7,
        "purge done jobs, then flush + full compaction to drop their tombstones",
    );
    let purged = store.purge(JobStatus::Done, now_ms() + 1).await?;
    let (before, after) = store.maintenance().await?;
    println!("    purged {purged}");
    println!(
        "    before: files per level {:?}, write amp {:.2}x",
        before.level_files, before.write_amplification
    );
    println!(
        "    after:  files per level {:?}, write amp {:.2}x",
        after.level_files, after.write_amplification
    );

    step(8, "close, reopen, and check nothing changed (recovery)");
    let counts = store.report().await?.counts;
    store.close().await?;
    let reopened = JobStore::open(&dir, demo_options()).await?;
    let again = reopened.report().await?.counts;
    if counts != again {
        bail!("counts changed across reopen: {counts:?} vs {again:?}");
    }
    println!("    identical after reopen: {again:?}");
    reopened.close().await?;
    if owned {
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_file(&backup).ok();
    } else {
        println!(
            "\ndata left in {} and backup in {}",
            dir.display(),
            backup.display()
        );
    }
    println!("\ndemo ok");
    Ok(())
}

async fn crash_demo((dir, owned): (PathBuf, bool)) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let mut child = Command::new(exe)
        .args(["crash-child", "--dir"])
        .arg(&dir)
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().context("child stdout")?;
    // Read acked ids on a thread; the parent kills the child after ~1s.
    let reader = std::thread::spawn(move || {
        let mut acked = Vec::new();
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if let Some(id) = line
                .strip_prefix("acked ")
                .and_then(|s| s.parse::<u64>().ok())
            {
                acked.push(id);
            }
        }
        acked
    });
    let started = Instant::now();
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    child.kill()?; // SIGKILL: no destructors, no flush, no close()
    child.wait()?;
    let acked = reader.join().expect("reader thread");
    println!(
        "killed the writer after {:?}; it had acknowledged {} jobs",
        started.elapsed(),
        acked.len()
    );
    if acked.is_empty() {
        bail!("child acknowledged nothing before the kill");
    }

    let store = JobStore::open(&dir, Options::default()).await?;
    let mut missing = Vec::new();
    for id in &acked {
        if store.get(*id).await?.is_none() {
            missing.push(*id);
        }
    }
    let report = store.report().await?;
    store.close().await?;
    if owned {
        std::fs::remove_dir_all(&dir).ok();
    } else {
        println!("data left in {}", dir.display());
    }
    if !missing.is_empty() {
        bail!(
            "{} acknowledged jobs were lost: {:?}",
            missing.len(),
            &missing[..missing.len().min(10)]
        );
    }
    println!("all {} acknowledged jobs survived SIGKILL (store holds {} pending — in-flight unacked writes may also land)", acked.len(), report.counts["pending"]);
    println!("crash-demo ok");
    Ok(())
}

async fn crash_child(dir: PathBuf) -> anyhow::Result<()> {
    let store = JobStore::open(
        &dir,
        Options {
            memtable_size: 64 * 1024,
            ..Default::default()
        },
    )
    .await?;
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for n in 0.. {
                let job = store
                    .enqueue(
                        NewJob {
                            kind: "crash".into(),
                            payload: serde_json::json!({ "t": t, "n": n }),
                            max_attempts: None,
                        },
                        now_ms(),
                    )
                    .await?;
                // Printed only after enqueue returned, i.e. after fdatasync.
                let mut out = std::io::stdout().lock();
                writeln!(out, "acked {}", job.id)?;
                out.flush()?;
            }
            anyhow::Ok(())
        }));
    }
    for t in tasks {
        t.await??;
    }
    Ok(())
}

async fn serve(dir: PathBuf, addr: SocketAddr) -> anyhow::Result<()> {
    let store = JobStore::open(&dir, Options::default()).await?;
    // Jobs left running by a previous crash (or a dead worker) come back after their lease.
    let sweeper = {
        let store = store.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                match store.requeue_expired(now_ms()).await {
                    Ok(0) => {}
                    Ok(n) => tracing::info!(requeued = n, "expired leases requeued"),
                    Err(e) => tracing::warn!(error = %e, "requeue sweep failed"),
                }
            }
        })
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, dir = %dir.display(), "jobqueue listening");
    axum::serve(listener, api::router(store.clone()))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    sweeper.abort();
    store.close().await?; // drains the WAL writer and flushes memtables
    Ok(())
}

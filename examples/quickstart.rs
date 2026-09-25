//! Quickstart: open a fresh `Db` in a temp dir, put 1k keys, force a flush to L0,
//! read them all back (now served from an SSTable, not the memtable), and print
//! rough stats including the per-level file/byte counts.

use driftdb::{Db, Result};
use std::time::Instant;

const N: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    // Use a temp directory so re-running the example is hermetic.
    let dir = std::env::temp_dir().join("driftdb-demo");
    // Best-effort cleanup of any prior run so the WAL replay path stays simple.
    let _ = std::fs::remove_dir_all(&dir);

    println!("opening driftdb at {}", dir.display());
    let db = Db::open(&dir).await?;

    println!("putting {N} keys...");
    let put_start = Instant::now();
    for i in 0..N {
        let key = format!("key-{:06}", i);
        let val = format!("val-{}", i);
        db.put(key.as_bytes(), val.as_bytes()).await?;
    }
    let put_elapsed = put_start.elapsed();

    println!("flushing to L0...");
    db.flush().await?;

    println!("reading {N} keys back (from SSTables)...");
    let get_start = Instant::now();
    for i in 0..N {
        let key = format!("key-{:06}", i);
        let expect = format!("val-{}", i);
        let got = db.get(key.as_bytes()).await?;
        assert_eq!(
            got.as_deref(),
            Some(expect.as_bytes()),
            "round-trip mismatch on {key}: got {got:?}",
        );
    }
    let get_elapsed = get_start.elapsed();

    let stats = db.stats();
    println!("stats:");
    println!("  puts:           {N}");
    println!("  put wall time:  {put_elapsed:?}");
    println!("  gets:           {N}");
    println!("  get wall time:  {get_elapsed:?}");
    println!("  level files:    {:?}", stats.level_files);
    println!("  level bytes:    {:?}", stats.level_bytes);
    println!("  user bytes:     {}", stats.user_bytes_written);
    println!("  disk bytes:     {}", stats.disk_bytes_written);
    println!("ok");
    Ok(())
}

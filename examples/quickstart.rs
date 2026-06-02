//! Quickstart: open a fresh `Db` in a temp dir, put 1k keys, read them all back,
//! assert equal, and print rough memtable stats.
//!
//! Phase 1: stays on the memtable-only path — `Db::get` only consults the active
//! memtable in this build, so no SST flush is required to round-trip values.
//! Once the flush thread + SST reader land in Phase 2, this example will still
//! work; it just exercises a smaller fraction of the engine.

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

    println!("reading {N} keys back...");
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

    println!("stats:");
    println!("  puts:           {N}");
    println!("  put wall time:  {put_elapsed:?}");
    println!("  gets:           {N}");
    println!("  get wall time:  {get_elapsed:?}");
    println!("  path:           memtable-only (SST flush lands in Phase 2)");
    println!("ok");
    Ok(())
}

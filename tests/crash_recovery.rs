//! Crash-recovery integration test (Phase-2 target).
//!
//! Sketch: open a `Db` rooted in a tempdir, write N items, drop the handle
//! (simulating a clean shutdown without explicit flush), then reopen and
//! assert that every item written before the drop is still readable. The
//! stronger test — `kill -9` style crash mid-write — needs a child process
//! harness and is tracked separately.
//!
//! Marked `#[ignore]` because Phase 1 ships with manifest replay + WAL replay
//! stubbed: a reopen today returns an empty `Db` because the WAL writer task
//! never gets a `Sync` on drop and the open path doesn't replay records. The
//! test stays in tree so `cargo test --test crash_recovery -- --ignored`
//! becomes the canonical recovery gate once Phase 2 lands.

use driftdb::Db;
use tempfile::TempDir;

const N: usize = 1_000;

#[tokio::test]
#[ignore = "Phase 2: requires WAL replay on Db::open"]
async fn reopen_sees_previously_written_keys() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    // First open: write N keys, drop the handle.
    {
        let db = Db::open(&path).await.expect("open #1");
        for i in 0..N {
            let key = format!("k{:08}", i);
            let val = format!("v{}", i);
            db.put(key.as_bytes(), val.as_bytes()).await.expect("put");
        }
        // Implicit drop here — WAL records are durable thanks to group-commit,
        // but no clean shutdown signal is sent.
    }

    // Second open: every key should still be present.
    let db = Db::open(&path).await.expect("open #2");
    for i in 0..N {
        let key = format!("k{:08}", i);
        let expect = format!("v{}", i);
        let got = db
            .get(key.as_bytes())
            .await
            .expect("get")
            .expect("key present after reopen");
        assert_eq!(got, expect.as_bytes(), "value mismatch on {key}");
    }
}

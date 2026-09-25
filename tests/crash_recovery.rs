//! Crash-recovery integration tests.
//!
//! Covers the "Recovery" section of `docs/plans/2026-09-25-driftdb-phase-2.md`: manifest
//! replay, orphan `.sst` cleanup, WAL replay (including a torn tail), and re-flushing whatever
//! was left in the WAL at close time.

use driftdb::{Db, Options, WriteBatch};
use std::io::Write;
use std::time::Duration;
use tempfile::TempDir;

const N: usize = 1_000;

fn key(i: usize) -> String {
    format!("k{i:08}")
}
fn val(i: usize) -> String {
    format!("v{i}")
}

/// Write every `(key, value)` pair via chunked `write_batch` calls instead of one `put` per
/// pair -- same end state (every op still lands, in order, durably), far fewer fsyncs than
/// awaiting each op one at a time.
async fn put_all(db: &Db, items: impl IntoIterator<Item = (String, String)>) {
    const CHUNK: usize = 200;
    let mut batch = WriteBatch::new();
    for (k, v) in items {
        batch = batch.put(k.into_bytes(), v.into_bytes());
        if batch.len() >= CHUNK {
            db.write_batch(std::mem::take(&mut batch))
                .await
                .expect("write_batch");
        }
    }
    if !batch.is_empty() {
        db.write_batch(batch).await.expect("write_batch");
    }
}

#[tokio::test]
async fn reopen_sees_previously_written_keys() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        put_all(&db, (0..N).map(|i| (key(i), val(i)))).await;
        // Implicit drop here -- WAL records are durable thanks to group-commit fsync, but no
        // explicit close()/flush() is called.
    }

    let db = Db::open(&path).await.expect("open #2");
    for i in 0..N {
        let got = db
            .get(key(i).as_bytes())
            .await
            .expect("get")
            .expect("key present after reopen");
        assert_eq!(got, val(i).as_bytes(), "value mismatch on {}", key(i));
    }
}

#[tokio::test]
async fn reopen_after_explicit_flush_serves_from_sstables_only() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        put_all(&db, (0..N).map(|i| (key(i), val(i)))).await;
        db.flush().await.expect("flush");
        let stats = db.stats();
        assert!(
            stats.level_bytes.first().copied().unwrap_or(0) > 0,
            "expected data to have landed in L0"
        );
        db.close().await.expect("close");
    }

    let db = Db::open(&path).await.expect("open #2");
    for i in 0..N {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        assert_eq!(got.as_deref(), Some(val(i).as_bytes()));
    }
}

#[tokio::test]
async fn reopen_with_data_split_across_sstables_and_wal() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        put_all(&db, (0..N / 2).map(|i| (key(i), val(i)))).await;
        db.flush().await.expect("flush first half to L0");
        put_all(&db, (N / 2..N).map(|i| (key(i), val(i)))).await;
        // Second half stays in the WAL only -- no flush, no close.
    }

    let db = Db::open(&path).await.expect("open #2");
    for i in 0..N {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        assert_eq!(got.as_deref(), Some(val(i).as_bytes()), "key {}", key(i));
    }
}

#[tokio::test]
async fn deletes_survive_reopen() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        put_all(&db, (0..N).map(|i| (key(i), val(i)))).await;
        db.flush().await.expect("flush");

        let mut batch = WriteBatch::new();
        for i in (0..N).filter(|i| i % 3 == 0) {
            batch = batch.delete(key(i).into_bytes());
        }
        db.write_batch(batch).await.expect("write_batch deletes");
    }

    let db = Db::open(&path).await.expect("open #2");
    for i in 0..N {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        if i % 3 == 0 {
            assert_eq!(got, None, "key {} should be deleted", key(i));
        } else {
            assert_eq!(got.as_deref(), Some(val(i).as_bytes()));
        }
    }
}

#[tokio::test]
async fn many_reopen_cycles_keep_last_seq_monotonic() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    let mut written = 0usize;
    for cycle in 0..5 {
        let db = Db::open(&path).await.expect("open");
        for i in 0..50 {
            let k = format!("cycle-{cycle}-{i}");
            db.put(k.as_bytes(), b"v").await.expect("put");
        }
        written += 50;

        // A snapshot taken right after reopening must see everything written in every prior
        // cycle plus this one.
        let snap = db.snapshot();
        for c in 0..=cycle {
            for i in 0..50 {
                let k = format!("cycle-{c}-{i}");
                assert_eq!(
                    snap.get(k.as_bytes()).expect("snapshot get"),
                    Some(b"v".to_vec()),
                    "missing {k} in cycle {cycle}"
                );
            }
        }
        assert!(snap.seq() as usize >= written);
    }
}

#[tokio::test]
async fn torn_wal_tail_is_truncated_and_acked_data_survives() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        put_all(&db, (0..N).map(|i| (key(i), val(i)))).await;
        db.close().await.expect("close");
    }

    // Append garbage bytes to the newest wal-*.log file, simulating a torn write.
    let mut newest: Option<(u64, std::path::PathBuf)> = None;
    for entry in std::fs::read_dir(&path).unwrap() {
        let p = entry.unwrap().path();
        if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
            if let Some(digits) = name
                .strip_prefix("wal-")
                .and_then(|s| s.strip_suffix(".log"))
            {
                if let Ok(n) = digits.parse::<u64>() {
                    if newest.as_ref().is_none_or(|(cur, _)| n > *cur) {
                        newest = Some((n, p));
                    }
                }
            }
        }
    }
    let (_, newest_path) = newest.expect("at least one wal file after close");
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&newest_path)
            .unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03])
            .unwrap();
    }

    let db = Db::open(&path).await.expect("open #2 after torn tail");
    for i in 0..N {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        assert_eq!(got.as_deref(), Some(val(i).as_bytes()), "key {}", key(i));
    }
}

#[tokio::test]
async fn orphan_sst_file_is_deleted_on_open() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        db.put(b"a", b"1").await.expect("put");
        db.flush().await.expect("flush");
        db.close().await.expect("close");
    }

    // Drop in a bogus .sst file with a number the manifest doesn't know about.
    let orphan = path.join("999999.sst");
    std::fs::write(&orphan, b"not a real sstable").unwrap();
    assert!(orphan.exists());

    let db = Db::open(&path).await.expect("open #2");
    assert_eq!(db.get(b"a").await.unwrap().as_deref(), Some(&b"1"[..]));
    assert!(
        !orphan.exists(),
        "orphan sst should have been deleted on open"
    );
}

#[tokio::test]
async fn small_options_reopen_roundtrip() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();
    let options = Options {
        memtable_size: 8 * 1024,
        target_file_size: 4 * 1024,
        l1_max_bytes: 16 * 1024,
        ..Options::default()
    };

    {
        let db = Db::open_with(&path, options.clone())
            .await
            .expect("open #1");
        put_all(&db, (0..2_000).map(|i| (key(i), val(i).repeat(4)))).await;
        db.compact().await.expect("compact");
    }

    let db = Db::open_with(&path, options).await.expect("open #2");
    for i in 0..2_000 {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        assert_eq!(got.as_deref(), Some(val(i).repeat(4).as_bytes()));
    }
}

/// Regression test for the flush-failure deadlock: once `fatal` is set, the frozen memtable that
/// failed to flush used to stay in `immutables` forever, and the background thread's shutdown
/// condition (`shutdown && immutables.is_empty() && !force_compact`) could then never become
/// true -- `close()` (and a plain `Drop`) would hang forever joining that thread.
#[tokio::test]
async fn close_after_flush_failure_does_not_hang() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    // Pre-place a directory at every SST path this scenario could plausibly need, so the first
    // real flush's `File::create(&sst_path)` deterministically fails with "is a directory" --
    // no filesystem-permission timing games, no race against the background thread picking up
    // the newly-frozen memtable before we can sabotage it. This still stands in for any flush
    // failure (full disk, permissions, ...) without depending on exactly how many file numbers
    // `open()`/`flush()` burn internally.
    for n in 1..=10u64 {
        std::fs::create_dir(path.join(format!("{n:06}.sst"))).expect("landmine dir");
    }

    let db = Db::open(&path).await.expect("open");
    db.put(b"a", b"1").await.expect("put");

    let flush_result = db.flush().await;
    assert!(
        flush_result.is_err(),
        "flush should fail once its SST path is blocked by a directory"
    );

    tokio::time::timeout(Duration::from_secs(10), db.close())
        .await
        .expect("close() must not hang after a flush failure")
        .expect("close");
}

/// Regression test: repeatedly opening and closing a db that's never written to used to leave
/// one empty `wal-*.log` behind per cycle forever (nothing ever deleted a WAL that replayed to
/// zero records). Now the empty ones get cleaned up on the next open.
#[tokio::test]
async fn empty_reopen_cycles_do_not_pile_up_wal_files() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    for _ in 0..5 {
        let db = Db::open(&path).await.expect("open");
        db.close().await.expect("close");
    }

    let wal_count = std::fs::read_dir(&path)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("wal-") && n.ends_with(".log"))
        })
        .count();
    assert!(
        wal_count <= 1,
        "expected at most 1 leftover wal file after 5 empty open/close cycles, found {wal_count}"
    );
}

//! Crash-recovery integration tests.
//!
//! Covers the "Recovery" section of `docs/plans/2026-09-25-driftdb-phase-2.md`: manifest
//! replay, orphan `.sst` cleanup, WAL replay (including a torn tail), and re-flushing whatever
//! was left in the WAL at close time.

use driftdb::{Db, Options};
use std::io::Write;
use tempfile::TempDir;

const N: usize = 1_000;

fn key(i: usize) -> String {
    format!("k{i:08}")
}
fn val(i: usize) -> String {
    format!("v{i}")
}

#[tokio::test]
async fn reopen_sees_previously_written_keys() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();

    {
        let db = Db::open(&path).await.expect("open #1");
        for i in 0..N {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
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
        for i in 0..N {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
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
        for i in 0..N / 2 {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
        db.flush().await.expect("flush first half to L0");
        for i in N / 2..N {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
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
        for i in 0..N {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
        db.flush().await.expect("flush");
        for i in 0..N {
            if i % 3 == 0 {
                db.delete(key(i).as_bytes()).await.expect("delete");
            }
        }
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
        for i in 0..N {
            db.put(key(i).as_bytes(), val(i).as_bytes())
                .await
                .expect("put");
        }
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
        for i in 0..2_000 {
            db.put(key(i).as_bytes(), val(i).repeat(4).as_bytes())
                .await
                .expect("put");
        }
        db.compact().await.expect("compact");
    }

    let db = Db::open_with(&path, options).await.expect("open #2");
    for i in 0..2_000 {
        let got = db.get(key(i).as_bytes()).await.expect("get");
        assert_eq!(got.as_deref(), Some(val(i).repeat(4).as_bytes()));
    }
}

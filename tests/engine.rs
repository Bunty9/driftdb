//! End-to-end engine test: random puts/deletes/overwrites checked against a `BTreeMap` model,
//! with forced flush/compaction mid-way, snapshot isolation across compaction, concurrent
//! writers, write-batch atomicity, and a final reopen-equals-model check.

use driftdb::{Db, Options, WriteBatch};
use std::collections::BTreeMap;
use std::time::Duration;
use tempfile::TempDir;

/// Small limits so flush/compaction actually run many times over the course of the test.
fn small_options() -> Options {
    Options {
        memtable_size: 64 * 1024,
        target_file_size: 32 * 1024,
        l1_max_bytes: 128 * 1024,
        l0_compaction_trigger: 4,
        level_multiplier: 4,
        max_levels: 5,
        commit_window: Duration::ZERO,
    }
}

/// Cheap xorshift PRNG so the test has no external `rand` dependency.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

async fn assert_matches_model(db: &Db, model: &BTreeMap<Vec<u8>, Vec<u8>>) {
    for (k, v) in model {
        let got = db.get(k).await.expect("get");
        assert_eq!(got.as_ref(), Some(v), "mismatch on key {k:?}");
    }
    let scanned = db.scan(..).await.expect("scan");
    let expected: Vec<(Vec<u8>, Vec<u8>)> =
        model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scanned, expected, "full scan mismatch");
}

#[tokio::test]
async fn random_workload_matches_btreemap_model_across_flush_and_compact() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng::new(42);
    const KEYSPACE: u64 = 500;
    const OPS: usize = 22_000;

    for i in 0..OPS {
        let k = format!("key-{:05}", rng.below(KEYSPACE)).into_bytes();
        if rng.below(5) == 0 && model.contains_key(&k) {
            db.delete(&k).await.expect("delete");
            model.remove(&k);
        } else {
            let v = format!("val-{i}-{}", rng.next_u64()).into_bytes();
            db.put(&k, &v).await.expect("put");
            model.insert(k, v);
        }

        if i == OPS / 3 {
            db.flush().await.expect("mid-workload flush");
        }
        if i == 2 * OPS / 3 {
            db.compact().await.expect("mid-workload compact");
        }
    }

    assert_matches_model(&db, &model).await;

    db.compact().await.expect("final compact");
    assert_matches_model(&db, &model).await;

    let stats = db.stats();
    assert!(
        stats.write_amplification() > 1.0,
        "expected write amplification > 1 after compactions, got {} (user={}, disk={})",
        stats.write_amplification(),
        stats.user_bytes_written,
        stats.disk_bytes_written
    );

    db.close().await.expect("close");

    // Reopen and check the model still matches.
    let db2 = Db::open_with(dir.path(), small_options())
        .await
        .expect("reopen");
    assert_matches_model(&db2, &model).await;
}

#[tokio::test]
async fn snapshot_isolation_survives_overwrites_deletes_and_compaction() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..300u32 {
        let k = format!("k{i:04}").into_bytes();
        let v = format!("orig-{i}").into_bytes();
        db.put(&k, &v).await.expect("put");
        model.insert(k, v);
    }
    db.flush().await.expect("flush");

    // Snapshot sees the "orig" values.
    let snap = db.snapshot();
    let snap_model = model.clone();

    // Overwrite every key and delete a third of them, then force compaction.
    for i in 0..300u32 {
        let k = format!("k{i:04}").into_bytes();
        if i % 3 == 0 {
            db.delete(&k).await.expect("delete");
        } else {
            let v = format!("new-{i}").into_bytes();
            db.put(&k, &v).await.expect("put");
        }
    }
    db.compact().await.expect("compact");

    // Live reads see the new state.
    for i in 0..300u32 {
        let k = format!("k{i:04}").into_bytes();
        let got = db.get(&k).await.expect("get");
        if i % 3 == 0 {
            assert_eq!(got, None);
        } else {
            assert_eq!(got, Some(format!("new-{i}").into_bytes()));
        }
    }

    // The snapshot taken before the overwrites/deletes/compaction still sees the old values --
    // compaction must not have dropped anything the open snapshot could still read.
    for (k, v) in &snap_model {
        assert_eq!(snap.get(k).expect("snapshot get"), Some(v.clone()));
    }
    let snap_scan = snap.scan(..).expect("snapshot scan");
    let expected: Vec<(Vec<u8>, Vec<u8>)> = snap_model.into_iter().collect();
    assert_eq!(snap_scan, expected);

    drop(snap);
    // After the snapshot drops, a further compaction should be able to GC the old versions.
    // We don't assert exact byte shrinkage (compaction is opportunistic), just that reads are
    // still correct and stats are queryable.
    db.compact().await.expect("post-snapshot compact");
    for i in 0..300u32 {
        let k = format!("k{i:04}").into_bytes();
        let got = db.get(&k).await.expect("get");
        if i % 3 == 0 {
            assert_eq!(got, None);
        } else {
            assert_eq!(got, Some(format!("new-{i}").into_bytes()));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writers_all_land() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    let mut tasks = Vec::new();
    for worker in 0..8u32 {
        let db = db.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..500u32 {
                let k = format!("w{worker}-k{i:04}").into_bytes();
                let v = format!("v{worker}-{i}").into_bytes();
                db.put(&k, &v).await.expect("put");
            }
        }));
    }
    for t in tasks {
        t.await.expect("join");
    }

    db.compact().await.expect("compact");

    for worker in 0..8u32 {
        for i in 0..500u32 {
            let k = format!("w{worker}-k{i:04}").into_bytes();
            let expected = format!("v{worker}-{i}").into_bytes();
            let got = db.get(&k).await.expect("get");
            assert_eq!(
                got,
                Some(expected),
                "missing key from worker {worker} idx {i}"
            );
        }
    }
}

#[tokio::test]
async fn write_batch_is_atomic_and_visible_via_snapshot() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    // Seed a key the batch will delete, so the batch mixes puts and a delete.
    db.put(b"to-delete", b"x").await.expect("seed put");

    let before = db.snapshot();
    assert_eq!(before.get(b"a").unwrap(), None);
    assert_eq!(before.get(b"b").unwrap(), None);
    assert_eq!(before.get(b"to-delete").unwrap(), Some(b"x".to_vec()));

    let batch = WriteBatch::new()
        .put(b"a".to_vec(), b"1".to_vec())
        .put(b"b".to_vec(), b"2".to_vec())
        .delete(b"to-delete".to_vec());
    db.write_batch(batch).await.expect("write_batch");

    // The pre-batch snapshot must not observe any part of the batch.
    assert_eq!(before.get(b"a").unwrap(), None);
    assert_eq!(before.get(b"b").unwrap(), None);
    assert_eq!(before.get(b"to-delete").unwrap(), Some(b"x".to_vec()));

    // A fresh snapshot (or a plain get) sees the whole batch at once.
    let after = db.snapshot();
    assert_eq!(after.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(after.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(after.get(b"to-delete").unwrap(), None);
}

#[tokio::test]
async fn reopen_at_the_end_equals_model() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().to_path_buf();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    {
        let db = Db::open_with(&path, small_options()).await.expect("open");
        let mut rng = Rng::new(7);
        for i in 0..5_000usize {
            let k = format!("k{:05}", rng.below(400)).into_bytes();
            if rng.below(4) == 0 && model.contains_key(&k) {
                db.delete(&k).await.expect("delete");
                model.remove(&k);
            } else {
                let v = format!("v{i}").into_bytes();
                db.put(&k, &v).await.expect("put");
                model.insert(k, v);
            }
        }
        db.compact().await.expect("compact");
    }

    let db = Db::open_with(&path, small_options()).await.expect("reopen");
    assert_matches_model(&db, &model).await;
}

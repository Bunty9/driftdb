//! End-to-end engine test: random puts/deletes/overwrites checked against a `BTreeMap` model,
//! with forced flush/compaction mid-way, snapshot isolation across compaction, concurrent
//! writers, write-batch atomicity, and a final reopen-equals-model check.

use driftdb::{Db, Options, WriteBatch};
use std::collections::BTreeMap;
use std::ops::Bound;
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
    // ponytail: batching ops through `write_batch` instead of awaiting one `put`/`delete` per
    // op is what took this test from ~150s to a couple of seconds -- each `write_batch` call is
    // still just one fsync, same as a single `put` was, so this doesn't weaken durability
    // coverage, it just stops paying for 22,000 of them individually. `BATCH` ops still land in
    // the same relative order (one `write_batch` awaited fully before the next is built), so the
    // model stays exact and flush/compact still land at the same op boundaries.
    const BATCH: usize = 64;

    let mut batch = WriteBatch::new();
    for i in 0..OPS {
        let k = format!("key-{:05}", rng.below(KEYSPACE)).into_bytes();
        if rng.below(5) == 0 && model.contains_key(&k) {
            batch = batch.delete(k.clone());
            model.remove(&k);
        } else {
            let v = format!("val-{i}-{}", rng.next_u64()).into_bytes();
            batch = batch.put(k.clone(), v.clone());
            model.insert(k, v);
        }

        let boundary = i == OPS / 3 || i == 2 * OPS / 3;
        if batch.len() >= BATCH || boundary || i + 1 == OPS {
            db.write_batch(std::mem::take(&mut batch))
                .await
                .expect("write_batch");
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
        let mut batch = WriteBatch::new();
        for i in 0..5_000usize {
            let k = format!("k{:05}", rng.below(400)).into_bytes();
            if rng.below(4) == 0 && model.contains_key(&k) {
                batch = batch.delete(k.clone());
                model.remove(&k);
            } else {
                let v = format!("v{i}").into_bytes();
                batch = batch.put(k.clone(), v.clone());
                model.insert(k, v);
            }
            if batch.len() >= 64 || i + 1 == 5_000 {
                db.write_batch(std::mem::take(&mut batch))
                    .await
                    .expect("write_batch");
            }
        }
        db.compact().await.expect("compact");
    }

    let db = Db::open_with(&path, small_options()).await.expect("reopen");
    assert_matches_model(&db, &model).await;
}

/// Regression test for `rotate()` returning `None` (nothing to wait for) whenever the active
/// memtable happened to be empty, even if an already-frozen memtable was still mid-flush. That
/// made `flush()`/`compact()` return early without actually waiting, so `compact()` could race
/// ahead of a flush that hadn't landed yet.
#[tokio::test]
async fn compact_waits_for_a_flush_started_by_auto_rotate() {
    let dir = TempDir::new().expect("tempdir");
    let mut opts = small_options();
    opts.memtable_size = 4 * 1024; // small enough that a handful of puts auto-rotates.
    let db = Db::open_with(dir.path(), opts).await.expect("open");

    // Write just enough to cross `memtable_size` and trigger an auto-rotate; the active
    // memtable is then empty again (everything moved into `immutables`) with nothing more
    // queued behind it.
    let mut batch = WriteBatch::new();
    for i in 0..500u32 {
        batch = batch.put(format!("k{i:04}").into_bytes(), vec![b'x'; 32]);
    }
    db.write_batch(batch).await.expect("write_batch");

    db.compact().await.expect("compact");

    let stats = db.stats();
    assert_eq!(
        stats.memtable_bytes, 0,
        "compact() must wait for the auto-rotated memtable to actually flush"
    );
    assert_eq!(
        stats.level_files.first().copied().unwrap_or(0),
        0,
        "forced compaction should have drained L0 too"
    );
}

/// `Bound::Excluded` on either end of a scan must actually exclude that key -- covers
/// `Excluded`/`Included`/`Unbounded` on both ends, and both `Db::scan` and `Snapshot::scan`
/// (they share the same `scan_at` implementation).
#[tokio::test]
async fn scan_bounds_are_exact() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    for i in 0..10u32 {
        let k = format!("k{i:02}").into_bytes();
        let v = format!("v{i}").into_bytes();
        db.put(&k, &v).await.expect("put");
    }

    let keys = |pairs: Vec<(Vec<u8>, Vec<u8>)>| -> Vec<String> {
        pairs
            .into_iter()
            .map(|(k, _)| String::from_utf8(k).unwrap())
            .collect()
    };

    // Excluded start: "k03" itself must not appear.
    let got = db
        .scan((
            Bound::Excluded(b"k03".to_vec()),
            Bound::Excluded(b"k06".to_vec()),
        ))
        .await
        .expect("scan");
    assert_eq!(keys(got), vec!["k04", "k05"]);

    // Included start, Excluded end.
    let got = db
        .scan((
            Bound::Included(b"k03".to_vec()),
            Bound::Excluded(b"k06".to_vec()),
        ))
        .await
        .expect("scan");
    assert_eq!(keys(got), vec!["k03", "k04", "k05"]);

    // Included start, Included end.
    let got = db
        .scan((
            Bound::Included(b"k03".to_vec()),
            Bound::Included(b"k06".to_vec()),
        ))
        .await
        .expect("scan");
    assert_eq!(keys(got), vec!["k03", "k04", "k05", "k06"]);

    // Excluded start, Unbounded end.
    let got = db
        .scan((Bound::Excluded(b"k07".to_vec()), Bound::Unbounded))
        .await
        .expect("scan");
    assert_eq!(keys(got), vec!["k08", "k09"]);

    // Fully unbounded.
    let got = db.scan(..).await.expect("scan");
    assert_eq!(got.len(), 10);

    // Snapshot::scan shares `scan_at`, so the same `Excluded` start must hold there too.
    let snap = db.snapshot();
    let got = snap
        .scan((
            Bound::Excluded(b"k03".to_vec()),
            Bound::Excluded(b"k06".to_vec()),
        ))
        .expect("snapshot scan");
    assert_eq!(keys(got), vec!["k04", "k05"]);
}

/// Oversized keys/values are rejected before they ever reach the writer thread, and the db
/// stays fully usable afterward (the rejection doesn't poison anything).
#[tokio::test]
async fn oversized_key_and_value_are_rejected_and_db_stays_usable() {
    let dir = TempDir::new().expect("tempdir");
    let db = Db::open_with(dir.path(), small_options())
        .await
        .expect("open");

    let oversized_key = vec![b'k'; driftdb::wal::MAX_KEY_LEN + 1];
    let err = db
        .put(&oversized_key, b"v")
        .await
        .expect_err("oversized key must be rejected");
    assert!(matches!(err, driftdb::Error::InvalidArgument(_)));

    // A value this large would take a while to allocate/hash for no test value; a few bytes
    // over the limit is enough to exercise the check.
    let oversized_value = vec![b'v'; driftdb::wal::MAX_VALUE_LEN + 1];
    let err = db
        .put(b"k", &oversized_value)
        .await
        .expect_err("oversized value must be rejected");
    assert!(matches!(err, driftdb::Error::InvalidArgument(_)));

    // Neither rejection should have touched the engine -- ordinary ops still work, including an
    // empty key/value (explicitly allowed).
    db.put(b"", b"").await.expect("empty key/value put");
    db.put(b"ok", b"still works").await.expect("put");
    assert_eq!(db.get(b"").await.unwrap(), Some(Vec::new()));
    assert_eq!(db.get(b"ok").await.unwrap(), Some(b"still works".to_vec()));
}

/// Regression test for the snapshot-registration race: `snapshot()`/`get()`/`scan()` used to
/// read `visible_seq` *before* registering in the snapshot table, so a concurrent compaction's
/// `oldest_snapshot()` could compute a floor newer than the seq a registering snapshot was about
/// to use, and GC a version that snapshot still needed. Runs a writer hammering one key
/// alongside a compactor while repeatedly registering snapshots, then checks after the fact that
/// every observed `(seq, value)` matches what the write history says should have been visible at
/// that exact seq -- any mismatch means a version was dropped out from under a live read.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn snapshot_never_observes_a_version_gcd_by_a_racing_compaction() {
    let dir = TempDir::new().expect("tempdir");
    let mut opts = small_options();
    opts.memtable_size = 4 * 1024;
    opts.l0_compaction_trigger = 2;
    let db = Db::open_with(dir.path(), opts).await.expect("open");

    let key = b"racer".to_vec();
    let seed_seq = db
        .write_batch(WriteBatch::new().put(key.clone(), b"v0".to_vec()))
        .await
        .expect("seed put");
    let mut history = vec![(seed_seq, b"v0".to_vec())];

    let writer_db = db.clone();
    let writer_key = key.clone();
    let writer = tokio::spawn(async move {
        let mut h = Vec::new();
        for i in 1..2_000u64 {
            let v = format!("v{i}").into_bytes();
            let seq = writer_db
                .write_batch(WriteBatch::new().put(writer_key.clone(), v.clone()))
                .await
                .expect("put");
            h.push((seq, v));
        }
        h
    });

    let compactor_db = db.clone();
    let compactor = tokio::spawn(async move {
        for _ in 0..30 {
            let _ = compactor_db.compact().await;
        }
    });

    let mut observed = Vec::new();
    for _ in 0..3_000 {
        let snap = db.snapshot();
        let got = snap.get(&key).expect("snapshot get");
        observed.push((snap.seq(), got));
        tokio::task::yield_now().await;
    }

    let mut writer_history = writer.await.expect("writer join");
    compactor.await.expect("compactor join");
    history.append(&mut writer_history);

    for (seq, got) in observed {
        let expected = history
            .iter()
            .filter(|(s, _)| *s <= seq)
            .max_by_key(|(s, _)| *s)
            .map(|(_, v)| v.clone());
        assert_eq!(
            got, expected,
            "snapshot registered at seq {seq} saw a version compaction had already GC'd"
        );
    }
}

use driftdb::Options;
use jobqueue::keys::{prefix_range, status_key, JOB_PREFIX};
use jobqueue::model::{Job, JobStatus, NewJob};
use jobqueue::store::{JobStore, StoreError, MAX_PAYLOAD_BYTES};
use std::collections::HashSet;

fn new_job(n: u64) -> NewJob {
    NewJob {
        kind: "email".into(),
        payload: serde_json::json!({ "n": n }),
        max_attempts: None,
    }
}

fn small_opts() -> Options {
    Options {
        memtable_size: 64 * 1024,
        target_file_size: 32 * 1024,
        l1_max_bytes: 128 * 1024,
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claimers_never_share_a_job() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..200 {
        store.enqueue(new_job(n), 1_000).await.unwrap();
    }
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let s = store.clone();
        tasks.push(tokio::spawn(async move {
            let mut got = Vec::new();
            while let Some(job) = s.claim(60_000, 2_000).await.unwrap() {
                got.push(job.id);
            }
            got
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    let unique: HashSet<u64> = all.iter().copied().collect();
    assert_eq!(all.len(), 200, "every job claimed exactly once");
    assert_eq!(unique.len(), 200, "no job claimed twice");
    store.close().await.unwrap();
}

#[tokio::test]
async fn enqueue_get_complete_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    let job = store.enqueue(new_job(1), 10).await.unwrap();
    assert_eq!(job.id, 1);
    assert_eq!(
        store.get(1).await.unwrap().unwrap().status,
        JobStatus::Pending
    );
    let claimed = store.claim(1_000, 20).await.unwrap().unwrap();
    assert_eq!(
        (claimed.id, claimed.status, claimed.lease_until),
        (1, JobStatus::Running, Some(1_020))
    );
    let done = store.complete(1, 30).await.unwrap();
    assert_eq!(done.status, JobStatus::Done);
    assert!(matches!(
        store.complete(1, 40).await,
        Err(StoreError::InvalidState { .. })
    ));
    assert!(matches!(
        store.complete(99, 40).await,
        Err(StoreError::NotFound(99))
    ));
    assert_eq!(store.get(99).await.unwrap(), None);
    store.close().await.unwrap();
}

#[tokio::test]
async fn fail_retries_then_dies() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    store
        .enqueue(
            NewJob {
                max_attempts: Some(2),
                ..new_job(1)
            },
            0,
        )
        .await
        .unwrap();
    store.claim(1_000, 1).await.unwrap().unwrap();
    let j = store.fail(1, "boom".into(), 2).await.unwrap();
    assert_eq!(
        (j.status, j.attempts, j.last_error.as_deref()),
        (JobStatus::Pending, 1, Some("boom"))
    );
    store.claim(1_000, 3).await.unwrap().unwrap();
    let j = store.fail(1, "boom again".into(), 4).await.unwrap();
    assert_eq!((j.status, j.attempts), (JobStatus::Dead, 2));
    assert!(
        store.claim(1_000, 5).await.unwrap().is_none(),
        "dead jobs are never claimed"
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn expired_lease_is_requeued() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    store.enqueue(new_job(1), 0).await.unwrap();
    store.claim(100, 1_000).await.unwrap().unwrap(); // lease_until = 1_100
    assert_eq!(store.requeue_expired(1_050).await.unwrap(), 0);
    assert_eq!(store.requeue_expired(1_100).await.unwrap(), 1);
    let j = store.get(1).await.unwrap().unwrap();
    assert_eq!((j.status, j.lease_until), (JobStatus::Pending, None));
    assert_eq!(j.last_error.as_deref(), Some("lease expired"));
    store.close().await.unwrap();
}

#[tokio::test]
async fn reopen_preserves_jobs_and_id_counter() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = JobStore::open(dir.path(), Options::default())
            .await
            .unwrap();
        for n in 0..5 {
            store.enqueue(new_job(n), 0).await.unwrap();
        }
        store.close().await.unwrap();
    }
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    for id in 1..=5 {
        assert!(
            store.get(id).await.unwrap().is_some(),
            "job {id} survived reopen"
        );
    }
    assert_eq!(
        store.enqueue(new_job(9), 0).await.unwrap().id,
        6,
        "ids never reused"
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn oversized_payload_is_rejected_without_consuming_an_id() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    let big = NewJob {
        kind: "x".into(),
        payload: serde_json::json!("y".repeat(MAX_PAYLOAD_BYTES + 1)),
        max_attempts: None,
    };
    assert!(matches!(
        store.enqueue(big, 0).await,
        Err(StoreError::PayloadTooLarge { .. })
    ));
    assert_eq!(store.enqueue(new_job(1), 0).await.unwrap().id, 1);
    store.close().await.unwrap();
}

#[tokio::test]
async fn second_open_reports_locked() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), Options::default())
        .await
        .unwrap();
    let err = JobStore::open(dir.path(), Options::default())
        .await
        .expect_err("second open must fail");
    assert!(
        err.to_string().contains("locked by another process"),
        "got: {err}"
    );
    store.close().await.unwrap();
}

#[tokio::test]
async fn index_matches_records_after_mixed_ops() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), small_opts()).await.unwrap();
    for n in 0..300 {
        store
            .enqueue(
                NewJob {
                    max_attempts: Some(2),
                    ..new_job(n)
                },
                n,
            )
            .await
            .unwrap();
    }
    let mut t = 1_000;
    while let Some(job) = store.claim(50, t).await.unwrap() {
        t += 1;
        match job.id % 4 {
            0 => {
                store.complete(job.id, t).await.unwrap();
            }
            1 => {
                store.fail(job.id, "x".into(), t).await.unwrap();
            }
            2 => {} // abandoned: stays running until the lease expires
            _ => {
                store.complete(job.id, t).await.unwrap();
            }
        }
        if job.id % 50 == 0 {
            store.db().flush().await.unwrap();
        }
    }
    store.requeue_expired(t + 1_000).await.unwrap();
    let records = store.db().scan(prefix_range(JOB_PREFIX)).await.unwrap();
    let index = store.db().scan(prefix_range(b"idx/status/")).await.unwrap();
    assert_eq!(records.len(), 300);
    assert_eq!(index.len(), 300, "exactly one index entry per job");
    for (_, v) in &records {
        let job: Job = serde_json::from_slice(v).unwrap();
        let k = status_key(job.status, job.id);
        assert!(
            index.iter().any(|(ik, _)| *ik == k),
            "index entry for job {} ({})",
            job.id,
            job.status
        );
    }
    store.close().await.unwrap();
}

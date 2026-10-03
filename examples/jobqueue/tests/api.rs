use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use jobqueue::{api::router, store::JobStore};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn call(
    app: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    let body = match body {
        Some(v) => {
            req = req.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

#[tokio::test]
async fn full_job_lifecycle_over_http() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), driftdb::Options::default())
        .await
        .unwrap();
    let app = router(store.clone());

    let (s, job) = call(
        &app,
        "POST",
        "/jobs",
        Some(json!({"kind": "email", "payload": {"to": "a@b.c"}})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    assert_eq!(job["id"], 1);
    assert_eq!(job["status"], "pending");

    assert_eq!(call(&app, "GET", "/jobs/1", None).await.0, StatusCode::OK);
    assert_eq!(
        call(&app, "GET", "/jobs/99", None).await.0,
        StatusCode::NOT_FOUND
    );

    let (s, claimed) = call(
        &app,
        "POST",
        "/jobs/claim",
        Some(json!({"lease_ms": 30000})),
    )
    .await;
    assert_eq!(
        (s, claimed["status"].as_str()),
        (StatusCode::OK, Some("running"))
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/jobs/claim",
            Some(json!({"lease_ms": 30000}))
        )
        .await
        .0,
        StatusCode::NO_CONTENT
    );

    let token = claimed["claim_token"].clone();
    assert_eq!(
        call(
            &app,
            "POST",
            "/jobs/1/complete",
            Some(json!({"claim_token": 999}))
        )
        .await
        .0,
        StatusCode::CONFLICT,
        "stale token"
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/jobs/1/complete",
            Some(json!({"claim_token": token}))
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/jobs/1/complete",
            Some(json!({"claim_token": token}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    assert_eq!(
        call(
            &app,
            "POST",
            "/jobs/1/fail",
            Some(json!({"claim_token": token, "error": "x"}))
        )
        .await
        .0,
        StatusCode::CONFLICT
    );

    let (s, done) = call(&app, "GET", "/jobs?status=done&limit=10", None).await;
    assert_eq!(
        (s, done.as_array().map(Vec::len)),
        (StatusCode::OK, Some(1))
    );

    let (s, report) = call(&app, "GET", "/report", None).await;
    assert_eq!((s, &report["counts"]["done"]), (StatusCode::OK, &json!(1)));
    assert_eq!(call(&app, "GET", "/stats", None).await.0, StatusCode::OK);
    let (s, m) = call(&app, "POST", "/admin/maintenance", None).await;
    assert_eq!(s, StatusCode::OK);
    assert!(m["after"]["level_files"].is_array());

    drop(app);
    store.close().await.unwrap();
}

#[tokio::test]
async fn bad_input_is_4xx_never_500() {
    let dir = tempfile::tempdir().unwrap();
    let store = JobStore::open(dir.path(), driftdb::Options::default())
        .await
        .unwrap();
    let app = router(store.clone());

    let (s, body) = call(&app, "GET", "/jobs?status=bogus", None).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("unknown status"));
    assert_eq!(
        call(&app, "GET", "/jobs/abc", None).await.0,
        StatusCode::BAD_REQUEST
    );

    let req = Request::builder()
        .method("POST")
        .uri("/jobs")
        .header("content-type", "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    assert!(app
        .clone()
        .oneshot(req)
        .await
        .unwrap()
        .status()
        .is_client_error());

    let huge = json!({"kind": "x", "payload": "y".repeat(jobqueue::store::MAX_PAYLOAD_BYTES + 1)});
    assert_eq!(
        call(&app, "POST", "/jobs", Some(huge)).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );

    drop(app);
    store.close().await.unwrap();
}

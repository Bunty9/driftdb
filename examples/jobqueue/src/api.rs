//! HTTP API over `JobStore`. Shows: sharing the store through axum state, and mapping
//! store/driftdb errors to status codes (client mistakes are 4xx; only storage faults are 500).

use crate::model::{JobStatus, NewJob};
use crate::now_ms;
use crate::store::{JobStore, StoreError};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::json;

pub fn router(store: JobStore) -> Router {
    Router::new()
        .route("/jobs", post(create).get(list))
        .route("/jobs/claim", post(claim))
        .route("/jobs/{id}", get(get_job))
        .route("/jobs/{id}/complete", post(complete))
        .route("/jobs/{id}/fail", post(fail))
        .route("/report", get(report))
        .route("/stats", get(stats))
        .route("/admin/maintenance", post(maintenance))
        // The body limit must exceed MAX_PAYLOAD_BYTES so oversized jobs reach the store and get
        // a 413 from our own check, with a JSON error body.
        .layer(axum::extract::DefaultBodyLimit::max(
            crate::store::MAX_PAYLOAD_BYTES * 4,
        ))
        .with_state(store)
}

pub enum ApiError {
    Store(StoreError),
    BadRequest(String),
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        ApiError::Store(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, msg) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Store(e) => {
                let status = match &e {
                    StoreError::NotFound(_) => StatusCode::NOT_FOUND,
                    StoreError::InvalidState { .. } | StoreError::LeaseLost { .. } => {
                        StatusCode::CONFLICT
                    }
                    StoreError::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
                    _ => {
                        tracing::error!(error = %e, "storage failure");
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                };
                (status, e.to_string())
            }
        };
        (status, Json(json!({ "error": msg }))).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn create(
    State(store): State<JobStore>,
    Json(new): Json<NewJob>,
) -> ApiResult<(StatusCode, Json<crate::model::Job>)> {
    Ok((
        StatusCode::CREATED,
        Json(store.enqueue(new, now_ms()).await?),
    ))
}

#[derive(Deserialize)]
struct ListQuery {
    status: String,
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    50
}

async fn list(State(store): State<JobStore>, Query(q): Query<ListQuery>) -> ApiResult<Response> {
    let status: JobStatus = q.status.parse().map_err(ApiError::BadRequest)?;
    Ok(Json(store.list(status, q.limit.min(1_000)).await?).into_response())
}

async fn get_job(State(store): State<JobStore>, Path(id): Path<u64>) -> ApiResult<Response> {
    let job = store.get(id).await?.ok_or(StoreError::NotFound(id))?;
    Ok(Json(job).into_response())
}

#[derive(Deserialize)]
struct ClaimBody {
    #[serde(default = "default_lease")]
    lease_ms: u64,
}

fn default_lease() -> u64 {
    30_000
}

async fn claim(State(store): State<JobStore>, Json(body): Json<ClaimBody>) -> ApiResult<Response> {
    Ok(match store.claim(body.lease_ms, now_ms()).await? {
        Some(job) => Json(job).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

/// Workers echo back the `claim_token` they got from `/jobs/claim` (fencing: a worker whose
/// lease expired and whose job was re-claimed gets 409 instead of overwriting the new owner).
#[derive(Deserialize)]
struct CompleteBody {
    claim_token: u64,
}

async fn complete(
    State(store): State<JobStore>,
    Path(id): Path<u64>,
    Json(body): Json<CompleteBody>,
) -> ApiResult<Response> {
    Ok(Json(store.complete(id, body.claim_token, now_ms()).await?).into_response())
}

#[derive(Deserialize)]
struct FailBody {
    claim_token: u64,
    error: String,
}

async fn fail(
    State(store): State<JobStore>,
    Path(id): Path<u64>,
    Json(body): Json<FailBody>,
) -> ApiResult<Response> {
    Ok(Json(
        store
            .fail(id, body.claim_token, body.error, now_ms())
            .await?,
    )
    .into_response())
}

async fn report(State(store): State<JobStore>) -> ApiResult<Response> {
    Ok(Json(store.report().await?).into_response())
}

async fn stats(State(store): State<JobStore>) -> Response {
    Json(store.stats()).into_response()
}

async fn maintenance(State(store): State<JobStore>) -> ApiResult<Response> {
    let (before, after) = store.maintenance().await?;
    Ok(Json(json!({ "before": before, "after": after })).into_response())
}

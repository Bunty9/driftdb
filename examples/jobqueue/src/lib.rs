//! A durable job queue built on driftdb — a reference for embedding `driftdb-lsm`.
//! Start with `store.rs` (the integration pattern), then `keys.rs` (key design).

pub mod api;
pub mod keys;
pub mod model;
pub mod store;

/// Wall-clock milliseconds since the Unix epoch. Store methods take `now` as a parameter
/// instead of calling this, so tests can control time.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before 1970")
        .as_millis() as u64
}

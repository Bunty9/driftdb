//! Key schema. driftdb orders keys bytewise, so the layout *is* the query plan:
//!
//! | key                          | value        |
//! |------------------------------|--------------|
//! | `job/<id>`                   | JSON `Job`   |
//! | `idx/status/<status>/<id>`   | empty        |
//! | `meta/next_id`               | u64 BE       |
//!
//! Ids are 16 lowercase hex digits (fixed width) so byte order == numeric order and keys
//! stay readable when debugging. A record and its index entry always change in the same
//! `WriteBatch`, so they cannot diverge — not even across a crash.

use crate::model::JobStatus;
use std::ops::Bound;

pub const JOB_PREFIX: &[u8] = b"job/";
pub const NEXT_ID_KEY: &[u8] = b"meta/next_id";

pub fn job_key(id: u64) -> Vec<u8> {
    format!("job/{id:016x}").into_bytes()
}

pub fn status_prefix(status: JobStatus) -> Vec<u8> {
    format!("idx/status/{}/", status.as_str()).into_bytes()
}

pub fn status_key(status: JobStatus, id: u64) -> Vec<u8> {
    let mut k = status_prefix(status);
    k.extend_from_slice(format!("{id:016x}").as_bytes());
    k
}

/// Parse the trailing `<id>` of a `job/…` or `idx/status/…/…` key.
pub fn id_from_key(key: &[u8]) -> Option<u64> {
    let tail = key.rsplit(|b| *b == b'/').next()?;
    if tail.len() != 16 {
        return None;
    }
    u64::from_str_radix(std::str::from_utf8(tail).ok()?, 16).ok()
}

/// Range covering every key that starts with `prefix` — the standard prefix scan for an
/// ordered KV store. The end bound is `prefix` with its last non-0xFF byte incremented
/// (bytes after it dropped); an all-0xFF (or empty) prefix has no finite upper bound.
pub fn prefix_range(prefix: &[u8]) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefix.to_vec());
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return (start, Bound::Excluded(end));
        }
    }
    (start, Bound::Unbounded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::JobStatus;
    use std::ops::Bound;

    #[test]
    fn ids_sort_numerically_as_bytes() {
        assert!(job_key(9) < job_key(10));
        assert!(job_key(255) < job_key(256));
        assert_eq!(job_key(26), b"job/000000000000001a".to_vec());
    }

    #[test]
    fn status_key_round_trips_id() {
        let k = status_key(JobStatus::Running, 42);
        assert_eq!(k, b"idx/status/running/000000000000002a".to_vec());
        assert_eq!(id_from_key(&k), Some(42));
        assert_eq!(id_from_key(&job_key(7)), Some(7));
        assert_eq!(id_from_key(b"idx/status/running/zz"), None);
    }

    #[test]
    fn prefix_range_edges() {
        assert_eq!(
            prefix_range(b"a/"),
            (
                Bound::Included(b"a/".to_vec()),
                Bound::Excluded(b"a0".to_vec())
            )
        );
        assert_eq!(
            prefix_range(&[0x61, 0xFF]),
            (
                Bound::Included(vec![0x61, 0xFF]),
                Bound::Excluded(vec![0x62])
            )
        );
        assert_eq!(
            prefix_range(&[0xFF, 0xFF]),
            (Bound::Included(vec![0xFF, 0xFF]), Bound::Unbounded)
        );
        assert_eq!(
            prefix_range(b""),
            (Bound::Included(vec![]), Bound::Unbounded)
        );
    }
}

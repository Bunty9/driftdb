//! # driftdb
//!
//! Embeddable LSM-tree key-value engine. Write path:
//! `put → WAL (group-commit fsync) → memtable → freeze → flush → L0 SST → compactor → L1+`.
//! Read path: memtable → frozen memtable(s) → L0 (newest first) → L1+ (binary search,
//! bloom-filtered inside each SST). Keys carry a `(user_key, seqno)` pair for MVCC snapshot
//! reads; a single dedicated writer thread group-commits batches to the WAL, and a single
//! background thread owns flush + leveled compaction.
//!
//! See `docs/plans/2026-09-25-driftdb-phase-2.md` for the full design.
//!
//! ## Quick start
//!
//! ```no_run
//! use driftdb::{Db, Result};
//!
//! # async fn run() -> Result<()> {
//! let db = Db::open("/tmp/driftdb-demo").await?;
//! db.put(b"hello", b"world").await?;
//! assert_eq!(db.get(b"hello").await?.as_deref(), Some(&b"world"[..]));
//! db.flush().await?; // force a flush to L0, just to demonstrate it's there
//! assert_eq!(db.get(b"hello").await?.as_deref(), Some(&b"world"[..]));
//! # Ok(())
//! # }
//! ```

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod compaction;
pub mod db;
pub mod error;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use crate::db::{Db, Options, Snapshot, Stats, WriteBatch};
pub use crate::error::{Error, Result};

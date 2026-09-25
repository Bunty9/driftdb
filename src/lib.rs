//! # driftdb
//!
//! Embeddable LSM-tree key-value engine. Write path:
//! `put → WAL (group-commit fsync) → memtable → freeze → flush → L0 SST → compactor → L1+`.
//! Read path: memtable → frozen memtable → L0 (newest first) → L1+ (bloom-filtered).
//! Keys carry a `(user_key, seqno)` pair for MVCC snapshot reads.
//!
//! See [`projects-l3-l4.md`](../../projects-l3-l4.md) § P5 for the full design.
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

pub use crate::db::{Db, Snapshot};
pub use crate::error::{Error, Result};

//! Leveled compaction scheduler.
//!
//! Strategy: L0 = N most recent flushes (may overlap on key range); L1+ = non-overlapping
//! sorted runs. Trigger: `|L_n| > base * mult^n`. Pick: oldest L_n SST + all overlapping
//! L_{n+1} SSTs → merge → write L_{n+1}.
//!
//! Defense (the interview answer):
//!   - Leveled gives `O(log N)` reads + low space amp; cost is write amplification ~5–10×.
//!   - For driftdb's intended embed targets (job-queue metadata, agent state), reads dominate;
//!     leveled wins.
//!   - The alternative tiered (size-tiered) trades cheaper writes for higher read amp and
//!     space amp — the right call only for write-dominated logs.
//!
//! See README "Design tradeoffs" § "leveled vs tiered".

use crate::error::Result;
use std::path::PathBuf;
use std::sync::Arc;

/// Plan emitted by the picker; consumed by `run_compaction`.
#[derive(Clone, Debug)]
pub struct CompactionPlan {
    /// Source level being compacted out of.
    pub level: u8,
    /// SSTs from `level` + overlapping `level+1` SSTs that will be merged.
    pub input_ssts: Vec<PathBuf>,
    /// Destination level for the merged output.
    pub output_level: u8,
}

/// State the compactor reads + writes. The real type lives in `db::DbState`; this trait keeps
/// the compactor decoupled from the `Db` shape so it can be unit-tested in isolation.
#[allow(dead_code)]
pub trait CompactionState: Send + Sync {
    /// Inspect level metadata and return a plan if compaction should run.
    fn pick_compaction(&self) -> Option<CompactionPlan>;
}

/// Background task that polls for compaction work. One per `Db` instance.
///
/// **Phase 1 status:** the loop body is stubbed so the task spawns + exits cleanly. The
/// merge writer and manifest update land in Phase 2.
pub async fn compactor<S: CompactionState + 'static>(state: Arc<S>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Some(plan) = state.pick_compaction() {
            // Best-effort; the real impl logs + records to metrics.
            let _ = run_compaction(state.as_ref(), plan).await;
        }
    }
}

/// Pure picker — given a state, choose the next compaction (or `None`).
///
/// **Phase 1 status:** always returns `None`. Phase 2 implements the size-threshold check.
#[allow(dead_code)]
pub fn pick_compaction<S: CompactionState>(_state: &S) -> Option<CompactionPlan> {
    None
}

/// Execute a plan: open input SST iterators, merge them in seqno-aware order, write the
/// output SST(s), append `SstAdded` + `SstDeleted` to the manifest, unlink the obsolete files.
///
/// **Phase 1 status:** no-op.
pub async fn run_compaction<S: CompactionState>(_state: &S, _plan: CompactionPlan) -> Result<()> {
    // Phase 2: merge-iterator + SstWriter + manifest append + unlink.
    Ok(())
}

//! Leveled compaction: picker + executor.
//!
//! `pick`/`pick_forced` are pure functions over per-level [`SstMeta`] lists (no I/O, no
//! `SstReader`s) so they're cheap to unit-test in isolation. `run` does the actual merge +
//! write + reopen work against real files and is exercised by the `tests/engine.rs`
//! integration test instead.
//!
//! Strategy: L0 = N most recent flushes (may overlap on key range, newest-first for reads);
//! L1+ = non-overlapping sorted runs. Trigger: L0 file count >= `l0_compaction_trigger`, or
//! `L_n` total bytes > `l1_max_bytes * level_multiplier^(n-1)`. Pick: all L0 (+ overlapping L1)
//! on the L0 path, else the L_n file with the smallest `smallest` key (simplest correct
//! round-robin proxy) + all overlapping `L_{n+1}` files. Output is split into multiple SSTs at
//! `target_file_size`, but only at a user-key boundary (never splitting versions of one key
//! across files).
//!
//! Rationale:
//!   - Leveled gives `O(log N)` reads + low space amp; cost is write amplification ~5-10x.
//!   - For driftdb's intended embed targets (job-queue metadata, agent state), reads dominate;
//!     leveled wins.
//!   - The alternative tiered (size-tiered) trades cheaper writes for higher read amp and
//!     space amp -- the right call only for write-dominated logs.

use crate::db::Options;
use crate::error::Result;
use crate::iter::{self, BoxIter};
use crate::manifest::SstMeta;
use crate::sstable::{sst_path, SstReader, SstWriter};
use std::io::BufWriter;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// One on-disk SSTable plus its open reader. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct Table {
    pub meta: SstMeta,
    pub reader: Arc<SstReader>,
}

/// Immutable, copy-on-write snapshot of the live SST file set. `levels[0]` = L0, kept
/// newest-first (index 0 = most recently flushed/added) so point reads short-circuit on the
/// newest table; `levels[n >= 1]` kept sorted by `smallest` ASC (non-overlapping runs).
#[derive(Debug, Default, Clone)]
pub struct Version {
    pub levels: Vec<Vec<Arc<Table>>>,
}

impl Version {
    /// Metadata-only view of every level, for feeding [`pick`]/[`pick_forced`] without cloning
    /// readers.
    pub fn level_metas(&self) -> Vec<Vec<SstMeta>> {
        self.levels
            .iter()
            .map(|l| l.iter().map(|t| t.meta.clone()).collect())
            .collect()
    }
}

/// Plan emitted by the picker; consumed by [`run`].
#[derive(Clone, Debug)]
pub struct CompactionPlan {
    /// Destination level for the merged output.
    pub output_level: u8,
    /// `(source level, file meta)` for every input file -- the picked level plus any
    /// overlapping files one level down.
    pub inputs: Vec<(u8, SstMeta)>,
}

/// True if `[a_smallest, a_largest]` and `[b.smallest, b.largest]` overlap (inclusive).
fn meta_overlaps(a_smallest: &[u8], a_largest: &[u8], b: &SstMeta) -> bool {
    a_smallest <= b.largest.as_slice() && b.smallest.as_slice() <= a_largest
}

fn overlaps(a: &SstMeta, b: &SstMeta) -> bool {
    meta_overlaps(&a.smallest, &a.largest, b)
}

/// `(min smallest, max largest)` across `metas`. Panics on an empty slice -- callers only use
/// this on a non-empty input set.
fn key_range(metas: &[&SstMeta]) -> (Vec<u8>, Vec<u8>) {
    let smallest = metas.iter().map(|m| &m.smallest).min().unwrap().clone();
    let largest = metas.iter().map(|m| &m.largest).max().unwrap().clone();
    (smallest, largest)
}

fn overlapping<'a>(files: &'a [SstMeta], lo: &[u8], hi: &[u8]) -> Vec<&'a SstMeta> {
    let probe = SstMeta {
        number: 0,
        smallest: lo.to_vec(),
        largest: hi.to_vec(),
        size: 0,
        max_seq: 0,
    };
    files.iter().filter(|f| overlaps(f, &probe)).collect()
}

/// Choose the next compaction, or `None` if nothing crosses the configured thresholds.
pub fn pick(levels: &[Vec<SstMeta>], options: &Options) -> Option<CompactionPlan> {
    pick_inner(levels, options, false)
}

/// Like [`pick`] but ignores the trigger thresholds -- any non-empty level is eligible. Used by
/// `Db::compact()` to drain everything down to the bottom-most level.
pub fn pick_forced(levels: &[Vec<SstMeta>], options: &Options) -> Option<CompactionPlan> {
    pick_inner(levels, options, true)
}

fn pick_inner(levels: &[Vec<SstMeta>], options: &Options, force: bool) -> Option<CompactionPlan> {
    let l0 = levels.first().map(|v| v.as_slice()).unwrap_or(&[]);
    let l0_triggered = !l0.is_empty() && (force || l0.len() >= options.l0_compaction_trigger);
    if l0_triggered {
        let refs: Vec<&SstMeta> = l0.iter().collect();
        let (lo, hi) = key_range(&refs);
        let l1 = levels.get(1).map(|v| v.as_slice()).unwrap_or(&[]);
        let overlap_l1 = overlapping(l1, &lo, &hi);
        let mut inputs: Vec<(u8, SstMeta)> = l0.iter().map(|m| (0u8, m.clone())).collect();
        inputs.extend(overlap_l1.into_iter().map(|m| (1u8, m.clone())));
        return Some(CompactionPlan {
            output_level: 1,
            inputs,
        });
    }

    for n in 1..options.max_levels.saturating_sub(1) {
        let Some(ln) = levels.get(n) else {
            continue;
        };
        if ln.is_empty() {
            continue;
        }
        let threshold = (options.l1_max_bytes as f64
            * (options.level_multiplier as f64).powi(n as i32 - 1)) as u64;
        let total: u64 = ln.iter().map(|m| m.size).sum();
        if !force && total <= threshold {
            continue;
        }
        // ponytail: always picking the globally-smallest key starves files further right in
        // the keyspace if writes keep landing on low keys (they'd never get their turn). A
        // real round-robin needs a persisted per-level cursor key (compact from where the last
        // compaction of this level left off, wrapping at the end) -- add one if a workload
        // shows a level growing unboundedly despite compaction running.
        let victim = ln
            .iter()
            .min_by(|a, b| a.smallest.cmp(&b.smallest))?
            .clone();
        let ln1 = levels.get(n + 1).map(|v| v.as_slice()).unwrap_or(&[]);
        let overlap_next = overlapping(ln1, &victim.smallest, &victim.largest);
        let mut inputs = vec![(n as u8, victim)];
        inputs.extend(overlap_next.into_iter().map(|m| ((n + 1) as u8, m.clone())));
        return Some(CompactionPlan {
            output_level: (n + 1) as u8,
            inputs,
        });
    }
    None
}

/// Result of [`run`]: what to record in the manifest and how to update the live `Version`.
#[derive(Debug)]
pub struct CompactionResult {
    pub output_level: u8,
    pub added: Vec<SstMeta>,
    /// `(level, number)` of every input file, now obsolete.
    pub deleted: Vec<(u8, u64)>,
    pub new_tables: Vec<Arc<Table>>,
}

/// Execute `plan`: merge every input table (bounded by `oldest_snapshot` + tombstone GC), write
/// one or more output SSTs under `dir` (split at `options.target_file_size`, only on a user-key
/// boundary), and return the manifest edit + new tables to install. Does not touch the manifest
/// or `Version` itself -- callers (the background thread in `db.rs`) apply the result under the
/// appropriate locks so this function stays testable and side-effect-scoped to the filesystem.
pub fn run(
    dir: &Path,
    options: &Options,
    plan: &CompactionPlan,
    version: &Version,
    oldest_snapshot: u64,
    next_file_number: &AtomicU64,
) -> Result<CompactionResult> {
    let mut tables: Vec<(u8, Arc<Table>)> = Vec::with_capacity(plan.inputs.len());
    for (level, meta) in &plan.inputs {
        let table = version
            .levels
            .get(*level as usize)
            .and_then(|l| l.iter().find(|t| t.meta.number == meta.number))
            .cloned()
            .expect("compaction input missing from version");
        tables.push((*level, table));
    }

    let refs: Vec<&SstMeta> = plan.inputs.iter().map(|(_, m)| m).collect();
    let (lo, hi) = key_range(&refs);
    let drop_tombstones = !version
        .levels
        .iter()
        .enumerate()
        .skip(plan.output_level as usize + 1)
        .any(|(_, files)| files.iter().any(|t| meta_overlaps(&lo, &hi, &t.meta)));

    // L0 inputs newest-first (higher file number = more recent), then everything else. Doesn't
    // affect correctness -- seqnos are globally unique so `merge` never actually breaks a tie
    // here -- but keeps the "newer source wins" convention consistent with the read path.
    let mut l0: Vec<&Arc<Table>> = tables
        .iter()
        .filter(|(l, _)| *l == 0)
        .map(|(_, t)| t)
        .collect();
    l0.sort_by_key(|a| std::cmp::Reverse(a.meta.number));
    let others: Vec<&Arc<Table>> = tables
        .iter()
        .filter(|(l, _)| *l != 0)
        .map(|(_, t)| t)
        .collect();

    let mut sources: Vec<BoxIter<'_>> = Vec::with_capacity(tables.len());
    for t in l0 {
        sources.push(Box::new(t.reader.iter()));
    }
    for t in others {
        sources.push(Box::new(t.reader.iter()));
    }

    let merged = iter::merge(sources);
    let filtered = iter::compaction_filter(merged, oldest_snapshot, drop_tombstones);

    let mut added = Vec::new();
    let mut new_tables = Vec::new();
    let mut writer: Option<SstWriter<BufWriter<std::fs::File>>> = None;
    let mut cur_number = 0u64;
    let mut last_key: Option<Vec<u8>> = None;

    let finish_current = |writer: SstWriter<BufWriter<std::fs::File>>,
                          number: u64,
                          added: &mut Vec<SstMeta>,
                          new_tables: &mut Vec<Arc<Table>>|
     -> Result<()> {
        let path = sst_path(dir, number);
        let (bufw, summary) = writer.finish()?;
        bufw.into_inner().map_err(|e| e.into_error())?.sync_all()?;
        let meta = SstMeta {
            number,
            smallest: summary.smallest,
            largest: summary.largest,
            size: summary.file_size,
            max_seq: summary.max_seq,
        };
        let reader = Arc::new(SstReader::open(&path)?);
        new_tables.push(Arc::new(Table {
            meta: meta.clone(),
            reader,
        }));
        added.push(meta);
        Ok(())
    };

    for entry in filtered {
        let (key, seq, val) = entry?;
        let need_rollover = match (&writer, &last_key) {
            (Some(w), Some(lk)) => w.estimated_size() >= options.target_file_size && key != *lk,
            _ => false,
        };
        if need_rollover {
            let w = writer.take().unwrap();
            finish_current(w, cur_number, &mut added, &mut new_tables)?;
        }
        if writer.is_none() {
            cur_number = next_file_number.fetch_add(1, Ordering::SeqCst);
            let file = std::fs::File::create(sst_path(dir, cur_number))?;
            writer = Some(SstWriter::new(BufWriter::new(file)));
        }
        writer.as_mut().unwrap().add(&key, seq, &val)?;
        last_key = Some(key);
    }
    if let Some(w) = writer {
        if !w.is_empty() {
            finish_current(w, cur_number, &mut added, &mut new_tables)?;
        }
    }
    if !added.is_empty() {
        crate::wal::sync_dir(dir)?;
    }

    let deleted = plan.inputs.iter().map(|(l, m)| (*l, m.number)).collect();
    Ok(CompactionResult {
        output_level: plan.output_level,
        added,
        deleted,
        new_tables,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(number: u64, smallest: &str, largest: &str, size: u64) -> SstMeta {
        SstMeta {
            number,
            smallest: smallest.as_bytes().to_vec(),
            largest: largest.as_bytes().to_vec(),
            size,
            max_seq: number,
        }
    }

    fn opts() -> Options {
        Options {
            l0_compaction_trigger: 4,
            l1_max_bytes: 1000,
            level_multiplier: 10,
            max_levels: 7,
            ..Options::default()
        }
    }

    #[test]
    fn overlaps_detects_intersection_and_disjoint_ranges() {
        let a = meta(1, "b", "e", 1);
        assert!(overlaps(&a, &meta(2, "a", "b", 1))); // touches at "b"
        assert!(overlaps(&a, &meta(2, "d", "z", 1))); // overlaps inside
        assert!(overlaps(&a, &meta(2, "c", "d", 1))); // contained
        assert!(!overlaps(&a, &meta(2, "f", "z", 1))); // strictly after
        assert!(!overlaps(&a, &meta(2, "a", "aa", 1))); // strictly before
    }

    #[test]
    fn no_plan_when_under_every_threshold() {
        let levels = vec![vec![meta(1, "a", "b", 10)]];
        assert!(pick(&levels, &opts()).is_none());
    }

    #[test]
    fn l0_trigger_picks_all_l0_plus_overlapping_l1() {
        let levels = vec![
            vec![
                meta(1, "a", "c", 1),
                meta(2, "b", "d", 1),
                meta(3, "e", "f", 1),
                meta(4, "a", "a", 1),
            ],
            vec![
                meta(10, "a", "b", 1),  // overlaps L0 range [a,f]
                meta(11, "z", "zz", 1), // does not overlap
            ],
        ];
        let plan = pick(&levels, &opts()).expect("plan");
        assert_eq!(plan.output_level, 1);
        let numbers: Vec<u64> = plan.inputs.iter().map(|(_, m)| m.number).collect();
        assert!(numbers.contains(&1));
        assert!(numbers.contains(&2));
        assert!(numbers.contains(&3));
        assert!(numbers.contains(&4));
        assert!(numbers.contains(&10));
        assert!(!numbers.contains(&11));
    }

    #[test]
    fn l1_over_budget_picks_smallest_file_plus_overlapping_l2() {
        let levels = vec![
            vec![],                                               // L0 empty
            vec![meta(1, "a", "c", 800), meta(2, "d", "f", 800)], // total 1600 > 1000
            vec![meta(10, "a", "b", 1), meta(11, "e", "z", 1)],
        ];
        let plan = pick(&levels, &opts()).expect("plan");
        assert_eq!(plan.output_level, 2);
        let numbers: Vec<u64> = plan.inputs.iter().map(|(_, m)| m.number).collect();
        // smallest "smallest" key among L1 files is "a" (file 1).
        assert!(numbers.contains(&1));
        assert!(!numbers.contains(&2));
        assert!(numbers.contains(&10)); // overlaps [a,c]
        assert!(!numbers.contains(&11)); // [e,z] does not overlap [a,c]
    }

    #[test]
    fn bottom_level_never_triggers_as_a_source() {
        let mut options = opts();
        options.max_levels = 2; // only L0, L1 -- L1 is the bottom, never a compaction source
        let levels = vec![vec![], vec![meta(1, "a", "z", 1_000_000)]];
        assert!(pick(&levels, &options).is_none());
    }

    #[test]
    fn forced_pick_ignores_thresholds() {
        let levels = vec![vec![meta(1, "a", "b", 1)]]; // 1 L0 file, trigger is 4
        assert!(pick(&levels, &opts()).is_none());
        let plan = pick_forced(&levels, &opts()).expect("forced plan");
        assert_eq!(plan.output_level, 1);
        assert_eq!(plan.inputs.len(), 1);
    }

    #[test]
    fn forced_pick_drains_small_l1_too() {
        let levels = vec![vec![], vec![meta(1, "a", "b", 1)]]; // well under l1_max_bytes
        assert!(pick(&levels, &opts()).is_none());
        let plan = pick_forced(&levels, &opts()).expect("forced plan");
        assert_eq!(plan.output_level, 2);
    }
}

//! Free-space allocator for the `mem.upper` data area (design §4.1).
//!
//! All coordinates are data-area-relative 512B sectors. The free-list is pure
//! derived state: it is rebuilt by scanning the metadata entries at open and
//! never persisted, so a crash can never leave it stale (crash safety by
//! construction). Allocation is best-fit over an interval set — no size
//! classes. In-place overwrites bypass the allocator entirely; only shrinks
//! (tail reclaim) and old-extent releases touch it, and run appends bump the
//! high-water mark when no hole fits.

use anyhow::{ensure, Result};
use std::collections::BTreeSet;

/// `(start_sector, len_sectors)`, ordered by start.
pub type Extent = (u64, u32);

#[derive(Debug, Default)]
pub struct FreeList {
    free: BTreeSet<Extent>,
    /// High-water mark: next never-allocated sector.
    data_end: u64,
    free_sectors: u64,
}

impl FreeList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rebuilds the free-list from the occupied extents, validating that they
    /// are sorted, disjoint and within the derived high-water mark.
    pub fn rebuild(mut occupied: Vec<Extent>) -> Result<Self> {
        occupied.sort_unstable();
        let mut free = BTreeSet::new();
        let mut cursor = 0u64;
        let mut free_sectors = 0u64;
        for &(start, len) in &occupied {
            ensure!(len > 0, "free-list rebuild: zero-length extent");
            ensure!(
                start >= cursor,
                "free-list rebuild: overlapping extents at sector {start}"
            );
            if start > cursor {
                let gap = (start - cursor) as u32;
                free.insert((cursor, gap));
                free_sectors += u64::from(gap);
            }
            cursor = start + u64::from(len);
        }
        Ok(Self {
            free,
            data_end: cursor,
            free_sectors,
        })
    }

    /// Raises the high-water mark (e.g. derived from the file length) so
    /// tail space freed after the last occupied extent stays reusable.
    pub fn extend_data_end(&mut self, end: u64) {
        if end > self.data_end {
            let gained = (end - self.data_end) as u32;
            self.free.insert((self.data_end, gained));
            self.free_sectors += u64::from(gained);
            self.data_end = end;
        }
    }

    /// Best-fit allocation: the smallest hole that fits, else bump.
    pub fn alloc(&mut self, sectors: u32) -> u64 {
        assert!(sectors > 0, "free-list alloc: zero sectors");
        let mut best: Option<Extent> = None;
        for &(start, len) in &self.free {
            if len < sectors {
                continue;
            }
            let better = match best {
                None => true,
                Some((_, best_len)) => len < best_len,
            };
            if better {
                best = Some((start, len));
            }
        }
        if let Some((start, len)) = best {
            self.free.remove(&(start, len));
            self.free_sectors -= u64::from(sectors);
            if len > sectors {
                self.free
                    .insert((start + u64::from(sectors), len - sectors));
            }
            return start;
        }
        let start = self.data_end;
        self.data_end += u64::from(sectors);
        start
    }

    /// Returns an extent to the free-list, coalescing with neighbors.
    pub fn free(&mut self, start: u64, sectors: u32) {
        if sectors == 0 {
            return;
        }
        let mut merged_start = start;
        let mut merged_len = sectors;
        if let Some(&prev) = self.free.range(..(start, u32::MAX)).next_back() {
            if prev.0 + u64::from(prev.1) == start {
                self.free.remove(&prev);
                merged_start = prev.0;
                merged_len += prev.1;
            }
        }
        if let Some(&next) = self.free.range((merged_start, 0)..).next() {
            if merged_start + u64::from(merged_len) == next.0 {
                self.free.remove(&next);
                merged_len += next.1;
            }
        }
        self.free.insert((merged_start, merged_len));
        self.free_sectors += u64::from(sectors);
    }

    pub fn total_free(&self) -> u64 {
        self.free_sectors
    }

    pub fn extent_count(&self) -> usize {
        self.free.len()
    }

    pub fn data_end(&self) -> u64 {
        self.data_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;

    #[test]
    fn alloc_bumps_then_reuses_freed_hole() {
        let mut list = FreeList::new();
        assert_eq!(list.alloc(4), 0);
        assert_eq!(list.alloc(2), 4);
        list.free(0, 4);
        // Best fit: the 4-sector hole is the only one.
        assert_eq!(list.alloc(4), 0);
        assert_eq!(list.total_free(), 0);
    }

    #[test]
    fn best_fit_picks_smallest_sufficient_hole() {
        let mut list = FreeList::rebuild(vec![(0, 100)]).unwrap();
        list.free(10, 2);
        list.free(20, 5);
        list.free(30, 3);
        assert_eq!(list.alloc(2), 10);
        assert_eq!(list.alloc(3), 30);
        assert_eq!(list.alloc(3), 20); // exact fit consumes the hole, splits (20,5)
        assert_eq!(list.alloc(2), 23); // the split remainder is the best fit
        assert_eq!(list.alloc(2), 100); // falls to bump at the high-water mark
    }

    #[test]
    fn free_coalesces_neighbors() {
        let mut list = FreeList::new();
        list.alloc(10);
        list.free(0, 4);
        list.free(6, 4);
        assert_eq!(list.extent_count(), 2);
        list.free(4, 2);
        assert_eq!(list.extent_count(), 1);
        assert_eq!(list.total_free(), 10);
    }

    #[test]
    fn rebuild_complements_occupied_and_derives_data_end() {
        let mut list = FreeList::rebuild(vec![(0, 4), (8, 2), (20, 4)]).unwrap();
        assert_eq!(list.data_end(), 24);
        assert_eq!(list.total_free(), 24 - 10);
        assert_eq!(list.extent_count(), 2);
        // Holes: [4..8) and [10..20).
        assert_eq!(list.alloc(4), 4);
        assert_eq!(list.alloc(10), 10);
        assert_eq!(list.alloc(1), 24); // bump
    }

    #[test]
    fn rebuild_rejects_overlaps() {
        assert!(FreeList::rebuild(vec![(0, 4), (2, 4)]).is_err());
        assert!(FreeList::rebuild(vec![(4, 4), (4, 4)]).is_err());
    }

    #[test]
    fn fuzz_against_naive_sector_model() {
        let mut rng = rand::rng();
        for round in 0..20 {
            let mut list = FreeList::new();
            let mut live: Vec<Extent> = Vec::new();
            let mut used = std::collections::HashSet::new();
            for _ in 0..2000 {
                if live.is_empty() || rng.random_bool(0.6) {
                    let sectors = rng.random_range(1..=64);
                    let start = list.alloc(sectors);
                    for s in start..start + u64::from(sectors) {
                        assert!(used.insert(s), "round {round}: sector {s} handed out twice");
                    }
                    live.push((start, sectors));
                } else {
                    let idx = rng.random_range(0..live.len());
                    let (start, sectors) = live.swap_remove(idx);
                    for s in start..start + u64::from(sectors) {
                        assert!(used.remove(&s), "round {round}: sector {s} freed twice");
                    }
                    list.free(start, sectors);
                }
            }
            // Rebuild from the surviving extents plus the allocator's
            // high-water mark (production derives it from the file length,
            // because freed tail extents are invisible to the occupied scan).
            let mut rebuilt = FreeList::rebuild(live).unwrap();
            rebuilt.extend_data_end(list.data_end());
            assert_eq!(rebuilt.data_end(), list.data_end(), "round {round}");
            assert_eq!(rebuilt.total_free(), list.total_free(), "round {round}");
            assert_eq!(rebuilt.extent_count(), list.extent_count(), "round {round}");
            // Independent oracle: free space below the high-water mark is
            // exactly the complement of the live extents.
            let expected_free = list.data_end() - used.len() as u64;
            assert_eq!(rebuilt.total_free(), expected_free, "round {round}");
        }
    }
}

//! Best-effort background pageout for vPaused sandboxes.
//!
//! `v_pause` stops the guest vCPUs immediately (unless `[v_pause].stop_vcpus`
//! keeps them running) and hands the guest-memory pageout to this module
//! instead of performing it synchronously: thousands
//! of concurrent pauses would otherwise funnel every
//! `process_madvise(MADV_PAGEOUT)` call into the host swap subsystem at the
//! same instant, and the burst serializes the whole machine on swap-slot and
//! zswap/zram locks.
//!
//! Two victim-selection paths exist:
//!
//! - Dirty-driven (default when `memory_snapshot.track_dirty_pages` is on):
//!   Firecracker reports the current KVM dirty-log window
//!   (`dirty_ranges`) and the ranges its balloon has discarded since the
//!   previous query (`discarded_ranges`, reading as zero afterwards). This
//!   module subtracts the discarded ranges — pages written and then freed
//!   within the same window are dead, and everything still in swap is absent
//!   from the window anyway — so `MADV_PAGEOUT` never walks pages the guest
//!   never wrote or has already freed, with no host page-table scan. The KVM
//!   log auto-clears per query, so the next window only contains pages
//!   faulted back in.
//! - Full-region fallback (dirty tracking off, e.g. PVM mode, or when the
//!   dirty query fails): the job advises the whole guest memory region list.
//!
//! A fixed pool of worker threads serves one FIFO queue. Each job belongs to
//! one sandbox and advises at most one chunk of guest memory per turn before
//! re-enqueueing itself, so pending sandboxes advance in round-robin order
//! and no large guest starves the rest. A job carries its own pidfd and
//! region list, so workers never hold sandbox locks. `v_resume` and teardown
//! only bump the owning [`PageoutControl`] generation; the worker notices at
//! the next chunk boundary and drops the job, leaving an in-flight chunk to
//! race the resumed guest (safe: MADV_PAGEOUT on a running process is
//! ordinary kernel reclaim — wasteful, never corrupting). Only the snapshot
//! path drains the in-flight chunk, which is bounded by `pageout_chunk_mib`.

use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use firecracker_client::models::{DirtyMemoryRange, DirtyMemoryRanges, GuestRegionUffdMapping};
use tracing::{debug, warn};

use crate::privileges::{run_with_scoped_capabilities, CAP_SYS_NICE};
use crate::types::SandboxId;

/// `process_madvise(MADV_PAGEOUT)` needs 4 KiB-aligned host ranges, matching
/// the guest page size reported by Firecracker.
const PAGEOUT_PAGE_SIZE: usize = 4096;

/// The kernel imports iovecs through the read/write path, which silently
/// clamps the length advised by one `process_madvise` call to `MAX_RW_COUNT`
/// (`INT_MAX & PAGE_MASK`, just under 2 GiB) and returns the clamped count as
/// success. Advise in chunks of at most this size so guest memory regions
/// larger than 2 GiB are fully paged out.
const MAX_ADVISE_CHUNK: usize = (i32::MAX as usize) & !(PAGEOUT_PAGE_SIZE - 1);

/// Upper bound on how long `PageoutControl::cancel_and_drain` waits for an
/// in-flight chunk before proceeding. A chunk is bounded by `pageout_chunk_mib`
/// and reclaim throughput; giving up only means a chunk races the caller
/// (safe: reclaiming a running or snapshotted guest is normal kernel
/// behavior), so the wait ends with a warning instead of blocking the
/// lifecycle forever.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Per-sandbox pageout lifecycle shared between the sandbox handle and the
/// worker pool. All operations are lock-free; `v_pause` submits, resume,
/// snapshot, and teardown cancel.
pub(crate) struct PageoutControl {
    /// Id of the newest submitted job; older jobs still queued or running are
    /// dropped once their id no longer matches.
    current_job: AtomicU64,
    /// Id of the job whose chunk is executing on a worker; 0 when idle.
    in_flight: AtomicU64,
    /// Whether a job is queued or running. Feeds cancellation accounting
    /// only, so approximate under submit/cancel races.
    pending: AtomicBool,
}

impl PageoutControl {
    pub(crate) fn new() -> Self {
        Self {
            current_job: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            pending: AtomicBool::new(false),
        }
    }

    /// Enqueues a best-effort pageout of `pidfd`'s guest memory `regions`,
    /// superseding any job still queued for this sandbox.
    pub(crate) fn spawn_job(
        control: Arc<Self>,
        sandbox_id: SandboxId,
        pidfd: OwnedFd,
        regions: Vec<GuestRegionUffdMapping>,
    ) {
        let job_id = control.current_job.fetch_add(1, Ordering::SeqCst) + 1;
        control.pending.store(true, Ordering::SeqCst);
        PAGEOUT_QUEUE.enqueue(PageoutJob {
            id: job_id,
            sandbox_id,
            pidfd,
            regions,
            cursor: PageoutCursor::default(),
            chunk_bytes: pageout_chunk_bytes(),
            control,
        });
    }

    /// Stops any queued or running job from advising further chunks. Cheap
    /// and safe from any context, including `Drop`.
    pub(crate) fn cancel(&self) {
        self.current_job.fetch_add(1, Ordering::SeqCst);
        if self.pending.swap(false, Ordering::SeqCst) {
            metrics::counter!("agentenv_v_pause_pageout_cancellations_total").increment(1);
        }
    }

    /// Cancels, then waits for an in-flight chunk to finish. Only the
    /// snapshot path needs the wait; see the module docs for why resume and
    /// teardown cancel without one.
    pub(crate) async fn cancel_and_drain(&self) {
        self.cancel();
        let deadline = Instant::now() + DRAIN_TIMEOUT;
        while self.in_flight.load(Ordering::SeqCst) != 0 {
            if Instant::now() >= deadline {
                warn!(
                    "vPause pageout drain timed out; an in-flight chunk may still race this operation (safe, only wasteful)"
                );
                return;
            }
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }

    fn is_current(&self, job_id: u64) -> bool {
        self.current_job.load(Ordering::SeqCst) == job_id
    }

    /// Waits until the sandbox's newest pageout job settles: completed,
    /// failed, or cancelled with nothing queued or in flight. Primarily for
    /// tests and diagnostics; production flows rely on best-effort semantics
    /// or on [`PageoutControl::cancel_and_drain`].
    pub(crate) async fn wait_until_settled(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if !self.pending.load(Ordering::SeqCst) && self.in_flight.load(Ordering::SeqCst) == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                anyhow::bail!("vPause pageout did not settle within {timeout:?}");
            }
            tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
        }
    }

    /// Marks the sandbox job-free if `job_id` is still the newest job; a
    /// superseded or cancelled job must not clear the successor's pending
    /// flag.
    fn note_finished(&self, job_id: u64) {
        if self.is_current(job_id) {
            self.pending.store(false, Ordering::SeqCst);
        }
    }
}

fn pageout_chunk_bytes() -> usize {
    let chunk_mib = crate::cfg::ConfigManager::global_config()
        .v_pause
        .pageout_chunk_mib
        .max(1);
    chunk_mib.saturating_mul(1024 * 1024)
}

/// One queued pageout. Moves to the back of the queue after every chunk
/// while incomplete, giving round-robin fairness across sandboxes.
struct PageoutJob {
    id: u64,
    sandbox_id: SandboxId,
    pidfd: OwnedFd,
    regions: Vec<GuestRegionUffdMapping>,
    cursor: PageoutCursor,
    chunk_bytes: usize,
    control: Arc<PageoutControl>,
}

/// Position within the region list: index of the region being advised plus
/// the bytes already advised inside it. Regions before the index are fully
/// paged out.
#[derive(Default)]
struct PageoutCursor {
    region_idx: usize,
    region_advised: usize,
}

/// Advises at most `max_bytes` of `regions` starting at `cursor`, advancing
/// the cursor past everything successfully advised. Returns the bytes advised
/// and whether the cursor reached the end of the region list.
fn pageout_chunk(
    pidfd: RawFd,
    regions: &[GuestRegionUffdMapping],
    cursor: &mut PageoutCursor,
    max_bytes: usize,
) -> Result<(u64, bool)> {
    let mut advised: u64 = 0;
    while cursor.region_idx < regions.len() && (advised as usize) < max_bytes {
        let region = &regions[cursor.region_idx];
        ensure!(
            region.page_size == PAGEOUT_PAGE_SIZE as i32,
            "unsupported guest page size {}",
            region.page_size
        );
        let base = usize::try_from(region.base_host_virt_addr)
            .context("guest memory base address is invalid")?;
        let len = usize::try_from(region.size).context("guest memory region size is invalid")?;
        ensure!(
            len > 0 && len % PAGEOUT_PAGE_SIZE == 0,
            "guest memory region is not page aligned"
        );
        ensure!(
            base % PAGEOUT_PAGE_SIZE == 0 && base.checked_add(len).is_some(),
            "guest memory region is invalid"
        );
        // The cursor only rests at region boundaries or inside a validated
        // region.
        debug_assert!(cursor.region_advised < len);

        let chunk = (len - cursor.region_advised)
            .min(max_bytes - advised as usize)
            .min(MAX_ADVISE_CHUNK);
        let iovec = libc::iovec {
            iov_base: (base + cursor.region_advised) as *mut libc::c_void,
            iov_len: chunk,
        };
        let result = unsafe {
            libc::syscall(
                libc::SYS_process_madvise,
                pidfd,
                &iovec,
                1usize,
                libc::MADV_PAGEOUT,
                0u32,
            )
        };
        if result < 0 {
            return Err(std::io::Error::last_os_error()).with_context(|| {
                format!(
                    "page out guest memory region at {:#x}",
                    base + cursor.region_advised
                )
            });
        }
        ensure!(
            result > 0,
            "guest memory pageout made no progress at {:#x}",
            base + cursor.region_advised
        );
        cursor.region_advised += result as usize;
        advised += result as u64;
        if cursor.region_advised == len {
            cursor.region_idx += 1;
            cursor.region_advised = 0;
        }
    }
    let complete = cursor.region_idx >= regions.len();
    Ok((advised, complete))
}

/// Subtracts coalesced, ascending, disjoint `discarded` intervals from
/// coalesced, ascending, disjoint `window` intervals, matching on
/// `image_offset`. Both lists come from Firecracker coalescing per slot in
/// ascending slot order. Surviving pieces keep the source range's host base,
/// offset by their position inside it.
fn subtract_ranges(
    window: &[DirtyMemoryRange],
    discarded: &[DirtyMemoryRange],
) -> Vec<DirtyMemoryRange> {
    let mut result = Vec::new();
    let mut disc_idx = 0usize;
    for range in window {
        let range_end = range.image_offset + range.length;
        // Skip discards that end before this window range begins.
        while disc_idx < discarded.len()
            && discarded[disc_idx].image_offset + discarded[disc_idx].length <= range.image_offset
        {
            disc_idx += 1;
        }
        let mut start = range.image_offset;
        let mut disc_i = disc_idx;
        while disc_i < discarded.len() && discarded[disc_i].image_offset < range_end {
            let cut_start = discarded[disc_i].image_offset.max(start);
            let cut_end =
                (discarded[disc_i].image_offset + discarded[disc_i].length).min(range_end);
            if cut_start > start {
                push_victim(&mut result, range, start, cut_start - start);
            }
            start = start.max(cut_end);
            if start >= range_end {
                break;
            }
            disc_i += 1;
        }
        if start < range_end {
            push_victim(&mut result, range, start, range_end - start);
        }
    }
    result
}

/// Appends the surviving piece `[start, start + length)` (image offsets) of
/// `range`, rebuilding the host base address for the piece.
fn push_victim(
    result: &mut Vec<DirtyMemoryRange>,
    range: &DirtyMemoryRange,
    start: i64,
    length: i64,
) {
    result.push(DirtyMemoryRange {
        base_host_virt_addr: range.base_host_virt_addr + (start - range.image_offset),
        image_offset: start,
        length,
    });
}

/// Converts Firecracker's dirty-log window minus balloon free-page-reporting
/// discards into advise targets. Pages written and then discarded within the
/// same window are dead (their content reads as zero and they are typically
/// non-present); pages still in swap never appear in the window; pages
/// discarded before the window and since re-written stay, because the discard
/// report only covers the window since the last query. Returns `None` when
/// nothing survives; an error means the response cannot be advised as-is and
/// the caller should fall back to the full region list.
pub(crate) fn pageout_regions_from_dirty(
    ranges: &DirtyMemoryRanges,
) -> Result<Option<Vec<GuestRegionUffdMapping>>> {
    ensure!(
        ranges.page_size == PAGEOUT_PAGE_SIZE as i32,
        "unsupported pageout page size {}",
        ranges.page_size
    );
    let victims = subtract_ranges(&ranges.dirty_ranges, &ranges.discarded_ranges);
    if victims.is_empty() {
        return Ok(None);
    }
    let regions = victims
        .iter()
        .map(|range| {
            let base = usize::try_from(range.base_host_virt_addr)
                .context("pageout range base address is invalid")?;
            let len = usize::try_from(range.length).context("pageout range length is invalid")?;
            ensure!(
                len > 0 && len % PAGEOUT_PAGE_SIZE == 0,
                "pageout range is not page aligned"
            );
            ensure!(
                base % PAGEOUT_PAGE_SIZE == 0 && base.checked_add(len).is_some(),
                "pageout range is invalid"
            );
            Ok(GuestRegionUffdMapping::new(
                range.base_host_virt_addr,
                range.length,
                range.image_offset,
                ranges.page_size,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(regions))
}

/// Shared FIFO queue plus a fixed pool of worker threads. The queue lives for
/// the process lifetime; workers are plain threads because every chunk blocks
/// in the kernel for the duration of one `process_madvise`.
struct PageoutQueue {
    sender: Sender<PageoutJob>,
    receiver: Arc<Mutex<Receiver<PageoutJob>>>,
    depth: AtomicU64,
}

static PAGEOUT_QUEUE: LazyLock<Arc<PageoutQueue>> = LazyLock::new(|| {
    let workers = crate::cfg::ConfigManager::global_config()
        .v_pause
        .pageout_workers
        .max(1);
    PageoutQueue::new(workers)
});

impl PageoutQueue {
    fn new(worker_count: usize) -> Arc<Self> {
        let (sender, receiver) = std::sync::mpsc::channel::<PageoutJob>();
        let queue = Arc::new(Self {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            depth: AtomicU64::new(0),
        });
        for index in 0..worker_count {
            std::thread::Builder::new()
                .name(format!("aenv-vpause-pageout-{index}"))
                .spawn({
                    let queue = Arc::clone(&queue);
                    move || worker_loop(queue)
                })
                .expect("spawn vPause pageout worker");
        }
        queue
    }

    /// Hands `job` to a worker, keeping the queue-depth gauge in step. The
    /// count rolls back if the queue is gone, which only happens at process
    /// teardown.
    fn enqueue(&self, job: PageoutJob) {
        self.depth.fetch_add(1, Ordering::Relaxed);
        self.publish_depth();
        if self.sender.send(job).is_err() {
            self.depth.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn publish_depth(&self) {
        metrics::gauge!("agentenv_v_pause_pageout_queue_depth")
            .set(self.depth.load(Ordering::Relaxed) as f64);
    }
}

fn worker_loop(queue: Arc<PageoutQueue>) {
    loop {
        let mut job = {
            let guard = queue.receiver.lock().expect("vPause pageout queue lock");
            match guard.recv() {
                Ok(job) => job,
                Err(_) => return,
            }
        };
        // Account for the dequeue before running so depth never counts a job
        // a worker already owns.
        queue.depth.fetch_sub(1, Ordering::Relaxed);
        queue.publish_depth();

        if !job.control.is_current(job.id) {
            continue;
        }

        let PageoutJob {
            id,
            ref sandbox_id,
            ref pidfd,
            ref regions,
            ref mut cursor,
            chunk_bytes,
            ref control,
        } = job;

        control.in_flight.store(id, Ordering::SeqCst);
        // Split borrows so the scoped-closure can advise through `cursor`
        // while reading the immutable job fields.
        let outcome = run_with_scoped_capabilities(&[CAP_SYS_NICE], || {
            pageout_chunk(pidfd.as_raw_fd(), regions, cursor, chunk_bytes)
        });
        control.in_flight.store(0, Ordering::SeqCst);

        match outcome {
            Ok((advised, complete)) => {
                if advised > 0 {
                    metrics::counter!("agentenv_v_pause_pageout_advised_bytes_total")
                        .increment(advised);
                }
                if complete {
                    control.note_finished(id);
                    debug!(sandbox_id = %sandbox_id, "vPause guest memory pageout complete");
                    metrics::counter!("agentenv_v_pause_pageout_completions_total").increment(1);
                } else if control.is_current(id) {
                    // Round-robin: go to the back of the queue and let other
                    // sandboxes advance one chunk first.
                    queue.enqueue(job);
                } else {
                    control.note_finished(id);
                }
            }
            Err(error) => {
                warn!(
                    sandbox_id = %sandbox_id,
                    error = %error,
                    "vPause pageout chunk failed; dropping remaining pageout"
                );
                control.note_finished(id);
                metrics::counter!("agentenv_v_pause_pageout_failures_total").increment(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pidfd_for_current_process() -> Result<rustix::fd::OwnedFd> {
        let pid = rustix::process::Pid::from_raw(std::process::id() as i32).unwrap();
        let pidfd = rustix::process::pidfd_open(pid, rustix::process::PidfdFlags::empty())?;
        Ok(pidfd)
    }

    #[test]
    #[ignore = "requires CAP_SYS_NICE and active swap"]
    fn pageout_chunk_advise_regions_larger_than_max_rw_count() -> Result<()> {
        // One page past the per-syscall clamp: a single process_madvise call
        // silently advises only MAX_ADVISE_CHUNK bytes, so the chunk helper
        // must loop inside the region even with an unbounded budget.
        let len = MAX_ADVISE_CHUNK + PAGEOUT_PAGE_SIZE;
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        ensure!(base != libc::MAP_FAILED, "mmap failed");
        // Touch sparsely so reclaim stays fast while covering both chunks.
        let touched: Vec<usize> = (0..len).step_by(2 * 1024 * 1024).collect();
        for &offset in &touched {
            unsafe { base.cast::<u8>().add(offset).write(0x5a) };
        }
        let pidfd = pidfd_for_current_process()?;
        let region = GuestRegionUffdMapping::new(base as i64, len as i64, 0, 4096);
        let mut cursor = PageoutCursor::default();
        let (advised, complete) =
            pageout_chunk(pidfd.as_raw_fd(), &[region], &mut cursor, usize::MAX)?;
        let preserved = touched
            .iter()
            .all(|&offset| unsafe { base.cast::<u8>().add(offset).read() } == 0x5a);
        unsafe { libc::munmap(base, len) };
        assert!(preserved);
        assert_eq!(advised, len as u64);
        assert!(complete);
        Ok(())
    }

    #[test]
    #[ignore = "requires CAP_SYS_NICE and active swap"]
    fn pageout_chunk_budgets_spans_multiple_calls_and_regions() -> Result<()> {
        let len = 4 * 1024 * 1024;
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        ensure!(base != libc::MAP_FAILED, "mmap failed");
        unsafe { base.cast::<u8>().write_bytes(0x5a, len) };
        let pidfd = pidfd_for_current_process()?;
        let regions = [
            GuestRegionUffdMapping::new(base as i64, (len / 2) as i64, 0, 4096),
            GuestRegionUffdMapping::new(
                base.wrapping_add(len / 2) as i64,
                (len / 2) as i64,
                0,
                4096,
            ),
        ];
        let mut cursor = PageoutCursor::default();
        let mut advised_total = 0u64;
        let mut calls = 0;
        loop {
            let (advised, complete) =
                pageout_chunk(pidfd.as_raw_fd(), &regions, &mut cursor, 1024 * 1024)?;
            advised_total += advised;
            calls += 1;
            if complete {
                break;
            }
        }
        let preserved =
            (0..len).all(|offset| unsafe { base.cast::<u8>().add(offset).read() } == 0x5a);
        unsafe { libc::munmap(base, len) };
        assert!(preserved);
        assert_eq!(advised_total, len as u64);
        assert_eq!(calls, 4);
        Ok(())
    }

    /// The dirty-driven victims are the dirty-log window minus the balloon
    /// discard log: pieces cut at the discarded pages keep the source range's
    /// host base, and a fully covered window yields nothing.
    #[test]
    fn pageout_regions_from_dirty_subtracts_discards() -> Result<()> {
        let base = 0x7f00_0000_0000i64;
        let ranges = DirtyMemoryRanges::new(
            4096,
            (8 * PAGEOUT_PAGE_SIZE) as i64,
            Vec::new(),
            vec![
                DirtyMemoryRange::new(base, 0, (3 * PAGEOUT_PAGE_SIZE) as i64),
                DirtyMemoryRange::new(
                    base + 5 * PAGEOUT_PAGE_SIZE as i64,
                    (5 * PAGEOUT_PAGE_SIZE) as i64,
                    PAGEOUT_PAGE_SIZE as i64,
                ),
            ],
            vec![DirtyMemoryRange::new(
                base + PAGEOUT_PAGE_SIZE as i64,
                PAGEOUT_PAGE_SIZE as i64,
                PAGEOUT_PAGE_SIZE as i64,
            )],
        );

        let regions = pageout_regions_from_dirty(&ranges)
            .expect("conversion succeeds")
            .expect("victims survive");
        assert_eq!(regions.len(), 3);
        assert_eq!(regions[0].base_host_virt_addr, base);
        assert_eq!(regions[0].offset, 0);
        assert_eq!(regions[0].size, PAGEOUT_PAGE_SIZE as i64);
        assert_eq!(
            regions[1].base_host_virt_addr,
            base + 2 * PAGEOUT_PAGE_SIZE as i64
        );
        assert_eq!(regions[1].offset, (2 * PAGEOUT_PAGE_SIZE) as i64);
        assert_eq!(regions[1].size, PAGEOUT_PAGE_SIZE as i64);
        assert_eq!(
            regions[2].base_host_virt_addr,
            base + 5 * PAGEOUT_PAGE_SIZE as i64
        );
        assert_eq!(regions[2].offset, (5 * PAGEOUT_PAGE_SIZE) as i64);
        Ok(())
    }

    #[test]
    fn pageout_regions_from_dirty_reports_empty_window() -> Result<()> {
        let ranges = DirtyMemoryRanges::new(4096, 0, Vec::new(), Vec::new(), Vec::new());
        assert!(pageout_regions_from_dirty(&ranges)?.is_none());

        // A window fully covered by discards also yields nothing to advise.
        let covered = DirtyMemoryRanges::new(
            4096,
            (2 * PAGEOUT_PAGE_SIZE) as i64,
            Vec::new(),
            vec![DirtyMemoryRange::new(
                0x1000_0000,
                0,
                (2 * PAGEOUT_PAGE_SIZE) as i64,
            )],
            vec![DirtyMemoryRange::new(
                0x1000_0000,
                0,
                (2 * PAGEOUT_PAGE_SIZE) as i64,
            )],
        );
        assert!(pageout_regions_from_dirty(&covered)?.is_none());
        Ok(())
    }

    #[test]
    fn pageout_regions_from_dirty_rejects_unsupported_page_size() {
        let ranges = DirtyMemoryRanges::new(
            65536,
            PAGEOUT_PAGE_SIZE as i64,
            Vec::new(),
            vec![DirtyMemoryRange::new(0, 0, PAGEOUT_PAGE_SIZE as i64)],
            Vec::new(),
        );
        assert!(pageout_regions_from_dirty(&ranges).is_err());
    }

    #[test]
    fn pageout_control_cancel_supersedes_jobs() {
        let control = PageoutControl::new();
        // Job ids start at 1; a cancel before any submit leaves nothing
        // current and is idempotent.
        control.cancel();
        control.cancel();
        assert!(!control.is_current(1));
        assert!(!control.is_current(0));
    }
}

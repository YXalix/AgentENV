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
//!   Firecracker reports its cumulative dirty view (`ranges`, every page the
//!   guest wrote since the memory baseline), the current dirty-log window
//!   (`dirty_ranges`, writes since the previous query, cleared by it), and
//!   the ranges its balloon has discarded since the previous query
//!   (`discarded_ranges`, reading as zero afterwards, cleared by the query).
//!   The discard log is a per-query delta while the union accumulates, so
//!   the caller folds each query's delta into a host-side cumulative
//!   discard mask ([`advance_discard_mask`]) and the victims are the union
//!   minus that mask minus the window, so `MADV_PAGEOUT` never walks pages
//!   the guest never wrote or has already freed, with no host page-table
//!   scan. Pages the guest re-wrote after a discard hold live data again
//!   and drop back off the mask on the next fold, because the rewrite
//!   appears in the window of that round. The window is excluded because
//!   resume re-accesses the
//!   last run round's writes within seconds: advising them would only buy a
//!   swap-out now followed by a swap-in on resume, while sparing them keeps
//!   the per-round working set resident and the stale remainder — swapped
//!   out once — costs only a walk without I/O afterwards. A first query's
//!   window equals the union (nothing was cleared before), so the first
//!   pause after a baseline advises nothing.
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
use rustix::event::{poll, PollFd, PollFlags, Timespec};
use tracing::{debug, warn};

use crate::observability::prometheus::result_status;
use crate::privileges::{run_with_scoped_capabilities, CAP_SYS_NICE, CAP_SYS_PTRACE};
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
    /// superseding any job still queued for this sandbox. `pid` feeds the
    /// resident-set telemetry only and `path` labels where the victim list
    /// came from (`dirty` or `full`).
    pub(crate) fn spawn_job(
        control: Arc<Self>,
        sandbox_id: SandboxId,
        pid: Option<i32>,
        pidfd: OwnedFd,
        regions: Vec<GuestRegionUffdMapping>,
        path: &'static str,
    ) {
        let job_id = control.current_job.fetch_add(1, Ordering::SeqCst) + 1;
        control.pending.store(true, Ordering::SeqCst);
        let target_bytes: u64 = regions
            .iter()
            .map(|region| u64::try_from(region.size).unwrap_or(0))
            .sum();
        if target_bytes > 0 {
            metrics::counter!("agentenv_v_pause_pageout_target_bytes_total", "path" => path)
                .increment(target_bytes);
        }
        metrics::counter!("agentenv_v_pause_pageout_target_regions_total", "path" => path)
            .increment(regions.len() as u64);
        let rss_before = pid.and_then(process_rss_bytes);
        if let Some(rss) = rss_before {
            metrics::histogram!("agentenv_v_pause_pageout_process_rss_bytes", "phase" => "before")
                .record(rss as f64);
        }
        PAGEOUT_QUEUE.enqueue(PageoutJob {
            id: job_id,
            sandbox_id,
            pid,
            pidfd,
            regions,
            cursor: PageoutCursor::default(),
            chunk_bytes: pageout_chunk_bytes(),
            control,
            rss_before,
            enqueued_at: Instant::now(),
            rounds: 0,
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

/// Best-effort resident-set size of `pid` in bytes, for telemetry only:
/// `None` when the process is gone or `/proc` is unreadable.
fn process_rss_bytes(pid: i32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    parse_vm_rss(&status)
}

/// Whether the process behind `pidfd` has already exited: a pidfd becomes
/// readable on task death, so a zero-timeout poll is a race-free probe.
fn target_process_exited(pidfd: &OwnedFd) -> bool {
    let mut pollfds = [PollFd::new(pidfd, PollFlags::IN)];
    let timeout = Timespec::try_from(Duration::ZERO).expect("zero timeout always fits a timespec");
    poll(&mut pollfds, Some(&timeout)).is_ok() && pollfds[0].revents().contains(PollFlags::IN)
}

/// Parses the `VmRSS:` line (KiB) of a `/proc/<pid>/status` body into bytes.
fn parse_vm_rss(status: &str) -> Option<u64> {
    let kib: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()?;
    Some(kib.saturating_mul(1024))
}

/// One queued pageout. Moves to the back of the queue after every chunk
/// while incomplete, giving round-robin fairness across sandboxes.
struct PageoutJob {
    id: u64,
    sandbox_id: SandboxId,
    /// Firecracker process pid, when readable; feeds the resident-set
    /// telemetry only.
    pid: Option<i32>,
    pidfd: OwnedFd,
    regions: Vec<GuestRegionUffdMapping>,
    cursor: PageoutCursor,
    chunk_bytes: usize,
    control: Arc<PageoutControl>,
    /// Resident-set size observed at submit, for the reclaimed-bytes
    /// comparison at completion.
    rss_before: Option<u64>,
    enqueued_at: Instant,
    /// Completed chunk turns; zero until the job is first re-enqueued.
    rounds: u32,
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
/// coalesced, ascending, disjoint `dirty` intervals, matching on
/// `image_offset`. Both lists come from Firecracker coalescing per slot in
/// ascending slot order. Surviving pieces keep the source range's host base,
/// offset by their position inside it.
fn subtract_ranges(
    dirty: &[DirtyMemoryRange],
    discarded: &[DirtyMemoryRange],
) -> Vec<DirtyMemoryRange> {
    let mut result = Vec::new();
    let mut disc_idx = 0usize;
    for range in dirty {
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

/// Merges coalesced, ascending, disjoint `a` and `b` into one coalesced,
/// ascending, disjoint list, matching on `image_offset` and coalescing
/// overlapping and adjacent ranges. Kept pieces carry the earlier piece's
/// host base; ranges of one memory slot share the same base layout, so for
/// a merged span that choice is equivalent.
fn union_ranges(a: &[DirtyMemoryRange], b: &[DirtyMemoryRange]) -> Vec<DirtyMemoryRange> {
    let mut result: Vec<DirtyMemoryRange> = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() || j < b.len() {
        let next = if j >= b.len() || (i < a.len() && a[i].image_offset <= b[j].image_offset) {
            let range = a[i].clone();
            i += 1;
            range
        } else {
            let range = b[j].clone();
            j += 1;
            range
        };
        if let Some(last) = result.last_mut() {
            let last_end = last.image_offset + last.length;
            let next_end = next.image_offset + next.length;
            if next.image_offset <= last_end {
                last.length = last_end.max(next_end) - last.image_offset;
                continue;
            }
        }
        result.push(next);
    }
    result
}

/// Folds one query's balloon discard log into the sandbox's cumulative
/// discard mask. `window` pages the guest re-wrote since the previous query
/// hold live data again and come off the mask; `discards` — pages freed
/// since the previous query — join it. The fold is needed because
/// Firecracker clears its discard log on every query while the dirty union
/// keeps accumulating: without the host-side mask every round would re-walk
/// every balloon-freed page, and once the balloon inflates the dead pages
/// dominate the victims. A page discarded and re-written within one window
/// is indistinguishable by write order and stays masked; the residue is
/// bounded by a single round's churn.
pub(crate) fn advance_discard_mask(
    mask: Vec<DirtyMemoryRange>,
    window: &[DirtyMemoryRange],
    discards: &[DirtyMemoryRange],
) -> Vec<DirtyMemoryRange> {
    union_ranges(&subtract_ranges(&mask, window), discards)
}

/// Converts Firecracker's cumulative dirty view minus the cumulative balloon
/// free-page-reporting discard mask and the current dirty-log window into
/// advise targets. `discard_mask` accumulates the balloon's freed pages
/// across queries: Firecracker clears its discard log on every query while
/// the union accumulates, so within one query the fresh `discarded_ranges`
/// alone would miss every earlier round's frees. The fresh delta is
/// subtracted as well, so callers may pass the mask before or after folding
/// the delta into it. The union covers every page written since the memory
/// baseline, so pages swapped out earlier and only read back in since are
/// re-advised;
/// pages written and then discarded are dead (their content reads as zero
/// and they are typically non-present); pages still in swap stay in the
/// union but advising them costs a walk without I/O. The window — writes
/// since the previous query — is excluded: resume re-accesses the last run
/// round's writes within seconds, so advising them would only buy a
/// swap-out now followed by a swap-in on resume, and a first query's window
/// equals the union, leaving the first pause after a baseline nothing to
/// advise. Returns `None` when nothing survives; an error means the
/// response cannot be advised as-is and the caller should fall back to the
/// full region list.
pub(crate) fn pageout_regions_from_dirty(
    ranges: &DirtyMemoryRanges,
    discard_mask: &[DirtyMemoryRange],
) -> Result<Option<Vec<GuestRegionUffdMapping>>> {
    ensure!(
        ranges.page_size == PAGEOUT_PAGE_SIZE as i32,
        "unsupported pageout page size {}",
        ranges.page_size
    );
    let victims = subtract_ranges(
        &subtract_ranges(
            &ranges.ranges,
            &union_ranges(discard_mask, &ranges.discarded_ranges),
        ),
        &ranges.dirty_ranges,
    );
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
        // Queue latency only applies to the first pickup; later turns are
        // round-robin scheduling, already covered by the rounds counter.
        if job.rounds == 0 {
            metrics::histogram!("agentenv_v_pause_pageout_queue_wait_seconds")
                .record(job.enqueued_at.elapsed().as_secs_f64());
        }

        let PageoutJob {
            id,
            ref sandbox_id,
            pid,
            ref pidfd,
            ref regions,
            ref mut cursor,
            chunk_bytes,
            ref control,
            rss_before,
            enqueued_at,
            ref mut rounds,
        } = job;

        control.in_flight.store(id, Ordering::SeqCst);
        let chunk_started = Instant::now();
        // Split borrows so the scoped-closure can advise through `cursor`
        // while reading the immutable job fields.
        //
        // `process_madvise` on the Firecracker process rides the kernel's
        // ptrace-read access check (`mm_access`), which demands effective
        // `CAP_SYS_PTRACE` even for same-uid targets with an empty capability
        // set — without it every chunk fails with `EACCES`. `CAP_SYS_NICE`
        // covers the reclaim advice itself.
        let outcome = run_with_scoped_capabilities(&[CAP_SYS_NICE, CAP_SYS_PTRACE], || {
            pageout_chunk(pidfd.as_raw_fd(), regions, cursor, chunk_bytes)
        });
        control.in_flight.store(0, Ordering::SeqCst);
        metrics::histogram!(
            "agentenv_v_pause_pageout_chunk_duration_seconds",
            "status" => result_status(outcome.is_ok()),
        )
        .record(chunk_started.elapsed().as_secs_f64());

        match outcome {
            Ok((advised, complete)) => {
                if advised > 0 {
                    metrics::counter!("agentenv_v_pause_pageout_advised_bytes_total")
                        .increment(advised);
                }
                if complete {
                    control.note_finished(id);
                    let rss_after = pid.and_then(process_rss_bytes);
                    let reclaimed = match (rss_before, rss_after) {
                        (Some(before), Some(after)) => Some(before.saturating_sub(after)),
                        _ => None,
                    };
                    if let Some(rss) = rss_after {
                        metrics::histogram!(
                            "agentenv_v_pause_pageout_process_rss_bytes",
                            "phase" => "after",
                        )
                        .record(rss as f64);
                    }
                    if let Some(reclaimed) = reclaimed {
                        metrics::counter!("agentenv_v_pause_pageout_rss_reclaimed_bytes_total")
                            .increment(reclaimed);
                    }
                    debug!(
                        sandbox_id = %sandbox_id,
                        rounds = *rounds,
                        rss_before = ?rss_before,
                        rss_after = ?rss_after,
                        reclaimed = ?reclaimed,
                        "vPause guest memory pageout complete"
                    );
                    metrics::histogram!("agentenv_v_pause_pageout_job_duration_seconds")
                        .record(enqueued_at.elapsed().as_secs_f64());
                    metrics::counter!("agentenv_v_pause_pageout_completions_total").increment(1);
                } else if control.is_current(id) {
                    // Round-robin: go to the back of the queue and let other
                    // sandboxes advance one chunk first.
                    *rounds += 1;
                    metrics::counter!("agentenv_v_pause_pageout_requeue_rounds_total").increment(1);
                    queue.enqueue(job);
                } else {
                    control.note_finished(id);
                }
            }
            Err(error) => {
                // A chunk can also lose the race it was already allowed to
                // lose: the Firecracker process dies before or during the
                // advise (teardown, graceful shutdown, or a host OOM kill —
                // Firecracker runs with `oom_score_adj=1000`, so it dies
                // first under memory pressure), or the job is cancelled
                // while the chunk is in flight. The pageout is moot in both
                // cases; log quietly and account them separately so genuine
                // failures stay visible. `Debug` renders the whole error
                // chain; `Display` would stop at the address context and
                // hide the errno behind the failure.
                let superseded = !control.is_current(id);
                let target_gone = target_process_exited(pidfd);
                if superseded || target_gone {
                    let reason = if target_gone {
                        "target_gone"
                    } else {
                        "cancelled"
                    };
                    metrics::counter!(
                        "agentenv_v_pause_pageout_dropped_jobs_total",
                        "reason" => reason,
                    )
                    .increment(1);
                    debug!(
                        sandbox_id = %sandbox_id,
                        rounds = *rounds,
                        target_gone,
                        superseded,
                        error = ?error,
                        "vPause pageout dropped after losing its target"
                    );
                    control.note_finished(id);
                } else {
                    warn!(
                        sandbox_id = %sandbox_id,
                        error = ?error,
                        "vPause pageout chunk failed; dropping remaining pageout"
                    );
                    control.note_finished(id);
                    metrics::counter!("agentenv_v_pause_pageout_failures_total").increment(1);
                }
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

    /// The dirty-driven victims are the cumulative dirty view minus the
    /// balloon discard log: pieces cut at the discarded pages keep the source
    /// range's host base, and a fully covered union yields nothing.
    #[test]
    fn pageout_regions_from_dirty_subtracts_discards() -> Result<()> {
        let base = 0x7f00_0000_0000i64;
        let ranges = DirtyMemoryRanges::new(
            4096,
            (8 * PAGEOUT_PAGE_SIZE) as i64,
            vec![
                DirtyMemoryRange::new(base, 0, (3 * PAGEOUT_PAGE_SIZE) as i64),
                DirtyMemoryRange::new(
                    base + 5 * PAGEOUT_PAGE_SIZE as i64,
                    (5 * PAGEOUT_PAGE_SIZE) as i64,
                    PAGEOUT_PAGE_SIZE as i64,
                ),
            ],
            Vec::new(),
            vec![DirtyMemoryRange::new(
                base + PAGEOUT_PAGE_SIZE as i64,
                PAGEOUT_PAGE_SIZE as i64,
                PAGEOUT_PAGE_SIZE as i64,
            )],
        );

        let regions = pageout_regions_from_dirty(&ranges, &[])
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

    /// Window pages — the last run round's writes — are spared: resume
    /// re-accesses them within seconds, so only the stale union remainder is
    /// advised, in ascending slot order.
    #[test]
    fn pageout_regions_from_dirty_excludes_window() -> Result<()> {
        let base = 0x7f00_0000_0000i64;
        let page = PAGEOUT_PAGE_SIZE as i64;
        let ranges = DirtyMemoryRanges::new(
            4096,
            6 * page,
            vec![DirtyMemoryRange::new(base, 0, 6 * page)],
            vec![DirtyMemoryRange::new(base + 4 * page, 4 * page, page)],
            Vec::new(),
        );

        let regions = pageout_regions_from_dirty(&ranges, &[])
            .expect("conversion succeeds")
            .expect("victims survive");
        assert_eq!(regions.len(), 2);
        assert_eq!(regions[0].offset, 0);
        assert_eq!(regions[0].size, 4 * page);
        assert_eq!(regions[1].offset, 5 * page);
        Ok(())
    }

    /// The first query clears nothing beforehand, so its window equals the
    /// union and the first pause after a baseline advises nothing.
    #[test]
    fn pageout_regions_from_dirty_skips_first_query() -> Result<()> {
        let base = 0x7f00_0000_0000i64;
        let page = PAGEOUT_PAGE_SIZE as i64;
        let ranges = DirtyMemoryRanges::new(
            4096,
            2 * page,
            vec![DirtyMemoryRange::new(base, 0, 2 * page)],
            vec![DirtyMemoryRange::new(base, 0, 2 * page)],
            Vec::new(),
        );
        assert!(pageout_regions_from_dirty(&ranges, &[])?.is_none());
        Ok(())
    }

    /// Discards accumulated from earlier queries keep masking their pages
    /// even when the current query's discard log is empty — Firecracker
    /// clears it on every query while the union keeps the pages listed.
    #[test]
    fn pageout_regions_from_dirty_subtracts_accumulated_mask() -> Result<()> {
        let base = 0x7f00_0000_0000i64;
        let page = PAGEOUT_PAGE_SIZE as i64;
        let ranges = DirtyMemoryRanges::new(
            4096,
            6 * page,
            vec![DirtyMemoryRange::new(base, 0, 6 * page)],
            Vec::new(),
            Vec::new(),
        );
        let mask = vec![DirtyMemoryRange::new(base + page, page, 2 * page)];

        let regions = pageout_regions_from_dirty(&ranges, &mask)?.expect("victims survive");
        assert_eq!(regions.len(), 2);
        assert_eq!(regions[0].offset, 0);
        assert_eq!(regions[0].size, page);
        assert_eq!(regions[1].offset, 3 * page);
        Ok(())
    }

    /// `advance_discard_mask` drops pages the guest re-wrote in the last
    /// window — they hold live data again — and absorbs the fresh discard
    /// delta, keeping the mask coalesced and ascending.
    #[test]
    fn advance_discard_mask_unmasks_rewrites_and_absorbs_discards() {
        let base = 0x7f00_0000_0000i64;
        let page = PAGEOUT_PAGE_SIZE as i64;
        let mask = vec![
            DirtyMemoryRange::new(base + page, page, 2 * page),
            DirtyMemoryRange::new(base + 5 * page, 5 * page, 2 * page),
        ];
        let window = vec![DirtyMemoryRange::new(base + 2 * page, 2 * page, page)];
        let discards = vec![DirtyMemoryRange::new(base + 8 * page, 8 * page, page)];

        let mask = advance_discard_mask(mask, &window, &discards);
        assert_eq!(
            mask,
            vec![
                DirtyMemoryRange::new(base + page, page, page),
                DirtyMemoryRange::new(base + 5 * page, 5 * page, 2 * page),
                DirtyMemoryRange::new(base + 8 * page, 8 * page, page),
            ]
        );
    }

    /// `union_ranges` interleaves both inputs in image-offset order and
    /// coalesces overlapping and adjacent pieces.
    #[test]
    fn union_ranges_merges_overlapping_adjacent_and_interleaved() {
        let base = 0x7f00_0000_0000i64;
        let page = PAGEOUT_PAGE_SIZE as i64;
        let a = vec![
            DirtyMemoryRange::new(base, 0, 3 * page),
            DirtyMemoryRange::new(base + 5 * page, 5 * page, page),
        ];
        let b = vec![
            DirtyMemoryRange::new(base + 2 * page, 2 * page, 2 * page),
            DirtyMemoryRange::new(base + 6 * page, 6 * page, 2 * page),
            DirtyMemoryRange::new(base + 10 * page, 10 * page, page),
        ];

        assert_eq!(
            union_ranges(&a, &b),
            vec![
                DirtyMemoryRange::new(base, 0, 4 * page),
                DirtyMemoryRange::new(base + 5 * page, 5 * page, 3 * page),
                DirtyMemoryRange::new(base + 10 * page, 10 * page, page),
            ]
        );
    }

    #[test]
    fn pageout_regions_from_dirty_reports_empty_union() -> Result<()> {
        let ranges = DirtyMemoryRanges::new(4096, 0, Vec::new(), Vec::new(), Vec::new());
        assert!(pageout_regions_from_dirty(&ranges, &[])?.is_none());

        // A union fully covered by discards also yields nothing to advise.
        let covered = DirtyMemoryRanges::new(
            4096,
            (2 * PAGEOUT_PAGE_SIZE) as i64,
            vec![DirtyMemoryRange::new(
                0x1000_0000,
                0,
                (2 * PAGEOUT_PAGE_SIZE) as i64,
            )],
            Vec::new(),
            vec![DirtyMemoryRange::new(
                0x1000_0000,
                0,
                (2 * PAGEOUT_PAGE_SIZE) as i64,
            )],
        );
        assert!(pageout_regions_from_dirty(&covered, &[])?.is_none());
        Ok(())
    }

    #[test]
    fn pageout_regions_from_dirty_rejects_unsupported_page_size() {
        let ranges = DirtyMemoryRanges::new(
            65536,
            PAGEOUT_PAGE_SIZE as i64,
            vec![DirtyMemoryRange::new(0, 0, PAGEOUT_PAGE_SIZE as i64)],
            Vec::new(),
            Vec::new(),
        );
        assert!(pageout_regions_from_dirty(&ranges, &[]).is_err());
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

    #[test]
    fn parse_vm_rss_reads_proc_status_line() {
        let status = "Name:\tfirecracker\nVmRSS:\t  123456 kB\nVmSwap:\t  1000 kB\n";
        assert_eq!(parse_vm_rss(status), Some(123456 * 1024));
        assert_eq!(parse_vm_rss("Name:\tfirecracker\n"), None);
        assert_eq!(parse_vm_rss("VmRSS:\tinvalid kB\n"), None);
        assert_eq!(parse_vm_rss("VmRSS:\t\n"), None);
    }
}

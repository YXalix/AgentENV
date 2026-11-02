//! Best-effort background pageout for vPaused sandboxes.
//!
//! `v_pause` stops the guest vCPUs immediately and hands the guest-memory
//! pageout to this module instead of performing it synchronously: thousands
//! of concurrent pauses would otherwise funnel every
//! `process_madvise(MADV_PAGEOUT)` call into the host swap subsystem at the
//! same instant, and the burst serializes the whole machine on swap-slot and
//! zswap/zram locks.
//!
//! A fixed pool of worker threads serves one FIFO queue. Each job belongs to
//! one sandbox and advises at most one chunk of guest memory per turn before
//! re-enqueueing itself, so pending sandboxes advance in round-robin order
//! and no large guest starves the rest. A job carries its own pidfd and
//! region list, so workers never hold sandbox locks. `v_resume`, teardown,
//! and snapshot paths only bump the owning [`PageoutControl`] generation; the
//! worker notices at the next chunk boundary and drops the job. An in-flight
//! chunk cannot be preempted, so draining waits at most one chunk (bounded by
//! `pageout_chunk_mib`).

use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use firecracker_client::models::GuestRegionUffdMapping;
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

    /// Cancels, then waits for an in-flight chunk to finish so the caller can
    /// safely snapshot or resume the guest.
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

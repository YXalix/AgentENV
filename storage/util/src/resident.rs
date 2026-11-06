//! Keeps host daemon processes resident under guest-memory swap pressure.
//!
//! vPause pages guest memory into host swap, so the host spends its life
//! close to reclaim pressure. If the swap also claims the AgentENV server or
//! the ublk daemon, every vPause resume turns into a storm of swap-in faults
//! on the daemon's own code, heap, and io_uring rings — exactly when resume
//! latency matters most. Call [`lock_process_memory`] early at startup, while
//! the resident set is still small.

use anyhow::{Context, Result};
use nix::sys::mman::{mlockall, MlockAllFlags};

/// Locks every current and future page of this process into RAM
/// (`mlockall(MCL_CURRENT | MCL_FUTURE)`).
///
/// Locking is bounded by `RLIMIT_MEMLOCK`; the installed systemd unit grants
/// `LimitMEMLOCK=infinity` and the spawned ublk daemon inherits it. With
/// `MCL_FUTURE`, later mappings (jemalloc growth, ublk I/O buffers) lock as
/// they appear, trading their reclaimability for guaranteed residency — the
/// daemon RSS is small next to the guest memory vPause reclaims.
///
/// Best-effort: failure is recoverable, so callers warn and continue with a
/// swappable daemon instead of refusing to start.
pub fn lock_process_memory() -> Result<()> {
    mlockall(MlockAllFlags::MCL_CURRENT | MlockAllFlags::MCL_FUTURE).context(
        "mlockall(MCL_CURRENT | MCL_FUTURE) failed; check RLIMIT_MEMLOCK (ulimit -l or LimitMEMLOCK)",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Needs real memlock headroom (the installed service sets
    /// `LimitMEMLOCK=infinity`); a default 64 KiB limit fails the call.
    #[test]
    #[ignore = "requires RLIMIT_MEMLOCK headroom beyond the current RSS"]
    fn lock_process_memory_succeeds_with_headroom() {
        lock_process_memory().expect("mlockall succeeds with memlock headroom");
    }
}

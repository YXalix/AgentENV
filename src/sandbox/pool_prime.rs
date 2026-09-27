//! Shared wait loop for startup warm-pool priming.

use std::time::{Duration, Instant};

use tracing::{info, warn};
use warm_pool::WarmPool;

const PRIME_POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Poll `pool` until it holds `target` idle entries or `timeout` elapses.
/// Timeout is logged, not returned: priming is best-effort and cold paths
/// stay available.
pub(crate) async fn wait_until_primed<T: Send>(
    pool: &WarmPool<T>,
    label: &str,
    target: usize,
    timeout: Duration,
) {
    let started = Instant::now();
    loop {
        let warm = pool.len();
        if warm >= target {
            info!(
                warm,
                elapsed_ms = started.elapsed().as_millis(),
                "{label} primed"
            );
            return;
        }
        if started.elapsed() >= timeout {
            warn!(
                warm,
                target, "{label} prime timed out; continuing with partial warm-up"
            );
            return;
        }
        tokio::time::sleep(PRIME_POLL_INTERVAL).await;
    }
}

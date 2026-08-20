//! Dedicated worker thread owning the [`UrmaDriver`].
//!
//! All driver calls (register/unregister/post) are funneled through a command
//! channel, and the same loop polls completions and resolves per-read
//! `oneshot` futures. Keeping driver access on one thread mirrors the
//! ublk/overlaybd worker patterns elsewhere in the workspace and sidesteps
//! any thread-safety surprises in `liburma`.
//!
//! The loop currently polls with a short idle sleep; wiring the context
//! `async_fd` into an event-driven wakeup is left as a follow-up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::oneshot;

use super::driver::{RegionAccess, RegionWire, UrmaCompletion, UrmaDriver, UrmaReadOp};

const IDLE_POLL_INTERVAL: Duration = Duration::from_micros(200);
/// Completions drained per poll iteration.
const POLL_BATCH: usize = 64;

enum Command {
    RegisterRegion {
        addr: usize,
        len: u64,
        access: RegionAccess,
        reply: oneshot::Sender<anyhow::Result<RegionWire>>,
    },
    UnregisterRegion {
        addr: usize,
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    PostRead {
        op: UrmaReadOp,
        reply: oneshot::Sender<anyhow::Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Async handle onto the worker-owned driver.
#[derive(Debug)]
pub(crate) struct UrmaIo {
    cmd_tx: Mutex<Option<std::sync::mpsc::Sender<Command>>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<anyhow::Result<()>>>>>,
    next_ctx: AtomicU64,
    shutdown_started: AtomicBool,
}

impl UrmaIo {
    pub(crate) fn start(driver: Box<dyn UrmaDriver>) -> Self {
        let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Command>();
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let worker_pending = Arc::clone(&pending);

        // The worker exits when it receives Shutdown (or when every sender is
        // dropped); detaching the JoinHandle keeps the API synchronous.
        let _ = std::thread::Builder::new()
            .name("aenv-p2p-ub".to_string())
            .spawn(move || run_worker(driver, cmd_rx, worker_pending))
            .expect("spawn ub P2P worker thread");

        Self {
            cmd_tx: Mutex::new(Some(cmd_tx)),
            pending,
            next_ctx: AtomicU64::new(1),
            shutdown_started: AtomicBool::new(false),
        }
    }

    async fn roundtrip<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<anyhow::Result<T>>) -> Command,
    ) -> anyhow::Result<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = make(reply_tx);
        {
            let guard = self.cmd_tx.lock().expect("urma io channel lock");
            let Some(tx) = guard.as_ref() else {
                anyhow::bail!("ub P2P worker is shut down");
            };
            tx.send(command)
                .map_err(|_| anyhow::anyhow!("ub P2P worker channel closed"))?;
        }
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("ub P2P worker dropped a reply"))?
    }

    pub(crate) async fn register_region(
        &self,
        addr: usize,
        len: u64,
        access: RegionAccess,
    ) -> anyhow::Result<RegionWire> {
        self.roundtrip(|reply| Command::RegisterRegion {
            addr,
            len,
            access,
            reply,
        })
        .await
    }

    pub(crate) async fn unregister_region(&self, addr: usize) -> anyhow::Result<()> {
        self.roundtrip(|reply| Command::UnregisterRegion { addr, reply })
            .await
    }

    pub(crate) async fn post_read(&self, op: UrmaReadOp) -> anyhow::Result<()> {
        self.roundtrip(|reply| Command::PostRead { op, reply })
            .await
    }

    /// Reserve a completion slot for an upcoming [`UrmaIo::post_read`] and
    /// return its `user_ctx` token plus the completion receiver.
    pub(crate) fn alloc_completion(&self) -> (u64, oneshot::Receiver<anyhow::Result<()>>) {
        let user_ctx = self.next_ctx.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("urma completion lock")
            .insert(user_ctx, tx);
        (user_ctx, rx)
    }

    /// Drop a completion slot whose read will no longer be awaited (e.g.
    /// after a timeout). Returns `false` if the completion already fired.
    pub(crate) fn cancel_completion(&self, user_ctx: u64) -> bool {
        self.pending
            .lock()
            .expect("urma completion lock")
            .remove(&user_ctx)
            .is_some()
    }

    pub(crate) async fn shutdown(&self) {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let sent = {
            let guard = self.cmd_tx.lock().expect("urma io channel lock");
            match guard.as_ref() {
                Some(tx) => tx.send(Command::Shutdown { reply: reply_tx }).is_ok(),
                None => false,
            }
        };
        if sent {
            let _ = reply_rx.await;
        }
        *self.cmd_tx.lock().expect("urma io channel lock") = None;
        self.pending.lock().expect("urma completion lock").clear();
    }
}

fn run_worker(
    driver: Box<dyn UrmaDriver>,
    cmd_rx: std::sync::mpsc::Receiver<Command>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<anyhow::Result<()>>>>>,
) {
    let mut completions: Vec<UrmaCompletion> = Vec::with_capacity(POLL_BATCH);
    loop {
        let command = match cmd_rx.recv_timeout(IDLE_POLL_INTERVAL) {
            Ok(command) => Some(command),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };

        match command {
            Some(Command::RegisterRegion {
                addr,
                len,
                access,
                reply,
            }) => {
                let _ = reply.send(driver.register_region(addr, len, access));
            }
            Some(Command::UnregisterRegion { addr, reply }) => {
                let _ = reply.send(driver.unregister_region(addr));
            }
            Some(Command::PostRead { op, reply }) => {
                let _ = reply.send(driver.post_read(op));
            }
            Some(Command::Shutdown { reply }) => {
                driver.shutdown();
                let _ = reply.send(());
                break;
            }
            None => {}
        }

        completions.clear();
        driver.poll_completions(&mut completions);
        if completions.is_empty() {
            continue;
        }
        let mut slots = pending.lock().expect("urma completion lock");
        for completion in completions.drain(..) {
            if let Some(sender) = slots.remove(&completion.user_ctx) {
                let result = if completion.status == 0 {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "ub read failed with URMA completion status {}",
                        completion.status
                    ))
                };
                let _ = sender.send(result);
            }
        }
    }

    // Drain late completions so awaited readers see errors, not hangs.
    let mut leftovers: Vec<UrmaCompletion> = Vec::new();
    driver.poll_completions(&mut leftovers);
    let mut slots = pending.lock().expect("urma completion lock");
    for completion in leftovers {
        if let Some(sender) = slots.remove(&completion.user_ctx) {
            let _ = sender.send(Err(anyhow::anyhow!("ub driver shut down mid-read")));
        }
    }
    for (_, sender) in slots.drain() {
        let _ = sender.send(Err(anyhow::anyhow!("ub driver shut down mid-read")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::urma::driver::LoopbackUrmaDriver;

    #[tokio::test]
    async fn reads_complete_through_worker_loop() {
        let driver = LoopbackUrmaDriver::new("worker-test");
        let source = [9u8, 8, 7, 6, 5, 4, 3, 2];
        let mut dest = [0u8; 4];
        let region = driver
            .register_region(
                source.as_ptr() as usize,
                source.len() as u64,
                RegionAccess::RemoteRead,
            )
            .expect("register");
        let io = UrmaIo::start(Box::new(driver));

        let (user_ctx, done) = io.alloc_completion();
        io.post_read(UrmaReadOp {
            remote_wire: region.wire,
            remote_addr: region.base_va + 3,
            len: 4,
            local_addr: dest.as_mut_ptr() as u64,
            peer_eid: [0; 16],
            peer_jetty_id: 1,
            user_ctx,
        })
        .await
        .expect("post read");

        done.await.expect("completion delivered").expect("read ok");
        assert_eq!(dest, source[3..7]);

        io.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_rejects_new_work() {
        let io = UrmaIo::start(Box::new(LoopbackUrmaDriver::new("shutdown-test")));
        io.shutdown().await;
        let err = io
            .register_region(0x1000, 4096, RegionAccess::LocalOnly)
            .await
            .expect_err("must fail");
        assert!(err.to_string().contains("shut down"));
    }
}

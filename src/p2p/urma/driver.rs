//! Driver seam between the `ub` P2P transport logic and the concrete URMA
//! implementation.
//!
//! [`UrmaDriver`] abstracts the small slice of the urma API the transport
//! needs: registering local memory for remote one-sided reads, importing
//! remote segment handles, binding local jetties to peer jetties, posting
//! READ work requests and polling completions. Two implementations exist:
//!
//! - [`LoopbackUrmaDriver`]: pure-Rust in-process fake used by tests and
//!   `p2p.ub.driver = "loopback"` dev configurations. Wire handles embed the
//!   raw `(va, len)` directly and reads are `memcpy`s, so two transports in
//!   one process can talk to each other without liburma or UB hardware.
//! - `LibUrmaDriver` (behind the `p2p-urma` cargo feature): real FFI calls
//!   into `liburma` as transcribed by the `urma-sys` crate.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

#[cfg(feature = "p2p-urma")]
pub(crate) mod lib_urma;

/// Security token required on both sides of segment/jetty imports. Mirrors
/// the constant used by Mooncake's UB transport so mixed fleets behave the
/// same way; it carries no secrecy. Only the FFI driver consumes it.
#[cfg_attr(not(feature = "p2p-urma"), allow(dead_code))]
pub(crate) const UB_TOKEN: u32 = 0xACFE;

/// Identity of this node's URMA endpoint, advertised inside artifact
/// descriptors so peers can import our segment and bind our jetties.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UbLocalHandle {
    /// URMA device name (e.g. `urma0`).
    pub device: String,
    /// 32 lowercase hex chars encoding the 16-byte EID.
    pub eid: String,
    /// IDs of the jetties peers may bind to.
    pub jetty_ids: Vec<u32>,
}

/// Access granted to a registered region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegionAccess {
    /// Remote peers may one-sided-read this region (published artifacts).
    RemoteRead,
    /// Region is a local scratch buffer (bounce pool); no remote access.
    LocalOnly,
}

/// Result of registering a local memory region for remote reads.
#[derive(Clone, Debug)]
pub(crate) struct RegionWire {
    /// Serialized remote-segment handle (`urma_seg_t` bytes for the FFI
    /// driver, a JSON blob for the loopback driver). Opaque to callers.
    pub wire: Vec<u8>,
    /// Base virtual address of the region; remote reads are addressed as
    /// `base_va + offset`.
    pub base_va: u64,
}

/// One posted one-sided READ.
#[derive(Clone, Debug)]
pub(crate) struct UrmaReadOp {
    /// Wire handle of the remote segment to read from.
    pub remote_wire: Vec<u8>,
    /// Absolute virtual address inside the remote segment.
    pub remote_addr: u64,
    /// Number of bytes to transfer.
    pub len: u32,
    /// Local destination address (inside a region previously registered with
    /// the same driver).
    pub local_addr: u64,
    /// 16-byte EID of the serving peer (for jetty binding); consumed by the
    /// FFI driver, ignored by the loopback driver.
    #[allow(dead_code)]
    pub peer_eid: [u8; 16],
    /// Remote jetty id to bind for this peer; same caveat as `peer_eid`.
    #[allow(dead_code)]
    pub peer_jetty_id: u32,
    /// Opaque completion token returned through [`UrmaCompletion`].
    pub user_ctx: u64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct UrmaCompletion {
    pub user_ctx: u64,
    /// `0` on success, an `URMA_CR_*` status otherwise.
    pub status: i32,
}

/// Raw driver operations. All methods are synchronous and must be safe to
/// call from the transport's dedicated worker thread.
pub(crate) trait UrmaDriver: Send + Sync {
    fn local_handle(&self) -> UbLocalHandle;

    /// Register `[addr, addr + len)` with the requested access.
    fn register_region(
        &self,
        addr: usize,
        len: u64,
        access: RegionAccess,
    ) -> anyhow::Result<RegionWire>;

    /// Release a previously registered region.
    fn unregister_region(&self, addr: usize) -> anyhow::Result<()>;

    /// Post one READ work request. Completion is reported asynchronously
    /// through [`UrmaDriver::poll_completions`].
    fn post_read(&self, op: UrmaReadOp) -> anyhow::Result<()>;

    /// Drain up to `out.capacity()` completions, appending them to `out` and
    /// returning how many were appended.
    fn poll_completions(&self, out: &mut Vec<UrmaCompletion>) -> usize;

    /// Release all driver resources. Idempotent.
    fn shutdown(&self);
}

// ---------------------------------------------------------------------------
// Loopback driver
// ---------------------------------------------------------------------------

/// In-process fake [`UrmaDriver`].
///
/// Wire handles are self-describing JSON of `(base_va, len)` so any two
/// loopback drivers in the same process interoperate; reads become `memcpy`s
/// executed at post time and completions queue up internally.
#[derive(Debug)]
pub(crate) struct LoopbackUrmaDriver {
    handle: UbLocalHandle,
    inner: Mutex<LoopbackInner>,
}

#[derive(Debug, Default)]
struct LoopbackInner {
    regions: Vec<(usize, u64)>,
    completions: Vec<UrmaCompletion>,
}

#[derive(Serialize, Deserialize)]
struct LoopbackWire {
    base_va: u64,
    len: u64,
}

impl LoopbackUrmaDriver {
    /// Create a driver whose advertised identity is derived from `name`.
    pub(crate) fn new(name: &str) -> Self {
        // Distinct-but-deterministic EIDs so tests can assert peer identity
        // propagation without coordinating out of band.
        let mut eid = [0u8; 16];
        eid[0] = b'l';
        eid[1] = b'b';
        for (index, byte) in name.as_bytes().iter().take(14).enumerate() {
            eid[index + 2] = *byte;
        }
        Self {
            handle: UbLocalHandle {
                device: format!("loopback-{name}"),
                eid: hex32(&eid),
                jetty_ids: vec![1, 2],
            },
            inner: Mutex::new(LoopbackInner::default()),
        }
    }
}

/// Encode 16 bytes as 32 lowercase hex chars, matching the wire form used by
/// the FFI driver's `urma-sys::eid_to_hex`.
fn hex32(raw: &[u8; 16]) -> String {
    let mut out = String::with_capacity(32);
    for byte in raw {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Parse 32 lowercase hex chars back into 16 bytes.
pub(crate) fn eid_from_hex32(value: &str) -> Option<[u8; 16]> {
    let bytes = value.as_bytes();
    if bytes.len() != 32 {
        return None;
    }
    let mut raw = [0u8; 16];
    for (index, slot) in raw.iter_mut().enumerate() {
        let high = (bytes[index * 2] as char).to_digit(16)?;
        let low = (bytes[index * 2 + 1] as char).to_digit(16)?;
        *slot = (high * 16 + low) as u8;
    }
    Some(raw)
}

impl UrmaDriver for LoopbackUrmaDriver {
    fn local_handle(&self) -> UbLocalHandle {
        self.handle.clone()
    }

    fn register_region(
        &self,
        addr: usize,
        len: u64,
        _access: RegionAccess,
    ) -> anyhow::Result<RegionWire> {
        let mut inner = self.inner.lock().expect("loopback driver mutex");
        inner.regions.push((addr, len));
        let wire = serde_json::to_vec(&LoopbackWire {
            base_va: addr as u64,
            len,
        })
        .expect("loopback wire serializes");
        Ok(RegionWire {
            wire,
            base_va: addr as u64,
        })
    }

    fn unregister_region(&self, addr: usize) -> anyhow::Result<()> {
        let mut inner = self.inner.lock().expect("loopback driver mutex");
        inner.regions.retain(|(base, _)| *base != addr);
        Ok(())
    }

    fn post_read(&self, op: UrmaReadOp) -> anyhow::Result<()> {
        let wire: LoopbackWire = serde_json::from_slice(&op.remote_wire)
            .map_err(|err| anyhow::anyhow!("invalid loopback wire handle: {err}"))?;
        let remote_end = op
            .remote_addr
            .checked_add(op.len as u64)
            .ok_or_else(|| anyhow::anyhow!("loopback read end overflow"))?;
        anyhow::ensure!(
            op.remote_addr >= wire.base_va && remote_end <= wire.base_va + wire.len,
            "loopback read [{:#x}, {remote_end:#x}) outside segment [{:#x}, {:#x})",
            op.remote_addr,
            wire.base_va,
            wire.base_va + wire.len,
        );

        // SAFETY: same-process loopback; both ranges were validated above
        // and originate from mappings owned by the two transports.
        unsafe {
            std::ptr::copy_nonoverlapping(
                op.remote_addr as *const u8,
                op.local_addr as *mut u8,
                op.len as usize,
            );
        }
        let mut inner = self.inner.lock().expect("loopback driver mutex");
        inner.completions.push(UrmaCompletion {
            user_ctx: op.user_ctx,
            status: 0,
        });
        Ok(())
    }

    fn poll_completions(&self, out: &mut Vec<UrmaCompletion>) -> usize {
        let mut inner = self.inner.lock().expect("loopback driver mutex");
        let drained = inner.completions.len();
        out.append(&mut inner.completions);
        drained
    }

    fn shutdown(&self) {
        let mut inner = self.inner.lock().expect("loopback driver mutex");
        inner.regions.clear();
        inner.completions.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_read_copies_registered_range() {
        let driver = LoopbackUrmaDriver::new("test");
        let source = [1u8, 2, 3, 4, 5, 6, 7, 8];
        let mut dest = [0u8; 8];
        let region = driver
            .register_region(
                source.as_ptr() as usize,
                source.len() as u64,
                RegionAccess::RemoteRead,
            )
            .expect("register");

        driver
            .post_read(UrmaReadOp {
                remote_wire: region.wire.clone(),
                remote_addr: region.base_va + 2,
                len: 4,
                local_addr: dest.as_mut_ptr() as u64,
                peer_eid: [0; 16],
                peer_jetty_id: 1,
                user_ctx: 42,
            })
            .expect("post read");

        let mut completions = Vec::new();
        assert_eq!(driver.poll_completions(&mut completions), 1);
        assert_eq!(completions[0].user_ctx, 42);
        assert_eq!(completions[0].status, 0);
        assert_eq!(&dest[..4], &source[2..6]);
    }

    #[test]
    fn loopback_read_rejects_out_of_range() {
        let driver = LoopbackUrmaDriver::new("test");
        let source = [0u8; 4];
        let dest = [0u8; 4];
        let region = driver
            .register_region(
                source.as_ptr() as usize,
                source.len() as u64,
                RegionAccess::RemoteRead,
            )
            .expect("register");

        let err = driver
            .post_read(UrmaReadOp {
                remote_wire: region.wire,
                remote_addr: region.base_va + 2,
                len: 8,
                local_addr: dest.as_ptr() as u64,
                peer_eid: [0; 16],
                peer_jetty_id: 1,
                user_ctx: 1,
            })
            .expect_err("read must be rejected");
        assert!(err.to_string().contains("outside segment"));
    }

    #[test]
    fn local_handle_eids_are_derived_from_name() {
        let a = LoopbackUrmaDriver::new("a").local_handle();
        let b = LoopbackUrmaDriver::new("b").local_handle();
        assert_ne!(a.eid, b.eid);
        assert!(a.device.contains("loopback-a"));
        assert_eq!(a.jetty_ids, vec![1, 2]);
    }
}

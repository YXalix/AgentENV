//! Real URMA driver over `liburma` via the `urma-sys` FFI bindings.
//!
//! Compiled only with the `p2p-urma` cargo feature. The driver manages one
//! device context, one polling JFC, a small set of pre-created jetties that
//! peers bind to, and caches of imported remote segments and bound peer
//! jetties. Only one-sided READ work requests are posted; completions are
//! drained with `urma_poll_jfc`.
//!
//! Ported semantics follow Mooncake's `UbTransport`/`UrmaContext`
//! (`mooncake-transfer-engine/src/transport/kunpeng_transport/urma/`), minus
//! the multi-device topology, SIEVE endpoint store and retry machinery: the
//! transport layer above provides bounded in-flight reads and timeout-based
//! failure instead.
//!
//! Async events (`urma_get_async_event` on the context async fd) are not yet
//! consumed; a port flap will surface as read errors instead of proactive
//! teardown. Tracked as a follow-up.

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_int;
use std::sync::Mutex;

use anyhow::Context as _;
use urma_sys as sys;

use super::{
    RegionAccess, RegionWire, UbLocalHandle, UrmaCompletion, UrmaDriver, UrmaReadOp, UB_TOKEN,
};

/// Settings for [`LibUrmaDriver`], resolved from `[p2p.ub]` config.
#[derive(Clone, Debug)]
pub(crate) struct LibUrmaDriverConfig {
    /// URMA device name, e.g. `urma0`. Empty picks the first listed device.
    pub device: String,
    /// Number of jetties peers may bind to.
    pub jetty_count: u32,
    /// Per-jetty send queue depth (max outstanding WRs).
    pub max_wr: u32,
    /// Completion queue depth.
    pub jfc_depth: u32,
}

struct BoundJetty {
    jetty: *mut sys::urma_jetty_t,
    tjetty: *mut sys::urma_target_jetty_t,
}

struct Registration {
    tseg: *mut sys::urma_target_seg_t,
    len: u64,
}

/// FFI driver over `liburma`.
pub(crate) struct LibUrmaDriver {
    ctx: *mut sys::urma_context_t,
    jfc: *mut sys::urma_jfc_t,
    /// Jettys peers bind to; their ids are advertised in the local handle.
    jettys: Vec<*mut sys::urma_jetty_t>,
    local: UbLocalHandle,
    max_wr: u32,
    state: Mutex<DriverState>,
}

#[derive(Default)]
struct DriverState {
    /// Registered local regions: base address → registration.
    registered: HashMap<usize, Registration>,
    /// Imported remote segments keyed by their wire form.
    imported_segs: HashMap<Vec<u8>, *mut sys::urma_target_seg_t>,
    /// Bound peer jetties keyed by (EID hex, jetty id), holding the local
    /// jetty picked round-robin.
    bound: HashMap<(String, u32), BoundJetty>,
    next_jetty: usize,
    shutdown: bool,
}

// Raw urma handles are only touched from the single worker thread and during
// shutdown; the Mutex additionally serializes the map mutations.
unsafe impl Send for LibUrmaDriver {}
unsafe impl Sync for LibUrmaDriver {}

fn check_status(op: &'static str, status: c_int) -> anyhow::Result<()> {
    if status == sys::URMA_SUCCESS || status == sys::URMA_EEXIST {
        return Ok(());
    }
    Err(anyhow::anyhow!("{op} failed with urma status {status}"))
}

fn flag_value_reg(access: RegionAccess) -> u32 {
    // urma_reg_seg_flag_t bitfield: token_policy:3, cacheable:1, dsva:1,
    // access:6, non_pin:1, user_iova:1, token_id_valid:1, reserved:18.
    let access_bits = match access {
        // External peers may read; local access always has full permissions.
        RegionAccess::RemoteRead => sys::URMA_ACCESS_READ,
        RegionAccess::LocalOnly => sys::URMA_ACCESS_LOCAL_ONLY,
    };
    (sys::URMA_TOKEN_NONE & 0x7) | (sys::URMA_NON_CACHEABLE << 3) | (access_bits << 5)
}

fn flag_value_import() -> u32 {
    // urma_import_seg_flag_t bitfield: cacheable:1, access:6, mapping:1.
    (sys::URMA_NON_CACHEABLE & 0x1)
        | ((sys::URMA_ACCESS_READ | sys::URMA_ACCESS_WRITE | sys::URMA_ACCESS_ATOMIC) << 1)
        | (sys::URMA_SEG_NOMAP << 7)
}

impl LibUrmaDriver {
    pub(crate) fn new(config: &LibUrmaDriverConfig) -> anyhow::Result<Self> {
        // SAFETY: FFI calls below follow the urma_api.h contracts; every
        // pointer passed is either owned stack data or a live heap object
        // returned by the library.
        unsafe {
            let status = sys::urma_init(std::ptr::null_mut());
            check_status("urma_init", status)?;

            let device = find_device(&config.device)?;
            let (eid_hex, eid_index) = device_eid(device)?;
            let ctx = sys::urma_create_context(device, eid_index);
            if ctx.is_null() {
                anyhow::bail!("urma_create_context failed for device {}", config.device);
            }

            let mut jfc_cfg = sys::urma_jfc_cfg_t {
                depth: config.jfc_depth,
                ..Default::default()
            };
            let jfc = sys::urma_create_jfc(ctx, &mut jfc_cfg);
            if jfc.is_null() {
                sys::urma_delete_context(ctx);
                anyhow::bail!("urma_create_jfc failed (depth {})", config.jfc_depth);
            }

            let mut jettys = Vec::with_capacity(config.jetty_count as usize);
            let mut jetty_ids = Vec::with_capacity(config.jetty_count as usize);
            for _ in 0..config.jetty_count {
                let jfs_cfg = sys::urma_jfs_cfg_t {
                    depth: config.max_wr,
                    trans_mode: sys::URMA_TM_RC,
                    max_sge: 1,
                    max_rsge: 1,
                    rnr_retry: sys::URMA_TYPICAL_RNR_RETRY,
                    err_timeout: sys::URMA_TYPICAL_ERR_TIMEOUT,
                    jfc,
                    ..Default::default()
                };
                let mut jetty_cfg = sys::urma_jetty_cfg_t {
                    jfs_cfg,
                    ..Default::default()
                };
                let jetty = sys::urma_create_jetty(ctx, &mut jetty_cfg);
                if jetty.is_null() {
                    anyhow::bail!("urma_create_jetty failed");
                }
                // SAFETY: jetty is a live object returned by urma; the prefix
                // layout (urma_ctx, jetty_id) matches urma_types.h.
                let jetty_id = std::ptr::addr_of!((*jetty).jetty_id).read_unaligned();
                jetty_ids.push(jetty_id.id);
                jettys.push(jetty);
            }

            Ok(Self {
                ctx,
                jfc,
                jettys,
                local: UbLocalHandle {
                    device: config.device.clone(),
                    eid: eid_hex,
                    jetty_ids,
                },
                max_wr: config.max_wr,
                state: Mutex::new(DriverState::default()),
            })
        }
    }

    fn ensure_live(&self) -> anyhow::Result<()> {
        let state = self.state.lock().expect("urma driver lock");
        if state.shutdown {
            anyhow::bail!("urma driver is shut down");
        }
        Ok(())
    }

    fn local_tseg_for(&self, addr: u64) -> Option<*mut sys::urma_target_seg_t> {
        let state = self.state.lock().expect("urma driver lock");
        state
            .registered
            .iter()
            .find(|(base, reg)| {
                let base = **base as u64;
                addr >= base && addr < base + reg.len
            })
            .map(|(_, reg)| reg.tseg)
    }
}

/// Pick the configured device, or the first available when unset. The
/// returned pointer is owned by the library's device list; the list itself
/// is freed before returning, which matches the mock semantics where device
/// objects outlive the list.
unsafe fn find_device(name: &str) -> anyhow::Result<*mut sys::urma_device_t> {
    if !name.is_empty() {
        let c_name = CString::new(name).context("device name contains NUL")?;
        let device = sys::urma_get_device_by_name(c_name.as_ptr() as *mut _);
        if device.is_null() {
            anyhow::bail!("urma device {name} not found");
        }
        return Ok(device);
    }

    let mut count: c_int = 0;
    let list = sys::urma_get_device_list(&mut count);
    if list.is_null() || count <= 0 {
        anyhow::bail!("no urma devices found");
    }
    let device = *list;
    sys::urma_free_device_list(list);
    Ok(device)
}

/// Read the device EID list and return `(hex, eid_index)` for index 0.
unsafe fn device_eid(device: *mut sys::urma_device_t) -> anyhow::Result<(String, u32)> {
    let mut count: u32 = 0;
    let list = sys::urma_get_eid_list(device, &mut count);
    if list.is_null() || count == 0 {
        anyhow::bail!("urma device has no EIDs");
    }
    let info = *list;
    let hex = sys::eid_to_hex(&info.eid.raw);
    sys::urma_free_eid_list(list);
    Ok((hex, info.eid_index))
}

impl UrmaDriver for LibUrmaDriver {
    fn local_handle(&self) -> UbLocalHandle {
        self.local.clone()
    }

    fn register_region(
        &self,
        addr: usize,
        len: u64,
        access: RegionAccess,
    ) -> anyhow::Result<RegionWire> {
        self.ensure_live()?;
        let mut cfg = sys::urma_seg_cfg_t {
            va: addr as u64,
            len,
            token_value: sys::urma_token_t { token: UB_TOKEN },
            flag: sys::urma_reg_seg_flag_t {
                value: flag_value_reg(access),
            },
            ..Default::default()
        };
        // SAFETY: cfg is live stack data; the returned segment is owned by
        // the library until urma_unregister_seg.
        let tseg = unsafe { sys::urma_register_seg(self.ctx, &mut cfg) };
        if tseg.is_null() {
            anyhow::bail!("urma_register_seg failed for {addr:#x} len {len}");
        }
        // SAFETY: tseg is live; `seg` is its public wire form.
        let wire = unsafe { (*tseg).seg }.to_wire();
        self.state
            .lock()
            .expect("urma driver lock")
            .registered
            .insert(addr, Registration { tseg, len });
        Ok(RegionWire {
            wire,
            base_va: addr as u64,
        })
    }

    fn unregister_region(&self, addr: usize) -> anyhow::Result<()> {
        let registration = self
            .state
            .lock()
            .expect("urma driver lock")
            .registered
            .remove(&addr);
        let Some(registration) = registration else {
            return Ok(());
        };
        // SAFETY: tseg belongs to a live registration created above.
        let status = unsafe { sys::urma_unregister_seg(registration.tseg) };
        check_status("urma_unregister_seg", status)
    }

    fn post_read(&self, op: UrmaReadOp) -> anyhow::Result<()> {
        self.ensure_live()?;
        let (jetty, tjetty) = self.bound_jetty(op.peer_eid, op.peer_jetty_id)?;
        let remote_tseg = self.import_segment(&op.remote_wire)?;
        let local_tseg = self
            .local_tseg_for(op.local_addr)
            .ok_or_else(|| anyhow::anyhow!("local address {:#x} not registered", op.local_addr))?;

        // SAFETY: all tseg/jetty pointers are live objects owned by this
        // driver; the stack WR is copied into the send queue by
        // urma_post_jetty_send_wr before returning.
        unsafe {
            let mut local_sge = sys::urma_sge_t {
                addr: op.local_addr,
                len: op.len,
                tseg: local_tseg,
                ..Default::default()
            };
            let mut remote_sge = sys::urma_sge_t {
                addr: op.remote_addr,
                len: op.len,
                tseg: remote_tseg,
                ..Default::default()
            };
            let mut wr = sys::urma_jfs_wr_t {
                opcode: sys::URMA_OPC_READ,
                flag: sys::urma_jfs_wr_flag_t {
                    value: sys::URMA_JFS_WR_COMPLETE_ENABLE,
                },
                tjetty,
                user_ctx: op.user_ctx,
                ..Default::default()
            };
            wr.rw.src = sys::urma_sg_t {
                sge: &mut remote_sge,
                num_sge: 1,
            };
            wr.rw.dst = sys::urma_sg_t {
                sge: &mut local_sge,
                num_sge: 1,
            };
            let mut bad_wr: *mut sys::urma_jfs_wr_t = std::ptr::null_mut();
            let status = sys::urma_post_jetty_send_wr(jetty, &mut wr, &mut bad_wr);
            check_status("urma_post_jetty_send_wr", status)
        }
    }

    fn poll_completions(&self, out: &mut Vec<UrmaCompletion>) -> usize {
        const BATCH: usize = 64;
        // SAFETY: jfc is live for the driver's lifetime; the CR array is
        // stack-owned.
        let count = unsafe {
            let mut crs = [sys::urma_cr_t::default(); BATCH];
            let polled = sys::urma_poll_jfc(self.jfc, BATCH as c_int, crs.as_mut_ptr());
            if polled <= 0 {
                return 0;
            }
            for cr in crs.iter().take(polled as usize) {
                out.push(UrmaCompletion {
                    user_ctx: cr.user_ctx,
                    status: cr.status,
                });
            }
            polled as usize
        };
        count
    }

    fn shutdown(&self) {
        let mut state = self.state.lock().expect("urma driver lock");
        if state.shutdown {
            return;
        }
        state.shutdown = true;

        // SAFETY: all handles below were created by this driver and are
        // released exactly once here (or via unregister_region).
        unsafe {
            for (_, registration) in state.registered.drain() {
                sys::urma_unregister_seg(registration.tseg);
            }
            for (_, tseg) in state.imported_segs.drain() {
                sys::urma_unimport_seg(tseg);
            }
            for (_, bound) in state.bound.drain() {
                sys::urma_unbind_jetty(bound.jetty);
                sys::urma_unimport_jetty(bound.tjetty);
            }
            for jetty in &self.jettys {
                sys::urma_delete_jetty(*jetty);
            }
            sys::urma_delete_jfc(self.jfc);
            sys::urma_delete_context(self.ctx);
            sys::urma_uninit();
        }
    }
}

impl LibUrmaDriver {
    fn import_segment(&self, wire: &[u8]) -> anyhow::Result<*mut sys::urma_target_seg_t> {
        let mut state = self.state.lock().expect("urma driver lock");
        if let Some(tseg) = state.imported_segs.get(wire) {
            return Ok(*tseg);
        }
        let mut seg = sys::urma_seg_t::from_wire(wire).ok_or_else(|| {
            anyhow::anyhow!("invalid remote segment handle ({} bytes)", wire.len())
        })?;
        let mut token = sys::urma_token_t { token: UB_TOKEN };
        let flag = sys::urma_import_seg_flag_t {
            value: flag_value_import(),
        };
        // SAFETY: seg is a live stack copy of the peer's published handle.
        let tseg = unsafe { sys::urma_import_seg(self.ctx, &mut seg, &mut token, 0, flag) };
        if tseg.is_null() {
            anyhow::bail!("urma_import_seg failed");
        }
        state.imported_segs.insert(wire.to_vec(), tseg);
        Ok(tseg)
    }

    fn bound_jetty(
        &self,
        peer_eid: [u8; 16],
        peer_jetty_id: u32,
    ) -> anyhow::Result<(*mut sys::urma_jetty_t, *mut sys::urma_target_jetty_t)> {
        let key = (sys::eid_to_hex(&peer_eid), peer_jetty_id);
        let mut state = self.state.lock().expect("urma driver lock");
        if let Some(bound) = state.bound.get(&key) {
            return Ok((bound.jetty, bound.tjetty));
        }

        let jetty = self.jettys[state.next_jetty % self.jettys.len()];
        state.next_jetty = state.next_jetty.wrapping_add(1);

        let mut rjetty = sys::urma_rjetty_t {
            trans_mode: sys::URMA_TM_RC,
            policy: sys::URMA_JETTY_GRP_POLICY_RR,
            type_: sys::URMA_JETTY,
            tp_type: sys::URMA_CTP,
            ..Default::default()
        };
        rjetty.jetty_id.eid = sys::urma_eid_t { raw: peer_eid };
        rjetty.jetty_id.id = peer_jetty_id;

        let mut token = sys::urma_token_t { token: UB_TOKEN };
        // SAFETY: rjetty/token are live stack data.
        let tjetty = unsafe { sys::urma_import_jetty(self.ctx, &mut rjetty, &mut token) };
        if tjetty.is_null() {
            anyhow::bail!("urma_import_jetty failed for jetty {peer_jetty_id}");
        }
        // SAFETY: both handles are live.
        let status = unsafe { sys::urma_bind_jetty(jetty, tjetty) };
        check_status("urma_bind_jetty", status)?;
        state.bound.insert(key, BoundJetty { jetty, tjetty });
        Ok((jetty, tjetty))
    }

    /// Number of jetties advertised to peers (for tests).
    #[allow(dead_code)]
    pub(crate) fn jetty_count(&self) -> usize {
        self.jettys.len()
    }

    /// Configured per-jetty send depth (for tests).
    #[allow(dead_code)]
    pub(crate) fn max_wr(&self) -> u32 {
        self.max_wr
    }
}

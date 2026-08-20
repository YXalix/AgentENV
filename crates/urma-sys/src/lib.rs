//! Raw FFI bindings to the openEuler UMDK URMA C API (`liburma`).
//!
//! The type and function declarations below are transcribed by hand from the
//! UMDK headers vendored under `include/` (upstream tag `v25.12.0.B081`,
//! MIT licensed). Only the subset needed for one-sided remote reads over
//! Kunpeng UB is declared; anything unused from the upstream API is left out.
//!
//! # Linking
//!
//! This crate declares the extern surface without a `#[link]` attribute, so
//! building it never requires `liburma` to be installed. Symbols are only
//! resolved when a consuming binary references the functions (the AgentENV
//! server does so under the `p2p-urma` cargo feature). Such binaries must
//! link against `liburma.so`, typically installed from the `umdk-urma-devel`
//! package on openEuler hosts.
//!
//! # Layout notes
//!
//! - `urma_eid_t` is a C union whose largest members are `u64`; it is modeled
//!   as a struct over the raw bytes with the union's alignment preserved.
//! - `urma_ubva_t` is `__attribute__((packed))` and is modeled with
//!   `#[repr(C, packed)]`. Field access must go through unaligned reads; the
//!   helpers on [`urma_seg_t`] do that.
//! - Structures containing `pthread_mutex_t`/`pthread_cond_t`
//!   (`urma_context_t`, `urma_jfc_t`, `urma_jetty_t`, ...) are opaque: they
//!   are only passed back to the C API as pointers. Where field access is
//!   required (e.g. reading a registered segment's wire form), the vendored
//!   API offers dedicated accessors or fully-defined sub-structs.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub const URMA_EID_SIZE: usize = 16;
pub const URMA_MAX_NAME: usize = 64;
pub const URMA_MAX_PATH: usize = 4096;

// ---------------------------------------------------------------------------
// Opcodes / flags / constants (from urma_opcode.h)
// ---------------------------------------------------------------------------

pub const URMA_TOKEN_NONE: u32 = 0;

pub const URMA_ACCESS_LOCAL_ONLY: u32 = 0x1 << 0;
pub const URMA_ACCESS_READ: u32 = 0x1 << 1;
pub const URMA_ACCESS_WRITE: u32 = 0x1 << 2;
pub const URMA_ACCESS_ATOMIC: u32 = 0x1 << 3;

pub const URMA_NON_CACHEABLE: u32 = 0;
pub const URMA_CACHEABLE: u32 = 1;

pub const URMA_SEG_NOMAP: u32 = 0;
pub const URMA_SEG_MAPPED: u32 = 1;

/// `urma_transport_mode_t`
pub const URMA_TM_RM: u32 = 0x1;
pub const URMA_TM_RC: u32 = 0x1 << 1;
pub const URMA_TM_UM: u32 = 0x1 << 2;

/// `urma_transport_type_t`
pub const URMA_TRANSPORT_INVALID: c_int = -1;
pub const URMA_TRANSPORT_UB: c_int = 0;
pub const URMA_TRANSPORT_IB: c_int = 1;
pub const URMA_TRANSPORT_IP: c_int = 2;
pub const URMA_TRANSPORT_SOFTUB: c_int = 3;
pub const URMA_TRANSPORT_HNS_UB: c_int = 5;

/// `urma_tp_type_t`
pub const URMA_RTP: u32 = 0;
pub const URMA_CTP: u32 = 1;
pub const URMA_UTP: u32 = 2;

/// `urma_target_type_t`
pub const URMA_JFR: u32 = 0;
pub const URMA_JETTY: u32 = 1;
pub const URMA_JETTY_GROUP: u32 = 2;

/// `urma_jetty_grp_policy_t`
pub const URMA_JETTY_GRP_POLICY_RR: u32 = 0;

/// `urma_place_order_t` / `urma_jfs_wr_flag_t.bs`
pub const URMA_JFS_WR_COMPLETE_ENABLE: u32 = 0x1 << 5;

/// `urma_opcode_t`
pub const URMA_OPC_WRITE: u32 = 0x00;
pub const URMA_OPC_READ: u32 = 0x10;
pub const URMA_OPC_SEND: u32 = 0x40;

/// `urma_status_t` (`c_int`)
pub const URMA_SUCCESS: c_int = 0;
pub const URMA_EAGAIN: c_int = 11;
pub const URMA_ENOMEM: c_int = 12;
pub const URMA_ETIMEOUT: c_int = 110;
pub const URMA_EINVAL: c_int = 22;
pub const URMA_EEXIST: c_int = 17;
pub const URMA_EINPROGRESS: c_int = 115;
pub const URMA_FAIL: c_int = 0x1000;

/// `urma_cr_status_t` (`c_int`)
pub const URMA_CR_SUCCESS: c_int = 0;
pub const URMA_CR_WR_FLUSH_ERR: c_int = 12;

/// `urma_jetty_attr_mask_t`
pub const JETTY_RX_THRESHOLD: u32 = 0x1;
pub const JETTY_STATE: u32 = 0x1 << 1;

/// `urma_jetty_state_t`
pub const URMA_JETTY_STATE_RESET: u32 = 0;
pub const URMA_JETTY_STATE_READY: u32 = 1;

pub const URMA_TYPICAL_RNR_RETRY: u8 = 7;
pub const URMA_TYPICAL_ERR_TIMEOUT: u8 = 17;

// ---------------------------------------------------------------------------
// Types (from urma_types.h)
// ---------------------------------------------------------------------------

/// C: `union urma_eid` — modeled over the raw bytes with the union's
/// alignment (largest member is `u64`).
#[repr(C, align(8))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct urma_eid_t {
    pub raw: [u8; URMA_EID_SIZE],
}

impl Default for urma_eid_t {
    fn default() -> Self {
        Self {
            raw: [0; URMA_EID_SIZE],
        }
    }
}

impl std::fmt::Debug for urma_eid_t {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "urma_eid_t({})", eid_to_hex(&self.raw))
    }
}

/// Format 16 raw EID bytes as 32 lowercase hex characters.
pub fn eid_to_hex(raw: &[u8; URMA_EID_SIZE]) -> String {
    let mut out = String::with_capacity(URMA_EID_SIZE * 2);
    for byte in raw {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Parse 32 hex characters into raw EID bytes.
pub fn eid_from_hex(value: &str) -> Option<[u8; URMA_EID_SIZE]> {
    let bytes = value.as_bytes();
    if bytes.len() != URMA_EID_SIZE * 2 {
        return None;
    }
    let mut raw = [0u8; URMA_EID_SIZE];
    for (index, slot) in raw.iter_mut().enumerate() {
        let high = (bytes[index * 2] as char).to_digit(16)?;
        let low = (bytes[index * 2 + 1] as char).to_digit(16)?;
        *slot = (high * 16 + low) as u8;
    }
    Some(raw)
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_init_attr_t {
    pub token: u64,
    pub uasid: u32,
}

/// C: `union urma_reg_seg_flag_t` — accessed through the `value` word.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_reg_seg_flag_t {
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_token_t {
    pub token: u32,
}

/// C: `urma_ubva_t` — upstream is `__attribute__((packed))`. The embedded
/// EID is spelled as raw bytes because a packed struct cannot carry the
/// aligned `urma_eid_t` representation (in C the packing collapses it to 16
/// plain bytes too).
#[repr(C, packed)]
#[derive(Clone, Copy)]
pub struct urma_ubva_t {
    pub eid: [u8; URMA_EID_SIZE],
    pub uasid: u32,
    pub va: u64,
}

impl Default for urma_ubva_t {
    fn default() -> Self {
        Self {
            eid: [0; URMA_EID_SIZE],
            uasid: 0,
            va: 0,
        }
    }
}

/// C: `union urma_seg_attr_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_seg_attr_t {
    pub value: u32,
}

/// Wire form of a registered memory segment. This is the only part of
/// [`urma_target_seg_t`] that is meaningful to remote peers; it is exactly
/// what gets serialized into artifact descriptors.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_seg_t {
    pub ubva: urma_ubva_t,
    pub len: u64,
    pub attr: urma_seg_attr_t,
    pub token_id: u32,
}

impl urma_seg_t {
    /// Serialize the segment into the byte form shared with peers.
    ///
    /// The wire form is ABI-coupled to the exact `liburma` version; all nodes
    /// in a cluster must run the same version (see crate docs).
    pub fn to_wire(&self) -> Vec<u8> {
        let bytes = self as *const Self as *const u8;
        // SAFETY: reading `size_of::<Self>()` bytes starting at a valid
        // reference to `self` is in-bounds; the copy is byte-wise.
        unsafe { std::slice::from_raw_parts(bytes, std::mem::size_of::<Self>()).to_vec() }
    }

    /// Parse the wire form produced by [`urma_seg_t::to_wire`].
    pub fn from_wire(wire: &[u8]) -> Option<Self> {
        if wire.len() != std::mem::size_of::<Self>() {
            return None;
        }
        // SAFETY: the length check above guarantees the read stays within the
        // slice, and `urma_seg_t` is a plain-old-data type with no padding
        // requirements beyond what `to_wire` produced.
        Some(unsafe { std::ptr::read_unaligned(wire.as_ptr() as *const Self) })
    }

    /// Base virtual address of the segment (unaligned read; struct is packed).
    pub fn base_va(&self) -> u64 {
        // SAFETY: field access on a packed struct via unaligned read.
        unsafe { std::ptr::addr_of!(self.ubva.va).read_unaligned() }
    }

    /// Segment length in bytes (aligned, but kept symmetric with `base_va`).
    pub fn seg_len(&self) -> u64 {
        self.len
    }
}

/// C: `union urma_import_seg_flag_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_import_seg_flag_t {
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_seg_cfg_t {
    pub va: u64,
    pub len: u64,
    pub token_id: *mut urma_token_id_t,
    pub token_value: urma_token_t,
    pub flag: urma_reg_seg_flag_t,
    pub user_ctx: u64,
    pub iova: u64,
}

impl Default for urma_seg_cfg_t {
    fn default() -> Self {
        Self {
            va: 0,
            len: 0,
            token_id: std::ptr::null_mut(),
            token_value: urma_token_t::default(),
            flag: urma_reg_seg_flag_t::default(),
            user_ctx: 0,
            iova: 0,
        }
    }
}

/// Opaque: contains `pthread_mutex_t`; only passed as a pointer.
#[repr(C)]
pub struct urma_context_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_device_t {
    pub name: [c_char; URMA_MAX_NAME],
    pub path: [c_char; URMA_MAX_PATH],
    pub type_: c_int,
    pub ops: *mut c_void,
    pub sysfs_dev: *mut c_void,
}

impl Default for urma_device_t {
    fn default() -> Self {
        Self {
            name: [0; URMA_MAX_NAME],
            path: [0; URMA_MAX_PATH],
            type_: URMA_TRANSPORT_INVALID,
            ops: std::ptr::null_mut(),
            sysfs_dev: std::ptr::null_mut(),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_eid_info_t {
    pub eid: urma_eid_t,
    pub eid_index: u32,
}

/// Opaque: contains `pthread_mutex_t`; only passed as a pointer.
#[repr(C)]
pub struct urma_jfc_t {
    _private: [u8; 0],
}

/// Opaque: receive-side jetty, unused for one-sided reads.
#[repr(C)]
pub struct urma_jfr_t {
    _private: [u8; 0],
}

/// C: `urma_jetty_t`. Only the documented prefix is modeled (context pointer,
/// jetty id, bound remote jetty); the remaining fields contain
/// `pthread_mutex_t`/`pthread_cond_t` and stay opaque. Never construct this
/// type — instances come from `urma_create_jetty`.
#[repr(C)]
pub struct urma_jetty_t {
    pub urma_ctx: *mut urma_context_t,
    pub jetty_id: urma_jetty_id_t,
    pub remote_jetty: *mut urma_target_jetty_t,
    _opaque_tail: [u8; 0],
}

/// Opaque imported remote jetty.
#[repr(C)]
pub struct urma_target_jetty_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jfc_cfg_t {
    pub depth: u32,
    pub flag: u32,
    pub ceqn: u32,
    pub jfce: *mut urma_jfce_t,
    pub user_ctx: u64,
}

/// Opaque completion-event handle.
#[repr(C)]
pub struct urma_jfce_t {
    _private: [u8; 0],
}

/// C: `union urma_jfs_flag_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jfs_flag_t {
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_jfs_cfg_t {
    pub depth: u32,
    pub flag: urma_jfs_flag_t,
    pub trans_mode: u32,
    pub priority: u8,
    pub max_sge: u8,
    pub max_rsge: u8,
    pub max_inline_data: u32,
    pub rnr_retry: u8,
    pub err_timeout: u8,
    pub jfc: *mut urma_jfc_t,
    pub user_ctx: u64,
}

impl Default for urma_jfs_cfg_t {
    fn default() -> Self {
        Self {
            depth: 0,
            flag: urma_jfs_flag_t::default(),
            trans_mode: 0,
            priority: 0,
            max_sge: 0,
            max_rsge: 0,
            max_inline_data: 0,
            rnr_retry: 0,
            err_timeout: 0,
            jfc: std::ptr::null_mut(),
            user_ctx: 0,
        }
    }
}

/// C: the receive half of `urma_jetty_cfg_t`'s anonymous union. The `shared`
/// variant (16 bytes) is the largest, so modeling the union as this struct is
/// layout-compatible; callers that want the deprecated `jfr_cfg` pointer
/// variant cannot express it, which is fine for one-sided reads.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jetty_recv_cfg_t {
    pub jfr: *mut urma_jfr_t,
    pub jfc: *mut urma_jfc_t,
}

/// Opaque jetty group.
#[repr(C)]
pub struct urma_jetty_grp_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_jetty_cfg_t {
    pub id: u32,
    pub flag: u32,
    pub jfs_cfg: urma_jfs_cfg_t,
    pub recv: urma_jetty_recv_cfg_t,
    pub jetty_grp: *mut urma_jetty_grp_t,
    pub user_ctx: u64,
}

impl Default for urma_jetty_cfg_t {
    fn default() -> Self {
        Self {
            id: 0,
            flag: 0,
            jfs_cfg: urma_jfs_cfg_t::default(),
            recv: urma_jetty_recv_cfg_t::default(),
            jetty_grp: std::ptr::null_mut(),
            user_ctx: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jetty_id_t {
    pub eid: urma_eid_t,
    pub uasid: u32,
    pub id: u32,
}

/// C: `union urma_import_jetty_flag_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_import_jetty_flag_t {
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_rjetty_t {
    pub jetty_id: urma_jetty_id_t,
    pub trans_mode: u32,
    pub policy: u32,
    pub type_: u32,
    pub flag: urma_import_jetty_flag_t,
    pub tp_type: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jetty_attr_t {
    pub mask: u32,
    pub rx_threshold: u32,
    pub state: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_target_seg_t {
    pub seg: urma_seg_t,
    pub user_ctx: u64,
    pub mva: u64,
    pub urma_ctx: *mut urma_context_t,
    pub token_id: *mut urma_token_id_t,
    pub handle: u64,
}

/// Opaque token-id object.
#[repr(C)]
pub struct urma_token_id_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_sge_t {
    pub addr: u64,
    pub len: u32,
    pub tseg: *mut urma_target_seg_t,
    pub user_tseg: *mut urma_user_tseg_t,
}

impl Default for urma_sge_t {
    fn default() -> Self {
        Self {
            addr: 0,
            len: 0,
            tseg: std::ptr::null_mut(),
            user_tseg: std::ptr::null_mut(),
        }
    }
}

/// Opaque user target segment (import-exemption path, unused here).
#[repr(C)]
pub struct urma_user_tseg_t {
    _private: [u8; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_sg_t {
    pub sge: *mut urma_sge_t,
    pub num_sge: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_rw_wr_t {
    pub src: urma_sg_t,
    pub dst: urma_sg_t,
    pub target_hint: u8,
    pub notify_data: u64,
}

/// C: `urma_jfs_wr_t`. The payload union is modeled by its largest member
/// (`urma_rw_wr_t`, the read/write payload used here).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_jfs_wr_t {
    pub opcode: u32,
    pub flag: urma_jfs_wr_flag_t,
    pub tjetty: *mut urma_target_jetty_t,
    pub user_ctx: u64,
    pub rw: urma_rw_wr_t,
    pub next: *mut urma_jfs_wr_t,
}

impl Default for urma_jfs_wr_t {
    fn default() -> Self {
        Self {
            opcode: 0,
            flag: urma_jfs_wr_flag_t::default(),
            tjetty: std::ptr::null_mut(),
            user_ctx: 0,
            rw: urma_rw_wr_t::default(),
            next: std::ptr::null_mut(),
        }
    }
}

/// C: `union urma_jfs_wr_flag_t`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct urma_jfs_wr_flag_t {
    pub value: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_cr_t {
    pub status: c_int,
    pub user_ctx: u64,
    pub opcode: c_int,
    pub flag: u8,
    pub completion_len: u32,
    pub local_id: u32,
    pub remote_id: urma_jetty_id_t,
    pub imm_data: u64,
    pub tpn: u32,
    pub user_data: u64,
}

impl Default for urma_cr_t {
    fn default() -> Self {
        Self {
            status: URMA_CR_SUCCESS,
            user_ctx: 0,
            opcode: 0,
            flag: 0,
            completion_len: 0,
            local_id: 0,
            remote_id: urma_jetty_id_t::default(),
            imm_data: 0,
            tpn: 0,
            user_data: 0,
        }
    }
}

/// C: `urma_async_event_t`; `element` is a union of pointers/u32 modeled as a
/// single word. Only passed through to `urma_ack_async_event`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct urma_async_event_t {
    pub urma_ctx: *const urma_context_t,
    pub element: u64,
    pub event_type: c_int,
    pub priv_: *mut c_void,
}

impl Default for urma_async_event_t {
    fn default() -> Self {
        Self {
            urma_ctx: std::ptr::null(),
            element: 0,
            event_type: 0,
            priv_: std::ptr::null_mut(),
        }
    }
}

// ---------------------------------------------------------------------------
// Function declarations (from urma_api.h)
// ---------------------------------------------------------------------------

extern "C" {
    pub fn urma_init(conf: *mut urma_init_attr_t) -> c_int;
    pub fn urma_uninit() -> c_int;

    pub fn urma_get_device_list(num_devices: *mut c_int) -> *mut *mut urma_device_t;
    pub fn urma_free_device_list(device_list: *mut *mut urma_device_t);
    pub fn urma_get_device_by_name(dev_name: *mut c_char) -> *mut urma_device_t;
    pub fn urma_get_eid_list(dev: *mut urma_device_t, cnt: *mut u32) -> *mut urma_eid_info_t;
    pub fn urma_free_eid_list(eid_list: *mut urma_eid_info_t);

    pub fn urma_create_context(dev: *mut urma_device_t, eid_index: u32) -> *mut urma_context_t;
    pub fn urma_delete_context(ctx: *mut urma_context_t) -> c_int;

    pub fn urma_create_jfc(
        ctx: *mut urma_context_t,
        jfc_cfg: *mut urma_jfc_cfg_t,
    ) -> *mut urma_jfc_t;
    pub fn urma_delete_jfc(jfc: *mut urma_jfc_t) -> c_int;

    pub fn urma_create_jetty(
        ctx: *mut urma_context_t,
        jetty_cfg: *mut urma_jetty_cfg_t,
    ) -> *mut urma_jetty_t;
    pub fn urma_delete_jetty(jetty: *mut urma_jetty_t) -> c_int;
    pub fn urma_query_jetty(
        jetty: *mut urma_jetty_t,
        cfg: *mut urma_jetty_cfg_t,
        attr: *mut urma_jetty_attr_t,
    ) -> c_int;
    pub fn urma_modify_jetty(jetty: *mut urma_jetty_t, attr: *mut urma_jetty_attr_t) -> c_int;

    pub fn urma_import_jetty(
        ctx: *mut urma_context_t,
        rjetty: *mut urma_rjetty_t,
        token_value: *mut urma_token_t,
    ) -> *mut urma_target_jetty_t;
    pub fn urma_unimport_jetty(tjetty: *mut urma_target_jetty_t) -> c_int;
    pub fn urma_bind_jetty(jetty: *mut urma_jetty_t, tjetty: *mut urma_target_jetty_t) -> c_int;
    pub fn urma_unbind_jetty(jetty: *mut urma_jetty_t) -> c_int;

    pub fn urma_register_seg(
        ctx: *mut urma_context_t,
        seg_cfg: *mut urma_seg_cfg_t,
    ) -> *mut urma_target_seg_t;
    pub fn urma_unregister_seg(target_seg: *mut urma_target_seg_t) -> c_int;
    pub fn urma_import_seg(
        ctx: *mut urma_context_t,
        seg: *mut urma_seg_t,
        token_value: *mut urma_token_t,
        addr: u64,
        flag: urma_import_seg_flag_t,
    ) -> *mut urma_target_seg_t;
    pub fn urma_unimport_seg(tseg: *mut urma_target_seg_t) -> c_int;

    pub fn urma_post_jetty_send_wr(
        jetty: *mut urma_jetty_t,
        wr: *mut urma_jfs_wr_t,
        bad_wr: *mut *mut urma_jfs_wr_t,
    ) -> c_int;
    pub fn urma_poll_jfc(jfc: *mut urma_jfc_t, cr_cnt: c_int, cr: *mut urma_cr_t) -> c_int;

    pub fn urma_get_async_event(ctx: *mut urma_context_t, event: *mut urma_async_event_t) -> c_int;
    pub fn urma_ack_async_event(event: *mut urma_async_event_t);
}

/// Size of the serialized segment wire form shared between nodes.
pub const URMA_SEG_WIRE_SIZE: usize = std::mem::size_of::<urma_seg_t>();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seg_wire_round_trip_preserves_fields() {
        let mut seg = urma_seg_t::default();
        seg.ubva.va = 0x1234_5678_9abc_def0;
        seg.ubva.uasid = 7;
        seg.len = 1 << 30;
        seg.token_id = 0xACFE;

        let wire = seg.to_wire();
        assert_eq!(wire.len(), URMA_SEG_WIRE_SIZE);
        let parsed = urma_seg_t::from_wire(&wire).expect("wire parses");
        assert_eq!(parsed.base_va(), 0x1234_5678_9abc_def0);
        assert_eq!(parsed.seg_len(), 1 << 30);
    }

    #[test]
    fn seg_wire_rejects_wrong_length() {
        assert!(urma_seg_t::from_wire(&[0u8; 4]).is_none());
    }

    #[test]
    fn eid_hex_round_trip() {
        let raw = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        let hex = eid_to_hex(&raw);
        assert_eq!(hex.len(), 32);
        assert_eq!(eid_from_hex(&hex), Some(raw));
        assert_eq!(eid_from_hex("zz"), None);
    }

    #[test]
    fn c_abi_struct_sizes_match_documented_layout() {
        // Guards against accidental layout regressions on the hand-written
        // transcription. Values were derived from the vendored headers for
        // LP64 targets; see include/urma_types.h.
        assert_eq!(std::mem::size_of::<urma_eid_t>(), 16);
        assert_eq!(std::mem::align_of::<urma_eid_t>(), 8);
        assert_eq!(std::mem::size_of::<urma_ubva_t>(), 28);
        assert_eq!(std::mem::size_of::<urma_seg_t>(), 48);
        assert_eq!(std::mem::size_of::<urma_jetty_id_t>(), 24);
        assert_eq!(std::mem::size_of::<urma_sge_t>(), 32);
        assert_eq!(std::mem::size_of::<urma_rw_wr_t>(), 48);
        assert_eq!(std::mem::size_of::<urma_jfs_wr_t>(), 80);
        assert_eq!(std::mem::size_of::<urma_cr_t>(), 80);
        assert_eq!(std::mem::size_of::<urma_seg_cfg_t>(), 48);
        assert_eq!(std::mem::size_of::<urma_jfc_cfg_t>(), 32);
    }
}

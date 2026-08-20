//! One-sided remote read over URMA, demonstrating the raw FFI surface.
//!
//! Unlike `wire_format`, this example references the `liburma` extern
//! symbols, so it only builds when the crate's `link-urma` feature is enabled
//! and `liburma.so` is installed (`umdk-urma-devel` on openEuler), and it only
//! runs on hosts with a Kunpeng UB device:
//!
//! ```text
//! cargo build -p urma-sys --features link-urma --example one_sided_read
//! ```
//!
//! Producer node — registers a 4 KiB buffer and prints the handle a peer
//! needs to read it:
//!
//! ```text
//! ./one_sided_read serve [device]
//! ```
//!
//! Consumer node — imports the advertised jetty + segment and pulls the
//! buffer with a single one-sided READ:
//!
//! ```text
//! ./one_sided_read fetch <eid-hex> <jetty-id> <seg-wire-hex> [device]
//! ```

use std::alloc::Layout;
use std::ffi::CString;
use std::os::raw::c_int;
use std::time::{Duration, Instant};

use urma_sys as sys;

/// Token both sides must agree on; matches AgentENV's `UB_TOKEN`.
const TOKEN: u32 = 0xACFE;
/// Local buffer size on both sides.
const BUF_LEN: usize = 4096;
/// Tag used to match our READ work request against polled completions.
const READ_USER_CTX: u64 = 0xACFE_0001;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        Some("serve") => serve(args.get(2).map(String::as_str)),
        Some("fetch") => {
            let (Some(eid), Some(jetty_id), Some(wire)) = (args.get(2), args.get(3), args.get(4))
            else {
                eprintln!("{}", usage(&args[0]));
                std::process::exit(2);
            };
            fetch(eid, jetty_id, wire, args.get(5).map(String::as_str))
        }
        _ => {
            eprintln!("{}", usage(&args[0]));
            std::process::exit(2);
        }
    };
    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} serve [device]\n  {program} fetch <eid-hex> <jetty-id> <seg-wire-hex> [device]"
    )
}

/// Producer side: register a buffer, advertise (EID, jetty id, segment wire
/// form), and stay alive so peers can read it.
fn serve(device: Option<&str>) -> Result<(), String> {
    let urma = Urma::new(device)?;
    let mut buf = AlignedBuf::new(BUF_LEN)?;
    let message = b"hello from the urma producer";
    buf.as_slice_mut()[..message.len()].copy_from_slice(message);

    let tseg = register_seg(&urma, &buf, sys::URMA_ACCESS_READ)?;
    // SAFETY: tseg is live until we unregister it below; `seg` is its public
    // wire form.
    let wire = unsafe { (*tseg).seg }.to_wire();

    println!("device:   {}", urma.device);
    println!("eid:      {}", urma.eid_hex);
    println!("jetty id: {}", urma.jetty_id);
    println!("seg wire: {}", hex_encode(&wire));
    println!("buffer registered; press Enter to exit");

    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);

    // SAFETY: tseg belongs to this context and is unregistered exactly once.
    check("urma_unregister_seg", unsafe {
        sys::urma_unregister_seg(tseg)
    })?;
    println!("ok");
    Ok(())
}

/// Consumer side: import the peer's jetty and segment, then complete one
/// one-sided READ into a freshly registered local buffer.
fn fetch(
    eid_hex: &str,
    jetty_id: &str,
    wire_hex: &str,
    device: Option<&str>,
) -> Result<(), String> {
    let peer_eid = sys::eid_from_hex(eid_hex)
        .ok_or_else(|| format!("invalid eid hex {eid_hex:?} (want 32 hex chars)"))?;
    let peer_jetty_id: u32 = jetty_id
        .parse()
        .map_err(|_| format!("invalid jetty id {jetty_id:?}"))?;
    let wire = hex_decode(wire_hex)?;
    let mut remote_seg = sys::urma_seg_t::from_wire(&wire)
        .ok_or_else(|| format!("seg wire must be {} bytes", sys::URMA_SEG_WIRE_SIZE))?;

    let urma = Urma::new(device)?;
    let buf = AlignedBuf::new(BUF_LEN)?;
    let local_tseg = register_seg(&urma, &buf, sys::URMA_ACCESS_LOCAL_ONLY)?;

    let result = fetch_inner(
        &urma,
        &buf,
        local_tseg,
        peer_eid,
        peer_jetty_id,
        &mut remote_seg,
    );

    // SAFETY: local_tseg belongs to this context and is unregistered once.
    check("urma_unregister_seg", unsafe {
        sys::urma_unregister_seg(local_tseg)
    })?;
    result
}

fn fetch_inner(
    urma: &Urma,
    buf: &AlignedBuf,
    local_tseg: *mut sys::urma_target_seg_t,
    peer_eid: [u8; sys::URMA_EID_SIZE],
    peer_jetty_id: u32,
    remote_seg: &mut sys::urma_seg_t,
) -> Result<(), String> {
    // SAFETY: every pointer below is either live stack data or a handle owned
    // by this function/`urma`; imported handles are released exactly once in
    // the cleanup block at the end, which runs on the error path too.
    unsafe {
        let mut rjetty = sys::urma_rjetty_t {
            trans_mode: sys::URMA_TM_RC,
            policy: sys::URMA_JETTY_GRP_POLICY_RR,
            type_: sys::URMA_JETTY,
            tp_type: sys::URMA_CTP,
            ..Default::default()
        };
        rjetty.jetty_id.eid = sys::urma_eid_t { raw: peer_eid };
        rjetty.jetty_id.id = peer_jetty_id;
        let mut token = sys::urma_token_t { token: TOKEN };
        let tjetty = sys::urma_import_jetty(urma.ctx, &mut rjetty, &mut token);
        if tjetty.is_null() {
            return Err("urma_import_jetty failed".into());
        }

        let mut remote_tseg: *mut sys::urma_target_seg_t = std::ptr::null_mut();
        let result = (|urma: &Urma| -> Result<(), String> {
            check("urma_bind_jetty", sys::urma_bind_jetty(urma.jetty, tjetty))?;

            let flag = sys::urma_import_seg_flag_t {
                value: flag_value_import(),
            };
            remote_tseg = sys::urma_import_seg(urma.ctx, remote_seg, &mut token, 0, flag);
            if remote_tseg.is_null() {
                return Err("urma_import_seg failed".into());
            }

            let read_len = remote_seg.seg_len().min(buf.len() as u64) as u32;
            let mut remote_sge = sys::urma_sge_t {
                addr: remote_seg.base_va(),
                len: read_len,
                tseg: remote_tseg,
                ..Default::default()
            };
            let mut local_sge = sys::urma_sge_t {
                addr: buf.va(),
                len: read_len,
                tseg: local_tseg,
                ..Default::default()
            };
            let mut wr = sys::urma_jfs_wr_t {
                opcode: sys::URMA_OPC_READ,
                flag: sys::urma_jfs_wr_flag_t {
                    value: sys::URMA_JFS_WR_COMPLETE_ENABLE,
                },
                tjetty,
                user_ctx: READ_USER_CTX,
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
            check(
                "urma_post_jetty_send_wr",
                sys::urma_post_jetty_send_wr(urma.jetty, &mut wr, &mut bad_wr),
            )?;
            // The stack WR is copied into the send queue by the post call,
            // so the SGEs above may go out of scope once the poll below
            // observes the completion.

            wait_for_completion(urma.jfc)?;
            println!(
                "read {} bytes from {:#x}: {:?}",
                read_len,
                remote_seg.base_va(),
                String::from_utf8_lossy(&buf.as_slice()[..read_len as usize])
            );
            Ok(())
        })(urma);

        if !remote_tseg.is_null() {
            let _ = check("urma_unimport_seg", sys::urma_unimport_seg(remote_tseg));
        }
        let _ = check("urma_unbind_jetty", sys::urma_unbind_jetty(urma.jetty));
        let _ = check("urma_unimport_jetty", sys::urma_unimport_jetty(tjetty));
        result?;
    }
    println!("ok");
    Ok(())
}

/// Spin on the JFC until our READ completes or a deadline passes.
///
/// # Safety
/// `jfc` must be a live completion queue.
unsafe fn wait_for_completion(jfc: *mut sys::urma_jfc_t) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut crs = [sys::urma_cr_t::default(); 16];
        // SAFETY: jfc is live per the caller; the CR array is stack-owned.
        let polled = unsafe { sys::urma_poll_jfc(jfc, crs.len() as c_int, crs.as_mut_ptr()) };
        for cr in crs.iter().take(polled.max(0) as usize) {
            if cr.user_ctx != READ_USER_CTX {
                continue;
            }
            if cr.status != sys::URMA_CR_SUCCESS {
                return Err(format!("read completed with error status {}", cr.status));
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for read completion".into());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
}

// ---------------------------------------------------------------------------
// Shared setup / teardown
// ---------------------------------------------------------------------------

/// One device context, one polling JFC, and one jetty — the minimal setup
/// for posting one-sided READ work requests.
struct Urma {
    device: String,
    eid_hex: String,
    jetty_id: u32,
    ctx: *mut sys::urma_context_t,
    jfc: *mut sys::urma_jfc_t,
    jetty: *mut sys::urma_jetty_t,
}

impl Urma {
    fn new(device: Option<&str>) -> Result<Self, String> {
        // SAFETY: FFI calls below follow the urma_api.h contracts; every
        // pointer passed is either owned stack data or a live object returned
        // by the library, and each failure step unwinds the previous ones.
        unsafe {
            check("urma_init", sys::urma_init(std::ptr::null_mut()))?;

            let init_result = (|| {
                let device = find_device(device)?;
                let device_name = device_name(device);
                let (eid_hex, eid_index) = device_eid(device)?;
                let ctx = sys::urma_create_context(device, eid_index);
                if ctx.is_null() {
                    return Err(format!("urma_create_context failed for {device_name}"));
                }

                let mut jfc_cfg = sys::urma_jfc_cfg_t {
                    depth: 64,
                    ..Default::default()
                };
                let jfc = sys::urma_create_jfc(ctx, &mut jfc_cfg);
                if jfc.is_null() {
                    sys::urma_delete_context(ctx);
                    return Err("urma_create_jfc failed".into());
                }

                let jfs_cfg = sys::urma_jfs_cfg_t {
                    depth: 16,
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
                    sys::urma_delete_jfc(jfc);
                    sys::urma_delete_context(ctx);
                    return Err("urma_create_jetty failed".into());
                }
                // SAFETY: jetty is a live object returned by urma; the prefix
                // layout (urma_ctx, jetty_id) matches urma_types.h.
                let jetty_id = std::ptr::addr_of!((*jetty).jetty_id).read_unaligned().id;

                Ok(Self {
                    device: device_name,
                    eid_hex,
                    jetty_id,
                    ctx,
                    jfc,
                    jetty,
                })
            })();
            if init_result.is_err() {
                sys::urma_uninit();
            }
            init_result
        }
    }
}

impl Drop for Urma {
    fn drop(&mut self) {
        // SAFETY: all handles were created in `new` and are released exactly
        // once here.
        unsafe {
            sys::urma_delete_jetty(self.jetty);
            sys::urma_delete_jfc(self.jfc);
            sys::urma_delete_context(self.ctx);
            sys::urma_uninit();
        }
    }
}

/// Pick the named device, or the first available when unset. The returned
/// pointer is owned by the library and outlives the freed device list.
///
/// # Safety
/// Calls into liburma; the returned device pointer borrows library state.
unsafe fn find_device(name: Option<&str>) -> Result<*mut sys::urma_device_t, String> {
    unsafe {
        if let Some(name) = name {
            let c_name = CString::new(name).map_err(|_| "device name contains NUL".to_string())?;
            let device = sys::urma_get_device_by_name(c_name.as_ptr() as *mut _);
            if device.is_null() {
                return Err(format!("urma device {name} not found"));
            }
            return Ok(device);
        }

        let mut count: c_int = 0;
        let list = sys::urma_get_device_list(&mut count);
        if list.is_null() || count <= 0 {
            return Err("no urma devices found".into());
        }
        let device = *list;
        sys::urma_free_device_list(list);
        Ok(device)
    }
}

/// Read the device EID list and return `(hex, eid_index)` for index 0.
///
/// # Safety
/// `device` must be a live device pointer from liburma.
unsafe fn device_eid(device: *mut sys::urma_device_t) -> Result<(String, u32), String> {
    unsafe {
        let mut count: u32 = 0;
        let list = sys::urma_get_eid_list(device, &mut count);
        if list.is_null() || count == 0 {
            return Err("urma device has no EIDs".into());
        }
        let info = *list;
        let hex = sys::eid_to_hex(&info.eid.raw);
        sys::urma_free_eid_list(list);
        Ok((hex, info.eid_index))
    }
}

/// # Safety
/// `device` must be a live device pointer from liburma.
// `c_char` is signed on x86_64 and unsigned on aarch64, so the cast is
// required on exactly one of the two.
#[allow(clippy::unnecessary_cast)]
unsafe fn device_name(device: *mut sys::urma_device_t) -> String {
    unsafe {
        let name = std::ptr::addr_of!((*device).name).read_unaligned();
        let end = name
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(sys::URMA_MAX_NAME);
        let bytes: Vec<u8> = name[..end].iter().map(|&c| c as u8).collect();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

// ---------------------------------------------------------------------------
// Segment registration
// ---------------------------------------------------------------------------

/// Page-aligned buffer whose address is handed to `urma_register_seg`.
struct AlignedBuf {
    ptr: *mut u8,
    len: usize,
    layout: Layout,
}

impl AlignedBuf {
    fn new(len: usize) -> Result<Self, String> {
        let layout = Layout::from_size_align(len, 4096).map_err(|err| err.to_string())?;
        // SAFETY: layout is valid; null is checked immediately.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            return Err(format!("failed to allocate {len} bytes"));
        }
        Ok(Self { ptr, len, layout })
    }

    fn va(&self) -> u64 {
        self.ptr as u64
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr/len come from a live allocation owned by self.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    fn as_slice_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr/len come from a live allocation owned by self, and
        // `&mut self` guarantees exclusivity.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with this exact layout in `new`.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) }
    }
}

/// Register `buf` with the given access bits and return the target segment.
fn register_seg(
    urma: &Urma,
    buf: &AlignedBuf,
    access: u32,
) -> Result<*mut sys::urma_target_seg_t, String> {
    let mut cfg = sys::urma_seg_cfg_t {
        va: buf.va(),
        len: buf.len() as u64,
        token_value: sys::urma_token_t { token: TOKEN },
        flag: sys::urma_reg_seg_flag_t {
            value: flag_value_reg(access),
        },
        ..Default::default()
    };
    // SAFETY: cfg is live stack data; the returned segment is owned by the
    // library until urma_unregister_seg.
    let tseg = unsafe { sys::urma_register_seg(urma.ctx, &mut cfg) };
    if tseg.is_null() {
        return Err(format!(
            "urma_register_seg failed for {:#x} len {}",
            buf.va(),
            buf.len()
        ));
    }
    Ok(tseg)
}

fn flag_value_reg(access: u32) -> u32 {
    // urma_reg_seg_flag_t bitfield: token_policy:3, cacheable:1, dsva:1,
    // access:6, non_pin:1, user_iova:1, token_id_valid:1, reserved:18.
    (sys::URMA_TOKEN_NONE & 0x7) | (sys::URMA_NON_CACHEABLE << 3) | (access << 5)
}

fn flag_value_import() -> u32 {
    // urma_import_seg_flag_t bitfield: cacheable:1, access:6, mapping:1.
    (sys::URMA_NON_CACHEABLE & 0x1)
        | ((sys::URMA_ACCESS_READ | sys::URMA_ACCESS_WRITE | sys::URMA_ACCESS_ATOMIC) << 1)
        | (sys::URMA_SEG_NOMAP << 7)
}

// ---------------------------------------------------------------------------
// Small helpers (the crate has no dependencies, so hex is done by hand)
// ---------------------------------------------------------------------------

fn check(op: &str, status: c_int) -> Result<(), String> {
    if status == sys::URMA_SUCCESS || status == sys::URMA_EEXIST {
        return Ok(());
    }
    Err(format!("{op} failed with urma status {status}"))
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("hex string must have an even length".into());
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character {:?}", pair[0] as char))?;
        let low = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex character {:?}", pair[1] as char))?;
        out.push((high * 16 + low) as u8);
    }
    Ok(out)
}

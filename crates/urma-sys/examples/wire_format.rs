//! Segment wire-format and EID helpers, exercised without URMA hardware.
//!
//! This example only touches the pure-Rust surface of `urma-sys` (no extern
//! symbols), so it builds and runs on any host — `liburma` is not required.
//!
//! Run with: `cargo run -p urma-sys --example wire_format`

use urma_sys as sys;

fn main() {
    // EIDs are 16 raw bytes, conventionally exchanged as 32 hex characters.
    let eid = sys::urma_eid_t {
        raw: [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f,
        ],
    };
    let hex = sys::eid_to_hex(&eid.raw);
    println!("eid: {eid:?}");
    println!("eid hex: {hex}");
    assert_eq!(sys::eid_from_hex(&hex), Some(eid.raw));

    // A registered segment's wire form is what a producer shares with peers
    // (e.g. embedded in an artifact descriptor). It is a fixed-size,
    // byte-for-byte copy of `urma_seg_t`.
    let mut seg = sys::urma_seg_t::default();
    seg.ubva.eid = eid.raw;
    seg.ubva.uasid = 0;
    seg.ubva.va = 0x0000_7f4a_1000_0000;
    seg.len = 64 * 1024;
    seg.attr = sys::urma_seg_attr_t { value: 0 };
    seg.token_id = 0;

    let wire = seg.to_wire();
    println!(
        "wire form: {} bytes (URMA_SEG_WIRE_SIZE = {})",
        wire.len(),
        sys::URMA_SEG_WIRE_SIZE
    );

    // The consumer side reconstructs the segment from those bytes. Note the
    // packed-struct accessors: `ubva.va` must be read via `base_va()`.
    let parsed = sys::urma_seg_t::from_wire(&wire).expect("valid wire form");
    println!("parsed base va: {:#x}", parsed.base_va());
    println!("parsed length:  {} bytes", parsed.seg_len());
    assert_eq!(parsed.base_va(), 0x0000_7f4a_1000_0000);
    assert_eq!(parsed.seg_len(), 64 * 1024);

    // Truncated or over-long byte strings are rejected instead of parsed
    // into garbage.
    assert!(sys::urma_seg_t::from_wire(&wire[..wire.len() - 1]).is_none());
    println!("ok");
}

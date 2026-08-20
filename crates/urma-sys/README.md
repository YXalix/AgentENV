# urma-sys

Raw FFI bindings to the openEuler UMDK URMA C API (`liburma`), used by the
AgentENV `ub` P2P transport backend for one-sided remote reads over Kunpeng
UB.

- `src/lib.rs` hand-transcribes the ABI subset needed for one-sided READ
  (device/context/jfc/jetty lifecycle, segment register/import, WR post and
  JFC poll).
- `include/` vendors the upstream headers for reference and future bindgen
  validation. Source: <https://atomgit.com/openeuler/umdk> (GitHub mirror:
  `openeuler-mirror/umdk`), tag `v25.12.0.B081`, MIT licensed, copyright
  Huawei Technologies Co., Ltd.

The wire form shared between nodes (`urma_seg_t` bytes) is ABI-coupled to the
exact `liburma` version: every node in a cluster must run the same version,
and the transcription here must be re-validated (via bindgen or the layout
unit tests) whenever the vendored headers are upgraded.

Linking: this crate intentionally omits `#[link]`, so building it alone does
not require `liburma`. Consumers that reference the extern functions enable
the `link-urma` feature, which makes `build.rs` emit
`cargo:rustc-link-lib=dylib=urma` (`umdk-urma-devel` on openEuler must be
installed so the linker can find `liburma.so`).

## Examples

- `cargo run -p urma-sys --example wire_format` — EID hex helpers and the
  `urma_seg_t` wire form; pure Rust, runs on any host.
- `cargo build -p urma-sys --features link-urma --example one_sided_read` —
  a full one-sided READ over UB (`serve` on the producer node, `fetch` on
  the consumer node). Gated behind the `link-urma` feature because it
  references the extern symbols and therefore needs `liburma.so` at link
  time; it only runs on hosts with a Kunpeng UB device.

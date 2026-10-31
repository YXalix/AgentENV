//! Offline diagnostic for a stable varlen mem root (design §6 on-air gate):
//! opens the composite exactly like the ublk daemon does (`image.json` with
//! `memUpper`), reads the whole address space, verifies every block payload
//! (crc, decompression), and diffs against the bare lower stack
//! (`lowers.json`). Usage:
//!
//!   cargo run -p overlaybd --example mem_upper_dump -- /tmp/mem/image.json /tmp/mem/lowers.json \
//!       [overlaybd-global.json]
//!
//! The global config defaults to $AENV_HOME/overlaybd/mem-overlaybd-global.json
//! when unset. Exit code 0 = composite reads clean; the diff map prints either way.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;

use overlaybd::image_file::{open_lower_stack_from_config, open_varlen_composite_from_config};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::ImageService;

const CHUNK: u64 = 4 << 20;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let image_config = PathBuf::from(
        args.next()
            .context("usage: mem_upper_dump <image.json> <lowers.json>")?,
    );
    let lowers_config = PathBuf::from(
        args.next()
            .context("usage: mem_upper_dump <image.json> <lowers.json>")?,
    );
    let global_config = match args.next() {
        Some(path) => PathBuf::from(path),
        None => {
            let home = std::env::var("AENV_HOME").unwrap_or_else(|_| "/var/lib/aenv".to_string());
            PathBuf::from(home).join("overlaybd/mem-overlaybd-global.json")
        }
    };
    let service = Arc::new(
        ImageService::from_config_path(global_config)
            .await
            .context("build image service")?,
    );

    let composite = open_varlen_composite_from_config(&service, &image_config)
        .await
        .context("open composite (daemon path)")?;
    let lower = open_lower_stack_from_config(&service, &lowers_config)
        .await
        .context("open lower stack")?;
    let size = composite.size().await?;
    let lower_size = lower.size().await?;
    println!("composite vsize = {size}, lower vsize = {lower_size}");

    let mut differing_blocks: Vec<u64> = Vec::new();
    let mut offset = 0u64;
    while offset < size {
        let len = CHUNK.min(size - offset) as usize;
        let a: Bytes = composite.read_at(offset, len).await.with_context(|| {
            format!("composite read at {offset} (+{len}) failed — daemon would fail here too")
        })?;
        let b = lower.read_at(offset, len).await.context("lower read")?;
        for (i, (x, y)) in a.chunks(16384).zip(b.chunks(16384)).enumerate() {
            if x != y {
                differing_blocks.push(offset / 16384 + i as u64);
            }
        }
        offset += len as u64;
    }
    println!(
        "composite full read OK (every payload crc/decompress verified); \
         blocks differing from lower: {} of {}",
        differing_blocks.len(),
        size.div_ceil(16384)
    );
    if !differing_blocks.is_empty() {
        let head: Vec<String> = differing_blocks
            .iter()
            .take(16)
            .map(|b| format!("{b} (@{:#x})", b * 16384))
            .collect();
        println!("first differing blocks: {}", head.join(", "));
    }
    Ok(())
}

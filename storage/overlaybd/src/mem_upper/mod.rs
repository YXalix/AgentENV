//! `mem.upper`: the private, bounded, single-file variable-length packed
//! memory layer for storm pause/resume (design
//! `DESIGN-lightweight-pause-v1`, layout V3).
//!
//! * [`writer`] owns the harvest engine: splice main path, readv fallback,
//!   skip-write, in-place placement, incremental metadata, epoch commit.
//! * [`reader`] owns the lazy adaptor (`MemUpperFile`, identity `moffset`)
//!   and the shading index (`VarlenIndex`) that turns a template stack plus
//!   this file into a composite read-only view with zero merge at open.
//!
//! The module is shared by the daemon (read side, via the image config
//! `memUpper` section) and the host crate (write side).

mod allocator;
mod format;
pub mod reader;
pub mod writer;

pub use format::{crc16, BlockEntry, MemUpperHeader, BLOCK_SIZE, ENC_LZ4, ENC_RAW};
pub use reader::{
    assemble_varlen_composite, read_mem_upper_epoch, MemUpperFile, MemUpperState, VarlenIndex,
};
pub use writer::{
    DirtyPage, Footprint, HarvestOptions, HarvestStats, HarvestViews, MemUpperWriter,
    RemoteBlockReader, DEFAULT_HARVEST_CONCURRENCY, VACUUM_BLOAT_THRESHOLD,
    VACUUM_FRAGMENTATION_THRESHOLD,
};

#[cfg(test)]
mod tests;

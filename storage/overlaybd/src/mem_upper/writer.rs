//! Harvest engine for `mem.upper` (design §4.2 step 3).
//!
//! One loop produces all three outputs (skip-write / in-place / append):
//! each worker fills a 16KiB scratch with the block's *previous* authority
//! (old payload decompressed, or the lower view for absent blocks), overlays
//! the dirty 4K pages read from the frozen guest RAM, decides skip/template
//! equivalence, compresses, and hands the payload to the serial placement
//! phase which owns the allocator and the metadata shadow.

use anyhow::{bail, ensure, Context, Result};
use futures_util::stream::{self, StreamExt};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::allocator::FreeList;
use super::format::{
    crc16, pread_exact, BlockEntry, MemUpperHeader, BLOCK_SIZE, ENC_LZ4, ENC_RAW, SECTOR_SIZE,
};
use crate::compression::zfile::{create_compressor, CompressArgs, CompressOptions, Compressor};
use crate::io::virtual_file::VirtualFile;

/// Dirty-bitmap guardrail: dirty bytes above this fraction of `mem_spec`
/// suggest the bitmap failed to reset at load and the incremental model is
/// suspect (design §4.2 hard invariant).
pub const DIRTY_GUARDRAIL_FRACTION: f64 = 0.6;

/// Vacuum trigger: free-space fraction of the data area.
pub const VACUUM_FRAGMENTATION_THRESHOLD: f64 = 0.35;
/// Vacuum trigger: file size over live payload bytes.
pub const VACUUM_BLOAT_THRESHOLD: f64 = 1.6;

pub const DEFAULT_HARVEST_CONCURRENCY: usize = 32;

/// Synchronous reader of guest RAM pages (`process_vm_readv` in the host
/// crate; synthetic RAM in tests).
pub trait RemoteBlockReader: Send + Sync {
    /// Reads exactly `dst.len()` bytes from remote address `addr`.
    fn read_exact_remote(&self, addr: u64, dst: &mut [u8]) -> Result<()>;
}

/// One dirty 4K page: logical byte offset inside guest RAM plus the host
/// virtual address that backs it.
#[derive(Clone, Copy, Debug)]
pub struct DirtyPage {
    pub logical_offset: u64,
    pub host_addr: u64,
}

#[derive(Clone, Debug)]
pub struct HarvestOptions {
    /// Harvest every block (vacuum / migration materialization); dirty pages
    /// are still overlaid from guest RAM, clean 4Ks come from the lower view.
    pub full: bool,
    /// Force the whole-block readv fallback for every touched block.
    pub force_readv: bool,
    pub concurrency: usize,
}

impl Default for HarvestOptions {
    fn default() -> Self {
        Self {
            full: false,
            force_readv: false,
            concurrency: DEFAULT_HARVEST_CONCURRENCY,
        }
    }
}

/// Read authorities the harvest loop consults. `stitch` serves blocks absent
/// from this file (lower-only for steady harvests; the old composite when
/// materializing a fresh file). `template` is always the lower stack — the
/// reclamation compare target of placement rule 1.
#[derive(Clone)]
pub struct HarvestViews {
    pub stitch: Arc<dyn VirtualFile>,
    pub template: Arc<dyn VirtualFile>,
}

impl HarvestViews {
    pub fn uniform(view: Arc<dyn VirtualFile>) -> Self {
        Self {
            stitch: Arc::clone(&view),
            template: view,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HarvestStats {
    pub blocks_scanned: u64,
    pub dirty_pages: u64,
    pub skipped: u64,
    pub template_equal: u64,
    pub written: u64,
    pub inplace_hits: u64,
    pub payload_bytes: u64,
    pub moved_bytes: u64,
    pub readv_fallback: u64,
    pub crc_invalid: u64,
}

/// Footprint snapshot for fragmentation monitoring and vacuum triggers.
#[derive(Clone, Copy, Debug)]
pub struct Footprint {
    pub file_bytes: u64,
    pub live_bytes: u64,
    pub free_bytes: u64,
    pub free_extents: usize,
    pub epoch: u64,
    /// Free-space fraction of the data area.
    pub fragmentation: f64,
}

enum Outcome {
    Skipped,
    TemplateEqual,
    Payload { enc: u8, payload: Vec<u8>, crc: u16 },
}

fn mem_upper_compress_args() -> CompressArgs {
    CompressArgs::new(CompressOptions::new(CompressOptions::LZ4, BLOCK_SIZE, 0))
}

pub struct MemUpperWriter {
    path: PathBuf,
    file: Arc<std::fs::File>,
    header: MemUpperHeader,
    /// RAM shadow of the metadata entries (8B per block, LE).
    meta: Vec<u64>,
    /// Dirty metadata page indices (512 entries per 4K page).
    dirty_meta_pages: BTreeSet<u32>,
    freelist: FreeList,
    live_sectors: u64,
    epoch: u64,
}

impl MemUpperWriter {
    /// Creates a fresh `mem.upper` with epoch 0, durably (file fsync plus
    /// parent directory fsync) so the first paused record never references a
    /// half-born file.
    pub fn create(path: &Path, mem_spec: u64) -> Result<Self> {
        let header = MemUpperHeader::new(mem_spec, 0);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create mem.upper {}", path.display()))?;
        let mut page = vec![0u8; 4096];
        header.encode_into(&mut page);
        pwrite_all(&file, 0, &page, "header")?;
        file.sync_all()
            .with_context(|| format!("sync new mem.upper {}", path.display()))?;
        sync_parent_dir(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(file),
            header,
            meta: vec![0; header.block_count() as usize],
            dirty_meta_pages: BTreeSet::new(),
            freelist: FreeList::new(),
            live_sectors: 0,
            epoch: 0,
        })
    }

    /// Opens an existing `mem.upper`: reads the header and the whole metadata
    /// area in one pread, then rebuilds the free-list in the same pass.
    pub fn open(path: &Path, expected_mem_spec: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .with_context(|| format!("open mem.upper {}", path.display()))?;
        let mut page = vec![0u8; 4096];
        pread_exact(&file, 0, &mut page, "header")?;
        let header = MemUpperHeader::decode(&page)?;
        ensure!(
            header.mem_spec == expected_mem_spec,
            "mem.upper {} holds mem_spec {} but sandbox expects {expected_mem_spec}",
            path.display(),
            header.mem_spec
        );
        let blocks = header.block_count() as usize;
        let meta_len = blocks * 8;
        let mut raw = vec![0u8; meta_len];
        // Untouched metadata pages are sparse holes: read what the file
        // actually holds and leave the tail zeroed (= absent entries).
        let file_len = file
            .metadata()
            .with_context(|| format!("stat mem.upper {}", path.display()))?
            .len();
        let available = file_len.saturating_sub(4096).min(meta_len as u64) as usize;
        if available > 0 {
            pread_exact(&file, 4096, &mut raw[..available], "metadata")?;
        }
        let meta: Vec<u64> = raw
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| u64::from_le_bytes(*c))
            .collect();
        let mut occupied = Vec::new();
        let mut live_sectors = 0u64;
        for &v in &meta {
            let entry = BlockEntry::decode(v);
            if entry.present() {
                occupied.push((u64::from(entry.off), entry.extent_sectors()));
                live_sectors += u64::from(entry.extent_sectors());
            }
        }
        let mut freelist = FreeList::rebuild(occupied)
            .with_context(|| format!("rebuild mem.upper free-list {}", path.display()))?;
        // The payload high-water mark follows the file length: freed tail
        // extents stay reusable without persisting allocator state.
        let high_water = file_len
            .saturating_sub(header.data_start)
            .div_ceil(SECTOR_SIZE);
        freelist.extend_data_end(high_water);
        let epoch = header.epoch;
        Ok(Self {
            path: path.to_path_buf(),
            file: Arc::new(file),
            header,
            meta,
            dirty_meta_pages: BTreeSet::new(),
            freelist,
            live_sectors,
            epoch,
        })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn header(&self) -> MemUpperHeader {
        self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn footprint(&self) -> Footprint {
        let data_area_bytes = self.freelist.data_end() * SECTOR_SIZE;
        let free_bytes = self.freelist.total_free() * SECTOR_SIZE;
        Footprint {
            file_bytes: self.header.data_start + data_area_bytes,
            live_bytes: self.live_sectors * SECTOR_SIZE,
            free_bytes,
            free_extents: self.freelist.extent_count(),
            epoch: self.epoch,
            fragmentation: if data_area_bytes == 0 {
                0.0
            } else {
                free_bytes.min(data_area_bytes) as f64 / data_area_bytes as f64
            },
        }
    }

    pub fn should_vacuum(&self) -> bool {
        let footprint = self.footprint();
        // Skip vacuuming while the live payload is a negligible slice of the
        // guest RAM (avoid churning tiny files).
        if footprint.live_bytes * 64 < self.header.mem_spec {
            return false;
        }
        let bloated =
            footprint.file_bytes as f64 > footprint.live_bytes as f64 * VACUUM_BLOAT_THRESHOLD;
        bloated || footprint.fragmentation > VACUUM_FRAGMENTATION_THRESHOLD
    }

    /// Harvests dirty blocks into this file (design §4.1 placement protocol).
    pub async fn harvest(
        &mut self,
        dirty: &[DirtyPage],
        remote: &dyn RemoteBlockReader,
        views: HarvestViews,
        opts: &HarvestOptions,
    ) -> Result<HarvestStats> {
        let block_size = u64::from(BLOCK_SIZE);
        let page_size = 4096u64;
        let mut per_block: BTreeMap<u32, Vec<(u32, u64)>> = BTreeMap::new();
        for page in dirty {
            ensure!(
                page.logical_offset % page_size == 0,
                "dirty page offset {} not 4K aligned",
                page.logical_offset
            );
            ensure!(
                page.logical_offset < self.header.mem_spec,
                "dirty page offset {} beyond mem_spec {}",
                page.logical_offset,
                self.header.mem_spec
            );
            let block = (page.logical_offset / block_size) as u32;
            let in_block = (page.logical_offset % block_size) as u32;
            per_block
                .entry(block)
                .or_default()
                .push((in_block, page.host_addr));
        }

        let dirty_bytes = dirty.len() as u64 * page_size;
        let guardrail = (self.header.mem_spec as f64 * DIRTY_GUARDRAIL_FRACTION) as u64;
        let mut force_readv = opts.force_readv;
        if dirty_bytes > guardrail {
            tracing::warn!(
                dirty_bytes,
                mem_spec = self.header.mem_spec,
                "dirty bitmap exceeds guardrail; suspecting accumulation, using readv fallback"
            );
            force_readv = true;
        }

        let blocks: Vec<u32> = if opts.full {
            (0..self.header.block_count() as u32).collect()
        } else {
            per_block.keys().copied().collect()
        };

        let mut stats = HarvestStats {
            blocks_scanned: blocks.len() as u64,
            dirty_pages: dirty.len() as u64,
            ..Default::default()
        };

        let decompressor: Arc<dyn Compressor> =
            Arc::from(create_compressor(&mem_upper_compress_args())?);
        let concurrency = opts.concurrency.max(1);
        let wave_len = concurrency * 32;

        for wave in blocks.chunks(wave_len) {
            let outcomes = stream::iter(wave.iter().copied())
                .map(|block| {
                    let file = Arc::clone(&self.file);
                    let stitch = Arc::clone(&views.stitch);
                    let template = Arc::clone(&views.template);
                    let decompressor = Arc::clone(&decompressor);
                    let entry_word = self.meta[block as usize];
                    let data_start = self.header.data_start;
                    let pages = per_block.get(&block).map(|p| p.as_slice()).unwrap_or(&[]);
                    async move {
                        process_block(
                            &file,
                            data_start,
                            block,
                            entry_word,
                            pages,
                            force_readv,
                            remote,
                            stitch,
                            template,
                            decompressor,
                        )
                        .await
                        .with_context(|| format!("harvest mem.upper block {block}"))
                    }
                })
                .buffer_unordered(concurrency)
                .collect::<Vec<Result<(Outcome, WorkerFlags)>>>()
                .await;
            // Serial placement phase owns the allocator and metadata shadow.
            for (idx, outcome) in outcomes.into_iter().enumerate() {
                let block = wave[idx];
                let (outcome, flags) = outcome?;
                if flags.readv_fallback {
                    stats.readv_fallback += 1;
                }
                if flags.crc_invalid {
                    stats.crc_invalid += 1;
                }
                self.place(block, outcome, &mut stats)?;
            }
        }
        Ok(stats)
    }

    fn place(&mut self, block: u32, outcome: Outcome, stats: &mut HarvestStats) -> Result<()> {
        let idx = block as usize;
        let old = BlockEntry::decode(self.meta[idx]);
        match outcome {
            Outcome::Skipped => {
                stats.skipped += 1;
            }
            Outcome::TemplateEqual => {
                stats.template_equal += 1;
                if old.present() {
                    self.freelist.free(u64::from(old.off), old.extent_sectors());
                    self.live_sectors -= u64::from(old.extent_sectors());
                    self.meta[idx] = 0;
                    self.dirty_meta_pages.insert(idx as u32 / 512);
                }
            }
            Outcome::Payload { enc, payload, crc } => {
                stats.written += 1;
                stats.payload_bytes += payload.len() as u64;
                let len = u16::try_from(payload.len()).expect("payload fits 15 bits");
                let need = BlockEntry {
                    enc,
                    len,
                    off: 0,
                    crc,
                }
                .extent_sectors();
                let off = if old.present() {
                    let old_extent = old.extent_sectors();
                    if need <= old_extent {
                        stats.inplace_hits += 1;
                        if old_extent > need {
                            self.freelist
                                .free(u64::from(old.off) + u64::from(need), old_extent - need);
                            self.live_sectors -= u64::from(old_extent - need);
                        }
                        u64::from(old.off)
                    } else {
                        stats.moved_bytes += payload.len() as u64;
                        self.freelist.free(u64::from(old.off), old_extent);
                        self.live_sectors -= u64::from(old_extent);
                        self.freelist.alloc(need)
                    }
                } else {
                    self.freelist.alloc(need)
                };
                let entry = BlockEntry {
                    enc,
                    len,
                    off: u32::try_from(off).expect("data area offset fits 31 bits"),
                    crc,
                };
                let file_off = self.header.data_start + off * SECTOR_SIZE;
                pwrite_all(&self.file, file_off, &payload, "payload")?;
                self.live_sectors += u64::from(need);
                self.meta[idx] = entry.encode();
                self.dirty_meta_pages.insert(idx as u32 / 512);
            }
        }
        Ok(())
    }

    /// Commit point: bump the header epoch, write the header plus dirty
    /// metadata pages, then a single `sync_all`. Payloads were written before
    /// metadata (design §5). An fsync error must prevent the paused record.
    pub fn commit(&mut self) -> Result<u64> {
        self.epoch += 1;
        self.header.epoch = self.epoch;
        let mut page = vec![0u8; 4096];
        self.header.encode_into(&mut page);
        pwrite_all(&self.file, 0, &page, "header epoch")?;
        self.flush_dirty_metadata()?;
        self.file
            .sync_all()
            .with_context(|| format!("fsync mem.upper {}", self.path.display()))?;
        self.dirty_meta_pages.clear();
        Ok(self.epoch)
    }

    /// Writes back dirty metadata pages, coalescing consecutive runs into one
    /// pwrite per run (I-5).
    fn flush_dirty_metadata(&self) -> Result<()> {
        let mut start: Option<u32> = None;
        let mut end: u32 = 0;
        for &page in &self.dirty_meta_pages {
            match start {
                None => {
                    start = Some(page);
                    end = page;
                }
                Some(_) if page == end + 1 => end = page,
                Some(s) => {
                    self.emit_metadata_run(s, end)?;
                    start = Some(page);
                    end = page;
                }
            }
        }
        if let Some(s) = start {
            self.emit_metadata_run(s, end)?;
        }
        Ok(())
    }

    fn emit_metadata_run(&self, start_page: u32, end_page: u32) -> Result<()> {
        let begin = start_page as usize * 512;
        let count = (end_page - start_page + 1) as usize * 512;
        let mut buf = Vec::with_capacity(count * 8);
        for v in &self.meta[begin..begin + count] {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        pwrite_all(
            &self.file,
            4096 + u64::from(start_page) * 4096,
            &buf,
            "metadata",
        )
    }
}

/// Worker body for one block. Returns the placement outcome plus per-block
/// flags for stats accounting.
#[derive(Clone, Copy, Default)]
struct WorkerFlags {
    readv_fallback: bool,
    crc_invalid: bool,
}

#[allow(clippy::too_many_arguments)]
async fn process_block(
    file: &std::fs::File,
    data_start: u64,
    block: u32,
    entry_word: u64,
    pages: &[(u32, u64)],
    force_readv: bool,
    remote: &dyn RemoteBlockReader,
    stitch: Arc<dyn VirtualFile>,
    template: Arc<dyn VirtualFile>,
    decompressor: Arc<dyn Compressor>,
) -> Result<(Outcome, WorkerFlags)> {
    let block_size = BLOCK_SIZE as usize;
    let old = BlockEntry::decode(entry_word);
    let mut scratch = vec![0u8; block_size];
    let mut flags = WorkerFlags::default();
    let mut fallback = force_readv;

    if old.present() {
        let mut payload = vec![0u8; old.len as usize];
        pread_exact(
            file,
            data_start + u64::from(old.off) * SECTOR_SIZE,
            &mut payload,
            "payload",
        )?;
        if crc16(&payload) != old.crc {
            // Torn or stale payload: never splice from it.
            flags.crc_invalid = true;
            fallback = true;
        } else {
            match old.enc {
                ENC_RAW => scratch[..payload.len()].copy_from_slice(&payload),
                ENC_LZ4 => {
                    let out = decompressor
                        .decompress(&payload, &mut scratch)
                        .with_context(|| format!("decompress old payload of block {block}"))?;
                    ensure!(out == block_size, "block {block} payload expands to {out}");
                }
                other => bail!("block {block} carries reserved enc {other}"),
            }
        }
    } else {
        stitch
            .read_at_into(u64::from(block) * u64::from(BLOCK_SIZE), &mut scratch)
            .await
            .with_context(|| format!("read stitch view for absent block {block}"))?;
    }

    if fallback {
        // Whole-block readv: content comes entirely from frozen guest RAM.
        // Old-payload distrust means skip-write is not allowed here.
        let base = anchor_host_base(block, pages)?;
        remote.read_exact_remote(base, &mut scratch)?;
        flags.readv_fallback = true;
    } else {
        // Splice main path: only dirty 4Ks come from guest RAM; clean 4Ks
        // keep the previous authority already sitting in scratch.
        let mut all_equal = true;
        let mut page_buf = vec![0u8; 4096];
        for &(in_block, addr) in pages {
            remote.read_exact_remote(addr, &mut page_buf)?;
            let begin = in_block as usize;
            if page_buf[..] != scratch[begin..begin + 4096] {
                all_equal = false;
                scratch[begin..begin + 4096].copy_from_slice(&page_buf);
            }
        }
        if all_equal {
            return Ok((
                if old.present() {
                    Outcome::Skipped
                } else {
                    Outcome::TemplateEqual
                },
                flags,
            ));
        }
    }

    // Placement rule 1: a block that now equals the lower view loses its
    // payload (`len = 0` + old extent reclaimed). When the block was absent
    // the scratch pre-fill already was lower content, so only re-check when
    // a payload exists (or the fallback replaced it).
    if old.present() || fallback {
        let mut lower_scratch = vec![0u8; block_size];
        template
            .read_at_into(u64::from(block) * u64::from(BLOCK_SIZE), &mut lower_scratch)
            .await
            .with_context(|| format!("read template view for block {block}"))?;
        if scratch[..] == lower_scratch[..] {
            return Ok((Outcome::TemplateEqual, flags));
        }
    }

    // Compress; a non-shrinking payload is stored raw.
    let mut compressor = create_compressor(&mem_upper_compress_args())?;
    let mut comp_buf = vec![0u8; compressor.max_dst_size()];
    let comp_len = compressor
        .compress(&scratch, &mut comp_buf)
        .with_context(|| format!("compress block {block}"))?;
    let (enc, payload): (u8, Vec<u8>) = if comp_len >= block_size {
        (ENC_RAW, scratch.clone())
    } else {
        (ENC_LZ4, comp_buf[..comp_len].to_vec())
    };
    let crc = crc16(&payload);
    Ok((Outcome::Payload { enc, payload, crc }, flags))
}

/// Derives the contiguous host range base of a block from any of its dirty
/// pages, validating the affine assumption across the block's dirty pages.
fn anchor_host_base(block: u32, pages: &[(u32, u64)]) -> Result<u64> {
    let &(in_block, addr) = pages
        .first()
        .ok_or_else(|| anyhow::anyhow!("readv fallback needs a dirty anchor in block {block}"))?;
    let base = addr
        .checked_sub(u64::from(in_block))
        .ok_or_else(|| anyhow::anyhow!("dirty page address underflow in block {block}"))?;
    for &(in_block, addr) in pages {
        ensure!(
            addr == base + u64::from(in_block),
            "dirty pages of block {block} are not address-contiguous"
        );
    }
    Ok(base)
}

pub(crate) fn pwrite_all(file: &std::fs::File, offset: u64, buf: &[u8], what: &str) -> Result<()> {
    let mut done = 0usize;
    while done < buf.len() {
        let n = file
            .write_at(&buf[done..], offset + done as u64)
            .with_context(|| format!("write mem.upper {what} at {offset}"))?;
        ensure!(n > 0, "short write of mem.upper {what} at {offset}");
        done += n;
    }
    Ok(())
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("sync parent directory {}", parent.display()))?;
    }
    Ok(())
}

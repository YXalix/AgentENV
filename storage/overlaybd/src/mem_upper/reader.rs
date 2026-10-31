//! Read side of `mem.upper`: the lazy file adaptor and the shading index
//! (design §4.3, I-2/I-3).
//!
//! `MemUpperFile` exposes guest-RAM logical offsets (identity `moffset`):
//! `read_at(logical_offset)` decompresses whole blocks on demand and never
//! serves absent blocks — the `VarlenIndex` shades those down to the lower
//! (template/legacy) stack, exactly like `ComboIndex` shades an upper over a
//! lower. The lower side reuses the premerged index cache, so opening the
//! composite costs one header read, one metadata read and a free-list scan —
//! zero k-way merge over the template.

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use super::format::{
    crc16, BlockEntry, MemUpperHeader, BLOCK_SECTORS, BLOCK_SIZE, ENC_LZ4, ENC_RAW,
};
use crate::compression::zfile::{create_compressor, CompressArgs, CompressOptions, Compressor};
use crate::io::virtual_file::VirtualFile;
use crate::lsmt::file::{LSMTFileType, LSMTReadOnlyFile};
use crate::lsmt::index::{LogIndex, ReadOnlyIndex, Segment, SegmentMapping};

/// Recently decompressed blocks retained per open file. Concurrent faults
/// rarely hit the same block, so this stays deliberately tiny (design §4.3).
const BLOCK_CACHE_CAPACITY: usize = 64;

/// Header plus metadata shared by the file adaptor and the index.
pub struct MemUpperState {
    file: Arc<dyn VirtualFile>,
    header: MemUpperHeader,
    entries: Vec<u64>,
}

impl MemUpperState {
    /// Validates the header and loads the whole metadata area (1MiB for a
    /// 2GiB `mem_spec`).
    pub async fn open(file: Arc<dyn VirtualFile>, expected_mem_spec: u64) -> Result<Self> {
        let mut header_page = BytesMut::zeroed(4096);
        file.read_at_into(0, &mut header_page)
            .await
            .context("read mem.upper header")?;
        let header = MemUpperHeader::decode(&header_page)?;
        ensure!(
            header.mem_spec == expected_mem_spec,
            "mem.upper holds mem_spec {} but sandbox expects {expected_mem_spec}",
            header.mem_spec
        );
        let blocks = header.block_count() as usize;
        let meta_len = blocks * 8;
        let mut raw = BytesMut::zeroed(meta_len);
        // Untouched metadata pages are sparse holes: read what exists and
        // treat the zero tail as absent entries.
        let file_len = file.size().await.context("stat mem.upper")?;
        let available = file_len.saturating_sub(4096).min(meta_len as u64) as usize;
        if available > 0 {
            file.read_at_into(4096, &mut raw[..available])
                .await
                .context("read mem.upper metadata")?;
        }
        let entries = raw
            .as_chunks::<8>()
            .0
            .iter()
            .map(|c| u64::from_le_bytes(*c))
            .collect();
        Ok(Self {
            file,
            header,
            entries,
        })
    }

    pub fn header(&self) -> MemUpperHeader {
        self.header
    }

    pub fn mem_spec(&self) -> u64 {
        self.header.mem_spec
    }

    /// Metadata entry of one block; `len == 0` means the lower stack is the
    /// authority.
    pub fn entry(&self, block: u32) -> BlockEntry {
        BlockEntry::decode(self.entries[block as usize])
    }

    fn block_count(&self) -> u32 {
        self.header.block_count() as u32
    }
}

struct BlockCache {
    map: HashMap<u32, (u64, Bytes)>,
    tick: u64,
}

impl BlockCache {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
            tick: 0,
        }
    }

    fn get(&mut self, block: u32) -> Option<Bytes> {
        let tick = &mut self.tick;
        self.map.get_mut(&block).map(|(stamp, bytes)| {
            *stamp = *tick;
            bytes.clone()
        })
    }

    fn put(&mut self, block: u32, bytes: Bytes) {
        if self.map.len() >= BLOCK_CACHE_CAPACITY && !self.map.contains_key(&block) {
            if let Some((&victim, _)) = self.map.iter().min_by_key(|(_, (stamp, _))| *stamp) {
                self.map.remove(&victim);
            }
        }
        self.tick += 1;
        self.map.insert(block, (self.tick, bytes));
    }
}

/// Virtual-file adaptor over `mem.upper` with identity `moffset` semantics:
/// offsets are guest-RAM logical offsets, expanded block by block (I-3).
pub struct MemUpperFile {
    state: Arc<MemUpperState>,
    decompressor: Arc<dyn Compressor>,
    cache: Mutex<BlockCache>,
}

impl MemUpperFile {
    pub fn new(state: Arc<MemUpperState>) -> Self {
        let args = CompressArgs::new(CompressOptions::new(CompressOptions::LZ4, BLOCK_SIZE, 0));
        let decompressor: Arc<dyn Compressor> =
            Arc::from(create_compressor(&args).expect("build lz4 decompressor"));
        Self {
            state,
            decompressor,
            cache: Mutex::new(BlockCache::new()),
        }
    }

    async fn block_bytes(&self, block: u32) -> Result<Bytes> {
        if let Some(hit) = self.cache.lock().unwrap().get(block) {
            return Ok(hit);
        }
        let entry = self.state.entry(block);
        ensure!(
            entry.present(),
            "mem.upper block {block} is absent but mapped (torn index?)"
        );
        let payload = self
            .state
            .file
            .read_at(
                self.state.header.data_start + u64::from(entry.off) * 512,
                entry.len as usize,
            )
            .await
            .with_context(|| format!("read payload of mem.upper block {block}"))?;
        ensure!(
            crc16(&payload) == entry.crc,
            "mem.upper block {block} payload crc mismatch"
        );
        let bytes = match entry.enc {
            ENC_RAW => payload,
            ENC_LZ4 => {
                let mut out = BytesMut::zeroed(BLOCK_SIZE as usize);
                let n = self
                    .decompressor
                    .decompress(&payload, &mut out)
                    .with_context(|| format!("decompress mem.upper block {block}"))?;
                ensure!(
                    n == BLOCK_SIZE as usize,
                    "mem.upper block {block} expands to {n}"
                );
                out.freeze()
            }
            other => bail!("mem.upper block {block} carries reserved enc {other}"),
        };
        self.cache.lock().unwrap().put(block, bytes.clone());
        Ok(bytes)
    }
}

#[async_trait]
impl VirtualFile for MemUpperFile {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        ensure!(
            len > 0,
            "mem.upper read_at with zero length at offset {offset}"
        );
        let block_size = u64::from(BLOCK_SIZE);
        ensure!(
            offset
                .checked_add(len as u64)
                .is_some_and(|end| end <= self.state.mem_spec()),
            "mem.upper read [{offset}, +{len}) beyond mem_spec {}",
            self.state.mem_spec()
        );
        let mut out = BytesMut::zeroed(len);
        let mut cursor = offset;
        let end = offset + len as u64;
        while cursor < end {
            let block = (cursor / block_size) as u32;
            let block_start = u64::from(block) * block_size;
            let in_block = (cursor - block_start) as usize;
            let span = ((block_start + block_size).min(end) - cursor) as usize;
            let bytes = self.block_bytes(block).await?;
            let begin = (cursor - offset) as usize;
            out[begin..begin + span].copy_from_slice(&bytes[in_block..in_block + span]);
            cursor += span as u64;
        }
        Ok(out.freeze())
    }

    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        let data = self.read_at(offset, dst.len()).await?;
        dst.copy_from_slice(&data);
        Ok(data.len())
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("mem.upper is opened read-only")
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.state.mem_spec())
    }
}

/// `LogIndex` shading the varlen upper over an immutable lower stack
/// (I-2: same shape as `ComboIndex` — upper shadows, gaps fall to lower).
pub struct VarlenIndex {
    state: Arc<MemUpperState>,
    lower: Option<Arc<ReadOnlyIndex>>,
    /// Tag of the mem.upper layer inside the composite layer list.
    upper_tag: u8,
    /// Lower mappings carry tags for the lower layer list; shift them past
    /// the mem.upper layer.
    lower_tag_base: u8,
}

impl VarlenIndex {
    pub fn new(state: Arc<MemUpperState>, lower: Option<Arc<ReadOnlyIndex>>) -> Self {
        Self {
            state,
            lower,
            upper_tag: 0,
            lower_tag_base: 1,
        }
    }
}

impl LogIndex for VarlenIndex {
    fn lookup(&self, query: Segment, dst: &mut Vec<SegmentMapping>) -> usize {
        if query.length == 0 {
            return 0;
        }
        let start_len = dst.len();
        let sectors_per_block = u64::from(BLOCK_SECTORS);
        let block_count = u64::from(self.state.block_count());

        // Collect present runs (block-aligned, clipped to the query window),
        // splitting at `Segment::MAX_LENGTH` like any other index does.
        let mut upper_results = Vec::new();
        let limit = query.end().div_ceil(sectors_per_block).min(block_count);
        let mut block = query.offset / sectors_per_block;
        while block < limit {
            if !self.state.entry(block as u32).present() {
                block += 1;
                continue;
            }
            let run_start = block;
            while block < limit && self.state.entry(block as u32).present() {
                block += 1;
            }
            let lo = (run_start * sectors_per_block).max(query.offset);
            let hi = (block * sectors_per_block).min(query.end());
            let mut cursor = lo;
            while cursor < hi {
                let len = (hi - cursor).min(u64::from(Segment::MAX_LENGTH)) as u32;
                // Identity moffset: physical offset == logical offset (I-3).
                upper_results.push(SegmentMapping::new(
                    cursor,
                    len,
                    cursor,
                    false,
                    self.upper_tag,
                ));
                cursor += u64::from(len);
            }
        }

        if self.lower.is_none() {
            dst.extend(upper_results);
            return dst.len() - start_len;
        }
        let lower = self.lower.as_ref().unwrap();

        // Fill gaps between the present runs from the lower stack, retagging
        // its mappings past the mem.upper layer (ComboIndex shape).
        let mut current_pos = query.offset;
        let end_pos = query.end();
        for m in upper_results {
            if m.offset() > current_pos {
                let gap_len = (m.offset() - current_pos) as u32;
                let mut gap_mappings = Vec::new();
                lower.lookup(Segment::new(current_pos, gap_len), &mut gap_mappings);
                for mut gap in gap_mappings {
                    gap.tag = gap.tag.saturating_add(self.lower_tag_base);
                    dst.push(gap);
                }
            }
            dst.push(m);
            current_pos = m.end();
        }
        if current_pos < end_pos {
            let gap_len = (end_pos - current_pos) as u32;
            let mut gap_mappings = Vec::new();
            lower.lookup(Segment::new(current_pos, gap_len), &mut gap_mappings);
            for mut gap in gap_mappings {
                gap.tag = gap.tag.saturating_add(self.lower_tag_base);
                dst.push(gap);
            }
        }
        dst.len() - start_len
    }
}

/// Assembles the composite read-only view: `[mem.upper, lower layers...]`
/// with the `VarlenIndex` shading absent blocks down to `lower`.
pub fn assemble_varlen_composite(
    state: Arc<MemUpperState>,
    lower: Option<LSMTReadOnlyFile>,
) -> Result<LSMTReadOnlyFile> {
    let mem_spec = state.mem_spec();
    let upper = Arc::new(MemUpperFile::new(Arc::clone(&state)));
    let mut layers: Vec<Arc<dyn VirtualFile>> = vec![upper];
    let mut lower_index = None;
    if let Some(lower) = lower {
        lower_index = Some(
            lower
                .index_view()
                .context("varlen composite requires a merged lower index view")?,
        );
        layers.extend(lower.get_lower_files());
    }
    let index = Arc::new(VarlenIndex::new(state, lower_index));
    let layer_count = layers.len();
    Ok(LSMTReadOnlyFile::from_layers_with_index(
        layers,
        index,
        mem_spec,
        vec![uuid::Uuid::nil(); layer_count],
        LSMTFileType::ReadOnly,
    ))
}

/// Reads only the header epoch of a `mem.upper` file, for the paused-record
/// epoch contract (D2-A).
pub fn read_mem_upper_epoch(path: &Path) -> Result<u64> {
    let file =
        std::fs::File::open(path).with_context(|| format!("open mem.upper {}", path.display()))?;
    let mut page = vec![0u8; 4096];
    super::format::pread_exact(&file, 0, &mut page, "header")?;
    Ok(MemUpperHeader::decode(&page)?.epoch)
}

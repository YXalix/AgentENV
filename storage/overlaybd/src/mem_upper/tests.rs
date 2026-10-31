//! In-process tests for `mem_upper` (design §7 checklist item 12 / I-11):
//! crash matrix via on-disk corruption, splice-vs-readv full byte compare
//! (the release gate), skip-write idempotence, template reclamation, epoch
//! contract, vacuum, and collapse-to-standard-layer (I-3).

use super::format::BLOCK_SIZE;
use super::reader::{assemble_varlen_composite, read_mem_upper_epoch, MemUpperState};
use super::writer::{
    DirtyPage, HarvestOptions, HarvestStats, HarvestViews, MemUpperWriter, RemoteBlockReader,
};
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

use crate::backend::local::LocalFile;
use crate::io::virtual_file::{VirtualFile, VirtualFileWriter};
use crate::lsmt::file::{
    compact_to, create_file_rw, open_file_ro, CommitArgs, LSMTReadOnlyFile, LayerInfo, RwLayout,
};
use crate::lsmt::index::Segment;
use crate::mem_upper::reader::MemUpperFile;

const MEM_SPEC: u64 = 8 * 1024 * 1024; // 512 blocks of 16KiB

/// Deterministic lower-stack content: a pure function of the address.
fn lower_byte(addr: u64) -> u8 {
    (addr.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) as u8
}

/// Direct pattern view; identical in content to the LSMT lower built by
/// [`build_pattern_lower`], used where a plain `VirtualFile` suffices.
struct PatternLower {
    size: u64,
}

#[async_trait]
impl VirtualFile for PatternLower {
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let data: Vec<u8> = (offset..offset + len as u64).map(lower_byte).collect();
        Ok(Bytes::from(data))
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("PatternLower is read-only")
    }

    async fn size(&self) -> Result<u64> {
        Ok(self.size)
    }
}

/// Synthetic guest RAM standing in for `process_vm_readv`.
struct SyntheticRam {
    mem: Vec<u8>,
    base_addr: u64,
}

impl SyntheticRam {
    fn new(base_addr: u64) -> Self {
        Self {
            mem: vec![0u8; MEM_SPEC as usize],
            base_addr,
        }
    }

    fn fill_from_lower(&mut self) {
        for (i, slot) in self.mem.iter_mut().enumerate() {
            *slot = lower_byte(i as u64);
        }
    }
}

impl RemoteBlockReader for SyntheticRam {
    fn read_exact_remote(&self, addr: u64, dst: &mut [u8]) -> Result<()> {
        let begin = addr
            .checked_sub(self.base_addr)
            .ok_or_else(|| anyhow::anyhow!("read below RAM base"))? as usize;
        if begin + dst.len() > self.mem.len() {
            bail!("read beyond synthetic RAM");
        }
        dst.copy_from_slice(&self.mem[begin..begin + dst.len()]);
        Ok(())
    }
}

/// Builds a standard LSMT read-only layer carrying the pattern content, so
/// tests exercise the same lower shape production code sees.
async fn build_pattern_lower(dir: &TempDir) -> LSMTReadOnlyFile {
    let data: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(dir.path().join("lower.data")).unwrap());
    let index: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(dir.path().join("lower.index")).unwrap());
    let mut args = LayerInfo::new(data, Some(index), MEM_SPEC);
    args.rw_layout = RwLayout::HybridLogStructured;
    let lower_rw = create_file_rw(args).await.unwrap();
    let mut chunk = vec![0u8; BLOCK_SIZE as usize];
    let block_size = u64::from(BLOCK_SIZE);
    for block in 0..(MEM_SPEC / block_size) {
        for (i, slot) in chunk.iter_mut().enumerate() {
            *slot = lower_byte(block * block_size + i as u64);
        }
        lower_rw.write_at(block * block_size, &chunk).await.unwrap();
    }
    let sealed: Arc<dyn VirtualFile> =
        Arc::new(LocalFile::new(dir.path().join("lower.sealed")).unwrap());
    lower_rw.commit(sealed).await.unwrap();
    open_file_ro(Arc::new(
        LocalFile::open_ro(dir.path().join("lower.sealed")).unwrap(),
    ))
    .await
    .unwrap()
}

fn dirty_pages_for(blocks: &[u32], pages_per_block: usize, rng: &mut StdRng) -> Vec<DirtyPage> {
    let mut dirty = Vec::new();
    for &block in blocks {
        let mut pages: Vec<u32> = (0..4).collect();
        for i in 0..pages_per_block {
            let j = i + rng.random_range(0..(4 - i));
            pages.swap(i, j);
        }
        for page in pages.into_iter().take(pages_per_block) {
            dirty.push(DirtyPage {
                logical_offset: u64::from(block) * u64::from(BLOCK_SIZE) + u64::from(page) * 4096,
                host_addr: 0x1000_0000
                    + u64::from(block) * u64::from(BLOCK_SIZE)
                    + u64::from(page) * 4096,
            });
        }
    }
    dirty
}

fn mutate(ram: &mut SyntheticRam, rng: &mut StdRng, dirty: &[DirtyPage]) {
    for page in dirty {
        let begin = (page.host_addr - 0x1000_0000) as usize;
        rng.fill(&mut ram.mem[begin..begin + 4096]);
    }
}

async fn open_state(path: &Path) -> MemUpperState {
    let file: Arc<dyn VirtualFile> = Arc::new(LocalFile::open_ro(path).unwrap());
    MemUpperState::open(file, MEM_SPEC).await.unwrap()
}

/// Reads the full authority chain: present blocks from `mem.upper`, absent
/// blocks from the lower view — what a composite would serve.
async fn read_via_chain(path: &Path, lower: &Arc<dyn VirtualFile>) -> Vec<u8> {
    let state = Arc::new(open_state(path).await);
    let upper = MemUpperFile::new(Arc::clone(&state));
    let block_size = BLOCK_SIZE as usize;
    let total = MEM_SPEC as usize / block_size;
    let mut out = vec![0u8; MEM_SPEC as usize];
    for b in 0..total {
        let range = b * block_size..(b + 1) * block_size;
        let offset = b as u64 * BLOCK_SIZE as u64;
        if state.entry(b as u32).present() {
            upper.read_at_into(offset, &mut out[range]).await.unwrap();
        } else {
            lower.read_at_into(offset, &mut out[range]).await.unwrap();
        }
    }
    out
}

async fn harvest_cycle(
    path: &Path,
    views: HarvestViews,
    ram: &SyntheticRam,
    dirty: &[DirtyPage],
    opts: &HarvestOptions,
) -> HarvestStats {
    let mut writer = MemUpperWriter::open(path, MEM_SPEC).unwrap();
    let stats = writer.harvest(dirty, ram, views, opts).await.unwrap();
    writer.commit().unwrap();
    stats
}

fn uniform_views(lower: LSMTReadOnlyFile) -> HarvestViews {
    HarvestViews::uniform(Arc::new(lower) as Arc<dyn VirtualFile>)
}

#[tokio::test]
async fn multi_cycle_storm_matches_model() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let lower = build_pattern_lower(&dir).await;

    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    // The model tracks what the composite must serve: lower content plus
    // every applied dirty write (the splice correctness chain).
    let mut model = ram.mem.clone();

    let mut rng = StdRng::seed_from_u64(42);
    let lower_views = uniform_views(lower);
    for cycle in 0..12u64 {
        let mut blocks: Vec<u32> = (0..(MEM_SPEC / u64::from(BLOCK_SIZE)) as u32).collect();
        for i in 0..24 {
            let j = i + rng.random_range(0..(blocks.len() - i));
            blocks.swap(i, j);
        }
        blocks.truncate(24);
        let dirty = dirty_pages_for(&blocks, 3, &mut rng);
        mutate(&mut ram, &mut rng, &dirty);
        for page in &dirty {
            let begin = (page.host_addr - 0x1000_0000) as usize;
            model[begin..begin + 4096].copy_from_slice(&ram.mem[begin..begin + 4096]);
        }
        let stats = harvest_cycle(
            &path,
            lower_views.clone(),
            &ram,
            &dirty,
            &HarvestOptions::default(),
        )
        .await;
        assert_eq!(stats.dirty_pages, dirty.len() as u64);

        let state = Arc::new(open_state(&path).await);
        let fresh_lower = build_pattern_lower(&dir).await;
        let composite = assemble_varlen_composite(state, Some(fresh_lower)).unwrap();
        let viewed = composite
            .read_at(0, MEM_SPEC as usize)
            .await
            .unwrap()
            .to_vec();
        assert_eq!(
            viewed, model,
            "cycle {cycle}: composite view diverged from model"
        );
    }
}

#[tokio::test]
async fn splice_and_readv_products_are_identical() {
    // Release gate (design §6): full byte compare of the splice product
    // against the whole-block readv product over identical RAM and bitmaps.
    let mut products: Vec<(Vec<u8>, Vec<u8>, TempDir)> = Vec::new();
    for force in [false, true] {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("mem.upper");
        MemUpperWriter::create(&path, MEM_SPEC).unwrap();
        let lower = build_pattern_lower(&dir).await;
        let mut ram = SyntheticRam::new(0x1000_0000);
        ram.fill_from_lower();
        let mut rng = StdRng::seed_from_u64(99);
        let blocks: Vec<u32> = vec![1, 2, 5, 9, 40, 100, 300, 511];
        let dirty = dirty_pages_for(&blocks, 4, &mut rng);
        mutate(&mut ram, &mut rng, &dirty);
        let mut writer = MemUpperWriter::open(&path, MEM_SPEC).unwrap();
        let opts = HarvestOptions {
            force_readv: force,
            ..Default::default()
        };
        let stats = writer
            .harvest(&dirty, &ram, uniform_views(lower), &opts)
            .await
            .unwrap();
        writer.commit().unwrap();
        if force {
            assert!(stats.readv_fallback > 0);
        } else {
            assert_eq!(stats.readv_fallback, 0);
        }
        let pattern: Arc<dyn VirtualFile> = Arc::new(PatternLower { size: MEM_SPEC });
        let served = read_via_chain(&path, &pattern).await;
        assert_eq!(served, ram.mem);
        let bytes = std::fs::read(&path).unwrap();
        products.push((bytes, served, dir));
    }
    let (splice_bytes, splice_served, keep_a) = products.pop().unwrap();
    let (readv_bytes, readv_served, keep_b) = products.pop().unwrap();
    assert_eq!(splice_bytes, readv_bytes, "on-disk products must match");
    assert_eq!(splice_served, readv_served);
    drop(keep_a);
    drop(keep_b);
}

#[tokio::test]
async fn second_harvest_with_unchanged_ram_writes_nothing() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let lower = build_pattern_lower(&dir).await;
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(5);
    let dirty = dirty_pages_for(&[3, 7, 77], 2, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    let first = harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    assert!(first.written > 0);
    let before = std::fs::read(&path).unwrap();
    let before_tail = before[4096..].to_vec();

    let lower = build_pattern_lower(&dir).await;
    let second = harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    assert_eq!(second.written, 0, "unchanged RAM must skip every block");
    assert_eq!(second.skipped, 3);
    let after = std::fs::read(&path).unwrap();
    // The header epoch legitimately bumps; everything past it must be frozen.
    assert_eq!(
        before_tail,
        after[4096..],
        "skip-write must not touch the file"
    );
}

#[tokio::test]
async fn template_equal_block_reclaims_extent() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(6);
    let block = 11u32;
    let dirty = dirty_pages_for(&[block], 4, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    let lower = build_pattern_lower(&dir).await;
    let wrote = harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    assert_eq!(wrote.written, 1);
    let busy = MemUpperWriter::open(&path, MEM_SPEC).unwrap().footprint();
    assert!(busy.live_bytes > 0);

    // Write the lower bytes back into RAM: the block becomes template-equal.
    let base = u64::from(block) * u64::from(BLOCK_SIZE);
    for i in 0..u64::from(BLOCK_SIZE) {
        ram.mem[(base + i) as usize] = lower_byte(base + i);
    }
    let lower = build_pattern_lower(&dir).await;
    let reclaimed = harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    assert_eq!(reclaimed.template_equal, 1, "block must collapse to len=0");
    let idle = MemUpperWriter::open(&path, MEM_SPEC).unwrap().footprint();
    assert_eq!(idle.live_bytes, 0);
    assert_eq!(idle.free_bytes, busy.live_bytes);

    // Absent blocks must fail loud through the adaptor.
    let state = Arc::new(open_state(&path).await);
    let upper = MemUpperFile::new(state);
    assert!(upper.read_at(0, 16).await.is_err());
}

#[tokio::test]
async fn torn_payload_fails_loud_and_next_harvest_recovers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(7);
    let dirty = dirty_pages_for(&[13, 14], 4, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    let lower = build_pattern_lower(&dir).await;
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    // Simulate a torn payload write: flip a byte inside the data area.
    let raw = std::fs::read(&path).unwrap();
    let data_start = MemUpperWriter::open(&path, MEM_SPEC)
        .unwrap()
        .header()
        .data_start as usize;
    let mut torn = raw.clone();
    torn[data_start + 512] ^= 0xFF;
    std::fs::write(&path, torn).unwrap();

    // The reader must fail loud on the torn block.
    let state = Arc::new(open_state(&path).await);
    let upper = MemUpperFile::new(state);
    assert!(
        upper.read_at(0, MEM_SPEC as usize).await.is_err(),
        "torn payload must fail loud"
    );

    // The next harvest detects the crc mismatch, falls back to whole-block
    // readv, and converges to the frozen RAM content.
    let lower = build_pattern_lower(&dir).await;
    let recovered = harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    assert!(recovered.crc_invalid >= 1);
    assert!(recovered.readv_fallback >= 1);
    let pattern: Arc<dyn VirtualFile> = Arc::new(PatternLower { size: MEM_SPEC });
    let served = read_via_chain(&path, &pattern).await;
    assert_eq!(served, ram.mem);
}

#[tokio::test]
async fn duplicated_metadata_entry_rejected_at_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(8);
    let dirty = dirty_pages_for(&[21], 4, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    let lower = build_pattern_lower(&dir).await;
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    // Duplicate block 21's entry word onto block 22: two blocks claiming the
    // same extent must be rejected while the free-list is rebuilt.
    let raw = std::fs::read(&path).unwrap();
    let word = raw[4096 + 21 * 8..4096 + 22 * 8].to_vec();
    let mut torn = raw;
    torn[4096 + 22 * 8..4096 + 23 * 8].copy_from_slice(&word);
    std::fs::write(&path, torn).unwrap();
    assert!(MemUpperWriter::open(&path, MEM_SPEC).is_err());
}

#[tokio::test]
async fn epoch_reads_and_bumps_per_commit() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    let mut writer = MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    assert_eq!(read_mem_upper_epoch(&path).unwrap(), 0);
    let e1 = writer.commit().unwrap();
    let e2 = writer.commit().unwrap();
    assert_eq!((e1, e2), (1, 2));
    assert_eq!(read_mem_upper_epoch(&path).unwrap(), 2);

    // Header tampering must fail the crc check (fail loud, not silent zero).
    let raw = std::fs::read(&path).unwrap();
    let mut tampered = raw.clone();
    tampered[24] ^= 0xFF; // epoch byte
    std::fs::write(&path, tampered).unwrap();
    assert!(read_mem_upper_epoch(&path).is_err());
}

#[tokio::test]
async fn full_harvest_into_fresh_file_materializes_composite() {
    // Vacuum / migration primitive: a fresh writer whose lower view is the
    // old composite must reproduce the exact content (design §4.4).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let lower = build_pattern_lower(&dir).await;
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(12);
    let mut dirty = Vec::new();
    for block in [2u32, 3, 4, 50, 51] {
        dirty.extend(dirty_pages_for(&[block], 2, &mut rng));
    }
    mutate(&mut ram, &mut rng, &dirty);
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    let new_path = dir.path().join("mem.upper.new");
    let mut fresh = MemUpperWriter::create(&new_path, MEM_SPEC).unwrap();
    let state = Arc::new(open_state(&path).await);
    let old_lower = build_pattern_lower(&dir).await;
    let old_composite: Arc<dyn VirtualFile> =
        Arc::new(assemble_varlen_composite(state, Some(old_lower)).unwrap());
    let opts = HarvestOptions {
        full: true,
        ..Default::default()
    };
    let verify_lower = Arc::clone(&old_composite);
    let pattern: Arc<dyn VirtualFile> = Arc::new(PatternLower { size: MEM_SPEC });
    let views = HarvestViews {
        stitch: old_composite,
        template: pattern,
    };
    fresh.harvest(&[], &ram, views, &opts).await.unwrap();
    fresh.commit().unwrap();
    let materialized = read_via_chain(&new_path, &verify_lower).await;
    assert_eq!(materialized, ram.mem, "fresh full harvest must match RAM");
}

#[tokio::test]
async fn vacuum_shrinks_fragmented_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let lower = build_pattern_lower(&dir).await;
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();

    // Fragment: write many blocks, then reset most of them to lower content.
    let mut rng = StdRng::seed_from_u64(77);
    let blocks: Vec<u32> = (0..80).collect();
    let dirty = dirty_pages_for(&blocks, 4, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    for &block in blocks.iter().take(70) {
        let base = u64::from(block) * u64::from(BLOCK_SIZE);
        for i in 0..u64::from(BLOCK_SIZE) {
            ram.mem[(base + i) as usize] = lower_byte(base + i);
        }
    }
    let lower = build_pattern_lower(&dir).await;
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;
    let fragmented = MemUpperWriter::open(&path, MEM_SPEC).unwrap();
    assert!(fragmented.footprint().free_bytes > 0);
    assert!(fragmented.footprint().fragmentation > 0.5);
    assert!(fragmented.should_vacuum());

    // Vacuum: full harvest into a fresh file, then swap names in place.
    let new_path = dir.path().join("mem.upper.new");
    let mut fresh = MemUpperWriter::create(&new_path, MEM_SPEC).unwrap();
    let state = Arc::new(open_state(&path).await);
    let old_lower = build_pattern_lower(&dir).await;
    let old_composite: Arc<dyn VirtualFile> =
        Arc::new(assemble_varlen_composite(state, Some(old_lower)).unwrap());
    let opts = HarvestOptions {
        full: true,
        ..Default::default()
    };
    let verify_lower = Arc::clone(&old_composite);
    let pattern: Arc<dyn VirtualFile> = Arc::new(PatternLower { size: MEM_SPEC });
    let views = HarvestViews {
        stitch: old_composite,
        template: pattern,
    };
    fresh.harvest(&[], &ram, views, &opts).await.unwrap();
    fresh.commit().unwrap();
    let vacuumed = fresh.footprint();
    assert!(vacuumed.file_bytes < fragmented.footprint().file_bytes);
    assert_eq!(vacuumed.free_bytes, 0);
    std::fs::rename(&new_path, &path).unwrap();
    let served = read_via_chain(&path, &verify_lower).await;
    assert_eq!(served, ram.mem);
}

#[tokio::test]
async fn collapse_to_standard_layer_matches_composite() {
    // I-3: identity-moffset semantics must let compact_to consume the varlen
    // view and produce a standard LSMT lower that reads identically — this
    // is exactly what capture/fork/publish reuse (design §4.4).
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();

    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(21);
    let dirty = dirty_pages_for(&[8, 9, 120, 400], 3, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);

    let lower = build_pattern_lower(&dir).await;
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    // Open the composite (fresh lower open so tags line up) and collapse.
    let state = Arc::new(open_state(&path).await);
    let lower_again = build_pattern_lower(&dir).await;
    let composite = assemble_varlen_composite(state, Some(lower_again)).unwrap();
    let composite_content = composite
        .read_at(0, MEM_SPEC as usize)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(composite_content, ram.mem);

    let query = Segment::new(0, (MEM_SPEC / 512) as u32);
    let mut mappings = Vec::new();
    composite.index().lookup(query, &mut mappings);
    assert!(!mappings.is_empty());

    let collapsed_path: PathBuf = dir.path().join("collapsed.commit");
    let dest: Arc<dyn VirtualFile> = Arc::new(LocalFile::new(&collapsed_path).unwrap());
    let writer: Arc<dyn storage_util::CompactWriter> = Arc::new(VirtualFileWriter::new(dest));
    let commit_args = CommitArgs::from_writer(writer);
    let layers = composite.get_lower_files();
    compact_to(&layers, &mappings, MEM_SPEC, commit_args)
        .await
        .unwrap();

    let collapsed = open_file_ro(Arc::new(LocalFile::open_ro(&collapsed_path).unwrap()))
        .await
        .unwrap();
    let collapsed_content = collapsed
        .read_at(0, MEM_SPEC as usize)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(
        collapsed_content, ram.mem,
        "collapsed standard layer must match the varlen composite"
    );
}

#[tokio::test]
async fn varlen_index_clips_and_orders_present_runs() {
    use crate::lsmt::index::{LogIndex, SegmentMapping};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("mem.upper");
    MemUpperWriter::create(&path, MEM_SPEC).unwrap();
    let mut ram = SyntheticRam::new(0x1000_0000);
    ram.fill_from_lower();
    let mut rng = StdRng::seed_from_u64(31);
    let dirty = dirty_pages_for(&[10, 11, 12, 200], 4, &mut rng);
    mutate(&mut ram, &mut rng, &dirty);
    let lower = build_pattern_lower(&dir).await;
    harvest_cycle(
        &path,
        uniform_views(lower),
        &ram,
        &dirty,
        &HarvestOptions::default(),
    )
    .await;

    let state = Arc::new(open_state(&path).await);
    let index = super::reader::VarlenIndex::new(state, None);
    // Present blocks 10..=12 form one contiguous run; block 200 stands alone.
    // (All offsets in 512B sectors; block = 32 sectors.)
    let mut dst = Vec::new();
    let added = index.lookup(Segment::new(0, (MEM_SPEC / 512) as u32), &mut dst);
    assert_eq!(added, dst.len());
    assert!(dst.iter().all(|m| m.tag == 0));
    assert!(dst.windows(2).all(|w| w[0].end() <= w[1].offset()));
    // A run covering blocks 10..=13 clipped to block 10..=12.
    let run1 = dst
        .iter()
        .find(|m| m.offset() == 10 * 32)
        .expect("run starts at block 10");
    assert_eq!(run1.length(), 3 * 32);
    assert_eq!(run1.moffset, run1.offset());
    let run2 = dst
        .iter()
        .find(|m| m.offset() == 200 * 32)
        .expect("run starts at block 200");
    assert_eq!(run2.length(), 32);

    // Sub-block clipping: query inside a block trims the run.
    let mut partial = Vec::new();
    index.lookup(Segment::new(10 * 32 + 5, 10), &mut partial);
    assert_eq!(partial.len(), 1);
    assert_eq!(partial[0].offset(), 10 * 32 + 5);
    assert_eq!(partial[0].length(), 10);
    assert_eq!(partial[0].moffset, 10 * 32 + 5);

    // MAX_LENGTH clipping keeps mappings sorted, non-overlapping and
    // identity-mapped; absent gaps simply do not appear (no lower here).
    let mut split = Vec::new();
    index.lookup(Segment::new(10 * 32, Segment::MAX_LENGTH * 3), &mut split);
    assert_eq!(split.len(), 2, "blocks 10..=12 and 200 are present");
    for window in split.windows(2) {
        assert!(window[0].end() <= window[1].offset());
    }
    for m in &split {
        assert_eq!(m.moffset, m.offset(), "identity moffset");
        assert!(m.length() <= Segment::MAX_LENGTH);
    }
    let _ = SegmentMapping::default();
}

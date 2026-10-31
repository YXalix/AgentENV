//! Varlen storm-pause wiring (design §4.2/§4.3): dirty-range mapping into
//! harvest pages, the stable per-sandbox mem root, legacy-stack splice
//! migration with the S-c throttle, vacuum, and epoch bookkeeping.
//!
//! The stable root lives beside the per-pause snapshot dirs
//! (`<artifact-base>/<sandbox>/mem`), so `delete_record_and_artifacts`
//! already covers its lifetime. `mem.upper` and `image.json`/`lowers.json`
//! are created durably (file + parent fsync) before the first record.

use anyhow::{ensure, Context, Result};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tokio::sync::Semaphore;

use firecracker_client::models::DirtyMemoryRanges;
use nix::unistd::Pid;
use tracing::{info, warn};

use overlaybd::config::{ImageConfig, LayerConfig, MemUpperConfig};
use overlaybd::image_file::{open_lower_stack_from_config, open_varlen_composite_from_config};
use overlaybd::mem_upper::{
    DirtyPage, Footprint, HarvestOptions, HarvestStats, HarvestViews, MemUpperWriter,
    RemoteBlockReader,
};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::ImageService;

use super::overlaybd_snapshot::write_bytes_atomically;
use super::process_vm_reader::ProcessVmReader;
use crate::cfg::ConfigManager;

/// S-c migration throttle: bounds concurrent first-time varlen migrations
/// (design D6-A). Sandboxes that cannot take a permit pause legacy — the
/// dual-path rollback / kill-switch basis.
const MIGRATION_CONCURRENCY: usize = 12;

fn migration_permits() -> &'static Semaphore {
    static PERMITS: OnceLock<Semaphore> = OnceLock::new();
    PERMITS.get_or_init(|| Semaphore::new(MIGRATION_CONCURRENCY))
}

/// The stable per-sandbox mem root derived from the per-pause snapshot dir.
pub(crate) fn stable_mem_root(snapshot_dir: &Path) -> PathBuf {
    snapshot_dir
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("mem")
}

impl RemoteBlockReader for ProcessVmReader {
    fn read_exact_remote(&self, addr: u64, dst: &mut [u8]) -> Result<()> {
        // Method-call syntax prefers the inherent `process_vm_readv` loop.
        ProcessVmReader::read_exact_remote_inherent(self, addr, dst)
    }
}

pub(crate) struct VarlenPauseOutcome {
    pub mem_upper_path: PathBuf,
    pub stable_image_config: PathBuf,
    pub epoch: u64,
}

pub(crate) enum VarlenPause {
    Harvested(VarlenPauseOutcome),
    /// Kill switch: the sandbox keeps the legacy per-pause layer path.
    Legacy,
}

/// Flattens Firecracker dirty ranges into per-4K harvest pages and returns
/// `mem_spec` alongside.
fn dirty_pages(dirty_ranges: &DirtyMemoryRanges) -> Result<(Vec<DirtyPage>, u64)> {
    const PAGE: u64 = 4096;
    let checked = |value: i64, field: &str| -> Result<u64> {
        u64::try_from(value)
            .with_context(|| format!("dirty {field} must be non-negative, got {value}"))
    };
    ensure!(
        dirty_ranges.page_size == 4096,
        "dirty memory page_size must be 4096, got {}",
        dirty_ranges.page_size
    );
    let memory_size = checked(dirty_ranges.memory_size, "memory_size")?;
    let mut pages = Vec::new();
    for range in &dirty_ranges.ranges {
        let base = checked(range.base_host_virt_addr, "base_host_virt_addr")?;
        let image_offset = checked(range.image_offset, "image_offset")?;
        let length = checked(range.length, "length")?;
        ensure!(length > 0, "dirty memory range length must be positive");
        ensure!(
            base % PAGE == 0,
            "dirty range base {base:#x} not 4K aligned"
        );
        ensure!(
            image_offset % PAGE == 0,
            "dirty range image_offset {image_offset:#x} not 4K aligned"
        );
        ensure!(
            length % PAGE == 0,
            "dirty range length {length} not 4K aligned"
        );
        ensure!(
            image_offset + length <= memory_size,
            "dirty range [{image_offset}, +{length}) exceeds memory_size {memory_size}"
        );
        for k in 0..(length / PAGE) {
            pages.push(DirtyPage {
                logical_offset: image_offset + k * PAGE,
                host_addr: base + k * PAGE,
            });
        }
    }
    Ok((pages, memory_size))
}

async fn image_service() -> Result<Arc<ImageService>> {
    let global_config = ConfigManager::global_config();
    let service =
        ImageService::from_config_path(&global_config.memory_snapshot.overlaybd_global_config_path)
            .await
            .context("build image service for varlen pause")?;
    Ok(Arc::new(service))
}

/// Harvests one varlen pause into the stable root.
///
/// * steady state (`current_mem_upper` is `Some`): incremental harvest with
///   the stable lowers as stitch/template view, vacuum when fragmented;
/// * migration: hardlinks the legacy stack's local layers into
///   `<stable>/legacy/`, writes `image.json` + `lowers.json`, then splices
///   the dirty pages over the legacy view — no full harvest, no spike.
pub(crate) async fn harvest_varlen_memory(
    firecracker_pid: Pid,
    dirty_ranges: &DirtyMemoryRanges,
    stable_root: &Path,
    current_mem_image_config: Option<&Path>,
    current_mem_upper: Option<&Path>,
    current_mem_virtual_size: u64,
) -> Result<VarlenPause> {
    let (dirty, mem_spec) = dirty_pages(dirty_ranges)?;
    if let Some(expected) = (current_mem_upper.is_some()).then_some(current_mem_virtual_size) {
        ensure!(
            expected == mem_spec,
            "mem_spec {mem_spec} disagrees with sandbox mem_virtual_size {expected}"
        );
    }
    tokio::fs::create_dir_all(stable_root)
        .await
        .with_context(|| format!("create stable mem root {}", stable_root.display()))?;
    let mem_upper_path = stable_root.join("mem.upper");
    let stable_image = stable_root.join("image.json");
    let stable_lowers = stable_root.join("lowers.json");
    let service = image_service().await?;

    let steady = match current_mem_upper {
        Some(current) => {
            ensure!(
                mem_upper_path.exists(),
                "varlen state expects {} to exist",
                mem_upper_path.display()
            );
            ensure!(
                stable_image.exists(),
                "varlen state expects {} to exist",
                stable_image.display()
            );
            let _ = current;
            true
        }
        None => {
            let legacy = current_mem_image_config
                .context("varlen migration requires the sandbox's launch mem image config")?;
            let permit = match migration_permits().try_acquire() {
                Ok(permit) => permit,
                Err(_) => {
                    metrics::counter!("agentenv_mem_pause_migration_throttled_total").increment(1);
                    info!("varlen migration throttled; pausing legacy");
                    return Ok(VarlenPause::Legacy);
                }
            };
            migrate_stable_images(
                legacy,
                stable_root,
                &mem_upper_path,
                &stable_image,
                &stable_lowers,
            )
            .await?;
            drop(permit);
            false
        }
    };

    let reader = ProcessVmReader::new(firecracker_pid);
    let mut writer = if steady {
        MemUpperWriter::open(&mem_upper_path, mem_spec)
            .with_context(|| format!("open {}", mem_upper_path.display()))?
    } else {
        MemUpperWriter::create(&mem_upper_path, mem_spec)
            .with_context(|| format!("create {}", mem_upper_path.display()))?
    };

    let epoch = if steady && writer.should_vacuum() {
        let footprint = writer.footprint();
        info!(
            file_bytes = footprint.file_bytes,
            live_bytes = footprint.live_bytes,
            fragmentation = footprint.fragmentation,
            "vacuuming fragmented mem.upper"
        );
        vacuum(
            &mem_upper_path,
            mem_spec,
            &reader,
            &dirty,
            &service,
            &stable_image,
            &stable_lowers,
        )
        .await?
    } else {
        let views = stable_views(&service, &stable_lowers).await?;
        let stats = writer
            .harvest(&dirty, &reader, views, &HarvestOptions::default())
            .await?;
        let epoch = writer.commit()?;
        report_harvest("incremental", &stats, &writer.footprint());
        epoch
    };

    Ok(VarlenPause::Harvested(VarlenPauseOutcome {
        mem_upper_path,
        stable_image_config: stable_image,
        epoch,
    }))
}

/// Full re-harvest into `mem.upper.new` over the old composite, then rename
/// in place (self-reseal: fragment vacuum + union collapse, design §4.1).
async fn vacuum(
    mem_upper_path: &Path,
    mem_spec: u64,
    reader: &ProcessVmReader,
    dirty: &[DirtyPage],
    service: &ImageService,
    stable_image: &Path,
    stable_lowers: &Path,
) -> Result<u64> {
    let new_path = mem_upper_path.with_extension("new");
    let _ = std::fs::remove_file(&new_path);
    let mut fresh = MemUpperWriter::create(&new_path, mem_spec)
        .with_context(|| format!("create vacuum target {}", new_path.display()))?;
    let composite: Arc<dyn VirtualFile> = Arc::new(
        open_varlen_composite_from_config(service, stable_image)
            .await
            .context("open old composite for vacuum")?,
    );
    let template: Arc<dyn VirtualFile> = Arc::new(
        open_lower_stack_from_config(service, stable_lowers)
            .await
            .context("open lowers for vacuum template view")?,
    );
    let views = HarvestViews {
        stitch: composite,
        template,
    };
    let opts = HarvestOptions {
        full: true,
        ..Default::default()
    };
    let stats = fresh.harvest(dirty, reader, views, &opts).await?;
    let epoch = fresh.commit()?;
    report_harvest("vacuum", &stats, &fresh.footprint());
    std::fs::rename(&new_path, mem_upper_path)
        .with_context(|| format!("swap vacuumed mem.upper into {}", mem_upper_path.display()))?;
    if let Some(parent) = mem_upper_path.parent() {
        std::fs::File::open(parent)
            .and_then(|dir| dir.sync_all())
            .with_context(|| format!("sync mem root {}", parent.display()))?;
    }
    Ok(epoch)
}

/// Steady harvest views: stitch = template = the stable lowers.
async fn stable_views(service: &ImageService, stable_lowers: &Path) -> Result<HarvestViews> {
    let lower: Arc<dyn VirtualFile> = Arc::new(
        open_lower_stack_from_config(service, stable_lowers)
            .await
            .context("open stable lowers for harvest")?,
    );
    Ok(HarvestViews::uniform(lower))
}

/// Builds the stable image pair for a legacy-state sandbox: hardlinks every
/// local legacy layer under `legacy/` (same-filesystem by construction for
/// artifact-tree layers; layers outside the tree keep their original paths),
/// then writes `image.json` (with `memUpper`) and `lowers.json` durably.
async fn migrate_stable_images(
    legacy_config: &Path,
    stable_root: &Path,
    mem_upper_path: &Path,
    stable_image: &Path,
    stable_lowers: &Path,
) -> Result<()> {
    let raw = std::fs::read_to_string(legacy_config)
        .with_context(|| format!("read legacy mem image {}", legacy_config.display()))?;
    let mut config: ImageConfig = serde_json::from_str(&raw)
        .with_context(|| format!("parse legacy mem image {}", legacy_config.display()))?;
    overlaybd::config::resolve_image_config_local_paths(legacy_config, &mut config);
    let legacy_dir = stable_root.join("legacy");
    tokio::fs::create_dir_all(&legacy_dir)
        .await
        .with_context(|| format!("create {}", legacy_dir.display()))?;
    for (index, layer) in config.lowers.iter_mut().enumerate() {
        if layer.file.is_empty() || layer.file.contains("://") {
            continue;
        }
        let source = PathBuf::from(&layer.file);
        let name = source
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "layer".to_string());
        let target = legacy_dir.join(format!("{index:04}-{name}"));
        match std::fs::hard_link(&source, &target) {
            Ok(()) => layer.file = target.display().to_string(),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                layer.file = target.display().to_string();
            }
            Err(err) => {
                // Cross-filesystem or otherwise unlinkable: keep the
                // original path. Layers outside the artifact tree are never
                // swept by the persister, so the reference stays valid.
                warn!(
                    layer = %source.display(),
                    error = %err,
                    "keeping legacy layer path in stable mem image"
                );
            }
        }
    }

    let mut with_upper = config.clone();
    with_upper.mem_upper = Some(MemUpperConfig {
        file: mem_upper_path.display().to_string(),
    });
    let image_json =
        serde_json::to_vec_pretty(&with_upper).context("serialize stable mem image config")?;
    write_bytes_atomically(stable_image, &image_json, "stable mem image config")?;
    let lowers_json =
        serde_json::to_vec_pretty(&config).context("serialize stable lowers config")?;
    write_bytes_atomically(stable_lowers, &lowers_json, "stable lowers config")?;
    info!(
        stable_root = %stable_root.display(),
        layers = config.lowers.len(),
        "migrated sandbox mem stack into stable varlen root"
    );
    Ok(())
}

fn report_harvest(kind: &'static str, stats: &HarvestStats, footprint: &Footprint) {
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "skipped")
        .increment(stats.skipped);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "template_equal")
        .increment(stats.template_equal);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "written")
        .increment(stats.written);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "readv_fallback")
        .increment(stats.readv_fallback);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "crc_invalid")
        .increment(stats.crc_invalid);
    metrics::counter!("agentenv_mem_pause_bytes_total", "kind" => "payload")
        .increment(stats.payload_bytes);
    metrics::counter!("agentenv_mem_pause_bytes_total", "kind" => "moved")
        .increment(stats.moved_bytes);
    metrics::gauge!("agentenv_mem_upper_file_bytes").set(footprint.file_bytes as f64);
    metrics::gauge!("agentenv_mem_upper_live_bytes").set(footprint.live_bytes as f64);
    metrics::gauge!("agentenv_mem_upper_free_bytes").set(footprint.free_bytes as f64);
    metrics::gauge!("agentenv_mem_upper_free_extents").set(footprint.free_extents as f64);
    metrics::gauge!("agentenv_mem_upper_fragmentation_ratio").set(footprint.fragmentation);
    metrics::gauge!("agentenv_mem_upper_epoch").set(footprint.epoch as f64);
}

/// Collapses the varlen view (`lowers ∪ mem.upper`) into one standard
/// OverlayBD layer for capture/fork/publish (design §4.4, T1). The identity
/// `moffset` semantics let the existing compact engine consume the composite
/// unchanged (I-3).
/// Writes the capture's intermediate parent mem config: the stable lowers
/// plus the collapsed lower, with no `memUpper` — the dirty layer captured
/// on top appends to this stack.
pub(crate) async fn write_collapsed_parent_config(
    stable_image: &Path,
    collapsed: &Path,
    parent_config: &Path,
) -> Result<()> {
    let raw = std::fs::read_to_string(stable_image)
        .with_context(|| format!("read {}", stable_image.display()))?;
    let mut config: ImageConfig =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", stable_image.display()))?;
    overlaybd::config::resolve_image_config_local_paths(stable_image, &mut config);
    config.mem_upper = None;
    config.lowers.push(LayerConfig {
        file: collapsed.display().to_string(),
        ..LayerConfig::default()
    });
    let bytes =
        serde_json::to_vec_pretty(&config).context("serialize collapsed parent mem config")?;
    write_bytes_atomically(parent_config, &bytes, "collapsed parent mem config")?;
    Ok(())
}

pub(crate) async fn collapse_varlen_mem(
    stable_image_config: &Path,
    output_dir: &Path,
) -> Result<PathBuf> {
    let service = image_service().await?;
    let composite = open_varlen_composite_from_config(&service, stable_image_config)
        .await
        .context("open varlen composite for collapse")?;
    let mem_spec = composite.size().await?;
    let query = overlaybd::index::Segment::new(
        0,
        u32::try_from(mem_spec / 512).context("mem_spec overflow")?,
    );
    let mut mappings = Vec::new();
    composite.index().lookup(query, &mut mappings);
    ensure!(
        !mappings.is_empty(),
        "varlen composite has no mappings to collapse"
    );
    let layers = composite.get_lower_files();
    let data_path = output_dir.join("collapsed.commit");
    let global_config = ConfigManager::global_config();
    let memory_output = crate::sandbox::ublk::OverlaybdCompactOutput::from_memory_snapshot_config(
        &global_config.memory_snapshot,
        &global_config.snapshot.publish_compression,
    );
    super::overlaybd_snapshot::publish_memory_overlaybd_layer(
        &layers,
        &mappings,
        mem_spec,
        &data_path,
        memory_output,
        super::overlaybd_snapshot::DIRECT_MEMORY_SNAPSHOT_COMPACTION_CONCURRENCY,
    )
    .await?;
    Ok(data_path)
}

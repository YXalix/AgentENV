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
use tracing::{error, info, warn};

use overlaybd::config::{ImageConfig, LayerConfig, MemUpperConfig};
use overlaybd::image_file::{open_lower_stack_from_config, open_varlen_composite_from_config};
use overlaybd::mem_upper::{
    DirtyPage, Footprint, HarvestOptions, HarvestStats, HarvestViews, MemUpperGeneration,
    MemUpperWriter, RemoteBlockReader,
};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::ImageService;

use super::overlaybd_snapshot::{sync_parent_directory, write_bytes_atomically};
use super::process_vm_reader::ProcessVmReader;
use super::sandbox::{LaunchMode, PauseMode};
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

/// Which memory-persistence path one pause takes (design §4.0). Decided once
/// up front from the pause mode and the launch state; everything downstream
/// matches on this instead of re-deriving the route from scattered
/// conditions.
#[derive(Debug)]
pub(crate) enum MemoryPlan {
    /// Storm pause on a resumed sandbox: harvest the dirty blocks into the
    /// stable `mem.upper`. `mem_upper: None` means the sandbox still runs a
    /// legacy layer stack, so the harvest first migrates it (throttled; a
    /// saturated budget falls back to a per-pause layer this cycle).
    VarlenHarvest {
        mem_upper: Option<PathBuf>,
        legacy_config: Option<PathBuf>,
        mem_spec: u64,
    },
    /// Capture on a varlen-lineage sandbox: collapse the composite into an
    /// immutable lower (`mem_parent.json`), materialize the dirty layer on
    /// top of it, then re-base the live stack onto the collapsed lower once
    /// the record lands (the deferred `ParentSwitch`, design §4.4 T1).
    CollapseForCapture {
        stable_image: PathBuf,
        mem_upper: PathBuf,
    },
    /// Everything else (fresh sandbox, capture on a legacy-lineage sandbox,
    /// throttled-migration fallback): materialize one OverlayBD layer for
    /// this pause over `parent`.
    PerPauseLayer { parent: Option<PathBuf> },
}

/// The single routing decision for one pause's memory stage.
pub(crate) fn plan_memory(mode: PauseMode, launch: &LaunchMode) -> MemoryPlan {
    match (mode, launch) {
        (PauseMode::Storm, LaunchMode::Resume(config)) => MemoryPlan::VarlenHarvest {
            mem_upper: config.mem_upper_path.clone(),
            legacy_config: Some(config.mem_overlaybd_config.image_config_path.clone()),
            mem_spec: config.mem_virtual_size,
        },
        (PauseMode::Capture, LaunchMode::Resume(config)) => match &config.mem_upper_path {
            Some(mem_upper) => MemoryPlan::CollapseForCapture {
                stable_image: config.mem_overlaybd_config.image_config_path.clone(),
                mem_upper: mem_upper.clone(),
            },
            None => MemoryPlan::PerPauseLayer {
                parent: Some(config.mem_overlaybd_config.image_config_path.clone()),
            },
        },
        (_, LaunchMode::Fresh(_)) => MemoryPlan::PerPauseLayer { parent: None },
    }
}

impl RemoteBlockReader for ProcessVmReader {
    fn read_exact_remote(&self, addr: u64, dst: &mut [u8]) -> Result<()> {
        // Method-call syntax resolves to the inherent `process_vm_readv`
        // loop, not this trait method.
        self.read_exact_remote(addr, dst)
    }
}

pub(crate) struct VarlenPauseOutcome {
    pub mem_upper_path: PathBuf,
    pub stable_image_config: PathBuf,
    pub epoch: MemUpperGeneration,
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
    if current_mem_upper.is_some() {
        ensure!(
            current_mem_virtual_size == mem_spec,
            "mem_spec {mem_spec} disagrees with sandbox mem_virtual_size {current_mem_virtual_size}"
        );
    }
    // Steady state is anchored where the launch record says it is: the
    // record's `mem.upper` owns this sandbox's varlen lineage. Re-deriving
    // from this pause's snapshot dir would silently switch roots when a
    // caller pauses into a different parent (ad-hoc `pause_to_dir` targets)
    // and then splice this pause's dirty pages into whatever root happens
    // to sit there. Migration has no lineage yet, so it derives fresh.
    let stable_root = match current_mem_upper {
        Some(current) => current
            .parent()
            .context("record mem_upper path has no parent")?
            .to_path_buf(),
        None => stable_root.to_path_buf(),
    };
    tokio::fs::create_dir_all(&stable_root)
        .await
        .with_context(|| format!("create stable mem root {}", stable_root.display()))?;
    let mem_upper_path = stable_root.join("mem.upper");
    let stable_image = stable_root.join("image.json");
    let stable_lowers = stable_root.join("lowers.json");
    let service = image_service().await?;

    let steady = match current_mem_upper {
        Some(current) => {
            ensure!(
                current == mem_upper_path,
                "record mem_upper {} disagrees with stable root {}",
                current.display(),
                mem_upper_path.display()
            );
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
                &stable_root,
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

    if parity_enabled() {
        if let Err(err) = verify_dirty_parity(&service, &stable_image, &reader, &dirty).await {
            warn!(?err, "mem.upper parity check could not run");
        }
    }

    Ok(VarlenPause::Harvested(VarlenPauseOutcome {
        mem_upper_path,
        stable_image_config: stable_image,
        epoch: epoch.into(),
    }))
}

/// Debug gate for the design §6 on-air check ("splice 产物 vs 整块 readv 产物
/// 全量字节对拍"): after every varlen harvest, re-read each dirty page through
/// the freshly committed composite and compare against the frozen guest RAM.
/// Opt-in via `AGENTENV_DEBUG_MEM_UPPER_PARITY`; adds pause latency, so it is
/// diagnostic tooling, not a production path.
fn parity_enabled() -> bool {
    std::env::var("AGENTENV_DEBUG_MEM_UPPER_PARITY").is_ok()
}

async fn verify_dirty_parity(
    service: &ImageService,
    stable_image: &Path,
    reader: &ProcessVmReader,
    dirty: &[DirtyPage],
) -> Result<()> {
    let composite = open_varlen_composite_from_config(service, stable_image)
        .await
        .context("open composite for dirty parity check")?;
    let mut expected = vec![0u8; 4096];
    let mut actual = vec![0u8; 4096];
    let mut mismatches = Vec::new();
    for page in dirty {
        reader.read_exact_remote(page.host_addr, &mut expected)?;
        let bytes = composite
            .read_at(page.logical_offset, 4096)
            .await
            .with_context(|| format!("parity read at {}", page.logical_offset))?;
        ensure!(bytes.len() == 4096, "parity short read");
        actual.copy_from_slice(&bytes);
        if expected != actual {
            mismatches.push(page.logical_offset);
        }
    }
    if mismatches.is_empty() {
        tracing::info!(
            pages = dirty.len(),
            "mem.upper dirty parity OK (composite == frozen guest RAM)"
        );
    } else {
        error!(
            mismatched_pages = mismatches.len(),
            checked = dirty.len(),
            first = ?mismatches.iter().take(8).collect::<Vec<_>>(),
            "mem.upper DIRTY PARITY FAILED: composite disagrees with frozen guest RAM"
        );
    }
    Ok(())
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
        sync_parent_directory(parent)?;
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
    // Migration owns the stable root from scratch: the launching state has no
    // mem_upper, so anything already sitting here belongs to a foreign
    // lifecycle (a re-run test, an interrupted pause). Layer files carry
    // deterministic names (`overlaybd.commit`), so reusing a stale root would
    // silently splice this pause's dirty pages over another lifecycle's
    // memory. Reset the root before hardlinking.
    if mem_upper_path.exists() {
        tokio::fs::remove_file(mem_upper_path)
            .await
            .with_context(|| format!("remove stale {}", mem_upper_path.display()))?;
    }
    if legacy_dir.exists() {
        tokio::fs::remove_dir_all(&legacy_dir)
            .await
            .with_context(|| format!("remove stale {}", legacy_dir.display()))?;
    }
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
                // Only adopt a pre-existing target when it is the very inode
                // we know (a same-run retry); a foreign file that happens to
                // carry the deterministic name must not be adopted silently.
                use std::os::unix::fs::MetadataExt;
                let same_inode = match (std::fs::metadata(&source), std::fs::metadata(&target)) {
                    (Ok(src), Ok(dst)) => src.dev() == dst.dev() && src.ino() == dst.ino(),
                    _ => false,
                };
                ensure!(
                    same_inode,
                    "stable legacy layer {} already exists as a foreign inode; refusing to adopt",
                    target.display()
                );
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
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "scanned")
        .increment(stats.blocks_scanned);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "skipped")
        .increment(stats.skipped);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "template_equal")
        .increment(stats.template_equal);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "written")
        .increment(stats.written);
    metrics::counter!("agentenv_mem_pause_blocks_total", "kind" => kind, "result" => "inplace_hit")
        .increment(stats.inplace_hits);
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

/// The stable mem config with `memUpper` stripped and the collapsed layer
/// appended: the parent stack both the capture's intermediate parent config
/// and the post-capture [`ParentSwitch`] write.
pub(crate) fn stable_config_with_collapsed(
    stable_image: &Path,
    collapsed: &Path,
) -> Result<ImageConfig> {
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
    Ok(config)
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
    let config = stable_config_with_collapsed(stable_image, collapsed)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn write_legacy_config(dir: &Path, payload: &[u8]) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let layer = dir.join("overlaybd.commit");
        std::fs::write(&layer, payload).unwrap();
        let config_path = dir.join("mem_image.json");
        std::fs::write(
            &config_path,
            format!(r#"{{"lowers":[{{"file":"{}"}}]}}"#, layer.display()),
        )
        .unwrap();
        config_path
    }

    /// A re-run migration into a stale stable root must reset it: layer files
    /// carry deterministic names, so adopting a foreign same-named hardlink
    /// (or a leftover `mem.upper`) would splice this pause's dirty pages over
    /// another lifecycle's memory.
    #[tokio::test]
    async fn migration_resets_stale_stable_root() {
        let tmp = tempfile::tempdir().unwrap();
        let first = write_legacy_config(&tmp.path().join("src-first"), b"first-run-bytes");
        let stable_root = tmp.path().join("stable");
        let mem_upper = stable_root.join("mem.upper");
        let stable_image = stable_root.join("image.json");
        let stable_lowers = stable_root.join("lowers.json");

        migrate_stable_images(
            &first,
            &stable_root,
            &mem_upper,
            &stable_image,
            &stable_lowers,
        )
        .await
        .unwrap();
        let adopted = stable_root.join("legacy").join("0000-overlaybd.commit");
        assert!(adopted.exists());

        // Foreign leftovers from the first lifecycle.
        std::fs::write(&mem_upper, b"stale").unwrap();
        // Re-run: same layer filename, new inode and content.
        let second = write_legacy_config(&tmp.path().join("src-second"), b"second-run-bytes");
        migrate_stable_images(
            &second,
            &stable_root,
            &mem_upper,
            &stable_image,
            &stable_lowers,
        )
        .await
        .unwrap();

        assert!(!mem_upper.exists(), "stale mem.upper must be removed");
        assert_eq!(
            std::fs::read(&adopted).unwrap(),
            b"second-run-bytes",
            "legacy target must carry this run's layer, not the foreign one"
        );
        let image: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&stable_image).unwrap()).unwrap();
        assert!(image.get("memUpper").is_some());
        let lowers: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&stable_lowers).unwrap()).unwrap();
        assert!(lowers.get("memUpper").is_none());
    }

    // ── In-process byte-parity harness ─────────────────────────────────
    //
    // `snapshot_chain_survives_after_parent_snapshot_handle_is_dropped`
    // (integration) times out on the first varlen resume: the guest keeps
    // running while envd never answers, i.e. the composite serves wrong
    // bytes without failing. This harness reproduces the whole content path
    // without a VM: a real multi-layer legacy mem stack ([base, D1, D2]
    // built with the production layer publisher), a synthetic guest RAM
    // advanced by per-page dirty deltas, the real migration + harvest code
    // reading that RAM through `ProcessVmReader` on this very process, and
    // the daemon-identical composite open (`open_varlen_composite_from_config`)
    // whose every byte must equal the final RAM image.

    use async_trait::async_trait;
    use bytes::Bytes;
    use firecracker_client::models::DirtyMemoryRange;
    use overlaybd::config::GlobalConfig;
    use overlaybd::index::{Segment, SegmentMapping};

    use crate::sandbox::firecracker::overlaybd_snapshot::publish_memory_overlaybd_layer;
    use crate::sandbox::ublk::OverlaybdCompactOutput;

    const HARNESS_MEM_SPEC: u64 = 8 * 1024 * 1024;
    const HARNESS_PAGE: u64 = 4096;
    const HARVEST_CONCURRENCY: usize = 8;

    fn next_u64(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    fn fill_pattern(buf: &mut [u8], seed: u64) {
        let mut state = seed.max(1);
        for chunk in buf.chunks_mut(8) {
            let word = next_u64(&mut state).to_le_bytes();
            chunk.copy_from_slice(&word[..chunk.len()]);
        }
    }

    /// Immutable `VirtualFile` over a pre-built RAM image.
    struct RamFile(Arc<Vec<u8>>);

    #[async_trait]
    impl VirtualFile for RamFile {
        async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
            let begin = offset as usize;
            ensure!(
                begin
                    .checked_add(len)
                    .is_some_and(|end| end <= self.0.len()),
                "RamFile read [{offset}, +{len}) beyond {}",
                self.0.len()
            );
            Ok(Bytes::copy_from_slice(&self.0[begin..begin + len]))
        }

        async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
            anyhow::bail!("RamFile is read-only")
        }

        async fn size(&self) -> Result<u64> {
            Ok(self.0.len() as u64)
        }
    }

    fn ram_file(bytes: &[u8]) -> Arc<dyn VirtualFile> {
        Arc::new(RamFile(Arc::new(bytes.to_vec())))
    }

    /// A 4K-aligned window inside a heap allocation standing in for
    /// Firecracker's guest-RAM mapping; `dirty_pages()` requires the host
    /// base to be page aligned. The storage never moves after construction.
    struct HostRam {
        storage: Vec<u8>,
        pad: usize,
    }

    impl HostRam {
        fn new(bytes: &[u8]) -> Self {
            let mut storage = vec![0u8; bytes.len() + HARNESS_PAGE as usize];
            let misalign = storage.as_ptr() as u64 % HARNESS_PAGE;
            let pad = ((HARNESS_PAGE - misalign) % HARNESS_PAGE) as usize;
            storage[pad..pad + bytes.len()].copy_from_slice(bytes);
            Self { storage, pad }
        }

        fn base(&self) -> u64 {
            self.storage[self.pad..].as_ptr() as u64
        }
    }

    /// Dirty-page set as Firecracker would report it: contiguous pages
    /// merged into ranges, host addresses affine to the image offsets.
    fn dirty_ranges_for(base: u64, dirty_pages: &[u64]) -> DirtyMemoryRanges {
        let mut pages = dirty_pages.to_vec();
        pages.sort_unstable();
        pages.dedup();
        let mut ranges: Vec<DirtyMemoryRange> = Vec::new();
        for page in pages {
            let offset = page * HARNESS_PAGE;
            match ranges.last_mut() {
                Some(last) if last.image_offset + last.length == offset as i64 => {
                    last.length += HARNESS_PAGE as i64;
                }
                _ => ranges.push(DirtyMemoryRange {
                    base_host_virt_addr: (base + offset) as i64,
                    image_offset: offset as i64,
                    length: HARNESS_PAGE as i64,
                }),
            }
        }
        DirtyMemoryRanges {
            page_size: HARNESS_PAGE as i32,
            memory_size: HARNESS_MEM_SPEC as i64,
            ranges,
        }
    }

    /// Pages → identity mappings into a full-RAM source file (the shape
    /// `dirty_ranges_to_segment_mappings` produces for legacy pauses).
    fn page_runs_to_mappings(pages: &[u64]) -> Vec<SegmentMapping> {
        let mut pages = pages.to_vec();
        pages.sort_unstable();
        pages.dedup();
        let mut runs: Vec<(u64, u64)> = Vec::new();
        for page in pages {
            let start = page * (HARNESS_PAGE / 512);
            match runs.last_mut() {
                Some(last) if last.0 + last.1 == start => last.1 += HARNESS_PAGE / 512,
                _ => runs.push((start, HARNESS_PAGE / 512)),
            }
        }
        let mut mappings = Vec::new();
        for (start, mut len) in runs {
            let mut at = start;
            while len > 0 {
                let chunk = len.min(u64::from(Segment::MAX_LENGTH));
                mappings.push(SegmentMapping::new(at, chunk as u32, at, false, 0));
                at += chunk;
                len -= chunk;
            }
        }
        mappings
    }

    async fn harness_image_service(tmp: &Path) -> Result<Arc<ImageService>> {
        let mut cfg = GlobalConfig::default();
        let cache = tmp.join("overlaybd-cache").display().to_string();
        cfg.registry_cache_dir = cache.clone();
        cfg.cache_config.cache_dir = cache;
        cfg.gzip_cache_config.cache_dir = tmp.join("gzip-cache").display().to_string();
        cfg.enable_audit = false;
        cfg.audit_path = tmp.join("audit.log").display().to_string();
        cfg.log_path = tmp.join("overlaybd.log").display().to_string();
        Ok(Arc::new(ImageService::new(cfg).await?))
    }

    async fn publish_layer(
        source: Arc<dyn VirtualFile>,
        mappings: &[SegmentMapping],
        output: &Path,
    ) -> Result<()> {
        if let Some(parent) = output.parent() {
            std::fs::create_dir_all(parent)?;
        }
        publish_memory_overlaybd_layer(
            &[source],
            mappings,
            HARNESS_MEM_SPEC,
            output,
            OverlaybdCompactOutput::Raw,
            HARVEST_CONCURRENCY,
        )
        .await
    }

    /// Full-image compare plus boundary-crossing odd windows, all through the
    /// same `VirtualFile::read_at_into` the daemon's ublk path settles into.
    async fn assert_composite_serves(
        composite: &overlaybd::index_file::LSMTReadOnlyFile,
        expected: &[u8],
        stage: &str,
    ) -> Result<()> {
        let mut buf = vec![0u8; 64 * 1024];
        let mut offset = 0u64;
        while offset < expected.len() as u64 {
            let len = buf.len().min(expected.len() - offset as usize);
            let n = composite
                .read_at_into(offset, &mut buf[..len])
                .await
                .with_context(|| format!("{stage} read at {offset}"))?;
            ensure!(n == len, "{stage} short read at {offset}: {n} != {len}");
            let window = &expected[offset as usize..offset as usize + len];
            if buf[..len] != window[..] {
                let first = buf[..len]
                    .iter()
                    .zip(window.iter())
                    .position(|(got, want)| got != want)
                    .expect("lengths equal, at least one byte differs");
                let at = offset as usize + first;
                anyhow::bail!(
                    "{stage} bytes differ at offset {at} (page {}): got {:02x?}.. want {:02x?}..",
                    at / 4096,
                    &buf[first..(first + 8).min(len)],
                    &expected[at..(at + 8).min(expected.len())],
                );
            }
            offset += len as u64;
        }
        // Odd windows that straddle 16K block edges and mem.upper payloads.
        for (start, len) in [
            (0usize, 4096usize),
            (16 * 1024 - 512, 1024),
            (32 * 1024 + 512, 8 * 1024 - 512),
            (48 * 1024, 4096),
            (64 * 1024 - 512, 1024),
        ] {
            let mut odd = vec![0u8; len];
            let n = composite.read_at_into(start as u64, &mut odd).await?;
            ensure!(n == len, "composite short odd read at {start}");
            ensure!(
                odd == expected[start..start + len],
                "composite odd window [{start}, +{len}) differs"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn varlen_migration_and_steady_harvest_serve_exact_ram_bytes() -> Result<()> {
        let tmp = tempfile::tempdir().context("tempdir")?;
        let service = harness_image_service(tmp.path()).await?;
        let total_pages = HARNESS_MEM_SPEC / HARNESS_PAGE;
        let mut rng = 0x5EED_0001u64;

        // ── Legacy stack: [base, D1, D2] exactly like two capture pauses ──
        let mut ram0 = vec![0u8; HARNESS_MEM_SPEC as usize];
        fill_pattern(&mut ram0, 0xAA11);
        let base_layer = tmp.path().join("gen0/mem_overlaybd/overlaybd.commit");
        publish_layer(
            ram_file(&ram0),
            &page_runs_to_mappings(&(0..total_pages).collect::<Vec<_>>()),
            &base_layer,
        )
        .await
        .context("publish base layer")?;

        let pick_pages = |state: &mut u64, count: u64| -> Vec<u64> {
            let mut set = std::collections::BTreeSet::new();
            while set.len() < count as usize {
                set.insert(next_u64(state) % total_pages);
            }
            set.into_iter().collect()
        };

        let dirty1 = pick_pages(&mut rng, total_pages / 4);
        let mut ram1 = ram0.clone();
        for &page in &dirty1 {
            let at = (page * HARNESS_PAGE) as usize;
            fill_pattern(&mut ram1[at..at + HARNESS_PAGE as usize], 0xC0DE ^ page);
        }
        let d1_layer = tmp.path().join("gen1/mem_overlaybd/overlaybd.commit");
        publish_layer(ram_file(&ram1), &page_runs_to_mappings(&dirty1), &d1_layer)
            .await
            .context("publish first dirty layer")?;

        let dirty2 = pick_pages(&mut rng, total_pages / 6);
        let mut ram2 = ram1.clone();
        for &page in &dirty2 {
            let at = (page * HARNESS_PAGE) as usize;
            fill_pattern(&mut ram2[at..at + HARNESS_PAGE as usize], 0xFEED ^ page);
        }
        let d2_layer = tmp.path().join("gen2/mem_overlaybd/overlaybd.commit");
        publish_layer(ram_file(&ram2), &page_runs_to_mappings(&dirty2), &d2_layer)
            .await
            .context("publish second dirty layer")?;

        let legacy_config = tmp.path().join("gen2/mem_image.json");
        std::fs::write(
            &legacy_config,
            serde_json::json!({
                "lowers": [
                    {"file": base_layer.display().to_string()},
                    {"file": d1_layer.display().to_string()},
                    {"file": d2_layer.display().to_string()},
                ],
            })
            .to_string(),
        )?;

        // Sanity: the legacy stack resolves to ram2 — what the resumed guest
        // started from. If this fails the harness layers are wrong, not the
        // varlen code.
        let legacy_stack = open_lower_stack_from_config(&service, &legacy_config).await?;
        assert_composite_serves(&legacy_stack, &ram2, "legacy-stack sanity").await?;

        // ── First varlen pause (migration): dirty3 = full blocks, partial
        // blocks, template-equal pages, and whole blocks rewritten with
        // identical bytes ──
        let mut dirty3 = std::collections::BTreeSet::new();
        let mut ram3 = ram2.clone();
        for block in 0..40u64 {
            for p in 0..4u64 {
                let page = block * 4 + p + 64;
                let at = (page * HARNESS_PAGE) as usize;
                fill_pattern(&mut ram3[at..at + HARNESS_PAGE as usize], 0x1234 ^ page);
                dirty3.insert(page);
            }
        }
        for page in (256..320u64).step_by(2) {
            let at = (page * HARNESS_PAGE) as usize;
            fill_pattern(&mut ram3[at..at + HARNESS_PAGE as usize], 0xBEEF ^ page);
            dirty3.insert(page);
        }
        for page in 512..536u64 {
            dirty3.insert(page); // dirty-marked but bytes unchanged
        }
        for block in 0..8u64 {
            for p in 0..4u64 {
                dirty3.insert(600 + block * 4 + p); // identical rewrite
            }
        }
        let dirty3_pages: Vec<u64> = dirty3.into_iter().collect();

        let stable_root = tmp.path().join("stable");
        let mem_upper = stable_root.join("mem.upper");
        let stable_image = stable_root.join("image.json");
        let stable_lowers = stable_root.join("lowers.json");
        migrate_stable_images(
            &legacy_config,
            &stable_root,
            &mem_upper,
            &stable_image,
            &stable_lowers,
        )
        .await
        .context("migrate legacy stack")?;

        let host3 = HostRam::new(&ram3);
        let (pages3, mem_spec) = dirty_pages(&dirty_ranges_for(host3.base(), &dirty3_pages))?;
        ensure!(mem_spec == HARNESS_MEM_SPEC, "harness mem_spec");
        ensure!(
            pages3.len() == dirty3_pages.len(),
            "harness dirty page flattening: {} != {}",
            pages3.len(),
            dirty3_pages.len()
        );

        let reader = ProcessVmReader::new(Pid::from_raw(std::process::id() as i32));
        let mut writer = MemUpperWriter::create(&mem_upper, HARNESS_MEM_SPEC)?;
        let views = HarvestViews::uniform(Arc::new(
            open_lower_stack_from_config(&service, &stable_lowers).await?,
        ) as Arc<dyn VirtualFile>);
        let stats = writer
            .harvest(&pages3, &reader, views, &HarvestOptions::default())
            .await
            .context("migration harvest")?;
        let epoch1 = writer.commit()?;
        assert!(stats.written > 0, "migration harvest must write blocks");

        // Daemon-identical open of the stable config must serve ram3 exactly.
        let composite = open_varlen_composite_from_config(&service, &stable_image).await?;
        assert_composite_serves(&composite, &ram3, "migration resume").await?;
        ensure!(
            MemUpperGeneration::current(&mem_upper)? == MemUpperGeneration::from(epoch1),
            "committed epoch must be observable"
        );
        drop(composite);

        // ── Steady harvest (cycle 4): re-dirty harvested blocks, revert
        // some to the lower view (template equality must free payloads), and
        // touch fresh blocks ──
        let mut dirty4 = std::collections::BTreeSet::new();
        let mut ram4 = ram3.clone();
        for p in 0..4u64 {
            let page = 64 + p; // re-dirty the first migration block
            let at = (page * HARNESS_PAGE) as usize;
            fill_pattern(&mut ram4[at..at + HARNESS_PAGE as usize], 0x9999 ^ page);
            dirty4.insert(page);
        }
        for block in 0..4u64 {
            for p in 0..4u64 {
                let page = 256 + block * 4 + p; // revert to the lower bytes
                let at = (page * HARNESS_PAGE) as usize;
                ram4[at..at + HARNESS_PAGE as usize]
                    .copy_from_slice(&ram2[at..at + HARNESS_PAGE as usize]);
                dirty4.insert(page);
            }
        }
        for page in 1500..1520u64 {
            let at = (page * HARNESS_PAGE) as usize; // fresh blocks
            fill_pattern(&mut ram4[at..at + HARNESS_PAGE as usize], 0x7777 ^ page);
            dirty4.insert(page);
        }
        let dirty4_pages: Vec<u64> = dirty4.into_iter().collect();

        // New "Firecracker process" — a fresh host mapping of the same RAM.
        drop(host3);
        let host4 = HostRam::new(&ram4);
        let (pages4, _) = dirty_pages(&dirty_ranges_for(host4.base(), &dirty4_pages))?;

        let mut writer = MemUpperWriter::open(&mem_upper, HARNESS_MEM_SPEC)?;
        let views = HarvestViews::uniform(Arc::new(
            open_lower_stack_from_config(&service, &stable_lowers).await?,
        ) as Arc<dyn VirtualFile>);
        writer
            .harvest(&pages4, &reader, views, &HarvestOptions::default())
            .await
            .context("steady harvest")?;
        writer.commit()?;

        let composite = open_varlen_composite_from_config(&service, &stable_image).await?;
        assert_composite_serves(&composite, &ram4, "steady resume").await?;
        Ok(())
    }
}

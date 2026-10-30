use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use overlaybd::backend::local::LocalFile;
use overlaybd::config::{LayerConfig, UpperMode};
use overlaybd::index_file::{merge_files_ro, CommitArgs};
use overlaybd::virtual_file::VirtualFile;
use overlaybd::zfile::{CompressArgs, CompressOptions, ZFileCompactWriter};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};
use uuid::Uuid;

use super::device::UblkDevice;
use crate::cfg::{
    MemorySnapshotConfig, OverlaybdCompressionAlgorithm, SnapshotPublishCompressionConfig,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OverlaybdCompactOutput {
    Raw,
    ZFile {
        algorithm: OverlaybdCompressionAlgorithm,
        workers: usize,
    },
}

impl OverlaybdCompactOutput {
    /// Maximum persistent compression threads. Prevents absurd config values
    /// from spawning thousands of OS threads during layer upload.
    const MAX_COMPRESSION_WORKERS: usize = 64;

    /// Resolve the `[snapshot.publish_compression]` switch into the layer
    /// output mode applied when recontainerizing local raw layers for upload.
    /// Capture paths never consult this: local layers always stay raw.
    pub(crate) fn from_publish_compression_config(
        config: &SnapshotPublishCompressionConfig,
    ) -> Self {
        if config.enabled {
            Self::ZFile {
                algorithm: config.algorithm,
                workers: config.workers.clamp(1, Self::MAX_COMPRESSION_WORKERS),
            }
        } else {
            Self::Raw
        }
    }

    /// Resolve the pause-time memory layer output mode from
    /// `[memory_snapshot].compression_enabled`. Disabled by default so pause
    /// keeps producing raw layers with no resume decompression cost; enabled,
    /// the algorithm and worker count come from
    /// `[snapshot.publish_compression]` so pause artifacts stay in the format
    /// publish uploads and pass through unchanged.
    pub(crate) fn from_memory_snapshot_config(
        memory: &MemorySnapshotConfig,
        publish: &SnapshotPublishCompressionConfig,
    ) -> Self {
        if !memory.compression_enabled {
            return Self::Raw;
        }
        Self::ZFile {
            algorithm: publish.algorithm,
            workers: publish.workers.clamp(1, Self::MAX_COMPRESSION_WORKERS),
        }
    }
}

pub(crate) async fn create_commit_args(
    output: Arc<dyn VirtualFile>,
    mode: OverlaybdCompactOutput,
    concurrency: usize,
) -> Result<CommitArgs> {
    let mut args = match mode {
        OverlaybdCompactOutput::Raw => CommitArgs::new(output),
        OverlaybdCompactOutput::ZFile { algorithm, workers } => {
            let algorithm = match algorithm {
                OverlaybdCompressionAlgorithm::Lz4 => CompressOptions::LZ4,
                OverlaybdCompressionAlgorithm::Zstd => CompressOptions::ZSTD,
            };
            let mut compress_args = CompressArgs::new(CompressOptions::new(
                algorithm,
                CompressOptions::DEFAULT_BLOCK_SIZE,
                0,
            ));
            compress_args.workers = workers.max(1);
            // Pool retention sized to chunk concurrency: steady-state chunks
            // recycle staging buffers instead of allocating fresh ones.
            let pool_capacity = concurrency.clamp(1, 1024);
            CommitArgs::from_writer(Arc::new(
                ZFileCompactWriter::with_pool_capacity(output, &compress_args, pool_capacity)
                    .await?,
            ))
        }
    };
    args.concurrency = concurrency;
    Ok(args)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OverlaybdConfig {
    pub image_config_path: PathBuf,
    pub read_only: bool,
    #[serde(default = "default_runtime_upper_mode")]
    pub runtime_upper_mode: UpperMode,
}

fn default_runtime_upper_mode() -> UpperMode {
    UpperMode::LogStructured
}

#[derive(Clone, Debug)]
pub(crate) struct OverlaybdRuntimeHandle {
    pub(crate) device: UblkDevice,
    pub(crate) image_config_path: PathBuf,
    pub(crate) actual_virtual_size: u64,
}

/// Kick writeback for a fully written commit file, keeping its page cache
/// resident, best-effort.
///
/// An immediately following resume reads the commit files back, so the pages
/// are deliberately kept cached. Only the writeback is started now —
/// `sync_file_range(SYNC_FILE_RANGE_WRITE)` is non-blocking — so the dirty
/// pool drains per file instead of piling up until the global writeback
/// threshold throttles every writer. Writeback completion is not awaited,
/// matching the durability of an unsynced file. Failures only downgrade the
/// hint and are logged; they never fail the surrounding capture.
pub(crate) async fn writeback_commit_file(path: &Path) {
    let blocking_path = path.to_path_buf();
    let outcome = tokio::task::spawn_blocking(move || writeback_file_blocking(&blocking_path))
        .await
        .unwrap_or_else(|error| Err(std::io::Error::other(error)));
    match outcome {
        Ok(size) => debug!(
            path = %path.display(),
            size_bytes = size,
            "kicked commit file writeback"
        ),
        Err(error) => warn!(
            path = %path.display(),
            %error,
            "failed to kick commit file writeback"
        ),
    }
}

#[cfg(target_os = "linux")]
fn writeback_file_blocking(path: &Path) -> std::io::Result<u64> {
    use std::os::fd::AsRawFd;

    let file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    // A zero length means "through end of file". WRITE only starts the flush;
    // pages stay cached once clean.
    // SAFETY: `sync_file_range` only reads its scalar arguments.
    let ret = unsafe { libc::sync_file_range(file.as_raw_fd(), 0, 0, libc::SYNC_FILE_RANGE_WRITE) };
    if ret != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(size)
}

#[cfg(not(target_os = "linux"))]
fn writeback_file_blocking(_path: &Path) -> std::io::Result<u64> {
    Ok(0)
}

/// Compact multiple overlaybd layers into one sealed commit at `output_path`.
///
/// Each input layer is opened through the tar adaptor + switch file chain so
/// raw and ZFile-compressed layers can be mixed freely. The merged commit is
/// written directly in the requested `mode` to a sibling temp file and
/// atomically renamed into place; on failure the temp file is removed and any
/// pre-existing output is left untouched.
pub(crate) async fn compact_layers(
    layers: &[LayerConfig],
    output_path: &Path,
    mode: OverlaybdCompactOutput,
) -> Result<Option<PathBuf>> {
    if layers.is_empty() {
        return Ok(None);
    }

    let mut src_files: Vec<Arc<dyn VirtualFile>> = Vec::with_capacity(layers.len());
    for layer in layers {
        let path = Path::new(&layer.file);
        let local: Arc<dyn VirtualFile> = Arc::new(
            LocalFile::open_ro(path)
                .with_context(|| format!("open layer for compaction: {}", path.display()))?,
        );
        let tar_adapted = overlaybd::backend::tar::new_tar_file_adaptor(local)
            .await
            .with_context(|| format!("adapt layer as tar file: {}", path.display()))?;
        let display = path.display().to_string();
        let switched =
            overlaybd::backend::switch::new_switch_file(tar_adapted, true, Some(&display))
                .await
                .with_context(|| format!("open layer via switch file: {}", path.display()))?;
        src_files.push(switched);
    }

    let lower_tmp = output_path.with_extension(format!("commit.{}.tmp", Uuid::now_v7()));
    let build_result: Result<()> = async {
        let output_file: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&lower_tmp).context("create compacted layer output file")?);
        let commit_args = create_commit_args(output_file, mode, 32).await?;
        merge_files_ro(&src_files, commit_args)
            .await
            .context("merge layers")?;
        tokio::fs::rename(&lower_tmp, output_path)
            .await
            .with_context(|| {
                format!(
                    "publish compacted layer to {} failed",
                    output_path.display()
                )
            })?;
        Ok(())
    }
    .await;

    if build_result.is_err() {
        let _ = tokio::fs::remove_file(&lower_tmp).await;
    }
    build_result?;

    writeback_commit_file(output_path).await;

    debug!(
        output = %output_path.display(),
        input_layers = layers.len(),
        "compacted overlaybd layers"
    );
    Ok(Some(output_path.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use overlaybd::backend::switch::new_switch_file;
    use overlaybd::backend::tar::new_tar_file_adaptor;
    use overlaybd::index_file::{LSMTFile, LSMTReadOnlyFile};
    use overlaybd::zfile::is_zfile;

    async fn create_sealed_layer(
        dir: &Path,
        name: &str,
        vsize: u64,
        writes: &[(u64, u8)],
        zfile_algo: Option<u8>,
    ) -> PathBuf {
        let data = Arc::new(
            LocalFile::new(dir.join(format!("{name}.data"))).expect("create layer data file"),
        );
        let index = Arc::new(
            LocalFile::new(dir.join(format!("{name}.index"))).expect("create layer index file"),
        );
        let layer = LSMTFile::create(data, Some(index), vsize, false)
            .await
            .expect("create layer");
        for (offset, byte) in writes {
            layer
                .write_at(*offset, &[*byte; 4096])
                .await
                .expect("write layer page");
        }
        let commit_path = dir.join(format!("{name}.commit"));
        let output: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::new(&commit_path).expect("create layer commit output"));
        match zfile_algo {
            Some(algo) => {
                let compress_args = CompressArgs::new(CompressOptions::new(
                    algo,
                    CompressOptions::DEFAULT_BLOCK_SIZE,
                    0,
                ));
                let writer = Arc::new(
                    ZFileCompactWriter::new(output, &compress_args)
                        .await
                        .expect("create zfile compact writer"),
                );
                layer
                    .commit_with_args(CommitArgs::from_writer(writer))
                    .await
                    .expect("commit zfile layer");
            }
            None => {
                layer
                    .commit_with_args(CommitArgs::new(output))
                    .await
                    .expect("commit raw layer");
            }
        }
        commit_path
    }

    async fn read_sealed_layer(path: &Path, len: usize) -> (i32, Vec<u8>) {
        let local: Arc<dyn VirtualFile> =
            Arc::new(LocalFile::open_ro(path).expect("open sealed layer"));
        let zfile_flag = is_zfile(local.clone()).await.expect("probe zfile header");
        let display = path.display().to_string();
        let tar_adapted = new_tar_file_adaptor(local).await.expect("tar adaptor");
        let switched = new_switch_file(tar_adapted, true, Some(&display))
            .await
            .expect("switch file");
        let layer = LSMTReadOnlyFile::open(switched)
            .await
            .expect("open sealed layer as LSMT");
        let data = layer.read_at(0, len).await.expect("read sealed layer");
        (zfile_flag, data.to_vec())
    }

    fn layer_config(path: &Path) -> LayerConfig {
        LayerConfig {
            file: path.display().to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn compact_layers_mixed_input_and_configured_output() {
        let temp = tempfile::tempdir().expect("tempdir");
        let vsize = 3 * 4096u64;
        let bottom = create_sealed_layer(
            temp.path(),
            "bottom",
            vsize,
            &[(0, 0x11), (4096, 0x11), (8192, 0x11)],
            None,
        )
        .await;
        let middle = create_sealed_layer(
            temp.path(),
            "middle",
            vsize,
            &[(4096, 0x22)],
            Some(CompressOptions::LZ4),
        )
        .await;
        let top = create_sealed_layer(
            temp.path(),
            "top",
            vsize,
            &[(8192, 0x33)],
            Some(CompressOptions::ZSTD),
        )
        .await;
        let layers = [&bottom, &middle, &top]
            .into_iter()
            .map(|path| layer_config(path))
            .collect::<Vec<_>>();

        // Later layers take precedence over earlier ones on overlapping pages.
        let mut expected = vec![0u8; vsize as usize];
        expected[..4096].fill(0x11);
        expected[4096..8192].fill(0x22);
        expected[8192..].fill(0x33);

        for (name, mode, expected_zfile) in [
            ("raw", OverlaybdCompactOutput::Raw, 0),
            (
                "lz4",
                OverlaybdCompactOutput::ZFile {
                    algorithm: OverlaybdCompressionAlgorithm::Lz4,
                    // Exercise the segmented parallel compression path.
                    workers: 2,
                },
                1,
            ),
            (
                "zstd",
                OverlaybdCompactOutput::ZFile {
                    algorithm: OverlaybdCompressionAlgorithm::Zstd,
                    workers: 1,
                },
                1,
            ),
        ] {
            let output_path = temp.path().join(format!("compacted-{name}.commit"));
            let published = compact_layers(&layers, &output_path, mode)
                .await
                .expect("compact mixed raw/lz4/zstd layers");
            assert_eq!(published.as_deref(), Some(output_path.as_path()));
            assert!(!output_path.with_extension("commit.tmp").exists());

            let (zfile_flag, data) = read_sealed_layer(&output_path, vsize as usize).await;
            assert_eq!(zfile_flag, expected_zfile, "mode {name}");
            assert_eq!(data, expected, "mode {name}");
        }

        // A failed compaction must neither clobber a previously published
        // output nor leave the sibling temp file behind.
        let output_path = temp.path().join("compacted-raw.commit");
        let before = std::fs::read(&output_path).expect("read published output");
        let bogus = temp.path().join("bogus.commit");
        std::fs::write(&bogus, vec![0xAB; 4096]).expect("write bogus layer");
        let mut broken = layers.clone();
        broken.push(layer_config(&bogus));
        compact_layers(&broken, &output_path, OverlaybdCompactOutput::Raw)
            .await
            .expect_err("invalid input layer must fail");
        assert_eq!(
            std::fs::read(&output_path).expect("read published output"),
            before
        );
        assert!(!output_path.with_extension("commit.tmp").exists());
    }

    #[test]
    fn compact_output_from_publish_compression_config() {
        let disabled = SnapshotPublishCompressionConfig {
            enabled: false,
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: 1,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&disabled),
            OverlaybdCompactOutput::Raw
        );

        let zstd = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Zstd,
            // Zero workers clamps to sequential.
            workers: 0,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&zstd),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Zstd,
                workers: 1,
            }
        );

        let clamped = SnapshotPublishCompressionConfig {
            enabled: true,
            algorithm: OverlaybdCompressionAlgorithm::Lz4,
            workers: usize::MAX,
        };
        assert_eq!(
            OverlaybdCompactOutput::from_publish_compression_config(&clamped),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Lz4,
                workers: OverlaybdCompactOutput::MAX_COMPRESSION_WORKERS,
            }
        );
    }

    #[test]
    fn compact_output_from_memory_snapshot_config() {
        let disabled = MemorySnapshotConfig::default();
        assert!(!disabled.compression_enabled);
        assert_eq!(
            OverlaybdCompactOutput::from_memory_snapshot_config(
                &disabled,
                &SnapshotPublishCompressionConfig {
                    enabled: true,
                    algorithm: OverlaybdCompressionAlgorithm::Zstd,
                    workers: 4,
                }
            ),
            OverlaybdCompactOutput::Raw
        );

        let enabled = MemorySnapshotConfig {
            compression_enabled: true,
            ..MemorySnapshotConfig::default()
        };
        // Algorithm and workers are inherited from the publish compression
        // config; oversized workers clamp to the maximum.
        assert_eq!(
            OverlaybdCompactOutput::from_memory_snapshot_config(
                &enabled,
                &SnapshotPublishCompressionConfig {
                    enabled: false,
                    algorithm: OverlaybdCompressionAlgorithm::Zstd,
                    workers: usize::MAX,
                }
            ),
            OverlaybdCompactOutput::ZFile {
                algorithm: OverlaybdCompressionAlgorithm::Zstd,
                workers: OverlaybdCompactOutput::MAX_COMPRESSION_WORKERS,
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn writeback_file_blocking_keeps_pages_resident() {
        use std::os::fd::AsRawFd;

        use nix::sys::statfs::{statfs, TMPFS_MAGIC};

        // Only run the residency assertions on a disk-backed filesystem;
        // tmpfs pages are the storage itself and always report resident.
        let disk_backed = [
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            std::env::temp_dir(),
        ]
        .into_iter()
        .find(|dir| {
            statfs(dir)
                .map(|mounted| mounted.filesystem_type() != TMPFS_MAGIC)
                .unwrap_or(false)
        });
        let Some(dir) = disk_backed else {
            return;
        };
        let temp = tempfile::tempdir_in(dir).expect("tempdir");
        let path = temp.path().join("written-back.commit");
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let page_count = 8;
        let len = page_count * page_size;
        std::fs::write(&path, vec![0x5Au8; len]).expect("write commit file");

        let file = std::fs::File::open(&path).expect("open commit file");
        let resident_pages = |file: &std::fs::File| -> usize {
            let map = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    file.as_raw_fd(),
                    0,
                )
            };
            assert_ne!(map, libc::MAP_FAILED);
            let mut residency = vec![0u8; page_count];
            assert_eq!(
                unsafe { libc::mincore(map, len, residency.as_mut_ptr()) },
                0
            );
            unsafe { libc::munmap(map, len) };
            residency.iter().filter(|flags| *flags & 1 != 0).count()
        };

        // Populate the page cache and confirm every page is resident first.
        std::fs::read(&path).expect("read commit file into page cache");
        assert_eq!(
            resident_pages(&file),
            page_count,
            "pages should be resident before the writeback kick"
        );

        let size = super::writeback_file_blocking(&path).expect("kick commit file writeback");
        assert_eq!(size, len as u64);
        assert_eq!(
            resident_pages(&file),
            page_count,
            "pages should stay resident after the writeback kick"
        );
    }
}

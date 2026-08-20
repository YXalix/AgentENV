//! `P2pTransport` implementation over URMA one-sided reads.
//!
//! Architecture: metadata flows over a tiny TCP catalog protocol (artifact
//! key → descriptor carrying the provider's `urma_seg_t` wire handle, EID
//! and jetty ids); bytes flow out-of-band as one-sided READs from the
//! provider's registered memory into local bounce buffers. There is no
//! per-transfer message to the provider: the provider only ever *publishes*
//! (mmap + register) and *unpublishes* (unregister).
//!
//! Publishing sources:
//! - `Path` + `Reference`: the file is mapped read-only and registered
//!   directly (zero-copy serving).
//! - `Path` + `Copy` / `Bytes`: bytes are copied into the shared publish
//!   arena, which is registered once at startup.
//!
//! Two driver backends exist behind [`UrmaDriver`]: the real `liburma` FFI
//! driver (cargo feature `p2p-urma`) and an in-process loopback driver used
//! by tests and `p2p.ub.driver = "loopback"`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context as _;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use futures::stream;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use super::driver::{
    eid_from_hex32, LoopbackUrmaDriver, RegionAccess, UbLocalHandle, UrmaDriver, UrmaReadOp,
};
use super::server::{request_descriptor, CatalogServer};
use super::store::{BouncePool, Mmap, UbArena};
use super::worker::UrmaIo;
use super::UB_BACKEND_ID;
use crate::p2p::catalog::PublishedArtifactCatalog;
use crate::p2p::config::ResolvedP2pConfig;
use crate::p2p::discovery::P2pPeerDiscovery;
use crate::p2p::error::{Error, Result};
use crate::p2p::transport::P2pTransport;
use crate::p2p::types::{
    P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProviderHint, P2pEndpoint, P2pPeer,
    P2pPublishMode, P2pPublishRequest, P2pPublishSource,
};
use crate::p2p::P2pByteStream;

const LOCATOR_VERSION: u32 = 1;
/// Local fast-path copies move memory in chunks of this size.
const LOCAL_COPY_CHUNK: usize = 8 * 1024 * 1024;

/// Wire descriptor locator for the ub backend, carried in
/// [`P2pArtifactDescriptor::backend_locator`] as JSON.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct UbLocator {
    v: u32,
    /// Base64 of the driver's opaque segment wire form (`urma_seg_t` bytes).
    wire: String,
    /// Absolute base virtual address of the artifact in the provider.
    base_va: u64,
    size: u64,
    handle: UbLocalHandle,
}

impl UbLocator {
    fn encode(&self) -> Result<String> {
        serde_json::to_string(self)
            .context("serialize ub locator")
            .map_err(Error::Internal)
    }

    fn decode(descriptor: &P2pArtifactDescriptor) -> Result<Self> {
        let raw =
            descriptor
                .backend_locator
                .as_deref()
                .ok_or_else(|| Error::InvalidDescriptor {
                    reason: "ub descriptor is missing backend_locator".to_string(),
                })?;
        let locator: UbLocator =
            serde_json::from_str(raw).map_err(|err| Error::InvalidDescriptor {
                reason: format!("invalid ub locator: {err}"),
            })?;
        if locator.v != LOCATOR_VERSION {
            return Err(Error::InvalidDescriptor {
                reason: format!("unsupported ub locator version {}", locator.v),
            });
        }
        Ok(locator)
    }

    fn wire_bytes(&self) -> Result<Vec<u8>> {
        base64::engine::general_purpose::STANDARD
            .decode(&self.wire)
            .map_err(|err| Error::InvalidDescriptor {
                reason: format!("invalid ub locator wire encoding: {err}"),
            })
    }
}

/// Backing memory of one published artifact.
#[derive(Debug)]
enum PublishedRegion {
    /// Sub-allocation of the shared publish arena (already registered).
    Arena { offset: u64 },
    /// Individually mapped + registered file (reference-mode publishes).
    File(Mmap),
}

struct UbInner {
    node_id: String,
    peer_discovery: Arc<dyn P2pPeerDiscovery>,
    catalog: PublishedArtifactCatalog,
    io: UrmaIo,
    handle: UbLocalHandle,
    arena: UbArena,
    arena_wire: super::driver::RegionWire,
    bounce: Arc<BouncePool>,
    /// base_va → backing memory, used by the local fast path and unpublish.
    regions: Mutex<HashMap<u64, Arc<PublishedRegion>>>,
    local_endpoint: P2pEndpoint,
    catalog_server: CatalogServer,
    lookup_timeout: Duration,
    fetch_timeout: Duration,
    slice_size: u64,
    read_permits: Semaphore,
}

/// URMA-backed artifact transport (`p2p.transport = "ub"`).
pub struct UrmaP2pTransport {
    inner: Arc<UbInner>,
}

impl UrmaP2pTransport {
    pub(crate) async fn new(
        config: ResolvedP2pConfig,
        node_id: String,
        peer_discovery: Arc<dyn P2pPeerDiscovery>,
    ) -> Result<Self> {
        let ub = &config.ub;
        if ub.slice_size == 0 || !ub.slice_size.is_multiple_of(4096) {
            return Err(Error::internal_message(
                "validate p2p.ub.slice_size_bytes",
                "must be a positive multiple of 4096",
            ));
        }
        if ub.arena_size < ub.slice_size {
            return Err(Error::internal_message(
                "validate p2p.ub.arena_size_bytes",
                "must be at least p2p.ub.slice_size_bytes",
            ));
        }

        let driver: Box<dyn UrmaDriver> = match ub.driver.as_str() {
            "loopback" => Box::new(LoopbackUrmaDriver::new(&node_id)),
            "ffi" => Self::ffi_driver(ub)?,
            other => {
                return Err(Error::internal_message(
                    "validate p2p.ub.driver",
                    format!("unknown driver {other:?}; expected \"ffi\" or \"loopback\""),
                ));
            }
        };
        let handle = driver.local_handle();
        let io = UrmaIo::start(driver);

        let arena = UbArena::new(ub.arena_size)
            .context("create ub publish arena")
            .map_err(Error::Internal)?;
        let arena_wire = io
            .register_region(
                arena.map().base_addr() as usize,
                arena.map().len() as u64,
                RegionAccess::RemoteRead,
            )
            .await
            .context("register ub publish arena")
            .map_err(Error::Internal)?;

        let bounce = BouncePool::new(ub.slice_size, ub.max_inflight_reads)
            .context("create ub bounce pool")
            .map_err(Error::Internal)?;
        io.register_region(
            bounce.map().base_addr() as usize,
            bounce.map().len() as u64,
            RegionAccess::LocalOnly,
        )
        .await
        .context("register ub bounce pool")
        .map_err(Error::Internal)?;

        // Bind the catalog listener before loading the catalog so the local
        // endpoint stored in it is the real one.
        let listen: SocketAddr = ub
            .catalog_listen_addr
            .as_deref()
            .or(config.listen_addr.as_deref())
            .unwrap_or("0.0.0.0:0")
            .parse()
            .context("parse ub catalog listen address")
            .map_err(Error::Internal)?;
        let listener = TcpListener::bind(listen)
            .await
            .context("bind ub P2P catalog listener")
            .map_err(Error::Internal)?;
        let bound = listener
            .local_addr()
            .context("read ub catalog listener address")
            .map_err(Error::Internal)?;
        let advertise = advertise_addr(bound);
        let local_endpoint = P2pEndpoint {
            backend: UB_BACKEND_ID.to_string(),
            address: advertise.to_string(),
        };

        let store_dir = config.store_dir.join(UB_BACKEND_ID);
        tokio::fs::create_dir_all(&store_dir)
            .await
            .with_context(|| format!("create ub P2P store dir {}", store_dir.display()))
            .map_err(Error::Internal)?;
        let catalog = PublishedArtifactCatalog::load(
            &store_dir.join("catalog.db"),
            &node_id,
            &local_endpoint,
        )
        .await?;
        // Segment handles reference this process's registered memory; entries
        // persisted by a previous run are unusable.
        catalog.reset().await?;

        let catalog_server = CatalogServer::start_with_listener(listener, catalog.clone()).await?;

        info!(
            node_id,
            endpoint = %local_endpoint.address,
            device = %handle.device,
            eid = %handle.eid,
            arena_bytes = ub.arena_size,
            slice_bytes = ub.slice_size,
            "ub artifact transport started"
        );

        Ok(Self {
            inner: Arc::new(UbInner {
                node_id,
                peer_discovery,
                catalog,
                io,
                handle,
                arena,
                arena_wire,
                bounce: Arc::new(bounce),
                regions: Mutex::new(HashMap::new()),
                local_endpoint,
                catalog_server,
                lookup_timeout: config.lookup_timeout,
                fetch_timeout: config.fetch_timeout,
                slice_size: ub.slice_size,
                read_permits: Semaphore::new(ub.max_inflight_reads as usize),
            }),
        })
    }

    #[cfg(feature = "p2p-urma")]
    fn ffi_driver(ub: &crate::p2p::config::ResolvedUbP2pConfig) -> Result<Box<dyn UrmaDriver>> {
        use super::driver::lib_urma::{LibUrmaDriver, LibUrmaDriverConfig};
        let config = LibUrmaDriverConfig {
            device: ub.device.clone(),
            jetty_count: ub.jetty_count.max(1),
            max_wr: ub.max_wr.max(1),
            jfc_depth: ub.jfc_depth.max(64),
        };
        let driver = LibUrmaDriver::new(&config)
            .context("initialize liburma driver")
            .map_err(Error::Internal)?;
        Ok(Box::new(driver))
    }

    #[cfg(not(feature = "p2p-urma"))]
    fn ffi_driver(_ub: &crate::p2p::config::ResolvedUbP2pConfig) -> Result<Box<dyn UrmaDriver>> {
        Err(Error::internal_message(
            "initialize ub P2P transport",
            "p2p.ub.driver = \"ffi\" requires building with the `p2p-urma` cargo feature",
        ))
    }
}

/// Replace an unspecified bound IP with the host's primary outbound address
/// (UDP connect chooses the route without sending packets).
fn advertise_addr(bound: SocketAddr) -> SocketAddr {
    if !bound.ip().is_unspecified() {
        return bound;
    }
    let probed =
        std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).and_then(|socket| {
            socket.connect((std::net::Ipv4Addr::new(192, 0, 2, 1), 80))?;
            socket.local_addr()
        });
    match probed {
        Ok(local) => SocketAddr::new(local.ip(), bound.port()),
        Err(err) => {
            warn!(error = %err, "could not determine routable IP for ub P2P catalog; advertising unspecified address");
            bound
        }
    }
}

/// Resolved fetch target: local memory or a remote one-sided read source.
enum ResolvedFetch {
    Local {
        base_va: u64,
    },
    Remote {
        locator: UbLocator,
        peer_eid: [u8; 16],
        peer_jetty_id: u32,
    },
}

impl UbInner {
    fn resolve_fetch(&self, descriptor: &P2pArtifactDescriptor) -> Result<ResolvedFetch> {
        let locator = UbLocator::decode(descriptor)?;
        if descriptor.providers.is_empty() {
            return Err(Error::InvalidDescriptor {
                reason: "ub descriptor has no providers".to_string(),
            });
        }
        if descriptor
            .providers
            .iter()
            .any(|provider| provider.is_local())
        {
            return Ok(ResolvedFetch::Local {
                base_va: locator.base_va,
            });
        }
        let peer_eid =
            eid_from_hex32(&locator.handle.eid).ok_or_else(|| Error::InvalidDescriptor {
                reason: format!("invalid ub provider EID {:?}", locator.handle.eid),
            })?;
        let peer_jetty_id =
            *locator
                .handle
                .jetty_ids
                .first()
                .ok_or_else(|| Error::InvalidDescriptor {
                    reason: "ub provider advertises no jetties".to_string(),
                })?;
        Ok(ResolvedFetch::Remote {
            locator,
            peer_eid,
            peer_jetty_id,
        })
    }

    /// Copy a range out of locally published memory.
    fn read_local(&self, base_va: u64, offset: u64, len: usize) -> Result<Vec<u8>> {
        let region = {
            let regions = self.regions.lock().expect("ub regions lock");
            regions.get(&base_va).cloned()
        };
        let region = region.ok_or_else(|| Error::InvalidDescriptor {
            reason: format!("no published region at base {base_va:#x}"),
        })?;
        match &*region {
            PublishedRegion::Arena { .. } => {
                let arena_base = self.arena.map().base_addr();
                let va = base_va + offset;
                Ok(self.arena.map().read_at(va - arena_base, len))
            }
            PublishedRegion::File(map) => Ok(map.read_at(offset, len)),
        }
    }

    /// One-sided-read `[offset, offset + len)` of a remote artifact into a
    /// fresh Vec. `len` must not exceed the slice size.
    async fn read_remote_chunk(
        self: &Arc<Self>,
        locator: &UbLocator,
        peer_eid: [u8; 16],
        peer_jetty_id: u32,
        offset: u64,
        len: u32,
    ) -> Result<Vec<u8>> {
        let _permit = self
            .read_permits
            .acquire()
            .await
            .map_err(|_| Error::internal_message("ub read permit", "transport shut down"))?;
        let block = self.bounce.alloc().ok_or_else(|| {
            Error::internal_message(
                "ub read",
                "bounce pool exhausted (timed-out blocks are recycled lazily)",
            )
        })?;

        let (user_ctx, mut done) = self.io.alloc_completion();
        let post = self
            .io
            .post_read(UrmaReadOp {
                remote_wire: locator.wire_bytes()?,
                remote_addr: locator.base_va + offset,
                len,
                local_addr: self.bounce.block_addr(block),
                peer_eid,
                peer_jetty_id,
                user_ctx,
            })
            .await;
        if let Err(err) = post {
            self.io.cancel_completion(user_ctx);
            self.bounce.free(block);
            return Err(Error::internal_message("post ub read", err));
        }

        let sleep = tokio::time::sleep(self.fetch_timeout);
        tokio::pin!(sleep);
        tokio::select! {
            result = &mut done => {
                let outcome = match result {
                    Ok(Ok(())) => Ok(self.bounce.read_block(block, len as usize)),
                    Ok(Err(err)) => Err(Error::internal_message("ub read completion", err)),
                    Err(_dropped) => Err(Error::internal_message("ub read", "driver worker stopped")),
                };
                self.bounce.free(block);
                outcome
            }
            _ = &mut sleep => {
                // The read may still land in the block later; only a reaper
                // that observes the hardware completion may recycle it.
                let bounce = Arc::clone(&self.bounce);
                tokio::spawn(async move {
                    let _ = done.await;
                    bounce.free(block);
                });
                Err(Error::Timeout {
                    operation: "ub read completion",
                })
            }
        }
    }

    async fn fetch_to_writer(
        self: &Arc<Self>,
        descriptor: &P2pArtifactDescriptor,
        mut write: impl FnMut(&[u8]) -> Result<()>,
    ) -> Result<u64> {
        let resolved = self.resolve_fetch(descriptor)?;
        let size = match &resolved {
            ResolvedFetch::Local { .. } => descriptor_size(descriptor)?,
            ResolvedFetch::Remote { locator, .. } => locator.size,
        };
        let mut offset = 0u64;
        while offset < size {
            let chunk = match &resolved {
                ResolvedFetch::Local { .. } => LOCAL_COPY_CHUNK as u64,
                ResolvedFetch::Remote { .. } => self.slice_size,
            };
            let len = chunk.min(size - offset) as usize;
            let data = match &resolved {
                ResolvedFetch::Local { base_va } => self.read_local(*base_va, offset, len)?,
                ResolvedFetch::Remote {
                    locator,
                    peer_eid,
                    peer_jetty_id,
                } => {
                    self.read_remote_chunk(locator, *peer_eid, *peer_jetty_id, offset, len as u32)
                        .await?
                }
            };
            write(&data)?;
            offset += len as u64;
        }
        Ok(size)
    }
}

fn descriptor_size(descriptor: &P2pArtifactDescriptor) -> Result<u64> {
    Ok(UbLocator::decode(descriptor)?.size)
}

#[async_trait]
impl P2pTransport for UrmaP2pTransport {
    async fn lookup_with_hints(
        &self,
        key: &P2pArtifactKey,
        hints: &[P2pArtifactProviderHint],
    ) -> Result<Option<P2pArtifactDescriptor>> {
        let inner = &self.inner;
        if let Some(descriptor) = inner.catalog.descriptor_for(key).await {
            return Ok(Some(descriptor));
        }
        let mut peers = inner
            .peer_discovery
            .peers_for_key(key)
            .await
            .unwrap_or_else(|err| {
                debug!(error = %err, "scheduler P2P artifact lookup failed");
                Vec::new()
            });
        if peers.is_empty() {
            peers = inner
                .peer_discovery
                .peers_with_hints(hints)
                .await
                .unwrap_or_else(|err| {
                    debug!(error = %err, "P2P peer discovery failed");
                    Vec::new()
                });
        }
        for peer in peers {
            if peer.node_id == inner.node_id || peer.endpoint == inner.local_endpoint {
                if let Some(descriptor) = inner.catalog.descriptor_for(key).await {
                    return Ok(Some(descriptor));
                }
                continue;
            }
            match inner.lookup_peer(&peer, key).await {
                Ok(Some(descriptor)) => return Ok(Some(descriptor)),
                Ok(None) => continue,
                Err(err) => {
                    debug!(peer = %peer.node_id, error = %err, "ub catalog lookup failed; trying remaining peers");
                }
            }
        }
        Ok(None)
    }

    async fn fetch(&self, descriptor: &P2pArtifactDescriptor, destination: &Path) -> Result<u64> {
        let mut file = std::fs::File::create(destination)
            .with_context(|| format!("create P2P fetch target {}", destination.display()))
            .map_err(Error::Internal)?;
        // Chunks arrive strictly in offset order, so plain appends are fine.
        let written = self
            .inner
            .fetch_to_writer(descriptor, |data| {
                use std::io::Write;
                file.write_all(data)
                    .map_err(|err| Error::internal_message("write fetched ub artifact", err))
            })
            .await?;
        Ok(written)
    }

    async fn fetch_bytes(&self, descriptor: &P2pArtifactDescriptor) -> Result<Bytes> {
        let size = descriptor_size(descriptor)? as usize;
        let mut buffer = Vec::with_capacity(size);
        self.inner
            .fetch_to_writer(descriptor, |data| {
                buffer.extend_from_slice(data);
                Ok(())
            })
            .await?;
        Ok(Bytes::from(buffer))
    }

    async fn fetch_byte_range(
        &self,
        descriptor: &P2pArtifactDescriptor,
        offset: u64,
        len: usize,
    ) -> Result<P2pByteStream> {
        let inner = Arc::clone(&self.inner);
        let resolved = inner.resolve_fetch(descriptor)?;
        let total = match &resolved {
            ResolvedFetch::Local { .. } => descriptor_size(descriptor)?,
            ResolvedFetch::Remote { locator, .. } => locator.size,
        };
        let end = offset.saturating_add(len as u64);
        if end > total || offset > total {
            return Err(Error::InvalidDescriptor {
                reason: format!("ub range [{offset}, {end}) exceeds artifact size {total}"),
            });
        }

        let state = (inner, resolved, offset, len as u64, false);
        let stream = stream::unfold(
            state,
            |(inner, resolved, mut cursor, mut remaining, mut failed)| async move {
                if failed || remaining == 0 {
                    return None;
                }
                let chunk = match &resolved {
                    ResolvedFetch::Local { .. } => LOCAL_COPY_CHUNK as u64,
                    ResolvedFetch::Remote { .. } => inner.slice_size,
                }
                .min(remaining);
                let result = match &resolved {
                    ResolvedFetch::Local { base_va } => {
                        inner.read_local(*base_va, cursor, chunk as usize)
                    }
                    ResolvedFetch::Remote {
                        locator,
                        peer_eid,
                        peer_jetty_id,
                    } => {
                        inner
                            .read_remote_chunk(
                                locator,
                                *peer_eid,
                                *peer_jetty_id,
                                cursor,
                                chunk as u32,
                            )
                            .await
                    }
                };
                match result {
                    Ok(data) => {
                        cursor += chunk;
                        remaining -= chunk;
                        Some((
                            Ok(Bytes::from(data)),
                            (inner, resolved, cursor, remaining, false),
                        ))
                    }
                    Err(err) => {
                        failed = true;
                        Some((Err(err), (inner, resolved, cursor, 0, failed)))
                    }
                }
            },
        );
        Ok(Box::pin(stream))
    }

    async fn publish(&self, request: &P2pPublishRequest) -> Result<()> {
        let inner = &self.inner;
        let (region, wire, base_va, size) = match (&request.source, request.publish_mode) {
            (P2pPublishSource::Path(path), P2pPublishMode::Reference) => {
                let (map, len) = Mmap::file_read_only(path)
                    .with_context(|| format!("map published artifact {}", path.display()))
                    .map_err(Error::Internal)?;
                let registered = inner
                    .io
                    .register_region(map.base_addr() as usize, len, RegionAccess::RemoteRead)
                    .await
                    .context("register published artifact region")
                    .map_err(Error::Internal)?;
                (
                    PublishedRegion::File(map),
                    registered.wire,
                    registered.base_va,
                    len,
                )
            }
            (source, _) => {
                let bytes = match source {
                    P2pPublishSource::Path(path) => tokio::fs::read(path)
                        .await
                        .with_context(|| format!("read published artifact {}", path.display()))
                        .map_err(Error::Internal)?,
                    P2pPublishSource::Bytes(bytes) => bytes.to_vec(),
                };
                let offset = inner.arena.alloc(bytes.len() as u64).ok_or_else(|| {
                    Error::internal_message(
                        "publish ub artifact",
                        format!("publish arena exhausted ({} bytes requested)", bytes.len()),
                    )
                })?;
                inner.arena.write(offset, &bytes);
                (
                    PublishedRegion::Arena { offset },
                    inner.arena_wire.wire.clone(),
                    inner.arena_wire.base_va + offset,
                    bytes.len() as u64,
                )
            }
        };

        let locator = UbLocator {
            v: LOCATOR_VERSION,
            wire: base64::engine::general_purpose::STANDARD.encode(wire),
            base_va,
            size,
            handle: inner.handle.clone(),
        };
        let descriptor = P2pArtifactDescriptor {
            key: request.key.clone(),
            providers: vec![crate::p2p::types::P2pArtifactProvider::Local],
            backend_locator: Some(locator.encode()?),
            metadata: request.metadata.clone(),
        };

        inner
            .regions
            .lock()
            .expect("ub regions lock")
            .insert(base_va, Arc::new(region));
        if let Err(err) = inner.catalog.upsert(descriptor).await {
            inner
                .regions
                .lock()
                .expect("ub regions lock")
                .remove(&base_va);
            return Err(err);
        }
        if let Err(err) = inner.peer_discovery.record_key(&request.key).await {
            warn!(error = %err, "failed to record published ub artifact in scheduler");
        }
        Ok(())
    }

    async fn unpublish(&self, key: &P2pArtifactKey) -> Result<bool> {
        let inner = &self.inner;
        let removed = inner.catalog.remove(key).await?;
        let Some(descriptor) = removed else {
            return Ok(false);
        };
        if let Ok(locator) = UbLocator::decode(&descriptor) {
            let region = inner
                .regions
                .lock()
                .expect("ub regions lock")
                .remove(&locator.base_va);
            if let Some(region) = region {
                match &*region {
                    PublishedRegion::Arena { offset } => inner.arena.free(*offset),
                    PublishedRegion::File(map) => {
                        if let Err(err) = inner.io.unregister_region(map.base_addr() as usize).await
                        {
                            warn!(error = %err, "failed to unregister unpublished ub artifact");
                        }
                    }
                }
            }
        }
        if let Err(err) = inner.peer_discovery.forget_key(key).await {
            warn!(error = %err, "failed to forget unpublished ub artifact in scheduler");
        }
        Ok(true)
    }

    fn local_endpoint(&self) -> Option<P2pEndpoint> {
        Some(self.inner.local_endpoint.clone())
    }

    async fn shutdown(&self) -> Result<()> {
        self.inner.catalog_server.shutdown();
        self.inner.io.shutdown().await;
        Ok(())
    }
}

impl UbInner {
    async fn lookup_peer(
        &self,
        peer: &P2pPeer,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        if peer.endpoint.backend != UB_BACKEND_ID {
            return Err(Error::InvalidDescriptor {
                reason: format!(
                    "peer {} endpoint backend {} is not {UB_BACKEND_ID}",
                    peer.node_id, peer.endpoint.backend
                ),
            });
        }
        let addr: SocketAddr =
            peer.endpoint
                .address
                .parse()
                .map_err(|err| Error::InvalidDescriptor {
                    reason: format!("invalid ub peer address {:?}: {err}", peer.endpoint.address),
                })?;
        tokio::time::timeout(self.lookup_timeout, request_descriptor(addr, key))
            .await
            .map_err(|_| Error::Timeout {
                operation: "lookup ub artifact from peer",
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;

    use crate::p2p::config::{ResolvedP2pConfig, ResolvedUbP2pConfig};
    use crate::p2p::discovery::{NoopP2pPeerDiscovery, StaticP2pPeerDiscovery};
    use crate::p2p::transport::P2pTransport;
    use crate::p2p::types::P2pPeer;
    use crate::p2p::P2pTransportKind;

    fn test_config(store_dir: &std::path::Path) -> ResolvedP2pConfig {
        ResolvedP2pConfig {
            transport: P2pTransportKind::Ub,
            store_dir: store_dir.to_path_buf(),
            listen_addr: Some("127.0.0.1:0".to_string()),
            lookup_timeout: Duration::from_secs(5),
            fetch_timeout: Duration::from_secs(5),
            peer_discovery_refresh_interval: Duration::from_secs(5),
            ub: ResolvedUbP2pConfig {
                device: "loopback".to_string(),
                driver: "loopback".to_string(),
                jetty_count: 1,
                max_wr: 16,
                jfc_depth: 64,
                slice_size: 4096,
                max_inflight_reads: 4,
                arena_size: 4 * 1024 * 1024,
                catalog_listen_addr: None,
            },
        }
    }

    struct TestNode {
        transport: UrmaP2pTransport,
        _temp: tempfile::TempDir,
    }

    async fn start_node(node_id: &str, peers: Vec<P2pPeer>) -> anyhow::Result<TestNode> {
        let temp = tempfile::tempdir()?;
        let discovery: Arc<dyn P2pPeerDiscovery> = if peers.is_empty() {
            Arc::new(NoopP2pPeerDiscovery)
        } else {
            Arc::new(StaticP2pPeerDiscovery::new(peers))
        };
        let transport =
            UrmaP2pTransport::new(test_config(temp.path()), node_id.to_string(), discovery).await?;
        Ok(TestNode {
            transport,
            _temp: temp,
        })
    }

    /// Patterned payload spanning many 4 KiB slices.
    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 + 7) as u8).collect()
    }

    #[tokio::test]
    async fn ub_publish_lookup_fetch_round_trip() -> anyhow::Result<()> {
        let provider = start_node("provider", Vec::new()).await?;
        let provider_peer = P2pPeer {
            node_id: "provider".to_string(),
            endpoint: provider
                .transport
                .local_endpoint()
                .expect("provider endpoint"),
        };
        let fetcher = start_node("fetcher", vec![provider_peer]).await?;

        let data = payload(300 * 1024);
        let key = "overlaybd-layer/v1/sha256:deadbeef".to_string();
        provider
            .transport
            .publish(&P2pPublishRequest::bytes(key.clone(), data.clone()))
            .await?;

        let descriptor = fetcher
            .transport
            .lookup(&key)
            .await?
            .expect("descriptor found");
        assert_eq!(descriptor.providers.len(), 1);

        let fetched = fetcher.transport.fetch_bytes(&descriptor).await?;
        assert_eq!(fetched.as_ref(), data.as_slice());

        // Byte-range reads must return exactly the requested window.
        let mut range = fetcher
            .transport
            .fetch_byte_range(&descriptor, 100_001, 77_777)
            .await?;
        let mut collected = Vec::new();
        while let Some(chunk) = range.next().await {
            collected.extend_from_slice(&chunk?);
        }
        assert_eq!(collected, data[100_001..100_001 + 77_777]);

        // Full fetch to a destination path.
        let temp = tempfile::tempdir()?;
        let dest = temp.path().join("layer.bin");
        let written = fetcher.transport.fetch(&descriptor, &dest).await?;
        assert_eq!(written, data.len() as u64);
        assert_eq!(std::fs::read(&dest)?, data);

        fetcher.transport.shutdown().await?;
        provider.transport.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn ub_publish_path_by_reference_round_trip() -> anyhow::Result<()> {
        let provider = start_node("provider", Vec::new()).await?;
        let provider_peer = P2pPeer {
            node_id: "provider".to_string(),
            endpoint: provider
                .transport
                .local_endpoint()
                .expect("provider endpoint"),
        };
        let fetcher = start_node("fetcher", vec![provider_peer]).await?;

        let temp = tempfile::tempdir()?;
        let layer_path = temp.path().join("layer.commit");
        let data = payload(150_000);
        std::fs::write(&layer_path, &data)?;

        let key = "overlaybd-layer/v1/sha256:cafe".to_string();
        provider
            .transport
            .publish(
                &P2pPublishRequest::file(key.clone(), layer_path.clone())
                    .with_publish_mode(P2pPublishMode::Reference),
            )
            .await?;

        let descriptor = fetcher
            .transport
            .lookup(&key)
            .await?
            .expect("descriptor found");
        let fetched = fetcher.transport.fetch_bytes(&descriptor).await?;
        assert_eq!(fetched.as_ref(), data.as_slice());

        // Local-provider fetch (the provider fetches its own descriptor).
        let own = provider
            .transport
            .lookup(&key)
            .await?
            .expect("local descriptor");
        let own_bytes = provider.transport.fetch_bytes(&own).await?;
        assert_eq!(own_bytes.as_ref(), data.as_slice());

        // Unpublish removes the catalog entry and the registration.
        assert!(provider.transport.unpublish(&key).await?);
        assert!(!provider.transport.unpublish(&key).await?);
        assert!(fetcher.transport.lookup(&key).await?.is_none());

        fetcher.transport.shutdown().await?;
        provider.transport.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn ub_range_fetch_rejects_out_of_bounds() -> anyhow::Result<()> {
        let node = start_node("solo", Vec::new()).await?;
        let key = "test/range".to_string();
        node.transport
            .publish(&P2pPublishRequest::bytes(key.clone(), vec![7u8; 100]))
            .await?;
        let descriptor = node.transport.lookup(&key).await?.expect("descriptor");
        match node.transport.fetch_byte_range(&descriptor, 50, 100).await {
            Err(err) => assert!(err.to_string().contains("exceeds artifact size")),
            Ok(_) => panic!("range past end must fail"),
        }
        node.transport.shutdown().await?;
        Ok(())
    }
}

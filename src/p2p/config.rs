use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

use crate::cfg::P2pConfig;

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    Disabled,
    Iroh,
    Ub,
}

impl P2pTransportKind {
    pub(crate) fn backend_id(self) -> Option<&'static str> {
        match self {
            Self::Disabled => None,
            Self::Iroh => Some(super::iroh::IROH_BACKEND_ID),
            Self::Ub => Some(super::urma::UB_BACKEND_ID),
        }
    }
}

/// Resolved `[p2p.ub]` settings for the URMA-backed transport. Some fields
/// are only read by the `p2p-urma` feature's FFI driver.
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "p2p-urma"), allow(dead_code))]
pub(crate) struct ResolvedUbP2pConfig {
    /// URMA device name (e.g. `urma0`); empty picks the first listed device.
    pub device: String,
    /// `ffi` (real liburma) or `loopback` (in-process test driver).
    pub driver: String,
    pub jetty_count: u32,
    pub max_wr: u32,
    pub jfc_depth: u32,
    pub slice_size: u64,
    pub max_inflight_reads: u32,
    pub arena_size: u64,
    /// Catalog TCP listen address; `None` falls back to `p2p.listen_addr`.
    pub catalog_listen_addr: Option<String>,
}

impl ResolvedUbP2pConfig {
    fn from_config(ub: &crate::cfg::UbP2pConfig) -> Self {
        Self {
            device: ub.device.clone(),
            driver: ub.driver.trim().to_string(),
            jetty_count: ub.jetty_count.max(1),
            max_wr: ub.max_wr.max(1),
            jfc_depth: ub.jfc_depth.max(64),
            slice_size: ub.slice_size_bytes,
            max_inflight_reads: ub.max_inflight_reads.max(1),
            arena_size: ub.arena_size_bytes,
            catalog_listen_addr: Some(ub.catalog_listen_addr.trim())
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedP2pConfig {
    pub transport: P2pTransportKind,
    pub store_dir: PathBuf,
    pub listen_addr: Option<String>,
    pub lookup_timeout: Duration,
    pub fetch_timeout: Duration,
    pub peer_discovery_refresh_interval: Duration,
    pub ub: ResolvedUbP2pConfig,
}

impl ResolvedP2pConfig {
    pub(crate) fn from_config(p2p: &P2pConfig) -> Self {
        let transport = if p2p.enabled {
            p2p.transport
        } else {
            P2pTransportKind::Disabled
        };

        Self {
            transport,
            store_dir: p2p.store_dir.clone(),
            listen_addr: Some(str::trim(p2p.listen_addr.as_str()))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string),
            lookup_timeout: Duration::from_millis(p2p.lookup_timeout_ms),
            fetch_timeout: Duration::from_millis(p2p.fetch_timeout_ms),
            peer_discovery_refresh_interval: Duration::from_secs(
                p2p.peer_discovery_refresh_interval_secs,
            )
            .max(Duration::from_secs(1)),
            ub: ResolvedUbP2pConfig::from_config(&p2p.ub),
        }
    }
}

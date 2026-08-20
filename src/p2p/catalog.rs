use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::local_store::{LocalKvStore, LocalStoreDurability};
use crate::p2p::error::Result;
use crate::p2p::types::{P2pArtifactDescriptor, P2pArtifactKey, P2pEndpoint, P2pPeer};

/// Upper bound for serialized catalog responses, shared by every backend's
/// catalog protocol encoding.
pub(crate) const MAX_CATALOG_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Upper bound for serialized catalog requests.
pub(crate) const MAX_CATALOG_REQUEST_BYTES: usize = 1024 * 1024;

#[cfg(not(test))]
const DB_DURABILITY: LocalStoreDurability = LocalStoreDurability::Wal;
#[cfg(test)]
const DB_DURABILITY: LocalStoreDurability = LocalStoreDurability::Memory;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogRequest {
    pub(crate) key: P2pArtifactKey,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct CatalogResponse {
    pub(crate) descriptor: Option<P2pArtifactDescriptor>,
}

/// Backend-neutral store of descriptors this node has published and can
/// advertise to peers. Persisted through the shared node-local key/value
/// store so publications survive restarts.
#[derive(Debug, Clone)]
pub(crate) struct PublishedArtifactCatalog {
    inner: Arc<RwLock<HashMap<P2pArtifactKey, P2pArtifactDescriptor>>>,
    store: LocalKvStore,
    local_provider: P2pPeer,
}

impl PublishedArtifactCatalog {
    pub(crate) async fn load(
        db_path: &Path,
        node_id: &str,
        local_endpoint: &P2pEndpoint,
    ) -> Result<Self> {
        let store = LocalKvStore::open(db_path.to_path_buf(), DB_DURABILITY)
            .await
            .with_context(|| format!("open P2P catalog {}", db_path.display()))?;

        let loaded = store
            .fold(HashMap::new(), |loaded, key, bytes| {
                let descriptor: P2pArtifactDescriptor = serde_json::from_slice(&bytes)
                    .with_context(|| {
                        format!("parse P2P catalog entry {}", String::from_utf8_lossy(&key))
                    })?;
                loaded.insert(descriptor.key.clone(), descriptor);
                Ok(())
            })
            .await
            .with_context(|| format!("scan P2P catalog {}", db_path.display()))?;

        tracing::debug!(
            catalog = %db_path.display(),
            entry_count = loaded.len(),
            "loaded persisted P2P catalog"
        );
        Ok(Self {
            inner: Arc::new(RwLock::new(loaded)),
            store,
            local_provider: P2pPeer {
                node_id: node_id.to_string(),
                endpoint: local_endpoint.clone(),
            },
        })
    }

    pub(crate) async fn descriptor_for(
        &self,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        self.inner.read().await.get(key).cloned()
    }

    /// Descriptor for serving to a remote peer: the provider list always
    /// reflects the *current* node identity instead of whatever the entry
    /// happened to store.
    pub(crate) async fn serving_descriptor(
        &self,
        key: &P2pArtifactKey,
    ) -> Option<P2pArtifactDescriptor> {
        self.inner
            .read()
            .await
            .get(key)
            .cloned()
            .map(|mut descriptor| {
                descriptor.providers = vec![self.local_provider.clone().into()];
                descriptor
            })
    }

    pub(crate) async fn upsert(&self, descriptor: P2pArtifactDescriptor) -> Result<()> {
        let bytes = serde_json::to_vec(&descriptor).context("serialize P2P catalog entry")?;
        self.store
            .put(descriptor.key.as_bytes(), bytes)
            .await
            .with_context(|| format!("persist P2P catalog entry {}", descriptor.key))?;
        let mut catalog = self.inner.write().await;
        catalog.insert(descriptor.key.clone(), descriptor);
        Ok(())
    }

    pub(crate) async fn remove(
        &self,
        key: &P2pArtifactKey,
    ) -> Result<Option<P2pArtifactDescriptor>> {
        self.store
            .delete(key.as_bytes())
            .await
            .with_context(|| format!("delete P2P catalog entry {key}"))?;
        let removed = self.inner.write().await.remove(key);
        Ok(removed)
    }

    /// Drop every entry. Used by backends whose registrations are
    /// process-local (e.g. `ub`, whose segment handles reference memory of
    /// the previous process instance) and therefore cannot serve entries
    /// persisted by an earlier run.
    pub(crate) async fn reset(&self) -> Result<()> {
        let keys: Vec<P2pArtifactKey> = self.inner.read().await.keys().cloned().collect();
        for key in keys {
            self.remove(&key).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use super::*;
    use crate::p2p::types::P2pArtifactProvider;

    fn endpoint(address: &str) -> P2pEndpoint {
        P2pEndpoint {
            backend: "ub".to_string(),
            address: address.to_string(),
        }
    }

    #[tokio::test]
    async fn persisted_entries_round_trip_and_remove() -> anyhow::Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let db_path = temp.path().join("catalog.db");
        let local_endpoint = endpoint("127.0.0.1:9000");
        let catalog = PublishedArtifactCatalog::load(&db_path, "node-a", &local_endpoint)
            .await
            .context("load catalog")?;

        let key = "test/p2p/catalog/round-trip".to_string();
        catalog
            .upsert(P2pArtifactDescriptor {
                key: key.clone(),
                providers: vec![P2pArtifactProvider::Local],
                backend_locator: Some("locator".to_string()),
                metadata: serde_json::json!({ "kind": "round-trip" }),
            })
            .await
            .context("upsert")?;
        let removed = catalog.remove(&key).await.context("remove")?;
        assert!(removed.is_some());
        assert!(catalog.descriptor_for(&key).await.is_none());
        // RocksDB holds an exclusive lock on the directory; close the first
        // handle before reopening the same path.
        drop(catalog);

        // Reopen from disk: the removal must also be persisted.
        let reopened = PublishedArtifactCatalog::load(&db_path, "node-a", &local_endpoint)
            .await
            .context("reopen catalog")?;
        assert!(reopened.descriptor_for(&key).await.is_none());
        Ok(())
    }
}

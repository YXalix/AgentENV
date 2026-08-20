use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};

use crate::p2p::catalog::{CatalogRequest, CatalogResponse, PublishedArtifactCatalog};

pub(super) const CATALOG_ALPN: &[u8] = b"/agentenv/artifact-catalog/v1";

/// Time to wait for the client to close the connection after we finish
/// sending. Prevents a slow or misbehaving peer from holding a handler task open.
const CONNECTION_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub(crate) struct CatalogProtocol {
    published_catalog: PublishedArtifactCatalog,
}

impl CatalogProtocol {
    pub(crate) fn new(published_catalog: PublishedArtifactCatalog) -> Self {
        Self { published_catalog }
    }

    pub(crate) async fn descriptor_for_response(
        &self,
        key: &crate::p2p::types::P2pArtifactKey,
    ) -> Option<crate::p2p::types::P2pArtifactDescriptor> {
        self.published_catalog.serving_descriptor(key).await
    }
}

impl ProtocolHandler for CatalogProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        let (mut send, mut recv) = connection.accept_bi().await?;
        let request_bytes = recv
            .read_to_end(crate::p2p::catalog::MAX_CATALOG_REQUEST_BYTES)
            .await
            .map_err(AcceptError::from_err)?;
        let request: CatalogRequest =
            serde_json::from_slice(&request_bytes).map_err(AcceptError::from_err)?;
        let descriptor = self.descriptor_for_response(&request.key).await;
        let found = descriptor.is_some();
        let response = CatalogResponse { descriptor };
        let response_bytes = serde_json::to_vec(&response).map_err(AcceptError::from_err)?;
        send.write_all(&response_bytes)
            .await
            .map_err(AcceptError::from_err)?;
        send.finish()?;
        tracing::trace!(key = %request.key, found, "served P2P catalog request");
        let _ = tokio::time::timeout(CONNECTION_CLOSE_TIMEOUT, connection.closed()).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;

    use super::*;
    use crate::p2p::types::{
        P2pArtifactDescriptor, P2pArtifactKey, P2pArtifactProvider, P2pEndpoint,
    };

    fn endpoint(address: &str) -> P2pEndpoint {
        P2pEndpoint {
            backend: "iroh".to_string(),
            address: address.to_string(),
        }
    }

    fn provider(node_id: &str, address: &str) -> P2pArtifactProvider {
        P2pArtifactProvider::from(crate::p2p::types::P2pPeer {
            node_id: node_id.to_string(),
            endpoint: endpoint(address),
        })
    }

    #[tokio::test]
    async fn response_descriptor_uses_current_local_provider() -> anyhow::Result<()> {
        let temp = tempfile::tempdir().context("create temp test dir")?;
        let local_endpoint = endpoint("fresh-local-endpoint");
        let catalog = PublishedArtifactCatalog::load(
            &temp.path().join("catalog.db"),
            "current-node",
            &local_endpoint,
        )
        .await
        .context("load catalog")?;
        let key: P2pArtifactKey = "test/p2p/catalog/accept-provider".to_string();
        catalog
            .upsert(P2pArtifactDescriptor {
                key: key.clone(),
                providers: vec![
                    P2pArtifactProvider::Local,
                    provider("stale-node", "stale-endpoint"),
                ],
                backend_locator: Some("blob-hash".to_string()),
                metadata: serde_json::json!({ "kind": "catalog-accept-test" }),
            })
            .await
            .context("upsert descriptor")?;
        let protocol = CatalogProtocol::new(catalog);

        let descriptor = protocol
            .descriptor_for_response(&key)
            .await
            .context("descriptor should be present")?;

        assert_eq!(descriptor.backend_locator, Some("blob-hash".to_string()));
        assert_eq!(
            descriptor.metadata,
            serde_json::json!({ "kind": "catalog-accept-test" })
        );
        assert_eq!(
            descriptor.providers,
            vec![P2pArtifactProvider::from(crate::p2p::types::P2pPeer {
                node_id: "current-node".to_string(),
                endpoint: local_endpoint,
            })]
        );
        Ok(())
    }
}

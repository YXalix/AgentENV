//! TCP catalog server/client for the `ub` transport.
//!
//! The catalog is the metadata channel of the transport: peers ask "who
//! serves artifact X and what is its URMA segment handle?" over a plain TCP
//! connection, then transfer bytes out-of-band via one-sided READs. Framing
//! is a 4-byte little-endian length prefix followed by a JSON
//! [`CatalogRequest`]/[`CatalogResponse`]; the message types are shared with
//! the iroh backend so the on-the-wire catalog shape stays backend-neutral.

use std::net::SocketAddr;

use anyhow::Context as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, trace, warn};

use crate::p2p::catalog::{
    CatalogRequest, CatalogResponse, PublishedArtifactCatalog, MAX_CATALOG_REQUEST_BYTES,
    MAX_CATALOG_RESPONSE_BYTES,
};
use crate::p2p::error::{Error, Result};
use crate::p2p::types::{P2pArtifactDescriptor, P2pArtifactKey};

/// Running catalog server. Dropping the handle aborts the accept loop.
pub(crate) struct CatalogServer {
    #[cfg_attr(not(test), allow(dead_code))]
    local_addr: SocketAddr,
    accept_task: tokio::task::JoinHandle<()>,
}

impl CatalogServer {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Bind `listen` and serve catalog lookups until the handle is dropped.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn start(
        listen: SocketAddr,
        catalog: PublishedArtifactCatalog,
    ) -> Result<Self> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("bind ub P2P catalog listener on {listen}"))
            .map_err(Error::Internal)?;
        Self::start_with_listener(listener, catalog).await
    }

    /// Serve on an already-bound listener (the transport binds first so it
    /// can advertise the concrete address in its local endpoint).
    pub(crate) async fn start_with_listener(
        listener: TcpListener,
        catalog: PublishedArtifactCatalog,
    ) -> Result<Self> {
        let local_addr = listener
            .local_addr()
            .context("read ub P2P catalog listener address")
            .map_err(Error::Internal)?;

        let accept_task = tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        let catalog = catalog.clone();
                        tokio::spawn(async move {
                            if let Err(err) = serve_connection(stream, catalog).await {
                                debug!(%peer, error = %err, "ub P2P catalog connection failed");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(error = %err, "ub P2P catalog accept failed");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                }
            }
        });

        Ok(Self {
            local_addr,
            accept_task,
        })
    }

    pub(crate) fn shutdown(&self) {
        self.accept_task.abort();
    }
}

impl Drop for CatalogServer {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    catalog: PublishedArtifactCatalog,
) -> anyhow::Result<()> {
    let request_len = stream.read_u32_le().await? as usize;
    anyhow::ensure!(
        request_len <= MAX_CATALOG_REQUEST_BYTES,
        "catalog request too large: {request_len} bytes"
    );
    let mut request_bytes = vec![0u8; request_len];
    stream.read_exact(&mut request_bytes).await?;
    let request: CatalogRequest =
        serde_json::from_slice(&request_bytes).context("parse ub P2P catalog request")?;

    let descriptor = catalog.serving_descriptor(&request.key).await;
    let found = descriptor.is_some();
    let response_bytes = serde_json::to_vec(&CatalogResponse { descriptor })
        .context("serialize catalog response")?;
    anyhow::ensure!(
        response_bytes.len() <= MAX_CATALOG_RESPONSE_BYTES,
        "catalog response too large: {} bytes",
        response_bytes.len()
    );
    stream.write_u32_le(response_bytes.len() as u32).await?;
    stream.write_all(&response_bytes).await?;
    trace!(key = %request.key, found, "served ub P2P catalog request");
    Ok(())
}

/// Client side of the catalog protocol: one request per connection.
pub(crate) async fn request_descriptor(
    catalog_addr: SocketAddr,
    key: &P2pArtifactKey,
) -> Result<Option<P2pArtifactDescriptor>> {
    let mut stream = TcpStream::connect(catalog_addr)
        .await
        .with_context(|| format!("connect to ub P2P catalog peer {catalog_addr}"))
        .map_err(Error::Internal)?;

    let request_bytes = serde_json::to_vec(&CatalogRequest { key: key.clone() })
        .context("serialize catalog request")
        .map_err(Error::Internal)?;
    stream
        .write_u32_le(request_bytes.len() as u32)
        .await
        .map_err(|err| Error::internal_message("write catalog request length", err))?;
    stream
        .write_all(&request_bytes)
        .await
        .map_err(|err| Error::internal_message("write catalog request", err))?;

    let response_len = stream
        .read_u32_le()
        .await
        .map_err(|err| Error::internal_message("read catalog response length", err))?
        as usize;
    if response_len > MAX_CATALOG_RESPONSE_BYTES {
        return Err(Error::InvalidDescriptor {
            reason: format!("catalog response too large: {response_len} bytes"),
        });
    }
    let mut response_bytes = vec![0u8; response_len];
    stream
        .read_exact(&mut response_bytes)
        .await
        .map_err(|err| Error::internal_message("read catalog response", err))?;
    let response: CatalogResponse = serde_json::from_slice(&response_bytes)
        .context("parse catalog response")
        .map_err(Error::Internal)?;
    Ok(response.descriptor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::p2p::types::{P2pArtifactProvider, P2pEndpoint};

    #[tokio::test]
    async fn catalog_round_trip_over_tcp() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let endpoint = P2pEndpoint {
            backend: "ub".to_string(),
            address: "127.0.0.1:1".to_string(),
        };
        let catalog =
            PublishedArtifactCatalog::load(&temp.path().join("catalog.db"), "node-a", &endpoint)
                .await?;
        let key = "overlaybd-layer/v1/sha256:abc".to_string();
        catalog
            .upsert(P2pArtifactDescriptor {
                key: key.clone(),
                providers: vec![P2pArtifactProvider::Local],
                backend_locator: Some("wire-locator".to_string()),
                metadata: serde_json::json!({"protocol": "agentenv-overlaybd-layer-v1"}),
            })
            .await?;

        let server = CatalogServer::start("127.0.0.1:0".parse().unwrap(), catalog).await?;

        let descriptor = request_descriptor(server.local_addr(), &key)
            .await?
            .expect("descriptor found");
        assert_eq!(descriptor.key, key);
        assert_eq!(descriptor.backend_locator.as_deref(), Some("wire-locator"));
        // The server must advertise itself, not the stored Local placeholder.
        assert!(matches!(
            descriptor.providers.as_slice(),
            [P2pArtifactProvider::Peer(peer)] if peer.node_id == "node-a"
        ));

        let missing = request_descriptor(server.local_addr(), &"nope".to_string()).await?;
        assert!(missing.is_none());
        Ok(())
    }
}

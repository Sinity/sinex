//! `sources.import_progress` — live rate/position/ETA/backlog for in-flight
//! paced historical imports (sinex-2n9).
//!
//! Read-only. Backed by the same NATS KV bucket
//! [`crate::runtime::scan_progress::ScanProgressStore`] that
//! `AdapterBackedSource::scan_historical`'s `ScanPacer` publishes to —
//! this handler is a pure read, never a second source of truth.

use crate::api::service_container::ServiceContainer;
use crate::runtime::scan_progress::{ScanProgressSnapshot, ScanProgressStore};
use sinex_primitives::Result;

pub use sinex_primitives::rpc::sources::{
    SourcesImportProgressRequest, SourcesImportProgressResponse,
};

pub async fn handle_sources_import_progress(
    services: &ServiceContainer,
    _request: SourcesImportProgressRequest,
) -> Result<SourcesImportProgressResponse> {
    let Some(nats_client) = services.nats_client() else {
        // No NATS in this deployment shape (e.g. edge/dry-run) — no imports
        // can be in flight either, so an empty list is the honest answer.
        return Ok(SourcesImportProgressResponse { imports: vec![] });
    };

    let env = services.environment();
    let namespace = std::env::var("SINEX_NAMESPACE").ok();
    let store = ScanProgressStore::open(nats_client, env, namespace.as_deref()).await?;
    let snapshots = store.list().await?;

    Ok(SourcesImportProgressResponse {
        imports: snapshots
            .into_iter()
            .map(ScanProgressSnapshot::into_rpc_entry)
            .collect(),
    })
}

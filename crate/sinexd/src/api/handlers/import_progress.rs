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
    let store = ScanProgressStore::open(nats_client, env, services.nats_namespace()).await?;
    let snapshots = store.list().await?;

    Ok(SourcesImportProgressResponse {
        imports: snapshots
            .into_iter()
            .map(ScanProgressSnapshot::into_rpc_entry)
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::{NatsPublisher, PacingController, RateBudget};
    use crate::runtime::scan_progress::ScanProgressTracker;
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn progress_store_uses_the_publishers_resolved_namespace(
        ctx: TestContext,
    ) -> TestResult<()> {
        let ctx = ctx.with_nats().dedicated().await?;
        let nats_client = ctx.nats_client();
        let publisher = NatsPublisher::with_namespace(
            nats_client.clone(),
            Some("flag-only-progress".to_string()),
        );
        let env = sinex_primitives::environment::environment();
        let store = ScanProgressStore::open(&nats_client, &env, publisher.namespace()).await?;
        let controller = PacingController::new(RateBudget::default_paced());
        let tracker = ScanProgressTracker::new(None);
        let snapshot = ScanProgressSnapshot::from_controller(
            "flag-only-progress-source",
            Timestamp::now(),
            &controller,
            &tracker,
            None,
        );
        store.publish(&snapshot).await?;

        let namespaced = ScanProgressStore::open(&nats_client, &env, publisher.namespace())
            .await?
            .list()
            .await?;
        let default = ScanProgressStore::open(&nats_client, &env, None)
            .await?
            .list()
            .await?;

        assert_eq!(namespaced.len(), 1);
        assert_eq!(namespaced[0].module_name, "flag-only-progress-source");
        assert!(default.is_empty());
        Ok(())
    }
}

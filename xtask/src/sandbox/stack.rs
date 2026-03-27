//! `TestCoreStack` — unified test fixture for the full sinex service stack.
//!
//! Composes NATS + ingestd + gateway into a single fixture with proper
//! startup ordering, readiness checks, and coordinated shutdown. Designed
//! for integration tests that need to exercise the full request path
//! (publish → ingest → persist → query via RPC).

use crate::sandbox::context::Sandbox;
use crate::sandbox::coordination::PipelineNamespace;
use crate::sandbox::nats::acquire_pipeline_permit;
use crate::sandbox::orchestrator::{
    TestGatewayConfig, TestGatewayHandle, TestIngestdConfig, TestIngestdHandle, start_test_gateway,
    start_test_ingestd_with_config,
};
use crate::sandbox::prelude::*;
use sinex_primitives::events::{OffsetKind, Publishable, SourceMaterial};
use sinex_primitives::{EventSource, EventType, Id, Timestamp};
use std::net::SocketAddr;
use tempfile::TempDir;
use tokio::runtime::Handle;
use tokio::sync::OwnedSemaphorePermit;
use tracing::info;

/// Default RPC bearer token used by the test stack.
/// Format: `<secret>:<role>` — the gateway parses the role suffix for authorization.
pub const TEST_RPC_TOKEN: &str = "test-stack-token:admin";

/// Unified test fixture that starts the full sinex service stack:
/// NATS (shared ephemeral) + ingestd (subprocess) + gateway (subprocess).
///
/// # Lifecycle
///
/// ```text
/// TestCoreStack::new(ctx)
///   1. Ensure shared NATS
///   2. Reset DB slot
///   3. Generate self-signed TLS certs
///   4. Start ingestd subprocess (with consumer readiness wait)
///   5. Start gateway subprocess (with TCP readiness wait)
///   → Ready: publish events, query via RPC
/// ```
///
/// # Usage
///
/// ```rust,ignore
/// #[sinex_test]
/// async fn test_full_stack(ctx: TestContext) -> TestResult<()> {
///     let stack = TestCoreStack::new(&ctx).await?;
///
///     // Seed data
///     stack.seed_material_with_events("fs-watcher", "file.created", 5).await?;
///
///     // Query via RPC
///     let url = stack.rpc_url();
///     let token = stack.rpc_token();
///     // ... use GatewayClient or sinexctl against url/token
///
///     stack.shutdown().await?;
///     Ok(())
/// }
/// ```
pub struct TestCoreStack<'ctx> {
    ctx: &'ctx Sandbox,
    ingestd: Option<TestIngestdHandle>,
    gateway: Option<TestGatewayHandle>,
    pipeline_permit: Option<OwnedSemaphorePermit>,
    _work_dir: TempDir,
    _cert_file: tempfile::NamedTempFile,
    _key_file: tempfile::NamedTempFile,
    rpc_token: String,
}

impl<'ctx> TestCoreStack<'ctx> {
    /// Start the full service stack against the given sandbox context.
    ///
    /// The sandbox must have NATS initialized (calls `ensure_shared_nats`).
    /// The DB slot is reset before starting services.
    pub async fn new(ctx: &'ctx Sandbox) -> TestResult<Self> {
        Self::with_token(ctx, TEST_RPC_TOKEN.to_string()).await
    }

    /// Start the stack with a custom RPC token.
    pub async fn with_token(ctx: &'ctx Sandbox, rpc_token: String) -> TestResult<Self> {
        ctx.ensure_shared_nats()?;
        ctx.reset_database_slot().await?;

        let nats = ctx.nats_handle()?;
        let namespace = ctx.pipeline_namespace().prefix().to_string();
        let pipeline_permit = Some(acquire_pipeline_permit(&namespace).await?);

        // Isolated work dir for ingestd WAL files
        let work_dir = tempfile::tempdir()?;

        // ── Step 1: Generate self-signed TLS certificates ──────────────
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
        let cert_file = tempfile::NamedTempFile::new()?;
        let key_file = tempfile::NamedTempFile::new()?;
        tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
        tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

        // ── Step 2: Start ingestd ──────────────────────────────────────
        let ingestd_config = TestIngestdConfig {
            nats: nats.connection_config(),
            database_url: ctx.database_url().to_string(),
            work_dir: Some(work_dir.path().to_path_buf()),
            namespace: Some(namespace.clone()),
            consumer_fetch_max_messages: 32,
            consumer_fetch_timeout_ms: 50,
            database_pool_size: 10,
        };
        let ingestd = start_test_ingestd_with_config(ingestd_config, Some(ctx)).await?;

        // ── Step 3: Start gateway ──────────────────────────────────────
        let gateway_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let gateway_config = TestGatewayConfig {
            listen_addr: gateway_addr,
            database_url: ctx.database_url().to_string(),
            nats_url: nats.client_url().to_string(),
            tls_cert: cert_file.path().to_path_buf(),
            tls_key: key_file.path().to_path_buf(),
            rpc_token: Some(rpc_token.clone()),
            rpc_rate_limit_disabled: true,
        };
        let gateway = start_test_gateway(gateway_config).await?;

        info!(
            gateway_addr = %gateway.addr,
            ingestd_stream = %ingestd.stream_name,
            "TestCoreStack ready"
        );

        Ok(Self {
            ctx,
            ingestd: Some(ingestd),
            gateway: Some(gateway),
            pipeline_permit,
            _work_dir: work_dir,
            _cert_file: cert_file,
            _key_file: key_file,
            rpc_token,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // Accessors
    // ════════════════════════════════════════════════════════════════════

    /// The underlying test sandbox.
    #[must_use]
    pub fn ctx(&self) -> &Sandbox {
        self.ctx
    }

    /// Database pool for direct queries.
    #[must_use]
    pub fn pool(&self) -> &DbPool {
        self.ctx.pool()
    }

    /// The gateway's bound address (e.g. `127.0.0.1:34567`).
    #[must_use]
    pub fn gateway_addr(&self) -> SocketAddr {
        self.gateway.as_ref().expect("stack not shut down").addr
    }

    /// Full HTTPS RPC URL for the gateway (e.g. `https://127.0.0.1:34567/rpc`).
    #[must_use]
    pub fn rpc_url(&self) -> String {
        format!("https://{}/rpc", self.gateway_addr())
    }

    /// The base HTTPS URL (without `/rpc` suffix).
    #[must_use]
    pub fn gateway_url(&self) -> String {
        format!("https://{}", self.gateway_addr())
    }

    /// Bearer token for RPC authentication.
    #[must_use]
    pub fn rpc_token(&self) -> &str {
        &self.rpc_token
    }

    /// The NATS client for direct JetStream operations.
    pub fn nats_client(&self) -> async_nats::Client {
        self.ctx.nats_client()
    }

    /// Pipeline namespace for subject/stream name construction.
    #[must_use]
    pub fn namespace(&self) -> &PipelineNamespace {
        self.ctx.pipeline_namespace()
    }

    /// The ingestd's JetStream stream name.
    #[must_use]
    pub fn stream_name(&self) -> &str {
        &self
            .ingestd
            .as_ref()
            .expect("stack not shut down")
            .stream_name
    }

    // ════════════════════════════════════════════════════════════════════
    // Publishing (delegates to PipelineScope-style helpers)
    // ════════════════════════════════════════════════════════════════════

    /// Publish a typed event through NATS and wait for ingestd to persist it.
    pub async fn publish<P: Publishable>(&self, payload: P) -> TestResult<EventId> {
        let source = payload.source();
        let event_type = payload.event_type();
        let json = payload.to_json_value()?;

        // Register source material for FK
        let material_id = Id::<SourceMaterial>::new();
        self.ctx
            .ensure_source_material(material_id, Some(source.as_str()))
            .await?;

        let event = sinex_primitives::events::Event::<serde_json::Value> {
            id: None,
            source,
            event_type,
            payload: json,
            ts_orig: Some(Timestamp::now()),
            host: crate::sandbox::local_test_host(),
            node_run_id: None,
            payload_schema_id: None,
            provenance: sinex_primitives::events::Provenance::Material {
                id: material_id,
                anchor_byte: 0,
                offset_start: None,
                offset_end: None,
                offset_kind: OffsetKind::Byte,
            },
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            node_model: None,
        };

        let event_id: uuid::Uuid = self.ctx.publish_prebuilt_event(&event).await?;
        let event_id = EventId::from_uuid(event_id);
        crate::sandbox::nats::wait_for_event_persisted(self.ctx, event_id).await?;
        Ok(event_id)
    }

    /// Wait for a specific total event count in the database.
    pub async fn wait_for_event_count(&self, count: usize) -> TestResult<usize> {
        crate::sandbox::timing::WaitHelpers::wait_for_event_count(
            self.ctx.pool(),
            count,
            crate::sandbox::timing::DEFAULT_WAIT_SECS,
        )
        .await
    }

    // ════════════════════════════════════════════════════════════════════
    // Material + Ledger Seeding
    // ════════════════════════════════════════════════════════════════════

    /// Create a source material and its temporal ledger row, returning the material ID.
    ///
    /// This is the minimum seeding needed for replay-ready test data: a material
    /// registered in `raw.source_material_registry` with a corresponding
    /// `raw.temporal_ledger` entry that records when/how the material was observed.
    pub async fn seed_material_with_ledger(
        &self,
        source_identifier: &str,
        source_type: &str,
        offset_range: (i64, i64),
    ) -> TestResult<Id<SourceMaterial>> {
        let material_id = self
            .ctx
            .create_source_material(Some(source_identifier))
            .await?;

        // Insert temporal ledger row. Use TRUNCATE-safe direct SQL since the
        // append-only trigger blocks UPDATE/DELETE but allows INSERT.
        sqlx::query(
            r"
            INSERT INTO raw.temporal_ledger
                (source_material_id, offset_start, offset_end, offset_kind,
                 ts_capture, precision, clock, source_type)
            VALUES ($1, $2, $3, 'byte', now(), 'exact', 'monotonic', $4)
            ",
        )
        .bind(material_id.to_uuid())
        .bind(offset_range.0)
        .bind(offset_range.1)
        .bind(source_type)
        .execute(self.ctx.pool())
        .await?;

        Ok(material_id)
    }

    /// Seed a complete replay-ready dataset: material + ledger + N events with provenance.
    ///
    /// Each event's provenance points into the seeded material at sequential byte offsets.
    /// Returns `(material_id, event_ids)`.
    pub async fn seed_material_with_events(
        &self,
        source: &str,
        event_type: &str,
        count: usize,
    ) -> TestResult<(Id<SourceMaterial>, Vec<EventId>)> {
        let total_bytes = (count * 100) as i64; // 100 bytes per event (synthetic)
        let material_id = self
            .seed_material_with_ledger(source, "realtime_capture", (0, total_bytes))
            .await?;

        let mut event_ids = Vec::with_capacity(count);
        for i in 0..count {
            let anchor_byte = (i * 100) as i64;

            let event = sinex_primitives::events::Event::<serde_json::Value> {
                id: None,
                source: EventSource::new(source)?,
                event_type: EventType::new(event_type)?,
                payload: serde_json::json!({ "index": i, "seeded": true }),
                ts_orig: Some(Timestamp::now()),
                host: crate::sandbox::local_test_host(),
                node_run_id: None,
                payload_schema_id: None,
                provenance: sinex_primitives::events::Provenance::Material {
                    id: material_id,
                    anchor_byte,
                    offset_start: Some(anchor_byte),
                    offset_end: Some(anchor_byte + 100),
                    offset_kind: OffsetKind::Byte,
                },
                associated_blob_ids: None,
                temporal_policy: None,
                semantics_version: None,
                scope_key: Some(format!("{source}:{event_type}")),
                equivalence_key: Some(format!("{source}:{event_type}:{i}")),
                created_by_operation_id: None,
                node_model: None,
            };

            let eid: uuid::Uuid = self.ctx.publish_prebuilt_event(&event).await?;
            event_ids.push(EventId::from_uuid(eid));
        }

        // Wait for all events to be persisted
        crate::sandbox::nats::wait_for_event_persisted(self.ctx, *event_ids.last().unwrap())
            .await?;

        // Belt-and-suspenders: verify count
        let actual = crate::sandbox::timing::WaitHelpers::wait_for_source_events(
            self.ctx.pool(),
            source,
            count,
            crate::sandbox::timing::DEFAULT_WAIT_SECS,
        )
        .await?;

        assert_eq!(actual, count, "seeded event count mismatch");

        Ok((material_id, event_ids))
    }

    // ════════════════════════════════════════════════════════════════════
    // Lifecycle
    // ════════════════════════════════════════════════════════════════════

    /// Gracefully shut down all services.
    pub async fn shutdown(mut self) -> TestResult<()> {
        if let Some(mut gw) = self.gateway.take() {
            gw.stop().await?;
        }
        if let Some(mut ingestd) = self.ingestd.take() {
            ingestd.stop().await?;
        }
        self.pipeline_permit.take();
        Ok(())
    }
}

impl Drop for TestCoreStack<'_> {
    fn drop(&mut self) {
        // Release permit first
        self.pipeline_permit.take();

        // Best-effort async cleanup
        let gateway = self.gateway.take();
        let ingestd = self.ingestd.take();

        if gateway.is_some() || ingestd.is_some() {
            if let Ok(handle) = Handle::try_current() {
                handle.spawn(async move {
                    if let Some(mut gw) = gateway {
                        let _ = gw.stop().await;
                    }
                    if let Some(mut ing) = ingestd {
                        let _ = ing.stop().await;
                    }
                });
            } else {
                // Fallback: sync cleanup
                if let Some(mut gw) = gateway {
                    let _ = futures::executor::block_on(gw.stop());
                }
                if let Some(mut ing) = ingestd {
                    let _ = futures::executor::block_on(ing.stop());
                }
            }
        }
    }
}

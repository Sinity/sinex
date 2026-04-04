//! Node runtime scaffold for integration tests.
//!
//! Provides fully wired runtime infrastructure for testing nodes,
//! including NATS connections, checkpoint management, and event emission.

use std::{collections::HashMap, sync::Arc};

use camino::Utf8PathBuf;
use sinex_node_sdk::{
    EventTransport,
    checkpoint::CheckpointManager,
    heartbeat::HeartbeatEmitter,
    nats_publisher::NatsPublisher,
    runtime::stream::{EventEmitter, NodeHandles, NodeRuntimeState, ServiceInfo},
};
use sinex_primitives::{Event, JsonValue, Uuid, constants::buffers::DEFAULT_EVENT_CHANNEL_SIZE};
use tokio::sync::mpsc;

use super::nats::create_or_open_kv_store;
use super::{EphemeralNats, Sandbox};

/// Fully wired runtime scaffold for node integration tests.
pub struct TestRuntime {
    pub runtime: NodeRuntimeState,
    pub event_rx: mpsc::Receiver<Event<JsonValue>>,
    pub nats: Arc<EphemeralNats>,
}

/// Builder for [`TestRuntime`].
pub struct TestRuntimeBuilder<'ctx> {
    ctx: &'ctx Sandbox,
    service_name: String,
    dry_run: bool,
    raw_config: HashMap<String, serde_json::Value>,
}

impl<'ctx> TestRuntimeBuilder<'ctx> {
    pub fn new(ctx: &'ctx Sandbox, service_name: impl Into<String>) -> Self {
        Self {
            ctx,
            service_name: service_name.into(),
            dry_run: false,
            raw_config: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    #[must_use]
    pub fn with_raw_config(mut self, raw_config: HashMap<String, serde_json::Value>) -> Self {
        self.raw_config = raw_config;
        self
    }

    pub async fn build(self) -> color_eyre::Result<TestRuntime> {
        let TestRuntimeBuilder {
            ctx,
            service_name,
            dry_run,
            raw_config,
        } = self;

        let nats_client = ctx.ensure_nats().await?;
        let nats = ctx.nats_handle()?;
        let publisher = Arc::new(NatsPublisher::new(nats_client.clone()));

        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_SIZE);
        let emitter = EventEmitter::new(event_tx, dry_run);

        // Create checkpoint KV store
        let js = async_nats::jetstream::new(nats_client);
        let kv = create_or_open_kv_store(
            &js,
            async_nats::jetstream::kv::Config {
                bucket: format!(
                    "KV_{}",
                    sinex_primitives::environment().nats_kv_bucket_name("sinex_checkpoints")
                ),
                history: 1,
                ..Default::default()
            },
        )
        .await?;

        let checkpoint_manager = Arc::new(CheckpointManager::new(
            kv,
            service_name.clone(),
            "test".to_string(),
            format!(
                "{}-{}",
                service_name,
                Uuid::now_v7().to_string().to_lowercase()
            ),
        ));

        let handles = NodeHandles::new(
            ctx.pool.clone(),
            checkpoint_manager,
            emitter.clone(),
            EventTransport::Nats(publisher),
            None,
            None,
        );

        let work_dir = Utf8PathBuf::from_path_buf(sinex_primitives::environment().temp_dir())
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex-test"));

        let service_info = ServiceInfo::new(
            service_name.clone(),
            service_name,
            crate::sandbox::local_test_host(),
            work_dir.clone().into_std_path_buf(),
            dry_run,
            format!("sandbox-instance-{}", Uuid::now_v7().simple()),
            env!("CARGO_PKG_VERSION").to_string(),
            None,
        );

        let runtime = NodeRuntimeState::new(service_info, handles, raw_config, work_dir);

        // Track the runtime's background pieces for deterministic teardown.
        ctx.register_background_handle("node-runtime", nats.process_handle())
            .await;

        Ok(TestRuntime {
            runtime,
            event_rx,
            nats,
        })
    }
}

impl TestRuntime {
    pub async fn new(ctx: &Sandbox, service_name: impl Into<String>) -> color_eyre::Result<Self> {
        TestRuntimeBuilder::new(ctx, service_name).build().await
    }

    #[must_use]
    pub fn heartbeat(&self, interval: sinex_primitives::Seconds) -> HeartbeatEmitter {
        self.runtime.heartbeat_emitter(interval)
    }

    pub fn acquisition_manager(
        &self,
        rotation_policy: sinex_node_sdk::RotationPolicy,
        source_type: impl Into<String>,
    ) -> sinex_node_sdk::NodeResult<sinex_node_sdk::AcquisitionManager> {
        self.runtime
            .acquisition_manager(rotation_policy, source_type)
    }
}

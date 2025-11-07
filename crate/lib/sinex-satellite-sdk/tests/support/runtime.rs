use std::{collections::HashMap, sync::Arc};

use camino::Utf8PathBuf;
use sinex_core::{types::ulid::Ulid, JsonValue};
use sinex_satellite_sdk::{checkpoint::CheckpointManager, nats_publisher::NatsPublisher};
use sinex_satellite_sdk::{
    event_processor::EventTransport,
    stream_processor::{EventEmitter, ProcessorHandles, ProcessorRuntimeState, ServiceInfo},
};
use sinex_test_utils::{prelude::*, EphemeralNats};
use tokio::sync::mpsc;

pub struct TestRuntime {
    pub runtime: ProcessorRuntimeState,
    pub event_rx: mpsc::UnboundedReceiver<sinex_core::db::models::Event<JsonValue>>,
    pub nats: EphemeralNats,
}

pub struct TestRuntimeBuilder<'ctx> {
    ctx: &'ctx TestContext,
    service_name: String,
    dry_run: bool,
    raw_config: HashMap<String, serde_json::Value>,
}

impl<'ctx> TestRuntimeBuilder<'ctx> {
    pub fn new(ctx: &'ctx TestContext, service_name: impl Into<String>) -> Self {
        Self {
            ctx,
            service_name: service_name.into(),
            dry_run: false,
            raw_config: HashMap::new(),
        }
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

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

        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;
        let publisher = Arc::new(NatsPublisher::new(nats_client));

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let emitter = EventEmitter::new(event_tx, dry_run);

        let checkpoint_manager = Arc::new(CheckpointManager::new(
            ctx.pool.clone(),
            service_name.clone(),
            "test".to_string(),
            format!("{}-{}", service_name, Ulid::new()),
        ));

        let handles = ProcessorHandles::new(
            ctx.pool.clone(),
            checkpoint_manager,
            emitter.clone(),
            EventTransport::Nats(publisher),
            None,
            None,
        );

        let work_dir = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex-test"));

        let service_info = ServiceInfo::new(
            service_name,
            gethostname::gethostname().to_string_lossy().to_string(),
            work_dir.clone().into_std_path_buf(),
            dry_run,
        );

        let runtime = ProcessorRuntimeState::new(service_info, handles, raw_config, work_dir);

        Ok(TestRuntime {
            runtime,
            event_rx,
            nats,
        })
    }
}

impl TestRuntime {
    pub async fn new(
        ctx: &TestContext,
        service_name: impl Into<String>,
    ) -> color_eyre::Result<Self> {
        TestRuntimeBuilder::new(ctx, service_name).build().await
    }

    pub fn heartbeat(&self, interval: u64) -> sinex_satellite_sdk::heartbeat::HeartbeatEmitter {
        self.runtime.heartbeat_emitter(interval)
    }

    pub fn acquisition_manager(
        &self,
        rotation_policy: sinex_satellite_sdk::RotationPolicy,
        source_type: impl Into<String>,
        source_path: impl Into<String>,
    ) -> sinex_satellite_sdk::SatelliteResult<sinex_satellite_sdk::AcquisitionManager> {
        self.runtime
            .acquisition_manager(rotation_policy, source_type, source_path)
    }
}

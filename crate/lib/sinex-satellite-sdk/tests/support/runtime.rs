use std::{collections::HashMap, sync::Arc};

use camino::Utf8PathBuf;
use sinex_core::{types::ulid::Ulid, JsonValue};
use sinex_satellite_sdk::stream_processor::{
    EventEmitter, EventTransport, ProcessorHandles, ProcessorRuntimeState, ServiceInfo,
};
use sinex_satellite_sdk::{checkpoint::CheckpointManager, nats_publisher::NatsPublisher};
use sinex_test_utils::{prelude::*, EphemeralNats};
use tokio::sync::mpsc;

pub struct TestRuntime {
    pub runtime: ProcessorRuntimeState,
    pub event_rx: mpsc::UnboundedReceiver<sinex_core::db::models::Event<JsonValue>>,
    pub nats: EphemeralNats,
}

impl TestRuntime {
    pub async fn new(ctx: &TestContext, service_name: impl Into<String>) -> color_eyre::Result<Self> {
        let service_name = service_name.into();

        let nats = EphemeralNats::start().await?;
        let nats_client = nats.connect().await?;
        let publisher = Arc::new(NatsPublisher::new(nats_client));

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let emitter = EventEmitter::new(event_tx, false);

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
            false,
        );

        let runtime = ProcessorRuntimeState::new(
            service_info,
            handles,
            HashMap::new(),
            work_dir,
        );

        Ok(Self { runtime, event_rx, nats })
    }
}

#![cfg(feature = "messaging")]

use async_trait::async_trait;
use sinex_node_sdk::confirmation_handler::{
    ConfirmedEventHandler, ProvisionalEvent, ProvisionalEventHandler,
};
use sinex_node_sdk::{
    JetStreamEventConsumer, JetStreamEventConsumerConfig, NodeResult, ProcessingModel, SinexError,
};
use std::sync::Arc;
use std::time::Duration;
use xtask::sandbox::prelude::*;

struct NoopHandler;

#[async_trait]
impl ProvisionalEventHandler for NoopHandler {
    async fn handle_provisional(&self, _event: &ProvisionalEvent) -> NodeResult<()> {
        Ok(())
    }

    async fn rollback_provisional(
        &self,
        _event_id: sinex_primitives::ids::Id<sinex_primitives::events::Event>,
    ) -> NodeResult<()> {
        Ok(())
    }
}

#[async_trait]
impl ConfirmedEventHandler for NoopHandler {
    async fn handle_confirmed(&self, _event: &ProvisionalEvent) -> NodeResult<()> {
        Ok(())
    }
}

#[sinex_test]
async fn test_consumer_config_defaults() -> TestResult<()> {
    let config = JetStreamEventConsumerConfig::default();
    assert_eq!(config.processing_model, ProcessingModel::StatelessWorker);
    assert_eq!(config.batch_size, 100);
    assert!(!config.enable_provisional_processing);
    Ok(())
}

#[sinex_test]
async fn running_flag_clears_after_startup_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let client = ctx.nats_client();
    let env = ctx.env().clone();
    let handler = Arc::new(NoopHandler);
    let consumer = JetStreamEventConsumer::new(
        client,
        env,
        JetStreamEventConsumerConfig::default(),
        handler,
        None,
    );

    let first = tokio::time::timeout(Duration::from_secs(5), consumer.run()).await?;
    assert!(first.is_err());

    let second = tokio::time::timeout(Duration::from_secs(5), consumer.run()).await?;
    if let Err(SinexError::Lifecycle(details)) = second {
        assert_ne!(details.message(), "Consumer already running");
    }

    Ok(())
}

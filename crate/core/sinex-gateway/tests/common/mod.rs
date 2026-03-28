#![allow(dead_code)]

use async_nats::{Client, jetstream};
use futures::StreamExt;
use sinex_gateway::{auth::Role, rpc_server::RpcAuthContext};
use sinex_gateway::{config::GatewayConfig, service_container::ServiceContainer};
use sinex_primitives::{environment, environment::SinexEnvironment, temporal};
use std::sync::Arc;
use xtask::sandbox::prelude::*;

pub struct NatsHarness {
    _ctx: TestContext,
    pub client: Client,
    pub env: SinexEnvironment,
    pub services: ServiceContainer,
}

impl NatsHarness {
    pub async fn start(ctx: TestContext) -> TestResult<Self> {
        let ctx = ctx.with_nats().dedicated().await?;
        let client = ctx.nats_client();
        let mut config = GatewayConfig::load();
        config.database_url = ctx.database_url().to_string();
        config.nats.url = ctx.nats_url().ok_or_else(|| {
            color_eyre::eyre::eyre!("dedicated NATS test context must expose a NATS URL")
        })?;
        let services = ServiceContainer::new(&config).await?;
        Ok(Self {
            _ctx: ctx,
            client,
            env: environment(),
            services,
        })
    }

    pub fn nats_handle(&self) -> TestResult<Arc<xtask::sandbox::EphemeralNats>> {
        self._ctx.nats_handle()
    }
}

pub fn admin_auth() -> RpcAuthContext {
    RpcAuthContext {
        token_prefix: "test****".to_string(),
        actor_id: "token:test****".to_string(),
        authenticated_at: temporal::now(),
        role: Role::Admin,
    }
}

pub async fn ensure_events_stream(
    client: &Client,
    env: &SinexEnvironment,
) -> TestResult<jetstream::stream::Stream> {
    let js = jetstream::new(client.clone());
    let stream_name = env.nats_stream_name("EVENTS");
    let events_subject = env.nats_subject("events.>");
    let mut streams = js.streams();
    while let Some(stream) = streams.next().await {
        let stream = stream?;
        if stream
            .config
            .subjects
            .iter()
            .any(|subject| subject == &events_subject)
        {
            return js.get_stream(&stream.config.name).await.map_err(Into::into);
        }
    }
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![events_subject],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 10_000,
            storage: jetstream::stream::StorageType::File,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

pub async fn ensure_dlq_stream(
    client: &Client,
    env: &SinexEnvironment,
    storage: jetstream::stream::StorageType,
) -> TestResult<jetstream::stream::Stream> {
    let js = jetstream::new(client.clone());
    let stream_name = env.nats_stream_name("EVENTS_DLQ");
    let dlq_subject = env.nats_subject("events.dlq.>");
    let mut streams = js.streams();
    while let Some(stream) = streams.next().await {
        let stream = stream?;
        if stream
            .config
            .subjects
            .iter()
            .any(|subject| subject == &dlq_subject)
        {
            return js.get_stream(&stream.config.name).await.map_err(Into::into);
        }
    }
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![dlq_subject],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 1000,
            storage,
            allow_direct: true,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

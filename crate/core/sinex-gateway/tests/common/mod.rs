use async_nats::{Client, jetstream};
use sinex_gateway::{auth::Role, rpc_server::RpcAuthContext};
use sinex_primitives::{environment, environment::SinexEnvironment, temporal};
use xtask::sandbox::{nats::EphemeralNats, prelude::*};

pub struct NatsHarness {
    _nats: EphemeralNats,
    pub client: Client,
    pub env: SinexEnvironment,
}

impl NatsHarness {
    pub async fn start() -> TestResult<Self> {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        Ok(Self {
            _nats: nats,
            client,
            env: environment(),
        })
    }
}

pub fn admin_auth() -> RpcAuthContext {
    RpcAuthContext {
        token_prefix: "test****".to_string(),
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
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![env.nats_subject("events.>")],
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
    let stream = js
        .get_or_create_stream(jetstream::stream::Config {
            name: stream_name,
            subjects: vec![env.nats_subject("events.dlq.>")],
            retention: jetstream::stream::RetentionPolicy::Limits,
            max_messages: 1000,
            storage,
            ..Default::default()
        })
        .await?;
    Ok(stream)
}

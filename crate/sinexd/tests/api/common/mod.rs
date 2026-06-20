#![allow(dead_code)]

use async_nats::{Client, jetstream};
use color_eyre::eyre::bail;
use futures::StreamExt;
use serde_json::{Value, json};
use sinex_db::{DbPool, repositories::DbPoolExt};
use sinex_primitives::{
    DynamicPayload, Id, environment, environment::SinexEnvironment, temporal,
    rpc::methods,
};
use sinexd::api::{auth::Role, rpc_server::RpcAuthContext};
use sinexd::api::{config::GatewayConfig, rpc_server, service_container::ServiceContainer};
use sinexd::runtime::{
    Checkpoint, ScanReport, SourceScanAck, SourceScanCommand, SourceScanProgress,
};
use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;
use tempfile::{NamedTempFile, TempDir};
use tokio::sync::watch;
use xtask::sandbox::{EnvGuard, prelude::*};

pub struct NatsHarness {
    _ctx: TestContext,
    _content_store_dir: TempDir,
    pub client: Client,
    pub env: SinexEnvironment,
    pub services: ServiceContainer,
}

impl NatsHarness {
    pub async fn start(ctx: TestContext) -> TestResult<Self> {
        let ctx = ctx.with_nats().dedicated().await?;
        let client = ctx.nats_client();
        let mut config = GatewayConfig::load()?;
        config.database_url = ctx.database_url().to_string();
        config.nats.url = ctx.nats_url().ok_or_else(|| {
            color_eyre::eyre::eyre!("dedicated NATS test context must expose a NATS URL")
        })?;
        let content_store_dir = TempDir::new()?;
        config.content_store_path = content_store_dir
            .path()
            .join("content-store")
            .to_string_lossy()
            .into_owned();
        let services = ServiceContainer::new(&config).await?;
        Ok(Self {
            _ctx: ctx,
            _content_store_dir: content_store_dir,
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
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS_DLQ");
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

pub struct FakeReplayScanSource {
    pub module_name: &'static str,
    pub source: &'static str,
    pub event_type: &'static str,
    pub material_id: Option<uuid::Uuid>,
    pub events_processed: u64,
}

impl FakeReplayScanSource {
    pub fn with_material(
        module_name: &'static str,
        source: &'static str,
        event_type: &'static str,
        material_id: uuid::Uuid,
        events_processed: u64,
    ) -> Self {
        Self {
            module_name,
            source,
            event_type,
            material_id: Some(material_id),
            events_processed,
        }
    }

    pub fn from_replay_command(
        module_name: &'static str,
        source: &'static str,
        event_type: &'static str,
        events_processed: u64,
    ) -> Self {
        Self {
            module_name,
            source,
            event_type,
            material_id: None,
            events_processed,
        }
    }
}

pub async fn spawn_fake_replay_scan_source(
    pool: DbPool,
    nats: Client,
    env: SinexEnvironment,
    spec: FakeReplayScanSource,
) -> TestResult<tokio::task::JoinHandle<()>> {
    let module_name = spec.module_name.to_string();
    let source = spec.source;
    let event_type = spec.event_type;
    let material_id = spec.material_id;
    let events_processed = spec.events_processed;
    let subject = env.nats_subject(&format!("sinex.control.sources.{module_name}.scan"));
    let mut sub = nats.subscribe(subject).await?;
    nats.flush().await?;

    let handle = tokio::spawn(async move {
        let Some(msg) = sub.next().await else { return };
        let Ok(command) = serde_json::from_slice::<SourceScanCommand>(&msg.payload) else {
            return;
        };
        let operation_id = command.operation_id;
        let progress_subject =
            env.nats_subject(&format!("sinex.control.replay.progress.{operation_id}"));

        if let Some(reply) = msg.reply {
            let ack = SourceScanAck {
                operation_id,
                module_name: module_name.clone(),
                accepted: true,
                error: None,
            };
            if let Ok(bytes) = serde_json::to_vec(&ack) {
                let _ = nats.publish(reply, bytes.into()).await;
            }
        }

        let material_id = material_id.or_else(|| {
            command
                .args
                .replay
                .as_ref()
                .and_then(|replay| replay.materials.first())
                .map(|material| material.source_material_id)
        });

        let Some(material_id) = material_id else {
            return;
        };

        for i in 0..events_processed {
            let Ok(event) = DynamicPayload::new(
                source,
                event_type,
                json!({ "path": format!("/tmp/{module_name}-replay-{operation_id}-{i}.txt") }),
            )
            .from_material(Id::from_uuid(material_id))
            .build() else {
                return;
            };

            let mut event = event;
            event.created_by_operation_id = Some(operation_id);

            if pool.events().insert(event).await.is_err() {
                return;
            }
        }

        let progress = SourceScanProgress {
            operation_id,
            module_name: module_name.clone(),
            events_processed,
            events_emitted: events_processed,
            final_report: Some(ScanReport {
                events_processed,
                duration: Duration::from_millis(5),
                final_checkpoint: Checkpoint::None,
                time_range: None,
                runtime_stats: HashMap::from([("events_emitted".into(), events_processed)]),
                successful_targets: vec![module_name.clone()],
                failed_targets: Vec::new(),
                warnings: Vec::new(),
            }),
            error: None,
        };
        if let Ok(bytes) = serde_json::to_vec(&progress) {
            let _ = nats.publish(progress_subject, bytes.into()).await;
        }
    });

    Ok(handle)
}

/// In-process gateway with full RPC server (TLS) and a `reqwest`-based client.
///
/// Several `replay_*_test.rs` files need this exact setup: self-signed certs,
/// rate-limit disabled, dynamic port, watch-channel shutdown. Lift it here so
/// tests focus on the scenario they assert.
pub struct LiveGateway {
    port: u16,
    token: String,
    _shutdown_tx: watch::Sender<bool>,
    _handle: tokio::task::JoinHandle<()>,
    _cert_file: NamedTempFile,
    _key_file: NamedTempFile,
    client: reqwest::Client,
}

impl LiveGateway {
    /// Start a gateway authenticated with the given token.
    pub async fn start(
        database_url: &str,
        token: &str,
        env_guard: &mut EnvGuard,
    ) -> TestResult<Self> {
        Self::start_with(database_url, token, None, env_guard).await
    }

    /// Start a gateway, optionally pinning the NATS URL (for tests that use a
    /// dedicated bus) and using a non-admin role token.
    pub async fn start_with(
        database_url: &str,
        token: &str,
        nats_url: Option<&str>,
        env_guard: &mut EnvGuard,
    ) -> TestResult<Self> {
        let cert =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
        let cert_file = NamedTempFile::new()?;
        let key_file = NamedTempFile::new()?;
        tokio::fs::write(cert_file.path(), cert.cert.pem()).await?;
        tokio::fs::write(key_file.path(), cert.key_pair.serialize_pem()).await?;

        env_guard.set(
            "SINEX_API_TLS_CERT",
            cert_file.path().to_string_lossy().to_string(),
        );
        env_guard.set(
            "SINEX_API_TLS_KEY",
            key_file.path().to_string_lossy().to_string(),
        );
        env_guard.clear("SINEX_API_TLS_CLIENT_CA");
        env_guard.set("SINEX_API_TOKEN", token);
        env_guard.set("DATABASE_URL", database_url);
        if let Some(nats_url) = nats_url {
            env_guard.set("SINEX_NATS_URL", nats_url);
        }

        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        drop(listener);

        let mut config = GatewayConfig::load()?;
        config.database_url = database_url.to_string();
        config.tcp_listen = format!("127.0.0.1:{port}");
        config.rpc_rate_limit_enabled = false;
        let services = ServiceContainer::new(&config).await?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            if let Err(e) = rpc_server::run(&config, services, shutdown_rx).await {
                eprintln!("Gateway RPC server failed: {e:#}");
            }
        });

        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
                Ok(_) => break,
                Err(_) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => bail!("Gateway port {port} not ready: {e}"),
            }
        }

        let client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()?;

        Ok(Self {
            port,
            token: token.to_string(),
            _shutdown_tx: shutdown_tx,
            _handle: handle,
            _cert_file: cert_file,
            _key_file: key_file,
            client,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn rpc_url(&self) -> String {
        format!("https://127.0.0.1:{}/rpc", self.port)
    }

    /// Authenticated JSON-RPC 2.0 call, returning the `result` field.
    /// Errors in the JSON-RPC envelope surface via `bail!`.
    pub async fn rpc(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> TestResult<serde_json::Value> {
        let response = self.rpc_envelope(method, params).await?;
        if let Some(error) = response.get("error") {
            bail!("JSON-RPC error on {method}: {error}");
        }
        response
            .get("result")
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("No 'result' field in JSON-RPC response"))
    }

    /// Authenticated JSON-RPC 2.0 call returning the full envelope (id /
    /// result / error / jsonrpc). Used by tests that probe error paths.
    pub async fn rpc_envelope(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> TestResult<serde_json::Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        let resp = self
            .client
            .post(self.rpc_url())
            .header("Authorization", format!("Bearer {}", self.token))
            .json(&body)
            .send()
            .await?;

        Ok(resp.json().await?)
    }

    /// Unauthenticated JSON-RPC 2.0 call, returning the raw response.
    pub async fn rpc_unauthed(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> TestResult<reqwest::Response> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        Ok(self.client.post(self.rpc_url()).json(&body).send().await?)
    }

    pub fn replay_operation_id(response: &Value) -> TestResult<String> {
        response["operation"]["operation_id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| color_eyre::eyre::eyre!("operation_id missing from replay response"))
    }

    pub async fn create_replay_operation(&self, params: Value) -> TestResult<Value> {
        self.rpc(methods::REPLAY_CREATE_OPERATION, params).await
    }

    pub async fn preview_replay_operation(&self, operation_id: &str) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_PREVIEW_OPERATION,
            json!({ "operation_id": operation_id }),
        )
        .await
    }

    pub async fn approve_replay_operation(
        &self,
        operation_id: &str,
        approver: &str,
    ) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_APPROVE_OPERATION,
            json!({ "operation_id": operation_id, "approver": approver }),
        )
        .await
    }

    pub async fn execute_replay_operation(
        &self,
        operation_id: &str,
        executor: &str,
    ) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_EXECUTE_OPERATION,
            json!({ "operation_id": operation_id, "executor": executor }),
        )
        .await
    }

    pub async fn submit_replay_operation(&self, operation_id: &str) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_SUBMIT_OPERATION,
            json!({ "operation_id": operation_id }),
        )
        .await
    }

    pub async fn cancel_replay_operation(
        &self,
        operation_id: &str,
        reason: &str,
    ) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_CANCEL_OPERATION,
            json!({ "operation_id": operation_id, "reason": reason }),
        )
        .await
    }

    pub async fn replay_operation_status(&self, operation_id: &str) -> TestResult<Value> {
        self.rpc(
            methods::REPLAY_OPERATION_STATUS,
            json!({ "operation_id": operation_id }),
        )
        .await
    }

    pub async fn list_replay_operations(&self) -> TestResult<Value> {
        self.rpc(methods::REPLAY_LIST_OPERATIONS, json!({})).await
    }

    pub async fn wait_for_replay_completed(
        &self,
        operation_id: &str,
        attempts: usize,
        interval: Duration,
    ) -> TestResult<Value> {
        let mut last_status = json!(null);
        for _ in 0..attempts {
            last_status = self.replay_operation_status(operation_id).await?;
            match last_status["operation"]["state"].as_str() {
                Some("Completed") => return Ok(last_status),
                Some("Failed") => bail!("Replay operation {operation_id} failed: {last_status}"),
                _ => tokio::time::sleep(interval).await,
            }
        }
        bail!("Replay operation {operation_id} did not complete in time: {last_status}")
    }
}

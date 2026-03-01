use std::{collections::VecDeque, sync::Arc};

use color_eyre::Result;
use serde_json::json;
use sinex_gateway::{
    native_messaging::{
        run_with_transport, NativeMessage, NativeMessagingConfig, NativeMessagingTransport,
        NativeResponse,
    },
    ServiceContainer,
};
use tokio::sync::Mutex;
use xtask::sandbox::{sinex_test, EnvGuard};

#[derive(Clone, Default)]
struct HarnessTransport {
    state: Arc<Mutex<TransportState>>,
}

#[derive(Default)]
struct TransportState {
    inbox: VecDeque<NativeMessage>,
    outbox: Vec<NativeResponse>,
}

fn set_default_capabilities(env_guard: &mut EnvGuard) {
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_CAPABILITIES",
        r#"{"chrome-extension://trusted-sinex":{"allowed_methods":["system.health"],"rate_limit_per_minute":null,"allowed_event_types":null}}"#,
    );
}

impl HarnessTransport {
    fn new(messages: Vec<NativeMessage>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TransportState {
                inbox: VecDeque::from(messages),
                outbox: Vec::new(),
            })),
        }
    }

    async fn responses(&self) -> Vec<NativeResponse> {
        let state = self.state.lock().await;
        state.outbox.clone()
    }
}

impl NativeMessagingTransport for HarnessTransport {
    async fn read_message(&mut self) -> Result<Option<NativeMessage>> {
        let mut state = self.state.lock().await;
        Ok(state.inbox.pop_front())
    }

    async fn write_message(&mut self, response: &NativeResponse) -> Result<()> {
        let mut state = self.state.lock().await;
        state.outbox.push(response.clone());
        Ok(())
    }
}

#[sinex_test]
async fn native_messaging_rejects_untrusted_extensions(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_NATS_URL", &ctx.nats_url().unwrap());
    env.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    set_default_capabilities(&mut env);
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let malicious_request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "system.health",
        "params": {},
        "id": "1",
        "extension_id": "chrome-extension://malicious",
    }))?;

    let transport = HarnessTransport::new(vec![malicious_request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(
        !responses.is_empty(),
        "native messaging should have responded to RPC request"
    );

    let first = &responses[0];
    let response_value = serde_json::to_value(first)?;
    let response_type = response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    assert_eq!(
        response_type,
        "error",
        "native messaging should reject RPC calls from extension IDs that are not in the trusted allow-list"
    );

    Ok(())
}

#[sinex_test]
async fn native_messaging_accepts_trusted_extension_with_secret(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_NATS_URL", &ctx.nats_url().unwrap());
    env.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex#s3cr3t",
    );
    set_default_capabilities(&mut env);
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "system.health",
        "params": {},
        "id": "1",
        "extension_id": "chrome-extension://trusted-sinex",
        "extension_secret": "s3cr3t",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(!responses.is_empty());
    let response_value = serde_json::to_value(&responses[0])?;
    let response_type = response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    assert_eq!(response_type, "response");

    Ok(())
}

#[sinex_test]
async fn native_messaging_rejects_missing_secret(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_NATS_URL", &ctx.nats_url().unwrap());
    env.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex#s3cr3t",
    );
    set_default_capabilities(&mut env);
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "system.health",
        "params": {},
        "id": "1",
        "extension_id": "chrome-extension://trusted-sinex",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(!responses.is_empty());
    let response_value = serde_json::to_value(&responses[0])?;
    let response_type = response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    assert_eq!(response_type, "error");
    Ok(())
}

#[sinex_test]
async fn native_messaging_rejects_untrusted_host(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_NATS_URL", &ctx.nats_url().unwrap());
    env.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    set_default_capabilities(&mut env);
    env.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "ping",
        "id": "host-check",
        "extension_id": "chrome-extension://trusted-sinex",
        "host": "malicious-host",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(!responses.is_empty());
    let response_value = serde_json::to_value(&responses[0])?;
    let response_type = response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    assert_eq!(response_type, "error");
    Ok(())
}

#[sinex_test]
async fn native_messaging_accepts_trusted_host_and_protocol(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let mut env = EnvGuard::new();
    env.set("SINEX_REPLAY_CONTROL_OPTIONAL", "1");
    env.set("SINEX_NATS_URL", &ctx.nats_url().unwrap());
    env.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    set_default_capabilities(&mut env);
    env.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
    env.set("SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION", "1");
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "ping",
        "id": "host-ok",
        "extension_id": "chrome-extension://trusted-sinex",
        "host": "sinex-host",
        "protocol_version": "1",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(!responses.is_empty());
    let response_value = serde_json::to_value(&responses[0])?;
    let response_type = response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    assert_eq!(response_type, "response");
    Ok(())
}

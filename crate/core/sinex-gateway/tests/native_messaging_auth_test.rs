use std::{collections::VecDeque, env, sync::Arc};

use async_trait::async_trait;
use color_eyre::Result;
use serde_json::json;
use sinex_gateway::{
    native_messaging::{
        run_with_transport, NativeMessage, NativeMessagingConfig, NativeMessagingTransport,
        NativeResponse,
    },
    ServiceContainer,
};
use sinex_test_utils::{sinex_test, EnvGuard, TestContext};
use tokio::sync::Mutex;

struct ReplayBypassGuard {
    previous: Option<String>,
}

impl ReplayBypassGuard {
    fn enable() -> Self {
        let previous = env::var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS").ok();
        env::set_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", "1");
        Self { previous }
    }
}

impl Drop for ReplayBypassGuard {
    fn drop(&mut self) {
        if let Some(ref value) = self.previous {
            env::set_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS", value);
        } else {
            env::remove_var("SINEX_ALLOW_REPLAY_CONTROL_BYPASS");
        }
    }
}

#[derive(Clone, Default)]
struct HarnessTransport {
    state: Arc<Mutex<TransportState>>,
}

#[derive(Default)]
struct TransportState {
    inbox: VecDeque<NativeMessage>,
    outbox: Vec<NativeResponse>,
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

#[async_trait]
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
    let _bypass = ReplayBypassGuard::enable();
    let mut env_guard = EnvGuard::new();
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let malicious_request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "analytics.event_count_by_source",
        "params": { "days_back": 1 },
        "id": "1",
        "extension_id": "chrome-extension://malicious",
    }))?;

    let transport = HarnessTransport::new(vec![malicious_request]);
    let probe = transport.clone();

    run_with_transport(services, config, transport).await?;

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
    let _bypass = ReplayBypassGuard::enable();
    let mut env_guard = EnvGuard::new();
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex#s3cr3t",
    );
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "analytics.event_count_by_source",
        "params": { "days_back": 1 },
        "id": "1",
        "extension_id": "chrome-extension://trusted-sinex",
        "extension_secret": "s3cr3t",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    run_with_transport(services, config, transport).await?;

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
    let _bypass = ReplayBypassGuard::enable();
    let mut env_guard = EnvGuard::new();
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex#s3cr3t",
    );
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::new(Some(db_url)).await?;

    let config = NativeMessagingConfig::from_env();

    let request: NativeMessage = serde_json::from_value(json!({
        "type": "rpc",
        "method": "analytics.event_count_by_source",
        "params": { "days_back": 1 },
        "id": "1",
        "extension_id": "chrome-extension://trusted-sinex",
    }))?;

    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    run_with_transport(services, config, transport).await?;

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
    let _bypass = ReplayBypassGuard::enable();
    let mut env_guard = EnvGuard::new();
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    env_guard.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
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

    run_with_transport(services, config, transport).await?;

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
    let _bypass = ReplayBypassGuard::enable();
    let mut env_guard = EnvGuard::new();
    env_guard.set(
        "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
        "chrome-extension://trusted-sinex",
    );
    env_guard.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
    env_guard.set("SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION", "1");
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

    run_with_transport(services, config, transport).await?;

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

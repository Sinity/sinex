use std::{collections::VecDeque, sync::Arc};

use serde_json::json;
use sinexd::api::{
    ServiceContainer,
    native_messaging::{
        NativeMessage, NativeMessagingConfig, NativeMessagingTransport, NativeResponse,
        run_with_transport,
    },
};
use sinex_primitives::Result;
use tokio::sync::Mutex;
use xtask::sandbox::{EnvGuard, sinex_test};

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
        r#"{"chrome-extension://trusted-sinex":{"allowed_methods":["system.health"],"rate_limit_per_minute":null}}"#,
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

fn response_error_message(response: &NativeResponse) -> Result<String> {
    let response_value = serde_json::to_value(response)?;
    Ok(response_value
        .get("error")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .to_string())
}

fn response_type(response: &NativeResponse) -> Result<String> {
    let response_value = serde_json::to_value(response)?;
    Ok(response_value
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_string())
}

async fn run_native_case(
    services: ServiceContainer,
    nats_url: &str,
    configure_env: impl FnOnce(&mut EnvGuard),
    request: NativeMessage,
) -> Result<NativeResponse> {
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_URL", nats_url);
    configure_env(&mut env);

    let config = NativeMessagingConfig::from_env()?;
    let transport = HarnessTransport::new(vec![request]);
    let probe = transport.clone();

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    run_with_transport(services, config, transport, shutdown_rx).await?;

    let responses = probe.responses().await;
    assert!(
        !responses.is_empty(),
        "native messaging should respond to harness request"
    );
    Ok(responses[0].clone())
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
async fn native_messaging_auth_and_config_matrix(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx.nats_url().unwrap().clone();
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::from_database_url(db_url).await?;

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex",
            );
            set_default_capabilities(env);
        },
        serde_json::from_value(json!({
            "type": "rpc",
            "method": "system.health",
            "params": {},
            "id": "1",
            "extension_id": "chrome-extension://malicious",
        }))?,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "error",
        "native messaging should reject RPC calls from extension IDs that are not in the trusted allow-list"
    );

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex#s3cr3t",
            );
            set_default_capabilities(env);
        },
        serde_json::from_value(json!({
            "type": "rpc",
            "method": "system.health",
            "params": {},
            "id": "1",
            "extension_id": "chrome-extension://trusted-sinex",
            "extension_secret": "s3cr3t",
        }))?,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "response",
        "native messaging should accept trusted extension with matching secret"
    );

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex#s3cr3t",
            );
            set_default_capabilities(env);
        },
        serde_json::from_value(json!({
            "type": "rpc",
            "method": "system.health",
            "params": {},
            "id": "1",
            "extension_id": "chrome-extension://trusted-sinex",
        }))?,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "error",
        "native messaging should reject trusted extension requests that omit the required secret"
    );

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex",
            );
            set_default_capabilities(env);
            env.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
        },
        serde_json::from_value(json!({
            "type": "ping",
            "id": "host-check",
            "extension_id": "chrome-extension://trusted-sinex",
            "host": "malicious-host",
        }))?,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "error",
        "native messaging should reject untrusted host names"
    );

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex",
            );
            set_default_capabilities(env);
            env.set("SINEX_NATIVE_MESSAGING_TRUSTED_HOSTS", "sinex-host");
            env.set("SINEX_NATIVE_MESSAGING_PROTOCOL_VERSION", "1");
        },
        serde_json::from_value(json!({
            "type": "ping",
            "id": "host-ok",
            "extension_id": "chrome-extension://trusted-sinex",
            "host": "sinex-host",
            "protocol_version": "1",
        }))?,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "response",
        "native messaging should accept trusted host and protocol"
    );

    let response = run_native_case(
        services.clone(),
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex",
            );
            env.set(
                "SINEX_NATIVE_MESSAGING_CAPABILITIES",
                r#"{"chrome-extension://trusted-sinex":{"allowed_methods":"system.health","rate_limit_per_minute":null}}"#,
            );
        },
        serde_json::from_value(json!({
        "type": "rpc",
        "method": "system.health",
        "params": {},
        "id": "capabilities-invalid",
        "extension_id": "chrome-extension://trusted-sinex",
    }))?,
    )
    .await?;
    assert!(
        response_error_message(&response)?.contains("SINEX_NATIVE_MESSAGING_CAPABILITIES"),
        "native messaging should surface invalid capabilities config"
    );

    let response = run_native_case(
        services,
        &nats_url,
        |env| {
            env.set(
                "SINEX_NATIVE_MESSAGING_TRUSTED_EXTENSIONS",
                "chrome-extension://trusted-sinex",
            );
            set_default_capabilities(env);
            env.set(
                "SINEX_NATIVE_MESSAGING_EXTENSION_ROLES",
                r#"{"chrome-extension://trusted-sinex":"superuser"}"#,
            );
        },
        serde_json::from_value(json!({
            "type": "rpc",
            "method": "system.health",
            "params": {},
            "id": "roles-invalid",
            "extension_id": "chrome-extension://trusted-sinex",
        }))?,
    )
    .await?;
    assert!(
        response_error_message(&response)?.contains("SINEX_NATIVE_MESSAGING_EXTENSION_ROLES"),
        "native messaging should surface invalid extension-role config"
    );

    Ok(())
}

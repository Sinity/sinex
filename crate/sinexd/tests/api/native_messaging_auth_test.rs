use std::{collections::VecDeque, sync::Arc};

use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::Result;
use sinex_primitives::event_contracts::{
    BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID, BROWSER_TAB_ACTIVATED_CONTRACT_ID,
};
use sinex_primitives::events::Provenance;
use sinex_primitives::rpc::methods;
use sinexd::api::{
    ServiceContainer,
    native_messaging::{
        NativeMessage, NativeMessagingConfig, NativeMessagingTransport, NativeResponse,
        run_with_transport,
    },
};
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

fn response_result(response: &NativeResponse) -> Result<serde_json::Value> {
    let response_value = serde_json::to_value(response)?;
    Ok(response_value
        .get("result")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
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
        services.clone(),
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

#[sinex_test]
async fn browser_capture_batch_is_capability_gated_and_acknowledged(
    ctx: TestContext,
) -> Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_url = ctx.nats_url().unwrap().clone();
    let db_url = ctx.database_url().to_string();
    let services = ServiceContainer::from_database_url(db_url).await?;

    let request = serde_json::from_value(json!({
        "type": "rpc",
        "method": methods::BROWSER_CAPTURE_BATCH,
        "params": {
            "profile_id": "qutebrowser:default",
            "producer_instance_id": "native-host:test",
            "batch_id": "browser-batch-1",
            "sequence_start": 41,
            "observations": [
                {
                    "kind": "navigation",
                    "observed_at": "2026-06-20T01:00:00Z",
                    "url": "https://example.test/a",
                    "title": "Example",
                    "tab_id": 7,
                    "window_id": 1,
                    "transition": "link"
                },
                {
                    "kind": "tab_activated",
                    "observed_at": "2026-06-20T01:00:01Z",
                    "tab_id": 7,
                    "window_id": 1,
                    "url": "https://example.test/a"
                }
            ]
        },
        "id": "browser-cap-denied",
        "extension_id": "chrome-extension://trusted-sinex",
    }))?;

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
        request,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "error",
        "browser capture must not bypass native capability allowlists"
    );
    assert!(
        response_error_message(&response)?.contains(methods::BROWSER_CAPTURE_BATCH),
        "capability denial should name the disallowed browser capture method"
    );

    let request = serde_json::from_value(json!({
        "type": "rpc",
        "method": methods::BROWSER_CAPTURE_BATCH,
        "params": {
            "profile_id": "qutebrowser:default",
            "producer_instance_id": "native-host:test",
            "batch_id": "browser-batch-1",
            "sequence_start": 41,
            "observations": [
                {
                    "kind": "navigation",
                    "observed_at": "2026-06-20T01:00:00Z",
                    "url": "https://example.test/a",
                    "title": "Example",
                    "tab_id": 7,
                    "window_id": 1,
                    "transition": "link"
                },
                {
                    "kind": "tab_activated",
                    "observed_at": "2026-06-20T01:00:01Z",
                    "tab_id": 7,
                    "window_id": 1,
                    "url": "https://example.test/a"
                }
            ]
        },
        "id": "browser-cap-ok",
        "extension_id": "chrome-extension://trusted-sinex",
    }))?;

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
                r#"{"chrome-extension://trusted-sinex":{"allowed_methods":["browser.capture_batch"],"rate_limit_per_minute":null}}"#,
            );
            env.set(
                "SINEX_NATIVE_MESSAGING_EXTENSION_ROLES",
                r#"{"chrome-extension://trusted-sinex":"write"}"#,
            );
        },
        request,
    )
    .await?;
    assert_eq!(
        response_type(&response)?,
        "response",
        "browser capture should accept trusted extension with method capability and write role"
    );

    let result = response_result(&response)?;
    assert_eq!(result["accepted_count"], 2);
    assert_eq!(result["first_sequence"], 41);
    assert_eq!(result["last_accepted_sequence"], 42);
    assert_eq!(
        result["actor_id"],
        "extension:chrome-extension://trusted-sinex"
    );
    assert_eq!(
        result["material_id"].as_str().is_some(),
        true,
        "browser capture response should name the source material batch"
    );
    let event_ids = result["event_ids"]
        .as_array()
        .expect("event_ids should be an array")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    assert_eq!(
        event_ids.len(),
        2,
        "browser capture should persist one event per observation"
    );
    let contracts = result["event_contract_ids"]
        .as_array()
        .expect("event_contract_ids should be an array")
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    assert!(contracts.contains(&BROWSER_NAVIGATION_OBSERVED_CONTRACT_ID));
    assert!(contracts.contains(&BROWSER_TAB_ACTIVATED_CONTRACT_ID));

    let events = services
        .pool()
        .events()
        .get_by_source(
            &sinex_primitives::domain::EventSource::from_static("browser"),
            sinex_primitives::Pagination::new(Some(10), Some(0)),
        )
        .await?;
    let persisted_batch_events = events
        .iter()
        .filter(|event| {
            event
                .payload
                .get("batch_id")
                .and_then(serde_json::Value::as_str)
                == Some("browser-batch-1")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        persisted_batch_events.len(),
        2,
        "native browser capture should create queryable browser events"
    );
    assert!(
        persisted_batch_events
            .iter()
            .all(|event| matches!(&event.provenance, Provenance::Material { id, .. } if id.to_string() == result["material_id"])),
        "browser events should be backed by the native-message batch material"
    );

    Ok(())
}

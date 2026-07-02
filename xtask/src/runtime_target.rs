use crate::config::{Config, workspace_root, workspace_state_root};
use crate::infra::stack::StackConfig;
use color_eyre::eyre::{Result, WrapErr};
use serde::Serialize;
use sinex_primitives::{
    RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeStatusWarning,
    RuntimeTargetDatabase, RuntimeTargetDescriptor, RuntimeTargetGateway, RuntimeTargetKind,
    RuntimeTargetNats, RuntimeTargetServices, RuntimeTargetState,
};
use std::path::PathBuf;

const CHECKOUT_DEV_GATEWAY_URL: &str = "https://127.0.0.1:19086";

/// Condensed target surface serialized by xtask status commands.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeTargetSummary {
    pub name: String,
    pub kind: RuntimeTargetKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_url: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub nats_servers: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_dir: Option<PathBuf>,
}

impl From<&RuntimeTargetDescriptor> for RuntimeTargetSummary {
    fn from(target: &RuntimeTargetDescriptor) -> Self {
        Self {
            name: target.name.clone(),
            kind: target.kind.clone(),
            source: target.source.clone(),
            source_path: target.source_path.clone(),
            database_url: target.database.url.clone(),
            nats_servers: target.nats.servers.clone(),
            gateway_url: target.gateway.base_url.clone(),
            state_dir: target.state.state_dir.clone(),
            cache_dir: target.state.cache_dir.clone(),
        }
    }
}

pub fn checkout_runtime_target(cfg: &Config) -> Result<RuntimeTargetDescriptor> {
    let stack_config = StackConfig::for_current_checkout()
        .wrap_err("failed to load checkout stack config for runtime target")?;
    let database_url = cfg
        .database_url
        .clone()
        .unwrap_or_else(|| stack_config.database_url());
    let nats_url = cfg
        .nats_url
        .clone()
        .unwrap_or_else(|| stack_config.nats_url());
    let tls_dir = workspace_root().join(".sinex/tls");
    let ca_cert_file = existing_path(tls_dir.join("ca.pem"));
    let client_cert_file = existing_path(tls_dir.join("client.pem"));
    let client_key_file = existing_path(tls_dir.join("client-key.pem"));
    let token_file = existing_path(checkout_runtime_target_token_file());

    Ok(RuntimeTargetDescriptor {
        version: 1,
        name: "checkout-local".to_string(),
        kind: RuntimeTargetKind::DevCheckout,
        source: Some("xtask checkout config".to_string()),
        source_path: Some(workspace_state_root()),
        database: RuntimeTargetDatabase {
            url: Some(database_url),
            host: None,
            port: Some(stack_config.postgres.port),
            name: Some(stack_config.postgres.database),
            user: Some(stack_config.postgres.user),
            password_file: None,
            password_required: false,
        },
        gateway: RuntimeTargetGateway {
            base_url: Some(
                cfg.gateway_url
                    .clone()
                    .unwrap_or_else(|| CHECKOUT_DEV_GATEWAY_URL.to_string()),
            ),
            token_file,
            token_role: None,
            ca_cert_file,
            client_cert_file,
            client_key_file,
            require_client_tls: true,
            insecure: false,
        },
        nats: RuntimeTargetNats {
            servers: vec![nats_url],
            environment: Some("dev".to_string()),
            token_file: None,
            creds_file: None,
            nkey_seed_file: None,
            ca_cert_file: None,
            client_cert_file: None,
            client_key_file: None,
        },
        state: RuntimeTargetState {
            state_dir: Some(cfg.state_dir.clone()),
            cache_dir: Some(cfg.cache_dir.clone()),
        },
        services: RuntimeTargetServices {
            managed_units: vec![
                "checkout-local:sinexd".to_string(),
                "checkout-local:sinexd".to_string(),
            ],
        },
        notes: vec![
            "Derived from the current checkout; deployed-host descriptors are not loaded implicitly"
                .to_string(),
        ],
    })
}

#[must_use]
pub fn checkout_runtime_target_path() -> PathBuf {
    workspace_state_root().join("state/runtime-target.json")
}

#[must_use]
pub fn checkout_runtime_target_token_file() -> PathBuf {
    workspace_state_root().join("state/dev-api-token")
}

#[must_use]
pub fn checkout_dev_gateway_url() -> &'static str {
    CHECKOUT_DEV_GATEWAY_URL
}

fn existing_path(path: PathBuf) -> Option<PathBuf> {
    path.exists().then_some(path)
}

#[must_use]
pub fn checkout_status_snapshot(
    target: RuntimeTargetDescriptor,
    signals: Vec<RuntimeStatusSignal>,
    warnings: Vec<RuntimeStatusWarning>,
) -> RuntimeStatusSnapshot {
    RuntimeStatusSnapshot {
        target,
        signals,
        warnings,
    }
}

pub fn signal(
    name: impl Into<String>,
    status: RuntimeStatusSignalStatus,
    source: impl Into<String>,
    message: Option<String>,
) -> RuntimeStatusSignal {
    RuntimeStatusSignal {
        name: name.into(),
        status,
        source: source.into(),
        message,
    }
}

pub fn warning(source: impl Into<String>, message: impl Into<String>) -> RuntimeStatusWarning {
    RuntimeStatusWarning {
        source: source.into(),
        message: message.into(),
    }
}

#[cfg(test)]
#[path = "runtime_target_test.rs"]
mod tests;

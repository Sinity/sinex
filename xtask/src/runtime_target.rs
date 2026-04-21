use crate::config::{Config, workspace_state_root};
use crate::infra::stack::StackConfig;
use color_eyre::eyre::{Result, WrapErr};
use serde::Serialize;
use sinex_primitives::{
    RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusSnapshot, RuntimeStatusWarning,
    RuntimeTargetDatabase, RuntimeTargetDescriptor, RuntimeTargetGateway, RuntimeTargetKind,
    RuntimeTargetNats, RuntimeTargetServices, RuntimeTargetState,
};
use std::path::PathBuf;

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
            base_url: cfg.gateway_url.clone(),
            token_file: None,
            ca_cert_file: None,
            client_cert_file: None,
            client_key_file: None,
            require_client_tls: false,
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
                "checkout-local:sinex-gateway".to_string(),
                "checkout-local:sinex-ingestd".to_string(),
            ],
        },
        notes: vec![
            "Derived from the current checkout; deployed-host descriptors are not loaded implicitly"
                .to_string(),
        ],
    })
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
mod tests {
    use super::*;
    use crate::config::Config;
    use xtask::sandbox::{EnvGuard, sinex_test};

    #[sinex_test]
    async fn checkout_runtime_target_uses_checkout_stack_without_descriptor() -> TestResult<()> {
        let mut env = EnvGuard::with_keys(&[
            "DATABASE_URL",
            "SINEX_NATS_URL",
            "SINEX_GATEWAY_URL",
            "SINEX_RPC_URL",
            "SINEX_GATEWAY_TCP_LISTEN",
            "SINEX_RUNTIME_TARGET_CONFIG",
        ]);
        env.clear("DATABASE_URL");
        env.clear("SINEX_NATS_URL");
        env.clear("SINEX_GATEWAY_URL");
        env.clear("SINEX_RPC_URL");
        env.clear("SINEX_GATEWAY_TCP_LISTEN");
        env.set("SINEX_RUNTIME_TARGET_CONFIG", "/definitely/not/loaded.json");

        let cfg = Config::from_env();
        let target = checkout_runtime_target(&cfg)?;

        assert_eq!(target.name, "checkout-local");
        assert_eq!(target.kind, RuntimeTargetKind::DevCheckout);
        assert_eq!(target.source.as_deref(), Some("xtask checkout config"));
        assert!(
            target
                .database
                .url
                .as_deref()
                .is_some_and(|url| url.contains("sinex_dev"))
        );
        assert_eq!(target.nats.servers.len(), 1);
        assert!(target.gateway.base_url.is_none());
        Ok(())
    }
}

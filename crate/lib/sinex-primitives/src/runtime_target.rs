use crate::{DeploymentReadinessDescriptor, DeploymentReadinessMode, Result, SinexError};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const DEFAULT_RUNTIME_TARGET_PATH: &str = "/etc/sinex/runtime-target.json";

/// Runtime target descriptor consumed by tools that need to probe one concrete
/// Sinex runtime.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetDescriptor {
    #[serde(default = "default_descriptor_version")]
    pub version: u32,
    #[serde(default = "default_runtime_target_name")]
    pub name: String,
    #[serde(default)]
    pub kind: RuntimeTargetKind,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub source_path: Option<PathBuf>,
    #[serde(default)]
    pub database: RuntimeTargetDatabase,
    #[serde(default)]
    pub gateway: RuntimeTargetGateway,
    #[serde(default)]
    pub nats: RuntimeTargetNats,
    #[serde(default)]
    pub state: RuntimeTargetState,
    #[serde(default)]
    pub services: RuntimeTargetServices,
    #[serde(default)]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeTargetKind {
    #[default]
    Unknown,
    DevCheckout,
    DeployedHost,
    Vm,
    Test,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetDatabase {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub password_file: Option<PathBuf>,
    #[serde(default)]
    pub password_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetGateway {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    #[serde(default)]
    pub ca_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub client_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub client_key_file: Option<PathBuf>,
    #[serde(default)]
    pub require_client_tls: bool,
    #[serde(default)]
    pub insecure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetNats {
    #[serde(default)]
    pub servers: Vec<String>,
    #[serde(default)]
    pub environment: Option<String>,
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    #[serde(default)]
    pub creds_file: Option<PathBuf>,
    #[serde(default)]
    pub nkey_seed_file: Option<PathBuf>,
    #[serde(default)]
    pub ca_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub client_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub client_key_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetState {
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub cache_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeTargetServices {
    #[serde(default)]
    pub managed_units: Vec<String>,
}

/// Source-attributed status snapshot for one runtime target.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RuntimeStatusSnapshot {
    pub target: RuntimeTargetDescriptor,
    #[serde(default)]
    pub signals: Vec<RuntimeStatusSignal>,
    #[serde(default)]
    pub warnings: Vec<RuntimeStatusWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeStatusSignal {
    pub name: String,
    pub status: RuntimeStatusSignalStatus,
    pub source: String,
    #[serde(default)]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStatusSignalStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
    Skipped,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeStatusWarning {
    pub source: String,
    pub message: String,
}

impl RuntimeTargetDescriptor {
    #[must_use]
    pub fn default_path() -> PathBuf {
        PathBuf::from(DEFAULT_RUNTIME_TARGET_PATH)
    }

    #[must_use]
    pub fn configured_path() -> Option<PathBuf> {
        match std::env::var_os("SINEX_RUNTIME_TARGET_CONFIG") {
            Some(path) if path.is_empty() => None,
            Some(path) => Some(PathBuf::from(path)),
            None => {
                let default = Self::default_path();
                default.is_file().then_some(default)
            }
        }
    }

    pub fn load() -> Result<Option<Self>> {
        let Some(path) = Self::configured_path() else {
            return Ok(None);
        };
        Self::load_from_path(path).map(Some)
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path).map_err(|error| {
            SinexError::configuration("failed to read runtime target descriptor")
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;
        let mut descriptor: Self = serde_json::from_str(&contents).map_err(|error| {
            SinexError::configuration("failed to parse runtime target descriptor")
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;
        descriptor.source_path = Some(path.to_path_buf());
        Ok(descriptor)
    }

    #[must_use]
    pub fn from_deployment_readiness(readiness: &DeploymentReadinessDescriptor) -> Self {
        let name = readiness.target.as_ref().map_or_else(
            || "deployed-host".to_string(),
            |target| format!("deployed-host:{}", target.user),
        );

        let source = readiness
            .source
            .clone()
            .or_else(|| Some("deployment-readiness".to_string()));
        let mut notes = Vec::new();
        if !matches!(readiness.mode, DeploymentReadinessMode::Enabled) {
            notes.push(format!("deployment readiness mode is {:?}", readiness.mode));
        }

        Self {
            version: 1,
            name,
            kind: RuntimeTargetKind::DeployedHost,
            source,
            source_path: None,
            database: RuntimeTargetDatabase {
                url: render_database_url(
                    readiness.database.user.as_deref(),
                    readiness.database.host.as_deref(),
                    readiness.database.port,
                    readiness.database.name.as_deref(),
                ),
                host: readiness.database.host.clone(),
                port: readiness.database.port,
                name: readiness.database.name.clone(),
                user: readiness.database.user.clone(),
                password_file: readiness.secrets.database_password_file.clone(),
                password_required: readiness.database.password_required,
            },
            gateway: RuntimeTargetGateway {
                base_url: readiness.gateway.base_url.clone(),
                token_file: readiness.secrets.gateway_admin_token_file.clone(),
                ca_cert_file: readiness.secrets.gateway_tls_trust_anchor_file.clone(),
                client_cert_file: None,
                client_key_file: None,
                require_client_tls: readiness.gateway.require_client_tls,
                insecure: false,
            },
            nats: RuntimeTargetNats {
                servers: readiness.nats.servers.clone(),
                environment: readiness
                    .database
                    .name
                    .clone()
                    .and_then(|name| name.strip_prefix("sinex_").map(ToString::to_string)),
                token_file: readiness.secrets.nats_token_file.clone(),
                creds_file: readiness.secrets.nats_creds_file.clone(),
                nkey_seed_file: readiness.secrets.nats_nkey_seed_file.clone(),
                ca_cert_file: readiness.secrets.nats_ca_cert_file.clone(),
                client_cert_file: readiness.secrets.nats_client_cert_file.clone(),
                client_key_file: readiness.secrets.nats_client_key_file.clone(),
            },
            state: RuntimeTargetState::default(),
            services: RuntimeTargetServices {
                managed_units: readiness.managed_units.clone(),
            },
            notes,
        }
    }
}

fn render_database_url(
    user: Option<&str>,
    host: Option<&str>,
    port: Option<u16>,
    name: Option<&str>,
) -> Option<String> {
    Some(format!(
        "postgresql://{}@{}:{}/{}",
        user?, host?, port?, name?
    ))
}

const fn default_descriptor_version() -> u32 {
    1
}

fn default_runtime_target_name() -> String {
    "runtime".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DeploymentDatabaseRuntime, DeploymentGatewayRuntime, DeploymentNatsRuntime,
        DeploymentReadinessMode, DeploymentSecrets, DeploymentTarget,
    };
    use std::env;

    #[test]
    fn configured_path_treats_empty_override_as_disabled() {
        let previous = env::var_os("SINEX_RUNTIME_TARGET_CONFIG");
        unsafe { env::set_var("SINEX_RUNTIME_TARGET_CONFIG", "") };

        let configured = RuntimeTargetDescriptor::configured_path();

        match previous {
            Some(value) => unsafe { env::set_var("SINEX_RUNTIME_TARGET_CONFIG", value) },
            None => unsafe { env::remove_var("SINEX_RUNTIME_TARGET_CONFIG") },
        }

        assert!(configured.is_none());
    }

    #[test]
    fn load_from_path_sets_source_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("runtime-target.json");
        std::fs::write(
            &path,
            r#"{"version":1,"name":"prod","kind":"deployed_host","gateway":{"base_url":"https://127.0.0.1:9999"}}"#,
        )
        .expect("write descriptor");

        let descriptor = RuntimeTargetDescriptor::load_from_path(&path).expect("descriptor loads");

        assert_eq!(descriptor.name, "prod");
        assert_eq!(descriptor.kind, RuntimeTargetKind::DeployedHost);
        assert_eq!(
            descriptor.gateway.base_url.as_deref(),
            Some("https://127.0.0.1:9999")
        );
        assert_eq!(descriptor.source_path.as_deref(), Some(path.as_path()));
    }

    #[test]
    fn deployment_readiness_maps_to_runtime_target() {
        let readiness = DeploymentReadinessDescriptor {
            version: 1,
            mode: DeploymentReadinessMode::Enabled,
            source: Some("nixos".to_string()),
            managed_units: vec!["sinex-gateway.service".to_string()],
            target: Some(DeploymentTarget {
                user: "sinity".to_string(),
                uid: Some(1000),
                home: Some(PathBuf::from("/home/sinity")),
            }),
            database: DeploymentDatabaseRuntime {
                enabled: true,
                host: Some("127.0.0.1".to_string()),
                port: Some(5432),
                name: Some("sinex_prod".to_string()),
                user: Some("sinex".to_string()),
                local_auth: Some("scram-sha-256".to_string()),
                password_required: true,
            },
            gateway: DeploymentGatewayRuntime {
                base_url: Some("https://127.0.0.1:9999".to_string()),
                require_client_tls: true,
            },
            nats: DeploymentNatsRuntime {
                servers: vec!["tls://127.0.0.1:4222".to_string()],
            },
            secrets: DeploymentSecrets {
                gateway_admin_token_file: Some(PathBuf::from(
                    "/run/agenix/sinex-gateway-admin-token",
                )),
                gateway_tls_trust_anchor_file: Some(PathBuf::from(
                    "/var/lib/sinex/run/gateway-ca.pem",
                )),
                nats_creds_file: Some(PathBuf::from("/run/agenix/sinex-nats-client-creds")),
                ..DeploymentSecrets::default()
            },
            ..DeploymentReadinessDescriptor::default()
        };

        let target = RuntimeTargetDescriptor::from_deployment_readiness(&readiness);

        assert_eq!(target.name, "deployed-host:sinity");
        assert_eq!(target.kind, RuntimeTargetKind::DeployedHost);
        assert_eq!(target.source.as_deref(), Some("nixos"));
        assert_eq!(
            target.database.url.as_deref(),
            Some("postgresql://sinex@127.0.0.1:5432/sinex_prod")
        );
        assert_eq!(target.nats.environment.as_deref(), Some("prod"));
        assert_eq!(
            target.gateway.token_file.as_deref(),
            Some(Path::new("/run/agenix/sinex-gateway-admin-token"))
        );
        assert_eq!(target.services.managed_units, ["sinex-gateway.service"]);
    }
}

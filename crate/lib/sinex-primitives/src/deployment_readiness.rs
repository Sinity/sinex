use crate::{Result, SinexError};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const DEFAULT_DESCRIPTOR_PATH: &str = "/etc/sinex/deployment-readiness.json";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentReadinessDescriptor {
    #[serde(default = "default_descriptor_version")]
    pub version: u32,
    #[serde(default)]
    pub mode: DeploymentReadinessMode,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub managed_units: Vec<String>,
    #[serde(default)]
    pub target: Option<DeploymentTarget>,
    #[serde(default)]
    pub database: DeploymentDatabaseRuntime,
    #[serde(default)]
    pub gateway: DeploymentGatewayRuntime,
    #[serde(default)]
    pub nats: DeploymentNatsRuntime,
    #[serde(default)]
    pub filesystem: DeploymentSurface,
    #[serde(default)]
    pub terminal: TerminalDeploymentSurface,
    #[serde(default)]
    pub desktop: DesktopDeploymentSurface,
    #[serde(default)]
    pub system: DeploymentSurface,
    #[serde(default)]
    pub document: DocumentDeploymentSurface,
    #[serde(default)]
    pub automata: AutomataDeploymentSurface,
    #[serde(default)]
    pub expectations: DeploymentExpectations,
    #[serde(default)]
    pub secrets: DeploymentSecrets,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentReadinessMode {
    #[default]
    Unknown,
    Prepared,
    Enabled,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentTarget {
    pub user: String,
    #[serde(default)]
    pub uid: Option<u32>,
    #[serde(default)]
    pub home: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentDatabaseRuntime {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub local_auth: Option<String>,
    #[serde(default)]
    pub password_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentGatewayRuntime {
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub require_client_tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentNatsRuntime {
    #[serde(default)]
    pub servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentSurface {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub instances: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TerminalHistorySource {
    pub path: PathBuf,
    pub shell: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TerminalDeploymentSurface {
    #[serde(flatten)]
    pub surface: DeploymentSurface,
    #[serde(default)]
    pub kitty_enabled: bool,
    #[serde(default)]
    pub history_sources: Vec<TerminalHistorySource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DesktopDeploymentSurface {
    #[serde(flatten)]
    pub surface: DeploymentSurface,
    #[serde(default)]
    pub clipboard_enabled: bool,
    #[serde(default)]
    pub activitywatch_db_path: Option<PathBuf>,
    #[serde(default)]
    pub runtime_dir: Option<PathBuf>,
    #[serde(default)]
    pub wayland_display: Option<String>,
    #[serde(default)]
    pub hyprland_instance_signature: Option<String>,
    #[serde(default)]
    pub hyprland_event_socket: Option<PathBuf>,
    #[serde(default)]
    pub hyprland_command_socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DocumentDeploymentSurface {
    #[serde(flatten)]
    pub surface: DeploymentSurface,
    #[serde(default)]
    pub allowed_roots: Vec<PathBuf>,
    #[serde(default)]
    pub scan_service_unit: Option<String>,
    #[serde(default)]
    pub timer_unit: Option<String>,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub run_on_boot: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AutomataDeploymentSurface {
    #[serde(flatten)]
    pub surface: DeploymentSurface,
    #[serde(default)]
    pub canonicalizer: bool,
    #[serde(default)]
    pub health_aggregator: bool,
    #[serde(default)]
    pub analytics_automaton: bool,
    #[serde(default)]
    pub session_detector: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentExpectations {
    #[serde(default)]
    pub schema_apply: bool,
    #[serde(default)]
    pub nats_streams: bool,
    #[serde(default)]
    pub gateway_ready: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DeploymentSecrets {
    #[serde(default)]
    pub database_password_file: Option<PathBuf>,
    #[serde(default)]
    pub gateway_admin_token_file: Option<PathBuf>,
    #[serde(default)]
    pub gateway_tls_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub gateway_tls_key_file: Option<PathBuf>,
    #[serde(default)]
    pub gateway_tls_trust_anchor_file: Option<PathBuf>,
    #[serde(default)]
    pub gateway_tls_client_ca_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_ca_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_client_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_client_key_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_token_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_creds_file: Option<PathBuf>,
    #[serde(default)]
    pub nats_nkey_seed_file: Option<PathBuf>,
}

impl DeploymentReadinessDescriptor {
    #[must_use]
    pub fn default_path() -> PathBuf {
        PathBuf::from(DEFAULT_DESCRIPTOR_PATH)
    }

    #[must_use]
    pub fn configured_path() -> Option<PathBuf> {
        match std::env::var_os("SINEX_DEPLOYMENT_READINESS_CONFIG") {
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
        let contents = std::fs::read_to_string(&path).map_err(|error| {
            SinexError::configuration("failed to read deployment readiness descriptor")
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;
        let descriptor = serde_json::from_str(&contents).map_err(|error| {
            SinexError::configuration("failed to parse deployment readiness descriptor")
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;
        Ok(Some(descriptor))
    }
}

const fn default_descriptor_version() -> u32 {
    1
}

#[cfg(test)]
mod tests {
    use super::DeploymentReadinessDescriptor;
    use std::env;

    #[test]
    fn configured_path_treats_empty_override_as_disabled() {
        let previous = env::var_os("SINEX_DEPLOYMENT_READINESS_CONFIG");
        unsafe { env::set_var("SINEX_DEPLOYMENT_READINESS_CONFIG", "") };

        let configured = DeploymentReadinessDescriptor::configured_path();

        match previous {
            Some(value) => unsafe { env::set_var("SINEX_DEPLOYMENT_READINESS_CONFIG", value) },
            None => unsafe { env::remove_var("SINEX_DEPLOYMENT_READINESS_CONFIG") },
        }

        assert!(configured.is_none());
    }
}

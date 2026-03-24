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
    pub target: Option<DeploymentTarget>,
    #[serde(default)]
    pub filesystem: DeploymentSurface,
    #[serde(default)]
    pub terminal: TerminalDeploymentSurface,
    #[serde(default)]
    pub desktop: DesktopDeploymentSurface,
    #[serde(default)]
    pub system: DeploymentSurface,
    #[serde(default)]
    pub automata: DeploymentSurface,
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
    pub history_sources: Vec<TerminalHistorySource>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct DesktopDeploymentSurface {
    #[serde(flatten)]
    pub surface: DeploymentSurface,
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
    pub gateway_tls_client_ca_file: Option<PathBuf>,
}

impl DeploymentReadinessDescriptor {
    #[must_use]
    pub fn default_path() -> PathBuf {
        PathBuf::from(DEFAULT_DESCRIPTOR_PATH)
    }

    #[must_use]
    pub fn configured_path() -> Option<PathBuf> {
        std::env::var_os("SINEX_DEPLOYMENT_READINESS_CONFIG")
            .map(PathBuf::from)
            .or_else(|| {
                let default = Self::default_path();
                default.is_file().then_some(default)
            })
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

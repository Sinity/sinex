use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use sinex_primitives::env as shared_env;
use sinex_primitives::{RuntimeTargetDescriptor, RuntimeTargetGatewayTokenRole};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::Result;
use crate::model::OutputFormat;

/// Effective CLI configuration.
///
/// Runtime connection/auth/TLS values are env/CLI-driven to match the rest of
/// the project. The config file stores only local user preferences.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Gateway RPC URL
    #[serde(default = "default_rpc_url")]
    pub rpc_url: String,

    /// Authentication token
    pub token: Option<String>,

    /// Token file path
    pub token_file: Option<String>,

    /// Role suffix to apply to a raw runtime token.
    pub token_role: Option<RuntimeTargetGatewayTokenRole>,

    /// Root CA certificate path
    pub ca_cert: Option<String>,

    /// Client certificate path (for mTLS)
    pub client_cert: Option<String>,

    /// Client private key path (for mTLS)
    pub client_key: Option<String>,

    /// Accept invalid certificates (dev only!)
    #[serde(default)]
    pub insecure: bool,

    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Default output format
    #[serde(default)]
    pub default_format: OutputFormat,

    /// User-defined command aliases
    #[serde(default)]
    pub aliases: HashMap<String, Vec<String>>,

    /// UI theme settings
    #[serde(default)]
    pub theme: ThemeConfig,

    /// Editor for interactive mode
    #[serde(default = "default_editor")]
    pub editor: String,

    /// Runtime target descriptor used to derive live connection settings.
    #[serde(skip)]
    pub runtime_target: Option<RuntimeTargetDescriptor>,
}

/// User-local `sinexctl` preferences stored in `config.toml`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct UserConfigFile {
    #[serde(default)]
    pub default_format: Option<OutputFormat>,
    #[serde(default)]
    pub aliases: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub theme: Option<ThemeConfig>,
    #[serde(default)]
    pub editor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeConfig {
    /// Table style: "rounded", "ascii", "modern", "minimal"
    #[serde(default = "default_table_style")]
    pub table_style: String,

    /// Success color
    #[serde(default = "default_success_color")]
    pub success_color: String,

    /// Error color
    #[serde(default = "default_error_color")]
    pub error_color: String,

    /// Warning color
    #[serde(default = "default_warning_color")]
    pub warning_color: String,
}

impl Default for ThemeConfig {
    fn default() -> Self {
        Self {
            table_style: default_table_style(),
            success_color: default_success_color(),
            error_color: default_error_color(),
            warning_color: default_warning_color(),
        }
    }
}

/// Default RPC URL for the gateway
#[must_use]
pub fn default_rpc_url() -> String {
    "https://127.0.0.1:9999".to_string()
}

fn default_timeout() -> u64 {
    30
}

fn default_editor() -> String {
    std::env::var("EDITOR").unwrap_or_else(|_| "vim".to_string())
}

fn default_table_style() -> String {
    "rounded".to_string()
}

fn default_success_color() -> String {
    "green".to_string()
}

fn default_error_color() -> String {
    "red".to_string()
}

fn default_warning_color() -> String {
    "yellow".to_string()
}

impl Config {
    /// Load effective configuration.
    ///
    /// Order:
    /// 1. built-in defaults
    /// 2. runtime env overrides
    /// 3. user preference file (format/theme/editor/aliases only)
    pub fn load() -> Result<Self> {
        let mut config = Self::default();
        config.apply_runtime_env_overrides();

        let config_path = Self::config_file_path()?;
        if config_path.exists() {
            let raw = fs::read_to_string(&config_path)?;
            let user_config: UserConfigFile = toml::from_str(&raw)?;
            config.apply_user_preferences(user_config);
        }

        Ok(config)
    }

    /// Get the path to the config file
    pub fn config_file_path() -> Result<PathBuf> {
        let project_dirs = ProjectDirs::from("com", "sinex", "sinexctl")
            .ok_or_else(|| color_eyre::eyre::eyre!("Could not determine config directory"))?;

        let config_dir = project_dirs.config_dir();
        Ok(config_dir.join("config.toml"))
    }

    /// Create a default config file if it doesn't exist
    pub fn init_config_file() -> Result<PathBuf> {
        let config_path = Self::config_file_path()?;

        if config_path.exists() {
            return Ok(config_path);
        }

        // Create parent directories
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write default config
        let default_config = include_str!("../config.example.toml");
        std::fs::write(&config_path, default_config)?;

        Ok(config_path)
    }

    /// Merge CLI arguments into config
    pub fn merge_cli_args(
        &mut self,
        rpc_url: Option<String>,
        token: Option<String>,
        token_file: Option<String>,
        ca_cert: Option<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
        insecure: bool,
        timeout: Option<u64>,
        format: Option<OutputFormat>,
    ) {
        if let Some(url) = rpc_url {
            self.rpc_url = url;
        }
        if let Some(t) = token {
            self.token = Some(t);
        }
        if let Some(tf) = token_file {
            self.token_file = Some(tf);
        }
        if let Some(ca) = ca_cert {
            self.ca_cert = Some(ca);
        }
        if let Some(cert) = client_cert {
            self.client_cert = Some(cert);
        }
        if let Some(key) = client_key {
            self.client_key = Some(key);
        }
        if insecure {
            self.insecure = true;
        }
        if let Some(t) = timeout {
            self.timeout = t;
        }
        if let Some(f) = format {
            self.default_format = f;
        }
    }

    pub fn apply_runtime_target(&mut self, target: RuntimeTargetDescriptor) {
        if let Some(base_url) = target.gateway.base_url.clone() {
            self.rpc_url = base_url;
        }
        if let Some(token_file) = target.gateway.token_file.clone() {
            self.token_file = Some(path_to_string(token_file));
        }
        self.token_role = target.gateway.token_role;
        if let Some(ca_cert) = target.gateway.ca_cert_file.clone() {
            self.ca_cert = Some(path_to_string(ca_cert));
        }
        if let Some(client_cert) = target.gateway.client_cert_file.clone() {
            self.client_cert = Some(path_to_string(client_cert));
        }
        if let Some(client_key) = target.gateway.client_key_file.clone() {
            self.client_key = Some(path_to_string(client_key));
        }
        if target.gateway.insecure {
            self.insecure = true;
        }
        self.runtime_target = Some(target);
    }

    fn apply_runtime_env_overrides(&mut self) {
        env_override("SINEX_RPC_URL", &mut self.rpc_url);
        env_option_override("SINEX_RPC_TOKEN", &mut self.token);
        env_option_override("SINEX_RPC_TOKEN_FILE", &mut self.token_file);
        env_option_override("SINEX_RPC_CA_CERT", &mut self.ca_cert);
        env_option_override("SINEX_RPC_CLIENT_CERT", &mut self.client_cert);
        env_option_override("SINEX_RPC_CLIENT_KEY", &mut self.client_key);
        env_bool_override("SINEX_RPC_INSECURE", &mut self.insecure);
        env_parse_override("SINEX_RPC_TIMEOUT_SECS", &mut self.timeout);
        env_parse_override("SINEX_TIMEOUT", &mut self.timeout);
    }

    fn apply_user_preferences(&mut self, user_config: UserConfigFile) {
        if let Some(default_format) = user_config.default_format {
            self.default_format = default_format;
        }
        if let Some(theme) = user_config.theme {
            self.theme = theme;
        }
        if let Some(editor) = user_config.editor {
            self.editor = editor;
        }
        self.aliases = user_config.aliases;
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rpc_url: default_rpc_url(),
            token: None,
            token_file: None,
            token_role: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
            insecure: false,
            timeout: default_timeout(),
            default_format: OutputFormat::default(),
            aliases: HashMap::new(),
            theme: ThemeConfig::default(),
            editor: default_editor(),
            runtime_target: None,
        }
    }
}

fn path_to_string(path: PathBuf) -> String {
    path.to_string_lossy().into_owned()
}

fn env_override(key: &str, target: &mut String) {
    if let Ok(value) = std::env::var(key) {
        *target = value;
    }
}

fn env_option_override(key: &str, target: &mut Option<String>) {
    if let Ok(value) = std::env::var(key) {
        *target = Some(value);
    }
}

fn env_bool_override(key: &str, target: &mut bool) {
    if let Ok(value) = std::env::var(key)
        && let Ok(parsed) = value.parse::<bool>()
    {
        *target = parsed;
    }
}

fn env_parse_override<T>(key: &str, target: &mut T)
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    if let Some(parsed) = shared_env::parse_optional(key, "") {
        *target = parsed;
    }
}

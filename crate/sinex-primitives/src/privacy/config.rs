//! Privacy engine configuration.
//!
//! Configuration is loaded in priority order:
//! 1. Environment variables (`SINEX_PRIVACY_*`) override everything
//! 2. TOML config file (`$SINEX_PRIVACY_CONFIG` or `$SINEX_STATE_DIR/privacy.toml`)
//! 3. Seed-neutral defaults

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::constants::env_vars;

use super::{PatternRule, RuleCategory, RuleOverride, Strategy};

/// Privacy engine configuration.
///
/// Loaded from environment variables via [`from_env()`](Self::from_env),
/// from a TOML file via [`from_file()`](Self::from_file), or constructed directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrivacyConfig {
    /// Master switch. When false, engine is a passthrough.
    pub enabled: bool,
    /// Which built-in categories to activate.
    pub builtin_categories: CategorySet,
    /// Additional user-defined rules (merged with, not replacing, builtins).
    #[serde(default)]
    pub extra_rules: Vec<PatternRule>,
    /// Overrides for built-in rules by name.
    #[serde(default)]
    pub overrides: HashMap<String, RuleOverride>,
    /// Default strategy for rules that don't specify one.
    pub default_strategy: Strategy,
    /// Optional override of default strategy for the Secret category.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_strategy: Option<Strategy>,
    /// Key configuration.
    pub key: KeyConfig,
    /// Enable per-rule match statistics.
    pub track_stats: bool,
}

impl Default for PrivacyConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            builtin_categories: CategorySet::None,
            extra_rules: Vec::new(),
            overrides: HashMap::new(),
            default_strategy: Strategy::Redact { label: None },
            secret_strategy: None,
            key: KeyConfig::default(),
            track_stats: false,
        }
    }
}

/// Which built-in rule categories to include.
///
/// Serializes as: `"all"`, `"none"`, or `["secret", "pii", ...]`.
#[derive(Debug, Clone)]
pub enum CategorySet {
    /// All built-in categories.
    All,
    /// Only these categories.
    Only(Vec<RuleCategory>),
    /// No built-in rules.
    None,
}

impl Serialize for CategorySet {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            CategorySet::All => serializer.serialize_str("all"),
            CategorySet::None => serializer.serialize_str("none"),
            CategorySet::Only(cats) => cats.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for CategorySet {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            List(Vec<RuleCategory>),
        }

        match Raw::deserialize(deserializer)? {
            Raw::Str(s) => match s.to_lowercase().as_str() {
                "all" => Ok(CategorySet::All),
                "none" => Ok(CategorySet::None),
                _ => Err(serde::de::Error::custom(format!(
                    "expected \"all\", \"none\", or a list of categories, got \"{s}\""
                ))),
            },
            Raw::List(cats) => {
                if cats.is_empty() {
                    Ok(CategorySet::None)
                } else {
                    Ok(CategorySet::Only(cats))
                }
            }
        }
    }
}

/// Key source for Encrypt and Hash strategies.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct KeyConfig {
    /// Path to a file containing a 256-bit key (raw 32 bytes or 64-char hex).
    #[serde(rename = "file", skip_serializing_if = "Option::is_none")]
    pub key_file: Option<String>,
    /// Hex-encoded key (development only).
    #[serde(rename = "hex", skip_serializing_if = "Option::is_none")]
    pub key_hex: Option<String>,
}

impl KeyConfig {
    /// Resolve the key, trying file first, then hex env var.
    pub fn resolve(&self) -> Option<[u8; 32]> {
        // Try file first
        if let Some(ref path) = self.key_file
            && let Ok(contents) = std::fs::read(path)
        {
            if contents.len() == 32 {
                let mut key = [0u8; 32];
                key.copy_from_slice(&contents);
                return Some(key);
            }
            // Try hex-encoded file content
            let hex = String::from_utf8_lossy(&contents).trim().to_string();
            if let Some(key) = parse_hex_key(&hex) {
                return Some(key);
            }
        }
        // Try hex string
        if let Some(ref hex) = self.key_hex {
            return parse_hex_key(hex);
        }
        None
    }
}

fn parse_hex_key(hex: &str) -> Option<[u8; 32]> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        key[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(key)
}

fn parse_strategy(s: &str) -> Option<Strategy> {
    match s.trim().to_lowercase().as_str() {
        "redact" => Some(Strategy::Redact { label: None }),
        "encrypt" => Some(Strategy::Encrypt),
        "hash" => Some(Strategy::Hash),
        "suppress" => Some(Strategy::Suppress),
        _ => None,
    }
}

fn invalid_env(var: &'static str, reason: impl Into<String>) -> PrivacyConfigError {
    PrivacyConfigError::InvalidEnv {
        var: var.to_string(),
        reason: reason.into(),
    }
}

fn parse_bool_env(var: &'static str, value: &str) -> Result<bool, PrivacyConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(invalid_env(
            var,
            format!("expected true/false or 1/0, got '{value}'"),
        )),
    }
}

fn parse_builtin_categories_env(value: &str) -> Result<CategorySet, PrivacyConfigError> {
    let raw = value.trim();
    match raw.to_ascii_lowercase().as_str() {
        "all" => Ok(CategorySet::All),
        "none" => Ok(CategorySet::None),
        _ => {
            let mut categories = Vec::new();
            let mut invalid = Vec::new();
            for part in raw.split(',') {
                let trimmed = part.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match trimmed.to_ascii_lowercase().as_str() {
                    "secret" | "secrets" => categories.push(RuleCategory::Secret),
                    "pii" => categories.push(RuleCategory::Pii),
                    "privacy" => categories.push(RuleCategory::Privacy),
                    "custom" => categories.push(RuleCategory::Custom),
                    _ => invalid.push(trimmed.to_string()),
                }
            }

            if !invalid.is_empty() {
                return Err(invalid_env(
                    env_vars::PRIVACY_BUILTIN,
                    format!(
                        "unknown categories: {}; expected all, none, or comma-separated secret/pii/privacy/custom",
                        invalid.join(", ")
                    ),
                ));
            }

            if categories.is_empty() {
                return Err(invalid_env(
                    env_vars::PRIVACY_BUILTIN,
                    "no valid categories were provided",
                ));
            }

            Ok(CategorySet::Only(categories))
        }
    }
}

fn parse_json_env<T>(var: &'static str, value: &str) -> Result<T, PrivacyConfigError>
where
    T: DeserializeOwned,
{
    serde_json::from_str(value)
        .map_err(|error| invalid_env(var, format!("failed to parse JSON override: {error}")))
}

fn parse_strategy_env(var: &'static str, value: &str) -> Result<Strategy, PrivacyConfigError> {
    parse_strategy(value).ok_or_else(|| {
        invalid_env(
            var,
            format!("expected redact, encrypt, hash, or suppress, got '{value}'"),
        )
    })
}

/// Resolve the configured config file path.
///
/// Checks `$SINEX_PRIVACY_CONFIG` first, then `$SINEX_STATE_DIR/privacy.toml`.
///
/// An explicit `SINEX_PRIVACY_CONFIG` path is always returned as-is so missing
/// or unreadable files fail honestly instead of silently falling back to
/// defaults.
fn configured_config_path() -> Result<Option<PathBuf>, PrivacyConfigError> {
    if let Some(explicit) = std::env::var_os(env_vars::PRIVACY_CONFIG) {
        let explicit = explicit.into_string().map_err(|value| {
            invalid_env(
                env_vars::PRIVACY_CONFIG,
                format!("contains a non-unicode path: {}", value.to_string_lossy()),
            )
        })?;
        return Ok(Some(PathBuf::from(explicit)));
    }
    if let Some(state_dir) = std::env::var_os("SINEX_STATE_DIR") {
        let state_dir = state_dir.into_string().map_err(|value| {
            invalid_env(
                "SINEX_STATE_DIR",
                format!("contains a non-unicode path: {}", value.to_string_lossy()),
            )
        })?;
        let path = PathBuf::from(state_dir).join("privacy.toml");
        if path.exists() {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

impl PrivacyConfig {
    /// Load configuration from a TOML file.
    ///
    /// Missing fields use defaults. The TOML format mirrors the struct fields:
    ///
    /// ```toml
    /// enabled = true
    /// builtin_categories = "all"       # or "none" or ["secret", "pii"]
    /// default_strategy = { action = "redact" }
    /// track_stats = false
    ///
    /// [key]
    /// file = "/path/to/privacy.key"
    /// # hex = "abc123..."  # dev only
    ///
    /// [overrides.email_address]
    /// enabled = false
    ///
    /// [overrides.ipv4_address]
    /// strategy = { action = "hash" }
    ///
    /// [[extra_rules]]
    /// name = "my_rule"
    /// description = "Custom pattern"
    /// category = "custom"
    /// matcher = { type = "regex", pattern = "my-secret-\\d+" }
    /// strategy = { action = "redact", label = "<MY_SECRET>" }
    /// contexts = ["command", "clipboard"]
    /// ```
    pub fn from_file(path: &Path) -> Result<Self, PrivacyConfigError> {
        let contents = std::fs::read_to_string(path).map_err(|e| PrivacyConfigError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        toml::from_str(&contents).map_err(|e| PrivacyConfigError::Parse {
            path: path.to_path_buf(),
            source: e,
        })
    }

    /// Load configuration from environment variables, with optional TOML file as base.
    ///
    /// Priority: env vars override file config, file config overrides defaults.
    ///
    /// File lookup: `$SINEX_PRIVACY_CONFIG` → `$SINEX_STATE_DIR/privacy.toml`.
    /// If no file exists, starts from defaults.
    ///
    /// | Variable | Default | Purpose |
    /// |----------|---------|---------|
    /// | `SINEX_PRIVACY_CONFIG` | — | Explicit path to TOML config file |
    /// | `SINEX_PRIVACY_ENABLED` | `true` | Master switch |
    /// | `SINEX_PRIVACY_BUILTIN` | `all` | `all`, `none`, or comma-separated categories |
    /// | `SINEX_PRIVACY_EXTRA_RULES` | `[]` | JSON array of `PatternRule` |
    /// | `SINEX_PRIVACY_OVERRIDES` | `{}` | JSON map of `name → RuleOverride` |
    /// | `SINEX_PRIVACY_DEFAULT_STRATEGY` | `redact` | Default strategy |
    /// | `SINEX_PRIVACY_SECRET_STRATEGY` | — | Override for Secret category |
    /// | `SINEX_PRIVACY_KEY_FILE` | — | Path to 256-bit key file |
    /// | `SINEX_PRIVACY_KEY` | — | Hex key (dev only) |
    /// | `SINEX_PRIVACY_STATS` | `false` | Per-rule match counting |
    pub fn from_env() -> Result<Self, PrivacyConfigError> {
        // Start from file config if available, otherwise defaults
        let mut config = match configured_config_path()? {
            Some(path) => Self::from_file(&path)?,
            None => Self::default(),
        };

        // Env vars override file config
        if let Ok(val) = std::env::var(env_vars::PRIVACY_ENABLED) {
            config.enabled = parse_bool_env(env_vars::PRIVACY_ENABLED, &val)?;
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_BUILTIN) {
            config.builtin_categories = parse_builtin_categories_env(&val)?;
        }

        if let Ok(json) = std::env::var(env_vars::PRIVACY_EXTRA_RULES) {
            config.extra_rules = parse_json_env(env_vars::PRIVACY_EXTRA_RULES, &json)?;
        }

        if let Ok(json) = std::env::var(env_vars::PRIVACY_OVERRIDES) {
            config.overrides = parse_json_env(env_vars::PRIVACY_OVERRIDES, &json)?;
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_DEFAULT_STRATEGY) {
            config.default_strategy = parse_strategy_env(env_vars::PRIVACY_DEFAULT_STRATEGY, &val)?;
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_SECRET_STRATEGY) {
            config.secret_strategy =
                Some(parse_strategy_env(env_vars::PRIVACY_SECRET_STRATEGY, &val)?);
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_KEY_FILE) {
            config.key.key_file = Some(val);
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_KEY) {
            config.key.key_hex = Some(val);
        }

        if let Ok(val) = std::env::var(env_vars::PRIVACY_STATS) {
            config.track_stats = parse_bool_env(env_vars::PRIVACY_STATS, &val)?;
        }

        Ok(config)
    }
}

/// Errors from config file loading.
#[derive(Debug)]
pub enum PrivacyConfigError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    InvalidEnv {
        var: String,
        reason: String,
    },
}

impl fmt::Display for PrivacyConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    f,
                    "failed to read privacy config at {}: {source}",
                    path.display()
                )
            }
            Self::Parse { path, source } => {
                write!(
                    f,
                    "failed to parse privacy config at {}: {source}",
                    path.display()
                )
            }
            Self::InvalidEnv { var, reason } => {
                write!(f, "invalid privacy environment override {var}: {reason}")
            }
        }
    }
}

impl StdError for PrivacyConfigError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Parse { source, .. } => Some(source),
            Self::InvalidEnv { .. } => None,
        }
    }
}

impl From<PrivacyConfigError> for crate::error::SinexError {
    fn from(err: PrivacyConfigError) -> Self {
        match &err {
            PrivacyConfigError::Io { path, source } => {
                crate::error::SinexError::configuration("failed to read privacy config")
                    .with_context("path", path.display())
                    .with_source(source)
            }
            PrivacyConfigError::Parse { path, source } => {
                crate::error::SinexError::parse("failed to parse privacy config")
                    .with_context("path", path.display())
                    .with_source(source)
            }
            PrivacyConfigError::InvalidEnv { var, reason } => {
                crate::error::SinexError::configuration("invalid privacy environment override")
                    .with_context("var", var)
                    .with_context("reason", reason)
            }
        }
    }
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;

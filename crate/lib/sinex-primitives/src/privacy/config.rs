//! Privacy engine configuration.
//!
//! Configuration is loaded in priority order:
//! 1. Environment variables (`SINEX_PRIVACY_*`) override everything
//! 2. TOML config file (`$SINEX_PRIVACY_CONFIG` or `$SINEX_STATE_DIR/privacy.toml`)
//! 3. Built-in defaults

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

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
            builtin_categories: CategorySet::All,
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
            && let Ok(contents) = std::fs::read(path) {
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

/// Resolve the default config file path.
///
/// Checks `$SINEX_PRIVACY_CONFIG` first, then `$SINEX_STATE_DIR/privacy.toml`.
fn default_config_path() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("SINEX_PRIVACY_CONFIG") {
        let path = PathBuf::from(explicit);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(state_dir) = std::env::var("SINEX_STATE_DIR") {
        let path = PathBuf::from(state_dir).join("privacy.toml");
        if path.exists() {
            return Some(path);
        }
    }
    std::option::Option::None
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
    #[must_use] 
    pub fn from_env() -> Self {
        // Start from file config if available, otherwise defaults
        let mut config = match default_config_path() {
            Some(path) => Self::from_file(&path).unwrap_or_default(),
            None => Self::default(),
        };

        // Env vars override file config
        if let Ok(val) = std::env::var("SINEX_PRIVACY_ENABLED") {
            config.enabled = val.eq_ignore_ascii_case("true") || val == "1";
        }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_BUILTIN") {
            config.builtin_categories = match val.to_lowercase().as_str() {
                "all" => CategorySet::All,
                "none" => CategorySet::None,
                _ => {
                    let cats: Vec<RuleCategory> = val
                        .split(',')
                        .filter_map(|s| match s.trim().to_lowercase().as_str() {
                            "secret" | "secrets" => Some(RuleCategory::Secret),
                            "pii" => Some(RuleCategory::Pii),
                            "privacy" => Some(RuleCategory::Privacy),
                            "custom" => Some(RuleCategory::Custom),
                            _ => None,
                        })
                        .collect();
                    if cats.is_empty() {
                        CategorySet::All
                    } else {
                        CategorySet::Only(cats)
                    }
                }
            };
        }

        if let Ok(json) = std::env::var("SINEX_PRIVACY_EXTRA_RULES")
            && let Ok(rules) = serde_json::from_str::<Vec<PatternRule>>(&json) {
                config.extra_rules = rules;
            }

        if let Ok(json) = std::env::var("SINEX_PRIVACY_OVERRIDES")
            && let Ok(overrides) = serde_json::from_str::<HashMap<String, RuleOverride>>(&json) {
                config.overrides = overrides;
            }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_DEFAULT_STRATEGY")
            && let Some(s) = parse_strategy(&val) {
                config.default_strategy = s;
            }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_SECRET_STRATEGY") {
            config.secret_strategy = parse_strategy(&val);
        }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_KEY_FILE") {
            config.key.key_file = Some(val);
        }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_KEY") {
            config.key.key_hex = Some(val);
        }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_STATS") {
            config.track_stats = val.eq_ignore_ascii_case("true") || val == "1";
        }

        config
    }
}

/// Errors from config file loading.
#[derive(Debug, thiserror::Error)]
pub enum PrivacyConfigError {
    #[error("failed to read privacy config at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse privacy config at {path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    async fn default_config_round_trips_through_toml() -> ::xtask::sandbox::TestResult<()> {
        let config = PrivacyConfig::default();
        let toml_str = toml::to_string_pretty(&config).expect("serialize");
        let parsed: PrivacyConfig = toml::from_str(&toml_str).expect("deserialize");

        assert!(parsed.enabled);
        assert!(matches!(parsed.builtin_categories, CategorySet::All));
        assert!(parsed.extra_rules.is_empty());
        assert!(parsed.overrides.is_empty());
        assert!(!parsed.track_stats);
        Ok(())
    }

    #[sinex_test]
    async fn category_set_deserializes_all_forms() -> ::xtask::sandbox::TestResult<()> {
        // String "all"
        let val: CategorySet = toml::from_str::<TomlWrap>("c = \"all\"").unwrap().c;
        assert!(matches!(val, CategorySet::All));

        // String "none"
        let val: CategorySet = toml::from_str::<TomlWrap>("c = \"none\"").unwrap().c;
        assert!(matches!(val, CategorySet::None));

        // Array of categories
        let val: CategorySet = toml::from_str::<TomlWrap>("c = [\"secret\", \"pii\"]")
            .unwrap()
            .c;
        match val {
            CategorySet::Only(cats) => {
                assert_eq!(cats.len(), 2);
                assert_eq!(cats[0], RuleCategory::Secret);
                assert_eq!(cats[1], RuleCategory::Pii);
            }
            other => panic!("expected Only, got {other:?}"),
        }

        // Empty array → None
        let val: CategorySet = toml::from_str::<TomlWrap>("c = []").unwrap().c;
        assert!(matches!(val, CategorySet::None));
        Ok(())
    }

    /// Helper for testing `CategorySet` deserialization in isolation.
    #[derive(Deserialize)]
    struct TomlWrap {
        c: CategorySet,
    }

    #[sinex_test]
    async fn from_file_parses_realistic_config() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("privacy.toml");
        std::fs::write(
            &path,
            r#"
enabled = true
builtin_categories = ["secret", "pii"]
default_strategy = { action = "encrypt" }
track_stats = true

[key]
file = "/tmp/test.key"

[overrides.email_address]
enabled = false

[overrides.ipv4_address]
strategy = { action = "hash" }

[[extra_rules]]
name = "my_rule"
description = "Custom pattern"
category = "custom"
matcher = { type = "regex", pattern = "CUSTOM-\\d+" }
strategy = { action = "redact", label = "<CUSTOM>" }
contexts = ["command"]
"#,
        )
        .unwrap();

        let config = PrivacyConfig::from_file(&path).unwrap();
        assert!(config.enabled);
        assert!(config.track_stats);
        assert!(matches!(config.default_strategy, Strategy::Encrypt));
        assert_eq!(config.key.key_file.as_deref(), Some("/tmp/test.key"));

        // Categories
        match &config.builtin_categories {
            CategorySet::Only(cats) => {
                assert_eq!(cats.len(), 2);
            }
            other => panic!("expected Only, got {other:?}"),
        }

        // Overrides
        assert_eq!(config.overrides.len(), 2);
        assert_eq!(config.overrides["email_address"].enabled, Some(false));
        assert!(matches!(
            config.overrides["ipv4_address"].strategy,
            Some(Strategy::Hash)
        ));

        // Extra rules
        assert_eq!(config.extra_rules.len(), 1);
        assert_eq!(config.extra_rules[0].name, "my_rule");
        Ok(())
    }

    #[sinex_test]
    async fn from_file_missing_fields_use_defaults() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("minimal.toml");
        std::fs::write(&path, "track_stats = true\n").unwrap();

        let config = PrivacyConfig::from_file(&path).unwrap();
        assert!(config.enabled); // default
        assert!(matches!(config.builtin_categories, CategorySet::All)); // default
        assert!(config.track_stats); // overridden
        Ok(())
    }

    #[sinex_test]
    async fn from_file_nonexistent_returns_error() -> ::xtask::sandbox::TestResult<()> {
        let result = PrivacyConfig::from_file(Path::new("/nonexistent/privacy.toml"));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to read"));
        Ok(())
    }

    #[sinex_test]
    async fn from_file_invalid_toml_returns_error() -> ::xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "enabled = [[[invalid").unwrap();

        let result = PrivacyConfig::from_file(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("failed to parse"));
        Ok(())
    }

    #[sinex_test]
    async fn key_config_toml_field_names() -> ::xtask::sandbox::TestResult<()> {
        // Verify the TOML-friendly field names (file/hex instead of key_file/key_hex)
        let toml_str = r#"
[key]
file = "/path/to/key"
hex = "abcd1234"
"#;
        let config: PrivacyConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.key.key_file.as_deref(), Some("/path/to/key"));
        assert_eq!(config.key.key_hex.as_deref(), Some("abcd1234"));
        Ok(())
    }
}

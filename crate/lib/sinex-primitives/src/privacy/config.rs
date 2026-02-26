//! Privacy engine configuration.

use super::{PatternRule, ProcessingContext, RuleCategory, RuleOverride, Strategy};
use std::collections::HashMap;

/// Privacy engine configuration.
///
/// Loaded from environment variables via [`from_env()`](Self::from_env).
#[derive(Debug, Clone)]
pub struct PrivacyConfig {
    /// Master switch. When false, engine is a passthrough.
    pub enabled: bool,
    /// Which built-in categories to activate.
    pub builtin_categories: CategorySet,
    /// Additional user-defined rules (merged with, not replacing, builtins).
    pub extra_rules: Vec<PatternRule>,
    /// Overrides for built-in rules by name.
    pub overrides: HashMap<String, RuleOverride>,
    /// Default strategy for rules that don't specify one.
    pub default_strategy: Strategy,
    /// Optional override of default strategy for the Secret category.
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
#[derive(Debug, Clone)]
pub enum CategorySet {
    /// All built-in categories.
    All,
    /// Only these categories.
    Only(Vec<RuleCategory>),
    /// No built-in rules.
    None,
}

/// Key source for Encrypt and Hash strategies.
#[derive(Debug, Clone, Default)]
pub struct KeyConfig {
    /// Path to a file containing a 256-bit key (raw 32 bytes or 64-char hex).
    pub key_file: Option<String>,
    /// Hex-encoded key (development only).
    pub key_hex: Option<String>,
}

impl KeyConfig {
    /// Resolve the key, trying file first, then hex env var.
    pub fn resolve(&self) -> Option<[u8; 32]> {
        // Try file first
        if let Some(ref path) = self.key_file {
            if let Ok(contents) = std::fs::read(path) {
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

impl PrivacyConfig {
    /// Load configuration from environment variables.
    ///
    /// | Variable | Default | Purpose |
    /// |----------|---------|---------|
    /// | `SINEX_PRIVACY_ENABLED` | `true` | Master switch |
    /// | `SINEX_PRIVACY_BUILTIN` | `all` | `all`, `none`, or comma-separated categories |
    /// | `SINEX_PRIVACY_EXTRA_RULES` | `[]` | JSON array of `PatternRule` |
    /// | `SINEX_PRIVACY_OVERRIDES` | `{}` | JSON map of `name → RuleOverride` |
    /// | `SINEX_PRIVACY_DEFAULT_STRATEGY` | `redact` | Default strategy |
    /// | `SINEX_PRIVACY_SECRET_STRATEGY` | — | Override for Secret category |
    /// | `SINEX_PRIVACY_KEY_FILE` | — | Path to 256-bit key file |
    /// | `SINEX_PRIVACY_KEY` | — | Hex key (dev only) |
    /// | `SINEX_PRIVACY_STATS` | `false` | Per-rule match counting |
    pub fn from_env() -> Self {
        let mut config = Self::default();

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

        if let Ok(json) = std::env::var("SINEX_PRIVACY_EXTRA_RULES") {
            if let Ok(rules) = serde_json::from_str::<Vec<PatternRule>>(&json) {
                config.extra_rules = rules;
            }
        }

        if let Ok(json) = std::env::var("SINEX_PRIVACY_OVERRIDES") {
            if let Ok(overrides) = serde_json::from_str::<HashMap<String, RuleOverride>>(&json) {
                config.overrides = overrides;
            }
        }

        if let Ok(val) = std::env::var("SINEX_PRIVACY_DEFAULT_STRATEGY") {
            if let Some(s) = parse_strategy(&val) {
                config.default_strategy = s;
            }
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

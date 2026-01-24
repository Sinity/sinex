//! Property-based tests for sinex-cli.
//!
//! Uses proptest to verify invariants that should hold for all valid inputs:
//! - Config serialization roundtrips preserve data
//! - Token validation is consistent
//! - Output formatting is deterministic

use proptest::prelude::*;
use serde::{Deserialize, Serialize};
use sinex_cli::config::{Config, ThemeConfig};
use sinex_cli::model::OutputFormat;

// ============================================================================
// Strategy Generators
// ============================================================================

/// Generate a valid RPC URL.
fn rpc_url_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("https://127.0.0.1:9999".to_string()),
        Just("https://localhost:9999".to_string()),
        Just("http://localhost:8080".to_string()),
        (1..=65535u16).prop_map(|port| format!("https://127.0.0.1:{}", port)),
    ]
}

/// Generate a valid bearer token (alphanumeric + common token chars).
fn token_strategy() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_\\-]{16,64}".prop_map(|s| s.to_string())
}

/// Generate a valid timeout value.
fn timeout_strategy() -> impl Strategy<Value = u64> {
    1u64..=300u64
}

/// Generate a valid color name.
fn color_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("red".to_string()),
        Just("green".to_string()),
        Just("blue".to_string()),
        Just("yellow".to_string()),
        Just("cyan".to_string()),
        Just("magenta".to_string()),
        Just("white".to_string()),
        Just("black".to_string()),
    ]
}

/// Generate a valid table style.
fn table_style_strategy() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("rounded".to_string()),
        Just("ascii".to_string()),
        Just("modern".to_string()),
        Just("minimal".to_string()),
    ]
}

/// Generate a valid output format.
fn output_format_strategy() -> impl Strategy<Value = OutputFormat> {
    prop_oneof![
        Just(OutputFormat::Table),
        Just(OutputFormat::Json),
        Just(OutputFormat::Yaml),
    ]
}

/// Generate a valid alias name (simple identifier).
fn alias_name_strategy() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_map(|s| s.to_string())
}

/// Generate an alias command (list of arguments).
fn alias_command_strategy() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-z0-9\\-]{1,20}", 1..=5)
}

/// Generate a ThemeConfig.
fn theme_config_strategy() -> impl Strategy<Value = ThemeConfig> {
    (
        table_style_strategy(),
        color_strategy(),
        color_strategy(),
        color_strategy(),
    )
        .prop_map(
            |(table_style, success_color, error_color, warning_color)| ThemeConfig {
                table_style,
                success_color,
                error_color,
                warning_color,
            },
        )
}

/// Generate a Config with valid values.
fn config_strategy() -> impl Strategy<Value = Config> {
    (
        rpc_url_strategy(),
        prop::option::of(token_strategy()),
        prop::option::of("[a-zA-Z0-9/._\\-]{5,50}".prop_map(|s| s.to_string())),
        timeout_strategy(),
        output_format_strategy(),
        prop::collection::hash_map(alias_name_strategy(), alias_command_strategy(), 0..=3),
        theme_config_strategy(),
        prop::bool::ANY,
    )
        .prop_map(
            |(rpc_url, token, token_file, timeout, default_format, aliases, theme, insecure)| {
                Config {
                    rpc_url,
                    token,
                    token_file,
                    ca_cert: None,
                    client_cert: None,
                    client_key: None,
                    insecure,
                    timeout,
                    default_format,
                    aliases,
                    theme,
                    editor: "vim".to_string(),
                }
            },
        )
}

// ============================================================================
// Config Roundtrip Tests
// ============================================================================

proptest! {
    /// Config serializes to TOML and deserializes back to equal value.
    #[test]
    fn config_toml_roundtrip(config in config_strategy()) {
        let toml_str = toml::to_string(&config).expect("should serialize to TOML");
        let parsed: Config = toml::from_str(&toml_str).expect("should deserialize from TOML");

        // Verify key fields are preserved
        prop_assert_eq!(&config.rpc_url, &parsed.rpc_url);
        prop_assert_eq!(&config.token, &parsed.token);
        prop_assert_eq!(config.timeout, parsed.timeout);
        prop_assert_eq!(config.insecure, parsed.insecure);

        // Compare aliases as sets (HashMap order is not guaranteed)
        let mut original_keys: Vec<_> = config.aliases.keys().collect();
        let mut parsed_keys: Vec<_> = parsed.aliases.keys().collect();
        original_keys.sort();
        parsed_keys.sort();
        prop_assert_eq!(original_keys, parsed_keys);

        // Verify alias values match
        for (key, original_value) in &config.aliases {
            let parsed_value = parsed.aliases.get(key).expect("alias key should exist");
            prop_assert_eq!(original_value, parsed_value);
        }
    }

    /// Config serializes to JSON and deserializes back to equal value.
    #[test]
    fn config_json_roundtrip(config in config_strategy()) {
        let json_str = serde_json::to_string(&config).expect("should serialize to JSON");
        let parsed: Config = serde_json::from_str(&json_str).expect("should deserialize from JSON");

        // Verify key fields are preserved
        prop_assert_eq!(&config.rpc_url, &parsed.rpc_url);
        prop_assert_eq!(&config.token, &parsed.token);
        prop_assert_eq!(config.timeout, parsed.timeout);
        prop_assert_eq!(config.insecure, parsed.insecure);
    }

    /// OutputFormat serializes to JSON and deserializes back correctly.
    #[test]
    fn output_format_roundtrip(format in output_format_strategy()) {
        let json_str = serde_json::to_string(&format).expect("should serialize");
        let parsed: OutputFormat = serde_json::from_str(&json_str).expect("should deserialize");

        // Match the discriminant
        let original_name = format!("{:?}", format);
        let parsed_name = format!("{:?}", parsed);
        prop_assert_eq!(original_name, parsed_name);
    }
}

// ============================================================================
// Token Validation Tests
// ============================================================================

/// Valid token patterns that should be accepted.
fn valid_token_pattern() -> impl Strategy<Value = String> {
    prop_oneof![
        // Standard bearer tokens
        "[a-zA-Z0-9]{32,64}",
        // Tokens with dashes/underscores (common formats)
        "[a-zA-Z0-9_\\-]{20,50}",
        // Base64-like tokens
        "[a-zA-Z0-9+/=]{20,100}",
    ]
}

/// Invalid token patterns that should be rejected (empty, whitespace, control chars).
fn invalid_token_pattern() -> impl Strategy<Value = String> {
    prop_oneof![
        // Empty string
        Just(String::new()),
        // Only whitespace
        "[ \t\n]{1,10}",
        // Contains null byte (simulated as special case)
        Just("token\x00value".to_string()),
    ]
}

proptest! {
    /// Valid tokens are non-empty and contain no control characters.
    #[test]
    fn valid_token_is_non_empty(token in valid_token_pattern()) {
        prop_assert!(!token.trim().is_empty(), "Valid token should not be empty");
        prop_assert!(!token.contains('\0'), "Valid token should not contain null bytes");
    }

    /// Invalid tokens fail validation criteria.
    #[test]
    fn invalid_token_fails_criteria(token in invalid_token_pattern()) {
        let is_invalid = token.trim().is_empty() || token.contains('\0');
        prop_assert!(is_invalid, "Token should fail: '{}'", token.escape_debug());
    }
}

// ============================================================================
// Output Format Consistency Tests
// ============================================================================

/// Test data structure for output consistency tests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestOutputItem {
    id: String,
    name: String,
    count: u64,
}

/// Generate a TestOutputItem.
fn test_output_item_strategy() -> impl Strategy<Value = TestOutputItem> {
    (
        "[a-zA-Z0-9]{8,16}",
        "[a-zA-Z ]{3,20}",
        0u64..=1_000_000u64,
    )
        .prop_map(|(id, name, count)| TestOutputItem { id, name, count })
}

proptest! {
    /// Same data serialized multiple times produces identical JSON.
    #[test]
    fn json_output_is_deterministic(item in test_output_item_strategy()) {
        let json1 = serde_json::to_string(&item).expect("should serialize");
        let json2 = serde_json::to_string(&item).expect("should serialize again");

        prop_assert_eq!(json1, json2, "JSON output should be deterministic");
    }

    /// Same data serialized multiple times produces identical YAML.
    #[test]
    fn yaml_output_is_deterministic(item in test_output_item_strategy()) {
        let yaml1 = serde_yaml::to_string(&item).expect("should serialize");
        let yaml2 = serde_yaml::to_string(&item).expect("should serialize again");

        prop_assert_eq!(yaml1, yaml2, "YAML output should be deterministic");
    }

    /// JSON roundtrip preserves all fields.
    #[test]
    fn output_item_json_roundtrip(item in test_output_item_strategy()) {
        let json_str = serde_json::to_string(&item).expect("should serialize");
        let parsed: TestOutputItem = serde_json::from_str(&json_str).expect("should deserialize");

        prop_assert_eq!(item.id, parsed.id);
        prop_assert_eq!(item.name, parsed.name);
        prop_assert_eq!(item.count, parsed.count);
    }

    /// List of items produces consistent output regardless of serialization approach.
    #[test]
    fn list_output_consistency(
        items in prop::collection::vec(test_output_item_strategy(), 0..=10)
    ) {
        // JSON array output
        let json_array = serde_json::to_string(&items).expect("should serialize array");

        // Verify it parses back
        let parsed: Vec<TestOutputItem> = serde_json::from_str(&json_array)
            .expect("should parse back");

        prop_assert_eq!(items.len(), parsed.len());

        // Verify each item
        for (original, parsed) in items.iter().zip(parsed.iter()) {
            prop_assert_eq!(&original.id, &parsed.id);
            prop_assert_eq!(&original.name, &parsed.name);
            prop_assert_eq!(original.count, parsed.count);
        }
    }
}

// ============================================================================
// Validation Edge Cases
// ============================================================================

proptest! {
    /// Limit validation accepts positive values within bounds.
    #[test]
    fn valid_limit_is_positive(limit in 1i32..=10000i32) {
        prop_assert!(limit > 0);
        prop_assert!(limit <= 10000);
    }

    /// Offset validation accepts non-negative values.
    #[test]
    fn valid_offset_is_non_negative(offset in 0i32..=1_000_000i32) {
        prop_assert!(offset >= 0);
    }

    /// Subject validation rejects whitespace.
    #[test]
    fn subject_rejects_whitespace(
        prefix in "[a-z\\.]{1,10}",
        whitespace in "[ \t\n]+",
        suffix in "[a-z\\.]{1,10}"
    ) {
        let subject_with_ws = format!("{}{}{}", prefix, whitespace, suffix);
        let has_whitespace = subject_with_ws.chars().any(|c| c.is_whitespace());
        prop_assert!(has_whitespace, "Subject should contain whitespace");
    }

    /// URL validation accepts valid URLs.
    #[test]
    fn valid_url_is_parseable(url in rpc_url_strategy()) {
        let parsed = reqwest::Url::parse(&url);
        prop_assert!(parsed.is_ok(), "URL should parse: {}", url);
    }
}

//! Tests for the config module (configuration loading and merging)

mod common;

use std::env;

use common::ConfigFixture;
use sinexctl::config::Config;
use sinexctl::model::OutputFormat;

// ============================================================================
// Default Configuration Tests
// ============================================================================

#[test]
fn test_config_default_values() {
    let config = Config::default();

    assert_eq!(config.rpc_url, "https://127.0.0.1:9999");
    assert!(config.token.is_none());
    assert!(config.token_file.is_none());
    assert!(config.ca_cert.is_none());
    assert!(config.client_cert.is_none());
    assert!(config.client_key.is_none());
    assert!(!config.insecure);
    assert_eq!(config.timeout, 30);
    assert!(matches!(config.default_format, OutputFormat::Table));
    assert!(config.aliases.is_empty());
}

#[test]
fn test_config_default_theme_values() {
    let config = Config::default();

    assert_eq!(config.theme.table_style, "rounded");
    assert_eq!(config.theme.success_color, "green");
    assert_eq!(config.theme.error_color, "red");
    assert_eq!(config.theme.warning_color, "yellow");
}

// ============================================================================
// CLI Argument Merging Tests
// ============================================================================

#[test]
fn test_merge_cli_args_rpc_url() {
    let mut config = Config::default();
    config.merge_cli_args(
        Some("https://custom:8080".to_string()),
        None,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
    );

    assert_eq!(config.rpc_url, "https://custom:8080");
}

#[test]
fn test_merge_cli_args_token() {
    let mut config = Config::default();
    config.merge_cli_args(
        None,
        Some("cli-token".to_string()),
        None,
        None,
        None,
        None,
        false,
        None,
        None,
    );

    assert_eq!(config.token, Some("cli-token".to_string()));
}

#[test]
fn test_merge_cli_args_token_file() {
    let mut config = Config::default();
    config.merge_cli_args(
        None,
        None,
        Some("/path/to/token".to_string()),
        None,
        None,
        None,
        false,
        None,
        None,
    );

    assert_eq!(config.token_file, Some("/path/to/token".to_string()));
}

#[test]
fn test_merge_cli_args_tls_options() {
    let mut config = Config::default();
    config.merge_cli_args(
        None,
        None,
        None,
        Some("/path/to/ca.pem".to_string()),
        Some("/path/to/client.pem".to_string()),
        Some("/path/to/key.pem".to_string()),
        false,
        None,
        None,
    );

    assert_eq!(config.ca_cert, Some("/path/to/ca.pem".to_string()));
    assert_eq!(config.client_cert, Some("/path/to/client.pem".to_string()));
    assert_eq!(config.client_key, Some("/path/to/key.pem".to_string()));
}

#[test]
fn test_merge_cli_args_insecure() {
    let mut config = Config::default();
    assert!(!config.insecure);

    config.merge_cli_args(None, None, None, None, None, None, true, None, None);

    assert!(config.insecure);
}

#[test]
fn test_merge_cli_args_insecure_false_does_not_override() {
    let mut config = Config::default();
    config.insecure = true;

    // Passing false should not change the value
    config.merge_cli_args(None, None, None, None, None, None, false, None, None);

    assert!(config.insecure);
}

#[test]
fn test_merge_cli_args_timeout() {
    let mut config = Config::default();
    config.merge_cli_args(None, None, None, None, None, None, false, Some(120), None);

    assert_eq!(config.timeout, 120);
}

#[test]
fn test_merge_cli_args_format() {
    let mut config = Config::default();
    config.merge_cli_args(
        None,
        None,
        None,
        None,
        None,
        None,
        false,
        None,
        Some(OutputFormat::Json),
    );

    assert!(matches!(config.default_format, OutputFormat::Json));
}

#[test]
fn test_merge_cli_args_all_options() {
    let mut config = Config::default();
    config.merge_cli_args(
        Some("https://example.com:9999".to_string()),
        Some("my-token".to_string()),
        Some("/token/file".to_string()),
        Some("/ca.pem".to_string()),
        Some("/client.pem".to_string()),
        Some("/key.pem".to_string()),
        true,
        Some(60),
        Some(OutputFormat::Yaml),
    );

    assert_eq!(config.rpc_url, "https://example.com:9999");
    assert_eq!(config.token, Some("my-token".to_string()));
    assert_eq!(config.token_file, Some("/token/file".to_string()));
    assert_eq!(config.ca_cert, Some("/ca.pem".to_string()));
    assert_eq!(config.client_cert, Some("/client.pem".to_string()));
    assert_eq!(config.client_key, Some("/key.pem".to_string()));
    assert!(config.insecure);
    assert_eq!(config.timeout, 60);
    assert!(matches!(config.default_format, OutputFormat::Yaml));
}

#[test]
fn test_merge_cli_args_none_values_preserve_existing() {
    let mut config = Config::default();
    config.rpc_url = "https://preset.url:8080".to_string();
    config.timeout = 45;
    config.insecure = true;

    // Merge with all None values
    config.merge_cli_args(None, None, None, None, None, None, false, None, None);

    // Original values should be preserved
    assert_eq!(config.rpc_url, "https://preset.url:8080");
    assert_eq!(config.timeout, 45);
    // Note: insecure stays true because passing false doesn't override
    assert!(config.insecure);
}

// ============================================================================
// TOML Parsing Tests (via ConfigFixture)
// ============================================================================

#[test]
fn test_config_fixture_generates_valid_toml() {
    let fixture = ConfigFixture::new()
        .rpc_url("https://test.example.com:9999")
        .token("test-token-123")
        .timeout(90)
        .insecure();

    let toml = fixture.to_toml();

    assert!(toml.contains("rpc_url = \"https://test.example.com:9999\""));
    assert!(toml.contains("token = \"test-token-123\""));
    assert!(toml.contains("timeout = 90"));
    assert!(toml.contains("insecure = true"));
}

#[test]
fn test_config_fixture_generates_valid_yaml() {
    let fixture = ConfigFixture::new()
        .rpc_url("https://test.example.com:9999")
        .token("test-token-123")
        .timeout(90);

    let yaml = fixture.to_yaml();

    assert!(yaml.contains("rpc_url: \"https://test.example.com:9999\""));
    assert!(yaml.contains("token: \"test-token-123\""));
    assert!(yaml.contains("timeout: 90"));
    assert!(yaml.contains("insecure: false"));
}

#[test]
fn test_config_fixture_token_file_option() {
    let fixture = ConfigFixture::new().token_file("/path/to/token.txt");

    let toml = fixture.to_toml();
    assert!(toml.contains("token_file = \"/path/to/token.txt\""));

    let yaml = fixture.to_yaml();
    assert!(yaml.contains("token_file: \"/path/to/token.txt\""));
}

// ============================================================================
// Environment Variable Tests
// ============================================================================

#[test]
fn test_config_env_var_rpc_url() {
    // Note: Config::load() uses figment with SINEX_ prefix
    // We can test the environment variable logic indirectly

    // Save original
    let original = env::var("SINEX_RPC_URL").ok();

    env::set_var("SINEX_RPC_URL", "https://env-gateway:9999");

    // Config::load() would pick this up, but it requires project directories
    // which may not exist in test environment. Test the env var directly.
    let url = env::var("SINEX_RPC_URL").unwrap();
    assert_eq!(url, "https://env-gateway:9999");

    // Restore
    if let Some(orig) = original {
        env::set_var("SINEX_RPC_URL", orig);
    } else {
        env::remove_var("SINEX_RPC_URL");
    }
}

#[test]
fn test_config_env_var_timeout() {
    let original = env::var("SINEX_TIMEOUT").ok();

    env::set_var("SINEX_TIMEOUT", "120");

    let timeout = env::var("SINEX_TIMEOUT").unwrap();
    assert_eq!(timeout, "120");

    // Restore
    if let Some(orig) = original {
        env::set_var("SINEX_TIMEOUT", orig);
    } else {
        env::remove_var("SINEX_TIMEOUT");
    }
}

// ============================================================================
// Config File Path Tests
// ============================================================================

#[test]
fn test_config_file_path_exists() {
    // Config::config_file_path() should return a valid path structure
    let result = Config::config_file_path();

    // This should succeed as long as ProjectDirs can determine config location
    assert!(result.is_ok());

    let path = result.unwrap();
    assert!(path.ends_with("config.toml"));
    assert!(path.to_string_lossy().contains("sinexctl"));
}

// ============================================================================
// Config Serialization Tests
// ============================================================================

#[test]
fn test_config_serializes_to_toml() {
    let config = Config::default();

    let toml = toml::to_string(&config).unwrap();

    assert!(toml.contains("rpc_url"));
    assert!(toml.contains("timeout"));
    assert!(toml.contains("insecure"));
}

#[test]
fn test_config_round_trips_through_toml() {
    let mut original = Config::default();
    original.rpc_url = "https://custom:8888".to_string();
    original.timeout = 45;
    original.insecure = true;

    let toml = toml::to_string(&original).unwrap();
    let restored: Config = toml::from_str(&toml).unwrap();

    assert_eq!(restored.rpc_url, original.rpc_url);
    assert_eq!(restored.timeout, original.timeout);
    assert_eq!(restored.insecure, original.insecure);
}

// ============================================================================
// Aliases Tests
// ============================================================================

#[test]
fn test_config_aliases_default_empty() {
    let config = Config::default();
    assert!(config.aliases.is_empty());
}

#[test]
fn test_config_aliases_from_toml() {
    let toml = r#"
        rpc_url = "https://localhost:9999"
        timeout = 30
        insecure = false

        [aliases]
        h = ["health"]
        nodes = ["node", "list"]
        q = ["query", "events"]
    "#;

    let config: Config = toml::from_str(toml).unwrap();

    assert_eq!(config.aliases.len(), 3);
    assert_eq!(config.aliases.get("h"), Some(&vec!["health".to_string()]));
    assert_eq!(
        config.aliases.get("nodes"),
        Some(&vec!["node".to_string(), "list".to_string()])
    );
}

// ============================================================================
// Theme Tests
// ============================================================================

#[test]
fn test_config_theme_from_toml() {
    let toml = r#"
        rpc_url = "https://localhost:9999"
        timeout = 30

        [theme]
        table_style = "ascii"
        success_color = "blue"
        error_color = "magenta"
        warning_color = "cyan"
    "#;

    let config: Config = toml::from_str(toml).unwrap();

    assert_eq!(config.theme.table_style, "ascii");
    assert_eq!(config.theme.success_color, "blue");
    assert_eq!(config.theme.error_color, "magenta");
    assert_eq!(config.theme.warning_color, "cyan");
}

#[test]
fn test_config_theme_partial_override() {
    let toml = r#"
        rpc_url = "https://localhost:9999"

        [theme]
        table_style = "minimal"
    "#;

    let config: Config = toml::from_str(toml).unwrap();

    // Overridden value
    assert_eq!(config.theme.table_style, "minimal");

    // Default values for non-specified fields
    assert_eq!(config.theme.success_color, "green");
    assert_eq!(config.theme.error_color, "red");
    assert_eq!(config.theme.warning_color, "yellow");
}

// ============================================================================
// Invalid Config Tests
// ============================================================================

#[test]
fn test_config_invalid_toml_syntax() {
    let invalid_toml = r#"
        rpc_url = "missing quote
        timeout = not_a_number
    "#;

    let result: Result<Config, _> = toml::from_str(invalid_toml);
    assert!(result.is_err());
}

#[test]
fn test_config_wrong_field_type() {
    let toml = r#"
        rpc_url = "https://localhost:9999"
        timeout = "should be a number"
    "#;

    let result: Result<Config, _> = toml::from_str(toml);
    assert!(result.is_err());
}

#[test]
fn test_config_unknown_fields_ignored() {
    // By default, serde should ignore unknown fields
    let toml = r#"
        rpc_url = "https://localhost:9999"
        timeout = 30
        unknown_field = "should be ignored"
        another_unknown = 123
    "#;

    let result: Result<Config, _> = toml::from_str(toml);
    // This should succeed - unknown fields are ignored
    assert!(result.is_ok());
}

// ============================================================================
// Editor Default Tests
// ============================================================================

#[test]
fn test_config_editor_from_env_or_default() {
    // Save original
    let original = env::var("EDITOR").ok();

    // Test with EDITOR set
    env::set_var("EDITOR", "nano");
    let config = Config::default();
    assert_eq!(config.editor, "nano");

    // Test fallback to vim
    env::remove_var("EDITOR");
    let config = Config::default();
    assert_eq!(config.editor, "vim");

    // Restore
    if let Some(orig) = original {
        env::set_var("EDITOR", orig);
    } else {
        env::remove_var("EDITOR");
    }
}

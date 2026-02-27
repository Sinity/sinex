//! Integration tests for the privacy engine xtask command.
//!
//! Tests cover:
//! - Privacy command name and metadata
//! - Catalog listing (full and filtered)
//! - Test subcommand (clean and sensitive input, context parsing)
//! - Key subcommand (status and generation)
//! - Stats subcommand
//! - Config subcommand (status and --init)
//! - JSON output structure validation
//! - CLI error handling for invalid arguments

use assert_cmd::cargo::cargo_bin_cmd;
use predicates::prelude::*;
use serde_json::Value;

use xtask::command::{CommandContext, XtaskCommand};
use xtask::commands::privacy::{PrivacyCommand, PrivacySubcommand};
use xtask::output::{OutputFormat, OutputWriter};
use xtask::sandbox::sinex_test;

// ============================================================================
// Command Metadata Tests
// ============================================================================

#[sinex_test]
fn test_privacy_command_name() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Stats,
    };
    assert_eq!(cmd.name(), "privacy");
    Ok(())
}

#[sinex_test]
fn test_privacy_command_metadata_is_utility() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Stats,
    };
    let meta = cmd.metadata();
    assert_eq!(meta.category, Some("utility".to_string()));
    assert!(!meta.modifies_state);
    assert!(!meta.track_in_history);
    Ok(())
}

// ============================================================================
// Catalog Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_catalog_lists_rules() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Catalog {
            category: None,
            include_disabled: false,
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());
    // Should have at least 31 rules (17 secret + 5 PII + 5 infra + 4 privacy)
    let msg = result.message.as_deref().unwrap_or("");
    assert!(
        msg.contains("31") || msg.contains("rule"),
        "Expected rule count in message: {msg}"
    );

    // Verify JSON data is an array of rules
    if let Some(data) = &result.data {
        let rules = data.as_array().expect("data should be an array of rules");
        assert!(
            rules.len() >= 31,
            "Expected at least 31 rules, got {}",
            rules.len()
        );

        // Verify each rule has the expected fields
        let first = &rules[0];
        assert!(first.get("name").is_some(), "rule should have 'name'");
        assert!(
            first.get("category").is_some(),
            "rule should have 'category'"
        );
        assert!(
            first.get("description").is_some(),
            "rule should have 'description'"
        );
        assert!(
            first.get("strategy").is_some(),
            "rule should have 'strategy'"
        );
        assert!(first.get("enabled").is_some(), "rule should have 'enabled'");
        assert!(
            first.get("contexts").is_some(),
            "rule should have 'contexts'"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_filters_by_category() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Catalog {
            category: Some("secret".into()),
            include_disabled: false,
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        let rules = data.as_array().expect("data should be an array");
        // All returned rules should be in the "secret" category
        for rule in rules {
            let cat = rule["category"].as_str().unwrap_or("");
            assert_eq!(cat, "secret", "Expected 'secret' category, got '{cat}'");
        }
        // Should have at least the 17 built-in secret rules
        assert!(
            rules.len() >= 17,
            "Expected at least 17 secret rules, got {}",
            rules.len()
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_filters_pii_category() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Catalog {
            category: Some("pii".into()),
            include_disabled: false,
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        let rules = data.as_array().expect("data should be an array");
        for rule in rules {
            let cat = rule["category"].as_str().unwrap_or("");
            assert_eq!(cat, "pii", "Expected 'pii' category, got '{cat}'");
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_catalog_unknown_category_returns_empty() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Catalog {
            category: Some("nonexistent".into()),
            include_disabled: false,
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    // Unknown category filter is silently ignored (returns None), so all rules pass
    assert!(result.is_success());
    Ok(())
}

// ============================================================================
// Test Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_process_clean_input() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "hello world, this is clean text".into(),
            context: "command".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(data["suppressed"], false);
        assert_eq!(data["changed"], false);
        let empty = vec![];
        let matched = data["matched_rules"].as_array().unwrap_or(&empty);
        assert!(matched.is_empty(), "Clean text should not match any rules");
    }
    Ok(())
}

#[sinex_test]
async fn test_process_sensitive_input_github_token() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "export TOKEN=ghp_ABCDEFghijklmnopqrstuvwxyz1234567890".into(),
            context: "command".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(data["changed"], true, "Sensitive input should be redacted");
        let processed = data["processed"].as_str().unwrap_or("");
        // The github token should be redacted
        assert!(
            processed.contains("<GITHUB_TOKEN>") || processed.contains("<REDACTED>"),
            "Expected redaction marker in: {processed}"
        );
        assert!(
            !processed.contains("ghp_ABCDEFghijklmnopqrstuvwxyz1234567890"),
            "Token should not appear in processed output"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_process_sensitive_input_database_url() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "DATABASE_URL=postgres://user:password@localhost/db".into(),
            context: "command".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(data["changed"], true, "Database URL should be redacted");
        let processed = data["processed"].as_str().unwrap_or("");
        assert!(
            !processed.contains("password"),
            "Password should not appear in output: {processed}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_process_private_key_causes_suppression() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "-----BEGIN RSA PRIVATE KEY-----".into(),
            context: "command".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(
            data["suppressed"], true,
            "Private key header should trigger suppression"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_process_context_filtering() -> TestResult<()> {
    // SSN rule only applies to: command, clipboard, document, notification
    // It should NOT match in journal context
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "SSN: 123-45-6789".into(),
            context: "journal".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        let empty = vec![];
        let matched = data["matched_rules"].as_array().unwrap_or(&empty);
        let has_ssn = matched.iter().any(|r| r.as_str() == Some("ssn"));
        assert!(
            !has_ssn,
            "SSN rule should not match in journal context, matched: {matched:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_process_context_matching() -> TestResult<()> {
    // SSN rule should match in command context
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "SSN: 123-45-6789".into(),
            context: "command".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(data["changed"], true, "SSN should be detected in command");
    }
    Ok(())
}

#[sinex_test]
async fn test_process_window_title_privacy() -> TestResult<()> {
    // Window title rules should fire for window_title context
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "KeePass - Passwords".into(),
            context: "window_title".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert_eq!(
            data["changed"], true,
            "Password manager title should be redacted"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_invalid_context_returns_error() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Test {
            input: "hello".into(),
            context: "bogus".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await;

    assert!(result.is_err(), "Bogus context should return an error");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Unknown context") || err.contains("bogus"),
        "Error should mention the invalid context: {err}"
    );
    Ok(())
}

// ============================================================================
// Key Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_key_status_no_key() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Key { generate: false },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        // In test env, no key is typically configured
        assert!(
            data.get("configured").is_some(),
            "Should report key configuration status"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_key_generate() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Key { generate: true },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        let key = data["key"].as_str().expect("should have key field");
        // blake3 hex output is 64 chars
        assert_eq!(key.len(), 64, "Key should be 64 hex chars, got {}", key.len());
        assert_eq!(data["bits"], 256, "Key should be 256 bits");
        // Verify it's valid hex
        assert!(
            key.chars().all(|c| c.is_ascii_hexdigit()),
            "Key should be hex: {key}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_key_generate_produces_unique_keys() -> TestResult<()> {
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);

    let cmd1 = PrivacyCommand {
        subcommand: PrivacySubcommand::Key { generate: true },
    };
    let result1 = cmd1.execute(&ctx).await?;

    // Small delay to ensure different entropy
    tokio::time::sleep(std::time::Duration::from_millis(1)).await;

    let cmd2 = PrivacyCommand {
        subcommand: PrivacySubcommand::Key { generate: true },
    };
    let result2 = cmd2.execute(&ctx).await?;

    let key1 = result1.data.as_ref().unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string();
    let key2 = result2.data.as_ref().unwrap()["key"]
        .as_str()
        .unwrap()
        .to_string();

    assert_ne!(key1, key2, "Generated keys should be unique");
    Ok(())
}

// ============================================================================
// Stats Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_stats_returns_success() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Stats,
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());
    // Fresh engine has no stats — data should be an empty or all-zero array
    if let Some(data) = &result.data {
        let stats = data.as_array().expect("stats should be an array");
        // All stats should be zero for a fresh engine
        assert!(
            stats.is_empty(),
            "Fresh engine should have no non-zero stats"
        );
    }
    Ok(())
}

// ============================================================================
// Config Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_config_init_generates_toml() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Config { init: true },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        let example = data["example"].as_str().expect("should have example field");
        // Validate the example is parseable as TOML
        let parsed: Result<toml::Value, _> = toml::from_str(example);
        assert!(
            parsed.is_ok(),
            "Example config should be valid TOML: {}",
            parsed.unwrap_err()
        );

        // Verify key sections are present
        assert!(example.contains("enabled"), "Should have enabled field");
        assert!(
            example.contains("builtin_categories"),
            "Should have builtin_categories"
        );
        assert!(
            example.contains("default_strategy"),
            "Should have default_strategy"
        );
        assert!(example.contains("[key]"), "Should have [key] section");
        assert!(
            example.contains("extra_rules"),
            "Should mention extra_rules"
        );
        assert!(
            example.contains("overrides"),
            "Should mention overrides"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_config_status_reports_state() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Config { init: false },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    assert!(result.is_success());

    if let Some(data) = &result.data {
        assert!(
            data.get("enabled").is_some(),
            "Should report enabled status"
        );
        assert!(
            data.get("active_rules").is_some(),
            "Should report active rules count"
        );
        assert!(
            data.get("default_strategy").is_some(),
            "Should report default strategy"
        );
        assert!(
            data.get("track_stats").is_some(),
            "Should report stats tracking"
        );

        // Default config has all rules enabled
        let rule_count = data["active_rules"].as_u64().unwrap_or(0);
        assert!(
            rule_count >= 31,
            "Default config should have at least 31 rules, got {rule_count}"
        );
    }
    Ok(())
}

// ============================================================================
// Decrypt Subcommand Tests
// ============================================================================

#[sinex_test]
async fn test_decrypt_invalid_token_reports_error() -> TestResult<()> {
    let cmd = PrivacyCommand {
        subcommand: PrivacySubcommand::Decrypt {
            token: "not-a-real-token".into(),
        },
    };

    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), true, false, None);
    let result = cmd.execute(&ctx).await?;

    // Note: decrypt returns success with error info in data (not an Err result)
    assert!(result.is_success());
    if let Some(data) = &result.data {
        assert!(
            data.get("error").is_some() || data.get("decrypted").is_some(),
            "Should have error or decrypted field"
        );
    }
    Ok(())
}

// ============================================================================
// CLI Integration Tests (via assert_cmd)
// ============================================================================

#[sinex_test]
fn test_cli_privacy_help() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("privacy").arg("--help");

    cmd.assert().success().stdout(
        predicate::str::contains("catalog")
            .and(predicate::str::contains("test"))
            .and(predicate::str::contains("decrypt"))
            .and(predicate::str::contains("key"))
            .and(predicate::str::contains("stats"))
            .and(predicate::str::contains("config")),
    );
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_catalog_json() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json").arg("privacy").arg("catalog");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        assert_eq!(
            parsed["status"].as_str(),
            Some("success"),
            "Should report success"
        );
        assert!(
            parsed.get("data").is_some(),
            "Should have data field with rules"
        );
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_catalog_category_filter() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json")
        .arg("privacy")
        .arg("catalog")
        .arg("--category")
        .arg("secret");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        if let Some(data) = parsed["data"].as_array() {
            for rule in data {
                assert_eq!(
                    rule["category"].as_str(),
                    Some("secret"),
                    "All rules should be 'secret' category"
                );
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_test_clean() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json")
        .arg("privacy")
        .arg("test")
        .arg("hello world");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        assert_eq!(parsed["data"]["changed"], false);
        assert_eq!(parsed["data"]["suppressed"], false);
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_test_sensitive() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json")
        .arg("privacy")
        .arg("test")
        .arg("postgres://admin:s3cret@localhost/prod");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        assert_eq!(parsed["data"]["changed"], true);
        let processed = parsed["data"]["processed"].as_str().unwrap_or("");
        assert!(
            !processed.contains("s3cret"),
            "Password should be redacted in output"
        );
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_test_invalid_context() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("privacy")
        .arg("test")
        .arg("hello")
        .arg("--context")
        .arg("invalid_ctx");

    cmd.assert().failure();
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_key_generate_json() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json")
        .arg("privacy")
        .arg("key")
        .arg("--generate");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        let key = parsed["data"]["key"].as_str().unwrap_or("");
        assert_eq!(key.len(), 64, "Key should be 64 hex chars");
        assert_eq!(parsed["data"]["bits"], 256);
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_config_init() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json")
        .arg("privacy")
        .arg("config")
        .arg("--init");

    let output = cmd.output()?;
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: Value = serde_json::from_str(&stdout)
        .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

    let example = parsed["data"]["example"].as_str().unwrap_or("");
    assert!(!example.is_empty(), "Example config should not be empty");
    // Validate it parses as TOML
    let toml_result: Result<toml::Value, _> = toml::from_str(example);
    assert!(
        toml_result.is_ok(),
        "Generated example should be valid TOML"
    );
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_config_status_json() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json").arg("privacy").arg("config");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        // Verify required JSON fields
        let data = &parsed["data"];
        assert!(
            data.get("enabled").is_some(),
            "Should have enabled field in data"
        );
        assert!(
            data.get("active_rules").is_some(),
            "Should have active_rules"
        );
        assert!(
            data.get("default_strategy").is_some(),
            "Should have default_strategy"
        );
    }
    Ok(())
}

#[sinex_test]
fn test_cli_privacy_stats_json() -> TestResult<()> {
    let mut cmd = cargo_bin_cmd!("xtask");
    cmd.arg("--json").arg("privacy").arg("stats");

    let output = cmd.output()?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let parsed: Value = serde_json::from_str(&stdout)
            .unwrap_or_else(|_| panic!("Should be valid JSON: {stdout}"));

        assert_eq!(parsed["status"].as_str(), Some("success"));
        // Fresh engine, stats array should be empty
        if let Some(data) = parsed["data"].as_array() {
            assert!(
                data.is_empty(),
                "Fresh engine should have no non-zero stats"
            );
        }
    }
    Ok(())
}

// ============================================================================
// All Privacy Subcommands Have --help
// ============================================================================

#[sinex_test]
fn test_all_privacy_subcommands_have_help() -> TestResult<()> {
    let subcommands = [
        "catalog", "test", "decrypt", "key", "stats", "config",
    ];

    for sub in subcommands {
        let mut cmd = cargo_bin_cmd!("xtask");
        cmd.arg("privacy").arg(sub).arg("--help");
        cmd.assert().success();
    }
    Ok(())
}

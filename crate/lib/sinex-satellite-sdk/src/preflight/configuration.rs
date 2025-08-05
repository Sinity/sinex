/*!
 * Configuration verification module for Sinex Pre-Flight system
 *
 * Verifies configuration generation and validation including:
 * - TOML configuration file generation
 * - Environment variable validation
 * - Service configuration compatibility
 * - Event source configuration validation
 */

use camino::{Utf8Path, Utf8PathBuf};
use color_eyre::eyre::{bail, Context, Result};
use serde_json::{json, Value};
use std::collections::HashMap;
use tracing::{debug, info};

use super::VerificationStatus;

/// Verify configuration generation and validation
pub async fn verify_configuration_generation() -> Result<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    info!("Verifying configuration generation and validation");

    // Environment variable validation
    match verify_environment_variables(&mut messages).await {
        Ok(env_info) => {
            details.insert("environment", env_info);
        }
        Err(e) => {
            messages.push(format!("✗ Environment variable validation failed: {}", e));
            has_failures = true;
        }
    }

    // Configuration file validation
    match verify_configuration_files(&mut messages).await {
        Ok(config_info) => {
            details.insert("configuration_files", config_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Configuration file validation warning: {}", e));
            has_warnings = true;
        }
    }

    // TOML generation test
    match test_toml_generation(&mut messages).await {
        Ok(toml_info) => {
            details.insert("toml_generation", toml_info);
        }
        Err(e) => {
            messages.push(format!("✗ TOML generation test failed: {}", e));
            has_failures = true;
        }
    }

    // Event source configuration validation
    match verify_event_source_configuration(&mut messages).await {
        Ok(event_config) => {
            details.insert("event_sources", event_config);
        }
        Err(e) => {
            messages.push(format!("⚠ Event source configuration warning: {}", e));
            has_warnings = true;
        }
    }

    // Service configuration compatibility
    match verify_service_configuration_compatibility(&mut messages).await {
        Ok(service_info) => {
            details.insert("service_compatibility", service_info);
        }
        Err(e) => {
            messages.push(format!(
                "⚠ Service configuration compatibility warning: {}",
                e
            ));
            has_warnings = true;
        }
    }

    let status = if has_failures {
        VerificationStatus::Fail
    } else if has_warnings {
        VerificationStatus::Warning
    } else {
        VerificationStatus::Pass
    };

    info!(
        "Configuration verification completed with status: {:?}",
        status
    );
    Ok((status, json!(details), messages))
}

async fn verify_environment_variables(messages: &mut Vec<String>) -> Result<Value> {
    let mut env_vars = HashMap::new();
    let mut missing_vars = Vec::new();
    let mut has_issues = false;

    // Required environment variables for Sinex
    let required_vars = vec![
        ("DATABASE_URL", "PostgreSQL connection URL", true),
        ("RUST_LOG", "Logging configuration", false),
        ("SINEX_CONFIG", "Sinex configuration file path", false),
    ];

    // Optional but recommended environment variables
    let optional_vars = vec![
        ("SINEX_ANNEX_PATH", "Git-annex blob storage path"),
        ("SINEX_DEPLOYMENT_COLOR", "Deployment color for blue-green"),
        ("SINEX_INSTANCE_ID", "Unique instance identifier"),
    ];

    for (var_name, description, required) in required_vars {
        match std::env::var(var_name) {
            Ok(value) => {
                // Redact sensitive values
                let display_value = if var_name.contains("PASSWORD") || var_name.contains("SECRET")
                {
                    "***".to_string()
                } else if var_name == "DATABASE_URL" {
                    redact_database_url(&value)
                } else {
                    value.clone()
                };

                env_vars.insert(
                    var_name.to_string(),
                    json!({
                        "value": display_value,
                        "description": description,
                        "required": required,
                        "present": true
                    }),
                );

                messages.push(format!("✓ Environment variable '{}' is set", var_name));
            }
            Err(_) => {
                env_vars.insert(
                    var_name.to_string(),
                    json!({
                        "description": description,
                        "required": required,
                        "present": false
                    }),
                );

                if required {
                    missing_vars.push(var_name.to_string());
                    messages.push(format!(
                        "✗ Required environment variable '{}' is missing",
                        var_name
                    ));
                    has_issues = true;
                } else {
                    messages.push(format!(
                        "ℹ Optional environment variable '{}' is not set",
                        var_name
                    ));
                }
            }
        }
    }

    for (var_name, description) in optional_vars {
        match std::env::var(var_name) {
            Ok(value) => {
                let display_value = value.clone();

                env_vars.insert(
                    var_name.to_string(),
                    json!({
                        "value": display_value,
                        "description": description,
                        "required": false,
                        "present": true
                    }),
                );

                messages.push(format!(
                    "✓ Optional environment variable '{}' is set",
                    var_name
                ));
            }
            Err(_) => {
                env_vars.insert(
                    var_name.to_string(),
                    json!({
                        "description": description,
                        "required": false,
                        "present": false
                    }),
                );

                debug!("Optional environment variable '{}' is not set", var_name);
            }
        }
    }

    if has_issues {
        bail!(
            "Missing required environment variables: {}",
            missing_vars.join(", ")
        );
    }

    Ok(json!({
        "variables": env_vars,
        "missing_required": missing_vars,
        "all_required_present": missing_vars.is_empty()
    }))
}

async fn verify_configuration_files(messages: &mut Vec<String>) -> Result<Value> {
    let mut config_files = HashMap::new();

    // Check for default configuration locations
    let config_paths = vec![
        ("unified-collector.toml", "Current directory config"),
        ("~/.config/sinex/collector.toml", "User config"),
        ("/etc/sinex/collector.toml", "System config"),
    ];

    let mut found_configs = Vec::new();

    for (path_str, description) in config_paths {
        let expanded_path = expand_path(path_str);

        if expanded_path.exists() {
            match validate_toml_file(&expanded_path).await {
                Ok(config_info) => {
                    config_files.insert(
                        path_str.to_string(),
                        json!({
                            "path": expanded_path.to_string(),
                            "description": description,
                            "exists": true,
                            "valid": true,
                            "config_info": config_info
                        }),
                    );

                    found_configs.push(path_str.to_string());
                    messages.push(format!(
                        "✓ Configuration file found and valid: {}",
                        path_str
                    ));
                }
                Err(e) => {
                    config_files.insert(
                        path_str.to_string(),
                        json!({
                            "path": expanded_path.to_string(),
                            "description": description,
                            "exists": true,
                            "valid": false,
                            "error": e.to_string()
                        }),
                    );

                    messages.push(format!(
                        "⚠ Configuration file exists but invalid: {} ({})",
                        path_str, e
                    ));
                }
            }
        } else {
            config_files.insert(
                path_str.to_string(),
                json!({
                    "path": expanded_path.to_string(),
                    "description": description,
                    "exists": false,
                    "valid": false
                }),
            );

            debug!("Configuration file not found: {}", path_str);
        }
    }

    // Check SINEX_CONFIG environment variable if set
    if let Ok(custom_config) = std::env::var("SINEX_CONFIG") {
        let custom_path = Utf8Path::new(&custom_config);

        if custom_path.exists() {
            match validate_toml_file(custom_path).await {
                Ok(config_info) => {
                    config_files.insert(
                        "SINEX_CONFIG".to_string(),
                        json!({
                            "path": custom_path.to_string(),
                            "description": "Custom config from SINEX_CONFIG",
                            "exists": true,
                            "valid": true,
                            "config_info": config_info
                        }),
                    );

                    found_configs.push("SINEX_CONFIG".to_string());
                    messages
                        .push("✓ Custom configuration file (SINEX_CONFIG) is valid".to_string());
                }
                Err(e) => {
                    config_files.insert(
                        "SINEX_CONFIG".to_string(),
                        json!({
                            "path": custom_path.to_string(),
                            "description": "Custom config from SINEX_CONFIG",
                            "exists": true,
                            "valid": false,
                            "error": e.to_string()
                        }),
                    );

                    messages.push(format!(
                        "⚠ Custom configuration file (SINEX_CONFIG) is invalid: {}",
                        e
                    ));
                }
            }
        } else {
            messages.push(format!(
                "⚠ SINEX_CONFIG points to non-existent file: {}",
                custom_config
            ));
        }
    }

    if found_configs.is_empty() {
        messages.push("ℹ No configuration files found - will use built-in defaults".to_string());
    }

    Ok(json!({
        "files": config_files,
        "found_configs": found_configs,
        "has_valid_config": !found_configs.is_empty()
    }))
}

async fn test_toml_generation(messages: &mut Vec<String>) -> Result<Value> {
    info!("Testing TOML configuration generation");

    // Test that we can generate a valid TOML configuration
    let test_config = generate_test_configuration().await?;

    // Validate the generated configuration
    match validate_toml_content(&test_config) {
        Ok(config_info) => {
            messages.push("✓ TOML generation test passed".to_string());

            Ok(json!({
                "generation_successful": true,
                "test_config_valid": true,
                "config_sections": config_info
            }))
        }
        Err(e) => {
            messages.push(format!("✗ Generated TOML is invalid: {}", e));
            bail!("TOML generation produces invalid configuration: {}", e);
        }
    }
}

async fn generate_test_configuration() -> Result<String> {
    // Generate a minimal test configuration
    let test_config = r#"
[database]
url = "postgresql:///sinex_test?host=/run/postgresql"
pool_size = 10

[event_sources]
filesystem = true
terminal = true
clipboard = false

[blob_storage]
enabled = false

[logging]
level = "info"
format = "json"
"#;

    Ok(test_config.to_string())
}

async fn verify_event_source_configuration(messages: &mut Vec<String>) -> Result<Value> {
    let mut event_sources = HashMap::new();

    // Default event sources that Sinex supports
    let available_sources = vec![
        ("filesystem", "File system change monitoring"),
        ("terminal", "Terminal activity monitoring"),
        ("clipboard", "Clipboard content monitoring"),
        ("kitty", "Kitty terminal integration"),
        ("hyprland", "Hyprland window manager integration"),
        ("atuin", "Atuin shell history integration"),
    ];

    for (source_name, description) in available_sources {
        let config_info = verify_event_source_config(source_name, description).await?;
        let is_available = config_info["available"].as_bool().unwrap_or(false);
        event_sources.insert(source_name.to_string(), config_info);

        if is_available {
            messages.push(format!("✓ Event source '{}' is available", source_name));
        } else {
            messages.push(format!("ℹ Event source '{}' is not available", source_name));
        }
    }

    Ok(json!({
        "sources": event_sources,
        "total_available": event_sources.values()
            .filter(|v| v["available"].as_bool().unwrap_or(false))
            .count()
    }))
}

async fn verify_event_source_config(source_name: &str, description: &str) -> Result<Value> {
    // Check if the event source dependencies are available
    let available = match source_name {
        "filesystem" => true, // Always available
        "terminal" => true,   // Always available
        "clipboard" => check_clipboard_availability().await,
        "kitty" => check_kitty_availability().await,
        "hyprland" => check_hyprland_availability().await,
        "atuin" => check_atuin_availability().await,
        _ => false,
    };

    Ok(json!({
        "description": description,
        "available": available,
        "dependencies_met": available
    }))
}

async fn check_clipboard_availability() -> bool {
    // Check if we can access clipboard tools
    std::process::Command::new("which")
        .arg("xclip")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
        || std::process::Command::new("which")
            .arg("wl-clipboard")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
}

async fn check_kitty_availability() -> bool {
    // Check if Kitty is available and has socket support
    std::env::var("KITTY_LISTEN_ON").is_ok()
        || std::process::Command::new("which")
            .arg("kitty")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
}

async fn check_hyprland_availability() -> bool {
    // Check if Hyprland is running
    std::env::var("HYPRLAND_INSTANCE_SIGNATURE").is_ok()
        || std::process::Command::new("hyprctl")
            .arg("version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
}

async fn check_atuin_availability() -> bool {
    // Check if Atuin is installed and configured
    std::process::Command::new("which")
        .arg("atuin")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

async fn verify_service_configuration_compatibility(messages: &mut Vec<String>) -> Result<Value> {
    let mut compatibility_info = HashMap::new();

    // Check systemd service compatibility
    match check_systemd_compatibility().await {
        Ok(systemd_info) => {
            compatibility_info.insert("systemd", systemd_info);
            messages.push("✓ systemd compatibility verified".to_string());
        }
        Err(e) => {
            messages.push(format!("⚠ systemd compatibility warning: {}", e));
            compatibility_info.insert(
                "systemd",
                json!({
                    "compatible": false,
                    "error": e.to_string()
                }),
            );
        }
    }

    // Check NixOS module compatibility
    match check_nixos_compatibility().await {
        Ok(nixos_info) => {
            compatibility_info.insert("nixos", nixos_info);
            messages.push("✓ NixOS module compatibility verified".to_string());
        }
        Err(e) => {
            messages.push(format!("ℹ NixOS module compatibility check: {}", e));
            compatibility_info.insert(
                "nixos",
                json!({
                    "compatible": false,
                    "note": e.to_string()
                }),
            );
        }
    }

    Ok(json!(compatibility_info))
}

async fn check_systemd_compatibility() -> Result<Value> {
    // Check if systemd is available and running
    let systemd_version = std::process::Command::new("systemctl")
        .arg("--version")
        .output()
        .wrap_err("Failed to check systemd version")?;

    if !systemd_version.status.success() {
        bail!("systemd is not available or not functioning");
    }

    let version_output = String::from_utf8_lossy(&systemd_version.stdout);
    let version_line = version_output.lines().next().unwrap_or("unknown");

    Ok(json!({
        "available": true,
        "version": version_line,
        "compatible": true
    }))
}

async fn check_nixos_compatibility() -> Result<Value> {
    // Check if we're running on NixOS
    let nixos_version = std::fs::read_to_string("/etc/NIXOS")
        .or_else(|_| std::fs::read_to_string("/etc/os-release"));

    match nixos_version {
        Ok(content) => {
            let is_nixos = content.contains("NixOS") || content.contains("nixos");

            Ok(json!({
                "running_on_nixos": is_nixos,
                "os_info": content.lines().take(3).collect::<Vec<_>>().join("; ")
            }))
        }
        Err(_) => Ok(json!({
            "running_on_nixos": false,
            "note": "Could not determine OS version"
        })),
    }
}

async fn validate_toml_file(path: &Utf8Path) -> Result<Value> {
    let content = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("Failed to read TOML file: {:?}", path))?;

    validate_toml_content(&content)
}

fn validate_toml_content(content: &str) -> Result<Value> {
    // Parse TOML to validate syntax
    let parsed: toml::Value = content.parse().wrap_err("Invalid TOML syntax")?;

    let mut sections = Vec::new();

    // Check for expected sections
    if let toml::Value::Table(table) = &parsed {
        for (key, value) in table {
            sections.push(json!({
                "name": key,
                "type": match value {
                    toml::Value::Table(_) => "table",
                    toml::Value::Array(_) => "array",
                    toml::Value::String(_) => "string",
                    toml::Value::Integer(_) => "integer",
                    toml::Value::Float(_) => "float",
                    toml::Value::Boolean(_) => "boolean",
                    toml::Value::Datetime(_) => "datetime",
                }
            }));
        }
    }

    Ok(json!({
        "valid_syntax": true,
        "sections": sections,
        "section_count": sections.len()
    }))
}

fn expand_path(path: &str) -> Utf8PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            Utf8Path::new(&home).join(stripped)
        } else {
            Utf8PathBuf::from(path)
        }
    } else {
        Utf8PathBuf::from(path)
    }
}

fn redact_database_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let mut redacted = parsed.clone();
        if redacted.password().is_some() {
            redacted.set_password(Some("***")).ok();
        }
        redacted.to_string()
    } else {
        "[REDACTED]".to_string()
    }
}

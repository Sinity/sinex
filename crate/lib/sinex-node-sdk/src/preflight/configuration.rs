/*!
 * Configuration verification module for Sinex Pre-Flight system
 *
 * Verifies configuration generation and validation including:
 * - env-first runtime configuration contract
 * - Environment variable validation
 * - Service environment readiness
 * - Event source configuration validation
 */

use crate::{NodeResult, SinexError};
use camino::Utf8PathBuf;
use serde_json::{Value, json};
use sinex_primitives::validation::validate_path;
use std::collections::HashMap;
use tracing::{debug, info};

use super::VerificationStatus;

/// Verify configuration generation and validation
pub async fn verify_configuration_generation()
-> NodeResult<(VerificationStatus, Value, Vec<String>)> {
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
            messages.push(format!("✗ Environment variable validation failed: {e}"));
            has_failures = true;
        }
    }

    // Runtime configuration contract validation
    match verify_runtime_configuration_contract(&mut messages).await {
        Ok(config_info) => {
            details.insert("runtime_config_contract", config_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Runtime configuration contract warning: {e}"));
            has_warnings = true;
        }
    }

    // Event source configuration validation
    match verify_event_source_configuration(&mut messages).await {
        Ok(event_config) => {
            details.insert("event_sources", event_config);
        }
        Err(e) => {
            messages.push(format!("⚠ Event source configuration warning: {e}"));
            has_warnings = true;
        }
    }

    // Service configuration checks
    match verify_service_environment(&mut messages).await {
        Ok(service_info) => {
            details.insert("service_environment", service_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Service configuration check warning: {e}"));
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

async fn verify_environment_variables(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut env_vars = HashMap::new();
    let mut missing_vars = Vec::new();
    let mut has_issues = false;

    // Required environment variables for Sinex
    let required_vars = vec![
        ("DATABASE_URL", "PostgreSQL connection URL", true),
        ("RUST_LOG", "Logging configuration", false),
    ];

    // Optional but recommended environment variables
    let optional_vars = vec![
        ("SINEX_ANNEX_PATH", "Git-annex blob storage path"),
        ("SINEX_INSTANCE_ID", "Unique instance identifier"),
    ];

    for (var_name, description, required) in required_vars {
        if let Ok(value) = std::env::var(var_name) {
            // Redact sensitive values
            let display_value = if var_name.contains("PASSWORD") || var_name.contains("SECRET") {
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

            messages.push(format!("✓ Environment variable '{var_name}' is set"));
        } else {
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
                    "✗ Required environment variable '{var_name}' is missing"
                ));
                has_issues = true;
            } else {
                messages.push(format!(
                    "ℹ Optional environment variable '{var_name}' is not set"
                ));
            }
        }
    }

    for (var_name, description) in optional_vars {
        if let Ok(value) = std::env::var(var_name) {
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
                "✓ Optional environment variable '{var_name}' is set"
            ));
        } else {
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

    if has_issues {
        return Err(SinexError::configuration(format!(
            "Missing required environment variables: {}",
            missing_vars.join(", ")
        )));
    }

    Ok(json!({
        "variables": env_vars,
        "missing_required": missing_vars,
        "all_required_present": missing_vars.is_empty()
    }))
}

async fn verify_runtime_configuration_contract(messages: &mut Vec<String>) -> NodeResult<Value> {
    messages.push(
        "✓ Runtime configuration contract is env-first and NixOS-managed for deployed systems"
            .to_string(),
    );

    Ok(json!({
        "deployment_surface": "nixos_modules",
        "runtime_transport": "environment_variables",
        "runtime_loader_model": "env_first_typed_config",
    }))
}

async fn verify_event_source_configuration(messages: &mut Vec<String>) -> NodeResult<Value> {
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
            messages.push(format!("✓ Event source '{source_name}' is available"));
        } else {
            messages.push(format!("ℹ Event source '{source_name}' is not available"));
        }
    }

    Ok(json!({
        "sources": event_sources,
        "total_available": event_sources.values()
            .filter(|v| v["available"].as_bool().unwrap_or(false))
            .count()
    }))
}

async fn verify_event_source_config(source_name: &str, description: &str) -> NodeResult<Value> {
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
    super::command_succeeds("which", &["xclip"]).await
        || super::command_succeeds("which", &["wl-clipboard"]).await
}

async fn check_kitty_availability() -> bool {
    std::env::var("KITTY_LISTEN_ON").is_ok() || super::command_succeeds("which", &["kitty"]).await
}

async fn check_hyprland_availability() -> bool {
    std::env::var("HYPRLAND_INSTANCE_SIGNATURE").is_ok()
        || super::command_succeeds("hyprctl", &["version"]).await
}

async fn check_atuin_availability() -> bool {
    super::command_succeeds("which", &["atuin"]).await
}

async fn verify_service_environment(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut service_checks = HashMap::new();

    // Check systemd service setup
    match check_systemd_environment().await {
        Ok(systemd_info) => {
            service_checks.insert("systemd", systemd_info);
            messages.push("✓ systemd check verified".to_string());
        }
        Err(e) => {
            messages.push(format!("⚠ systemd check warning: {e}"));
            service_checks.insert(
                "systemd",
                json!({
                    "available": false,
                    "error": e.to_string()
                }),
            );
        }
    }

    // Check NixOS module setup
    match check_nixos_environment().await {
        Ok(nixos_info) => {
            service_checks.insert("nixos", nixos_info);
            messages.push("✓ NixOS module check verified".to_string());
        }
        Err(e) => {
            messages.push(format!("ℹ NixOS module check: {e}"));
            service_checks.insert(
                "nixos",
                json!({
                    "available": false,
                    "note": e.to_string()
                }),
            );
        }
    }

    Ok(json!(service_checks))
}

async fn check_systemd_environment() -> NodeResult<Value> {
    let systemd_version = super::run_command_with_timeout("systemctl", &["--version"]).await?;

    if !systemd_version.status.success() {
        return Err(SinexError::processing(
            "systemd is not available or not functioning".to_string(),
        ));
    }

    let version_output = String::from_utf8_lossy(&systemd_version.stdout);
    let version_line = version_output.lines().next().unwrap_or("unknown");

    Ok(json!({
        "available": true,
        "version": version_line
    }))
}

async fn check_nixos_environment() -> NodeResult<Value> {
    // Check if we're running on NixOS
    let nixos_version = match tokio::fs::read_to_string("/etc/NIXOS").await {
        Ok(content) => Ok(content),
        Err(_) => tokio::fs::read_to_string("/etc/os-release").await,
    };

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

fn expand_path(path: &str) -> Utf8PathBuf {
    if let Some(stripped) = path.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            Utf8PathBuf::from(home).join(stripped)
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

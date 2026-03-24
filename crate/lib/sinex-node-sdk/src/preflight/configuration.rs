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
use serde_json::{Value, json};
use sinex_primitives::DeploymentReadinessDescriptor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{debug, info};

use super::{VerificationStatus, deployment_descriptor_result, runtime_database_expected};

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
            if !event_config
                .get("deployment_descriptor_loaded")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                messages.push(
                    "⚠ Deployment descriptor is missing; configuration readiness is reporting unconfigured sources instead of deployed intent".to_string(),
                );
                has_warnings = true;
            }
            if event_config
                .get("configured_unavailable_count")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
            {
                has_failures = true;
            }
            details.insert("event_sources", event_config);
        }
        Err(e) => {
            messages.push(format!("✗ Event source configuration failed: {e}"));
            has_failures = true;
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
    let database_expected = runtime_database_expected();

    // Required environment variables for Sinex
    let required_vars = vec![
        (
            "DATABASE_URL",
            "PostgreSQL connection URL",
            database_expected,
        ),
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
            } else if var_name == "DATABASE_URL" && !database_expected {
                messages.push(
                    "ℹ DATABASE_URL is intentionally optional for this deployment (edge mode or no runtime database expected)"
                        .to_string(),
                );
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
        "all_required_present": missing_vars.is_empty(),
        "runtime_database_expected": database_expected,
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
    let descriptor = deployment_descriptor_result("preflight configuration checks")?;
    let mut configured_unavailable = Vec::new();

    // Deployment readiness is config-derived: source availability follows the
    // staged descriptor, not whichever binaries or dotfiles happen to exist in
    // the invoking shell session.
    let available_sources = vec![
        ("filesystem", "File system change monitoring"),
        ("terminal", "Terminal activity monitoring"),
        ("clipboard", "Clipboard content monitoring"),
        ("kitty", "Kitty terminal integration"),
        ("hyprland", "Hyprland window manager integration"),
        ("activitywatch", "ActivityWatch desktop history integration"),
        ("atuin", "Atuin shell history integration"),
    ];

    for (source_name, description) in available_sources {
        let config_info = verify_event_source_config(source_name, description, descriptor.as_ref());
        let is_available = config_info["available"].as_bool().unwrap_or(false);
        let is_configured = config_info["configured"].as_bool().unwrap_or(false);
        event_sources.insert(source_name.to_string(), config_info);

        if is_available {
            messages.push(format!("✓ Event source '{source_name}' is available"));
        } else if is_configured {
            configured_unavailable.push(source_name.to_string());
            messages.push(format!(
                "✗ Event source '{source_name}' is configured but not currently available"
            ));
        } else {
            messages.push(format!(
                "ℹ Event source '{source_name}' is not configured by the deployment descriptor"
            ));
        }
    }

    Ok(json!({
        "deployment_descriptor_loaded": descriptor.is_some(),
        "sources": event_sources,
        "configured_unavailable": configured_unavailable,
        "configured_unavailable_count": configured_unavailable.len(),
        "total_available": event_sources.values()
            .filter(|v| v["available"].as_bool().unwrap_or(false))
            .count()
    }))
}

pub fn validate_readable_file(path: &Path) -> NodeResult<()> {
    std::fs::File::open(path).map(|_| ()).map_err(|error| {
        SinexError::processing("failed to open configured file")
            .with_context("path", path.display().to_string())
            .with_std_error(&error)
    })
}

fn validate_sqlite_tables(path: &Path, label: &str, tables: &[&str]) -> NodeResult<()> {
    use rusqlite::{Connection, OpenFlags};

    let conn =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|error| {
            SinexError::processing(format!("failed to open configured {label} database"))
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
        })?;

    let mut missing_tables = Vec::new();
    for table in tables {
        let exists: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [*table],
                |row| row.get(0),
            )
            .map_err(|error| {
                SinexError::processing(format!(
                    "failed to inspect configured {label} database table `{table}`"
                ))
                .with_context("path", path.display().to_string())
                .with_std_error(&error)
            })?;
        if !exists {
            missing_tables.push(*table);
        }
    }

    if !missing_tables.is_empty() {
        let missing = missing_tables
            .iter()
            .map(|table| format!("`{table}`"))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(
            SinexError::processing(format!(
                "configured {label} database is missing required table(s): {missing}"
            ))
            .with_context("path", path.display().to_string()),
        );
    }

    Ok(())
}

pub fn validate_atuin_history_db(path: &Path) -> NodeResult<()> {
    validate_sqlite_tables(path, "Atuin history", &["history"])
}

pub fn validate_fish_history_db(path: &Path) -> NodeResult<()> {
    validate_sqlite_tables(path, "Fish history", &["history"])
}

pub fn validate_activitywatch_db(path: &Path) -> NodeResult<()> {
    validate_sqlite_tables(path, "ActivityWatch history", &["events", "buckets"])
}

pub fn validate_terminal_history_source(shell: &str, path: &Path) -> NodeResult<()> {
    match shell {
        "atuin" => validate_atuin_history_db(path),
        "fish" => validate_fish_history_db(path).map_err(|error| {
            SinexError::configuration(
                "native Fish YAML history is unsupported; configure a SQLite-backed Fish history source"
                    .to_string(),
            )
            .with_context("path", path.display().to_string())
            .with_std_error(&error)
        }),
        "elvish" => Err(
            SinexError::configuration(
                "native Elvish history database is unsupported".to_string(),
            )
            .with_context("path", path.display().to_string()),
        ),
        _ => validate_readable_file(path),
    }
}

fn verify_event_source_config(
    source_name: &str,
    description: &str,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Value {
    let probe = match source_name {
        "filesystem" => probe_filesystem_source(descriptor),
        "terminal" => probe_terminal_source(descriptor),
        "clipboard" => probe_clipboard_source(descriptor),
        "kitty" => probe_kitty_source(descriptor),
        "hyprland" => probe_hyprland_source(descriptor),
        "activitywatch" => probe_activitywatch_source(descriptor),
        "atuin" => probe_atuin_source(descriptor),
        _ => EventSourceProbe::not_configured("Unknown event source"),
    };

    probe.into_json(description)
}

#[derive(Debug)]
struct EventSourceProbe {
    configured: bool,
    available: bool,
    reason: String,
    evidence_paths: Vec<PathBuf>,
}

impl EventSourceProbe {
    fn available(reason: impl Into<String>, evidence_paths: Vec<PathBuf>) -> Self {
        Self {
            configured: true,
            available: true,
            reason: reason.into(),
            evidence_paths,
        }
    }

    fn unavailable(reason: impl Into<String>, evidence_paths: Vec<PathBuf>) -> Self {
        Self {
            configured: true,
            available: false,
            reason: reason.into(),
            evidence_paths,
        }
    }

    fn not_configured(reason: impl Into<String>) -> Self {
        Self {
            configured: false,
            available: false,
            reason: reason.into(),
            evidence_paths: Vec::new(),
        }
    }

    fn into_json(self, description: &str) -> Value {
        json!({
            "description": description,
            "configured": self.configured,
            "available": self.available,
            "dependencies_met": self.available,
            "reason": self.reason,
            "evidence_paths": self.evidence_paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>(),
        })
    }
}

fn probe_filesystem_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    match descriptor {
        Some(descriptor) if descriptor.filesystem.enabled => EventSourceProbe::available(
            "Filesystem capture is enabled in the deployment descriptor",
            Vec::new(),
        ),
        Some(_) => EventSourceProbe::not_configured(
            "Filesystem capture is disabled in the deployment descriptor",
        ),
        None => EventSourceProbe::not_configured(
            "No deployment descriptor loaded; filesystem readiness is not config-derived",
        ),
    }
}

fn probe_terminal_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; terminal readiness is not config-derived",
        );
    };

    if !descriptor.terminal.surface.enabled {
        return EventSourceProbe::not_configured(
            "Terminal capture is disabled in the deployment descriptor",
        );
    }

    let evidence_paths: Vec<PathBuf> = descriptor
        .terminal
        .history_sources
        .iter()
        .map(|source| source.path.clone())
        .collect();
    if evidence_paths.is_empty() {
        return EventSourceProbe::unavailable(
            "Terminal capture is enabled but no history sources are configured",
            evidence_paths,
        );
    }

    let mut readable = Vec::new();
    let mut unreadable = Vec::new();
    for source in &descriptor.terminal.history_sources {
        match validate_terminal_history_source(&source.shell, &source.path) {
            Ok(()) => readable.push(format!("{}:{}", source.shell, source.path.display())),
            Err(error) => unreadable.push(format!(
                "{}:{} ({error})",
                source.shell,
                source.path.display()
            )),
        }
    }

    if !unreadable.is_empty() {
        EventSourceProbe::unavailable(
            format!(
                "Configured terminal history sources are unreadable or malformed: {}",
                unreadable.join(", ")
            ),
            evidence_paths,
        )
    } else if !readable.is_empty() {
        EventSourceProbe::available(
            format!(
                "{} configured terminal source(s) validated successfully",
                readable.len()
            ),
            evidence_paths,
        )
    } else {
        EventSourceProbe::unavailable(
            "Configured terminal history sources are missing",
            evidence_paths,
        )
    }
}

fn probe_clipboard_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; clipboard readiness is not config-derived",
        );
    };

    if !(descriptor.desktop.surface.enabled && descriptor.desktop.clipboard_enabled) {
        return EventSourceProbe::not_configured(
            "Clipboard capture is disabled in the deployment descriptor",
        );
    }

    EventSourceProbe::available(
        "Clipboard capture is enabled in the deployment descriptor",
        Vec::new(),
    )
}

fn probe_kitty_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; Kitty readiness is not config-derived",
        );
    };

    if !descriptor.terminal.kitty_enabled {
        return EventSourceProbe::not_configured(
            "Kitty integration is disabled in the deployment descriptor",
        );
    }

    EventSourceProbe::available(
        "Kitty integration is enabled in the deployment descriptor",
        Vec::new(),
    )
}

fn probe_hyprland_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; Hyprland readiness is not config-derived",
        );
    };

    if !descriptor.desktop.surface.enabled {
        return EventSourceProbe::not_configured(
            "Desktop capture is disabled in the deployment descriptor",
        );
    }

    if let Some(event_socket) = descriptor.desktop.hyprland_event_socket.clone() {
        return if event_socket.exists() {
            EventSourceProbe::available(
                "Configured Hyprland event socket is present",
                vec![event_socket],
            )
        } else {
            EventSourceProbe::unavailable(
                "Configured Hyprland event socket is missing",
                vec![event_socket],
            )
        };
    }

    let Some(runtime_dir) = descriptor.desktop.runtime_dir.clone() else {
        return EventSourceProbe::unavailable(
            "Desktop capture is enabled but no runtime_dir is declared",
            Vec::new(),
        );
    };

    let hypr_dir = runtime_dir.join("hypr");
    if let Some(signature) = descriptor.desktop.hyprland_instance_signature.clone() {
        let event_socket = hypr_dir.join(signature).join(".socket2.sock");
        return if event_socket.exists() {
            EventSourceProbe::available(
                "Configured Hyprland instance socket is present",
                vec![event_socket],
            )
        } else {
            EventSourceProbe::unavailable(
                "Configured Hyprland instance socket is missing",
                vec![event_socket],
            )
        };
    }

    let Ok(entries) = std::fs::read_dir(&hypr_dir) else {
        return EventSourceProbe::unavailable(
            "Hyprland runtime directory is missing or unreadable",
            vec![hypr_dir],
        );
    };

    let sockets: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .map(|path| path.join(".socket2.sock"))
        .filter(|path| path.exists())
        .collect();

    match sockets.as_slice() {
        [socket] => EventSourceProbe::available(
            "Resolved a single Hyprland event socket from the configured runtime directory",
            vec![socket.clone()],
        ),
        [] => EventSourceProbe::unavailable(
            "Configured Hyprland runtime directory contains no event socket",
            vec![hypr_dir],
        ),
        _ => EventSourceProbe::unavailable(
            "Configured Hyprland runtime directory contains multiple instances; set hyprland_instance_signature or hyprland_event_socket explicitly",
            sockets,
        ),
    }
}

fn probe_atuin_source(descriptor: Option<&DeploymentReadinessDescriptor>) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; Atuin readiness is not config-derived",
        );
    };

    if !descriptor.terminal.surface.enabled {
        return EventSourceProbe::not_configured(
            "Terminal capture is disabled in the deployment descriptor",
        );
    }

    let Some(path) = descriptor
        .terminal
        .history_sources
        .iter()
        .find(|source| source.shell == "atuin")
        .map(|source| source.path.clone())
    else {
        return EventSourceProbe::not_configured(
            "No Atuin history source is configured in the deployment descriptor",
        );
    };

    match validate_atuin_history_db(&path) {
        Ok(()) => EventSourceProbe::available(
            "Configured Atuin history database validated successfully",
            vec![path],
        ),
        Err(error) => EventSourceProbe::unavailable(
            format!("Configured Atuin history database is unreadable or malformed: {error}"),
            vec![path],
        ),
    }
}

fn probe_activitywatch_source(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> EventSourceProbe {
    let Some(descriptor) = descriptor else {
        return EventSourceProbe::not_configured(
            "No deployment descriptor loaded; ActivityWatch readiness is not config-derived",
        );
    };

    if !descriptor.desktop.surface.enabled {
        return EventSourceProbe::not_configured(
            "Desktop capture is disabled in the deployment descriptor",
        );
    }

    let Some(path) = descriptor.desktop.activitywatch_db_path.clone() else {
        return EventSourceProbe::unavailable(
            "Desktop capture is enabled but no ActivityWatch database path is configured",
            Vec::new(),
        );
    };

    match validate_activitywatch_db(&path) {
        Ok(()) => EventSourceProbe::available(
            "Configured ActivityWatch history database validated successfully",
            vec![path],
        ),
        Err(error) => EventSourceProbe::unavailable(
            format!(
                "Configured ActivityWatch history database is unreadable or malformed: {error}"
            ),
            vec![path],
        ),
    }
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

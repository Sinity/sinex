/*!
 * Service verification module for Sinex Pre-Flight system
 *
 * Verifies service dependencies and readiness including:
 * - SystemD service availability
 * - Service dependency validation
 * - Binary availability and version compatibility
 * - Service configuration validation
 */

use crate::{NodeResult, SinexError};
use serde_json::{json, Value};
use std::{collections::HashMap, fmt, process::Command, str::FromStr};
use tracing::{debug, info};

use super::VerificationStatus;

/// SystemD service status enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceStatus {
    Active,
    Inactive,
    Failed,
    Unknown,
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceStatus::Active => write!(f, "active"),
            ServiceStatus::Inactive => write!(f, "inactive"),
            ServiceStatus::Failed => write!(f, "failed"),
            ServiceStatus::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for ServiceStatus {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "active" => Ok(ServiceStatus::Active),
            "inactive" => Ok(ServiceStatus::Inactive),
            "failed" => Ok(ServiceStatus::Failed),
            _ => Ok(ServiceStatus::Unknown),
        }
    }
}

impl ServiceStatus {
    /// Check if the service status indicates the service is running
    pub fn is_running(&self) -> bool {
        matches!(self, ServiceStatus::Active)
    }

    /// Check if the service status indicates a problem
    pub fn has_issues(&self) -> bool {
        matches!(self, ServiceStatus::Failed | ServiceStatus::Unknown)
    }
}

/// Verify service dependencies and readiness
pub async fn verify_service_dependencies() -> NodeResult<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    info!("Verifying service dependencies and readiness");

    // Binary availability verification
    match verify_binary_availability(&mut messages).await {
        Ok(binary_info) => {
            details.insert("binaries", binary_info);
        }
        Err(e) => {
            messages.push(format!("✗ Binary availability check failed: {}", e));
            has_failures = true;
        }
    }

    // SystemD service verification
    match verify_systemd_services(&mut messages).await {
        Ok(systemd_info) => {
            details.insert("systemd_services", systemd_info);
        }
        Err(e) => {
            messages.push(format!("⚠ SystemD service verification warning: {}", e));
            has_warnings = true;
        }
    }

    // PostgreSQL service verification
    match verify_postgresql_service(&mut messages).await {
        Ok(postgres_info) => {
            details.insert("postgresql", postgres_info);
        }
        Err(e) => {
            messages.push(format!("✗ PostgreSQL service verification failed: {}", e));
            has_failures = true;
        }
    }

    // External dependencies verification
    match verify_external_dependencies(&mut messages).await {
        Ok(deps_info) => {
            details.insert("external_dependencies", deps_info);
        }
        Err(e) => {
            messages.push(format!(
                "⚠ External dependencies verification warning: {}",
                e
            ));
            has_warnings = true;
        }
    }

    // Service configuration compatibility
    match verify_service_configuration(&mut messages).await {
        Ok(config_info) => {
            details.insert("service_configuration", config_info);
        }
        Err(e) => {
            messages.push(format!(
                "⚠ Service configuration verification warning: {}",
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
        "Service dependency verification completed with status: {:?}",
        status
    );
    Ok((status, json!(details), messages))
}

async fn verify_binary_availability(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut binary_info = HashMap::new();
    let mut missing_binaries = Vec::new();

    // Required binaries for Sinex operation
    let required_binaries = vec![
        ("sinex-ingestd", "Ingestion daemon", true),
        ("sinex-gateway", "API gateway", true),
        ("sinex-preflight", "Pre-flight verification service", true),
        ("psql", "PostgreSQL client", true),
        ("systemctl", "SystemD control", true),
    ];

    // Optional but recommended binaries
    let optional_binaries = vec![
        ("git", "Git version control"),
        ("git-annex", "Git-annex blob storage"),
        ("kitty", "Kitty terminal emulator"),
        ("hyprctl", "Hyprland control"),
        ("atuin", "Shell history"),
    ];

    for (binary_name, description, required) in required_binaries {
        match check_binary_availability(binary_name).await {
            Ok(binary_data) => {
                binary_info.insert(
                    binary_name.to_string(),
                    json!({
                        "available": true,
                        "description": description,
                        "required": required,
                        "path": binary_data.path,
                        "version": binary_data.version
                    }),
                );

                messages.push(format!(
                    "✓ Required binary '{}' available at {}",
                    binary_name, binary_data.path
                ));
            }
            Err(e) => {
                binary_info.insert(
                    binary_name.to_string(),
                    json!({
                        "available": false,
                        "description": description,
                        "required": required,
                        "error": e.to_string()
                    }),
                );

                if required {
                    missing_binaries.push(binary_name.to_string());
                    messages.push(format!(
                        "✗ Required binary '{}' not found: {}",
                        binary_name, e
                    ));
                } else {
                    messages.push(format!(
                        "⚠ Optional binary '{}' not found: {}",
                        binary_name, e
                    ));
                }
            }
        }
    }

    for (binary_name, description) in optional_binaries {
        match check_binary_availability(binary_name).await {
            Ok(binary_data) => {
                binary_info.insert(
                    binary_name.to_string(),
                    json!({
                        "available": true,
                        "description": description,
                        "required": false,
                        "path": binary_data.path,
                        "version": binary_data.version
                    }),
                );

                messages.push(format!("✓ Optional binary '{}' available", binary_name));
            }
            Err(_) => {
                binary_info.insert(
                    binary_name.to_string(),
                    json!({
                        "available": false,
                        "description": description,
                        "required": false
                    }),
                );

                debug!("Optional binary '{}' not found", binary_name);
            }
        }
    }

    if !missing_binaries.is_empty() {
        return Err(SinexError::processing(format!(
            "Missing required binaries: {}",
            missing_binaries.join(", ")
        )));
    }

    Ok(json!({
        "binaries": binary_info,
        "missing_required": missing_binaries,
        "all_required_available": missing_binaries.is_empty()
    }))
}

#[derive(Debug)]
struct BinaryInfo {
    path: String,
    version: Option<String>,
}

async fn check_binary_availability(binary_name: &str) -> NodeResult<BinaryInfo> {
    // First check if binary exists in PATH
    let which_output = Command::new("which")
        .arg(binary_name)
        .output()
        .map_err(|e| SinexError::processing(format!("Failed to execute 'which' command: {}", e)))?;

    if !which_output.status.success() {
        return Err(SinexError::processing(format!(
            "Binary '{}' not found in PATH",
            binary_name
        )));
    }

    let path = String::from_utf8_lossy(&which_output.stdout)
        .trim()
        .to_string();

    // Try to get version information
    let version = get_binary_version(binary_name, &path).await;

    Ok(BinaryInfo { path, version })
}

async fn get_binary_version(binary_name: &str, _path: &str) -> Option<String> {
    // Try common version flags
    let version_flags = vec!["--version", "-V", "version"];

    for flag in version_flags {
        if let Ok(output) = Command::new(binary_name).arg(flag).output() {
            if output.status.success() {
                let version_output = String::from_utf8_lossy(&output.stdout);
                let first_line = version_output.lines().next().unwrap_or("").trim();
                if !first_line.is_empty() {
                    return Some(first_line.to_string());
                }
            }
        }
    }

    None
}

async fn verify_systemd_services(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut service_info = HashMap::new();

    // Sinex-related services that should be manageable
    let sinex_services = vec![
        "sinex-ingestd.service",
        "sinex-gateway.service",
        "sinex-fs-ingestor-1.service",
        "sinex-terminal-ingestor-1.service",
        "sinex-desktop-ingestor-1.service",
        "sinex-system-ingestor-1.service",
        "sinex-health-automaton.service",
    ];

    // System services that Sinex depends on
    let dependency_services = vec!["postgresql.service", "systemd-resolved.service"];

    for service_name in sinex_services {
        match check_systemd_service(service_name).await {
            Ok(service_data) => {
                service_info.insert(service_name.to_string(), service_data);

                if service_name.starts_with("sinex-") {
                    // For Sinex services, it's OK if they're not loaded yet (they will be after deployment)
                    messages.push(format!("ℹ Sinex service '{}' status checked", service_name));
                } else {
                    messages.push(format!("✓ Service '{}' is available", service_name));
                }
            }
            Err(e) => {
                service_info.insert(
                    service_name.to_string(),
                    json!({
                        "available": false,
                        "error": e.to_string()
                    }),
                );

                if service_name.starts_with("sinex-") {
                    messages.push(format!(
                        "ℹ Sinex service '{}' not yet configured (expected)",
                        service_name
                    ));
                } else {
                    messages.push(format!("⚠ Service '{}' check failed: {}", service_name, e));
                }
            }
        }
    }

    for service_name in dependency_services {
        match check_systemd_service(service_name).await {
            Ok(service_data) => {
                let status_str = service_data["status"].as_str().unwrap_or("unknown");
                let status = ServiceStatus::from_str(status_str).unwrap_or(ServiceStatus::Unknown);
                service_info.insert(service_name.to_string(), service_data.clone());
                if status.is_running() {
                    messages.push(format!("✓ Dependency service '{}' is active", service_name));
                } else {
                    messages.push(format!(
                        "⚠ Dependency service '{}' status: {}",
                        service_name, status
                    ));
                }
            }
            Err(e) => {
                service_info.insert(
                    service_name.to_string(),
                    json!({
                        "available": false,
                        "error": e.to_string()
                    }),
                );

                messages.push(format!(
                    "⚠ Dependency service '{}' check failed: {}",
                    service_name, e
                ));
            }
        }
    }

    Ok(json!({
        "services": service_info
    }))
}

async fn check_systemd_service(service_name: &str) -> NodeResult<Value> {
    let status_output = Command::new("systemctl")
        .args([
            "show",
            service_name,
            "--property=ActiveState,SubState,LoadState",
        ])
        .output()
        .map_err(|e| SinexError::processing(format!("Failed to execute systemctl show: {}", e)))?;

    if !status_output.status.success() {
        return Err(SinexError::processing(format!(
            "Failed to get service status for {}",
            service_name
        )));
    }

    let status_text = String::from_utf8_lossy(&status_output.stdout);
    let mut properties = HashMap::new();

    for line in status_text.lines() {
        if let Some((key, value)) = line.split_once('=') {
            properties.insert(key.to_string(), value.to_string());
        }
    }

    let active_state = properties
        .get("ActiveState")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let sub_state = properties
        .get("SubState")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    let load_state = properties
        .get("LoadState")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    Ok(json!({
        "available": true,
        "status": active_state,
        "sub_status": sub_state,
        "load_state": load_state,
        "is_active": active_state == "active",
        "is_loaded": load_state == "loaded"
    }))
}

async fn verify_postgresql_service(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut postgres_info = HashMap::new();

    // Check PostgreSQL service status
    match check_systemd_service("postgresql.service").await {
        Ok(service_data) => {
            postgres_info.insert("service", service_data.clone());

            let is_active = service_data["is_active"].as_bool().unwrap_or(false);
            if is_active {
                messages.push("✓ PostgreSQL service is active".to_string());

                // Test database connectivity
                match test_postgresql_connectivity().await {
                    Ok(conn_info) => {
                        postgres_info.insert("connectivity", conn_info);
                        messages.push("✓ PostgreSQL connectivity verified".to_string());
                    }
                    Err(e) => {
                        postgres_info.insert(
                            "connectivity",
                            json!({
                                "success": false,
                                "error": e.to_string()
                            }),
                        );
                        messages.push(format!("✗ PostgreSQL connectivity failed: {}", e));
                        return Err(SinexError::processing(format!(
                            "PostgreSQL connectivity test failed: {}",
                            e
                        )));
                    }
                }
            } else {
                let status = service_data["status"].as_str().unwrap_or("unknown");
                messages.push(format!(
                    "✗ PostgreSQL service is not active (status: {})",
                    status
                ));
                return Err(SinexError::processing(
                    "PostgreSQL service is not running".to_string(),
                ));
            }
        }
        Err(e) => {
            postgres_info.insert(
                "service",
                json!({
                    "available": false,
                    "error": e.to_string()
                }),
            );

            messages.push(format!("✗ PostgreSQL service check failed: {}", e));
            return Err(SinexError::processing(format!(
                "PostgreSQL service verification failed: {}",
                e
            )));
        }
    }

    Ok(json!(postgres_info))
}

async fn test_postgresql_connectivity() -> NodeResult<Value> {
    let database_url = super::resolve_database_url()?;

    let test_output = Command::new("psql")
        .arg(&database_url)
        .arg("-c")
        .arg("SELECT version();")
        .output()
        .map_err(|e| {
            SinexError::processing(format!("Failed to execute psql test command: {}", e))
        })?;

    if test_output.status.success() {
        let version_output = String::from_utf8_lossy(&test_output.stdout);
        let version_line = version_output
            .lines()
            .find(|line| line.contains("PostgreSQL"))
            .unwrap_or("PostgreSQL version unknown")
            .trim();

        Ok(json!({
            "success": true,
            "version": version_line,
            "connection_string": redact_password(&database_url)
        }))
    } else {
        let error_output = String::from_utf8_lossy(&test_output.stderr);
        Err(SinexError::processing(format!(
            "PostgreSQL connection test failed: {}",
            error_output.trim()
        )))
    }
}

async fn verify_external_dependencies(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut deps_info = HashMap::new();

    // Git and Git-Annex for blob storage
    match verify_git_dependencies().await {
        Ok(git_info) => {
            deps_info.insert("git", git_info);
            messages.push("✓ Git dependencies verified".to_string());
        }
        Err(e) => {
            deps_info.insert(
                "git",
                json!({
                    "available": false,
                    "error": e.to_string()
                }),
            );
            messages.push(format!("⚠ Git dependencies warning: {}", e));
        }
    }

    // Event source dependencies
    match verify_event_source_dependencies().await {
        Ok(event_deps) => {
            deps_info.insert("event_sources", event_deps);
            messages.push("✓ Event source dependencies checked".to_string());
        }
        Err(e) => {
            deps_info.insert(
                "event_sources",
                json!({
                    "error": e.to_string()
                }),
            );
            messages.push(format!("⚠ Event source dependencies warning: {}", e));
        }
    }

    Ok(json!(deps_info))
}

async fn verify_git_dependencies() -> NodeResult<Value> {
    let mut git_info = HashMap::new();

    // Check Git availability
    match check_binary_availability("git").await {
        Ok(git_binary) => {
            git_info.insert(
                "git_binary",
                json!({
                    "available": true,
                    "path": git_binary.path,
                    "version": git_binary.version
                }),
            );
        }
        Err(e) => {
            git_info.insert(
                "git_binary",
                json!({
                    "available": false,
                    "error": e.to_string()
                }),
            );
            return Err(SinexError::processing(format!(
                "Git binary not available: {}",
                e
            )));
        }
    }

    // Check Git-Annex availability (optional but recommended)
    match check_binary_availability("git-annex").await {
        Ok(annex_binary) => {
            git_info.insert(
                "git_annex",
                json!({
                    "available": true,
                    "path": annex_binary.path,
                    "version": annex_binary.version
                }),
            );
        }
        Err(_) => {
            git_info.insert(
                "git_annex",
                json!({
                    "available": false,
                    "note": "Git-annex not available - blob storage will be disabled"
                }),
            );
        }
    }

    Ok(json!(git_info))
}

async fn verify_event_source_dependencies() -> NodeResult<Value> {
    let mut event_deps = HashMap::new();

    // Check clipboard tools
    let clipboard_tools = vec!["xclip", "wl-clipboard"];
    let mut clipboard_available = false;

    for tool in clipboard_tools {
        if check_binary_availability(tool).await.is_ok() {
            clipboard_available = true;
            break;
        }
    }

    event_deps.insert(
        "clipboard",
        json!({
            "available": clipboard_available,
            "note": if clipboard_available {
                "Clipboard monitoring available"
            } else {
                "No clipboard tools found - clipboard monitoring disabled"
            }
        }),
    );

    // Check Hyprland
    let hyprland_available = std::env::var("HYPRLAND_INSTANCE_SIGNATURE").is_ok()
        || check_binary_availability("hyprctl").await.is_ok();

    event_deps.insert(
        "hyprland",
        json!({
            "available": hyprland_available,
            "note": if hyprland_available {
                "Hyprland integration available"
            } else {
                "Hyprland not detected - window manager integration disabled"
            }
        }),
    );

    // Check Kitty
    let kitty_available = std::env::var("KITTY_LISTEN_ON").is_ok()
        || check_binary_availability("kitty").await.is_ok();

    event_deps.insert(
        "kitty",
        json!({
            "available": kitty_available,
            "note": if kitty_available {
                "Kitty terminal integration available"
            } else {
                "Kitty not detected - terminal integration may be limited"
            }
        }),
    );

    Ok(json!(event_deps))
}

async fn verify_service_configuration(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut config_info = HashMap::new();

    // Check systemd unit file locations
    let unit_paths = vec![
        "/etc/systemd/system",
        "/usr/lib/systemd/system",
        "/lib/systemd/system",
    ];

    let mut found_unit_files = Vec::new();

    for unit_path in unit_paths {
        if let Ok(mut entries) = tokio::fs::read_dir(unit_path).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();

                if file_name_str.starts_with("sinex-") && file_name_str.ends_with(".service") {
                    found_unit_files.push(format!("{}/{}", unit_path, file_name_str));
                }
            }
        }
    }

    config_info.insert(
        "unit_files",
        json!({
            "found": found_unit_files,
            "count": found_unit_files.len()
        }),
    );

    if found_unit_files.is_empty() {
        messages.push(
            "ℹ No Sinex systemd unit files found (will be created during deployment)".to_string(),
        );
    } else {
        messages.push(format!(
            "ℹ Found {} existing Sinex systemd unit files",
            found_unit_files.len()
        ));
    }

    Ok(json!(config_info))
}

fn redact_password(url: &str) -> String {
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

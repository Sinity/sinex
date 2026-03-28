/*!
 * Service verification module for Sinex Pre-Flight system
 *
 * Verifies service dependencies and readiness including:
 * - `SystemD` service availability
 * - Service dependency validation
 * - Binary availability and version compatibility
 * - Service configuration validation
 */

use crate::{NodeResult, SinexError};
use serde_json::{Value, json};
use std::{collections::HashMap, fmt, str::FromStr};
use tracing::{debug, info};

use super::{
    VerificationStatus, deployment_descriptor_result, run_command_with_timeout,
    runtime_database_expected,
};

/// `SystemD` service status enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceStatus {
    Active,
    Inactive,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemdServiceDetails {
    pub active_state: String,
    pub sub_state: String,
    pub load_state: String,
    pub unit_type: Option<String>,
    pub notify_access: Option<String>,
    pub watchdog_usec: Option<u64>,
}

impl SystemdServiceDetails {
    pub fn from_show_output(output: &str) -> NodeResult<Self> {
        let mut active_state = None;
        let mut sub_state = None;
        let mut load_state = None;
        let mut unit_type = None;
        let mut notify_access = None;
        let mut watchdog_usec = None;

        for line in output.lines() {
            if let Some((key, value)) = line.split_once('=') {
                match key {
                    "ActiveState" => active_state = Some(value.to_string()),
                    "SubState" => sub_state = Some(value.to_string()),
                    "LoadState" => load_state = Some(value.to_string()),
                    "Type" => unit_type = Some(value.to_string()),
                    "NotifyAccess" => notify_access = Some(value.to_string()),
                    "WatchdogUSec" => {
                        watchdog_usec = Some(value.parse::<u64>().map_err(|error| {
                            SinexError::processing(
                                "Failed to parse systemd WatchdogUSec".to_string(),
                            )
                            .with_context("field", "WatchdogUSec")
                            .with_context("value", value.to_string())
                            .with_std_error(&error)
                        })?);
                    }
                    _ => {}
                }
            }
        }

        Ok(Self {
            active_state: active_state.unwrap_or_else(|| "unknown".to_string()),
            sub_state: sub_state.unwrap_or_else(|| "unknown".to_string()),
            load_state: load_state.unwrap_or_else(|| "unknown".to_string()),
            unit_type,
            notify_access,
            watchdog_usec,
        })
    }

    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.load_state == "loaded"
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active_state == "active"
    }

    #[must_use]
    pub fn notify_contract_violations(&self) -> Vec<String> {
        let mut violations = Vec::new();

        let type_value = self.unit_type.as_deref().unwrap_or("<unset>");
        if type_value != "notify" {
            violations.push(format!("type={type_value}"));
        }

        let notify_access_value = self.notify_access.as_deref().unwrap_or("<unset>");
        if notify_access_value != "main" {
            violations.push(format!("notify_access={notify_access_value}"));
        }

        match self.watchdog_usec {
            Some(value) if value > 0 => {}
            Some(value) => violations.push(format!("watchdog_usec={value}")),
            None => violations.push("watchdog_usec=<unset>".to_string()),
        }

        violations
    }

    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "available": self.is_loaded(),
            "status": self.active_state,
            "sub_status": self.sub_state,
            "load_state": self.load_state,
            "is_active": self.is_active(),
            "is_loaded": self.is_loaded(),
            "type": self.unit_type,
            "notify_access": self.notify_access,
            "watchdog_usec": self.watchdog_usec,
        })
    }
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
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, ServiceStatus::Active)
    }

    /// Check if the service status indicates a problem
    #[must_use]
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
            messages.push(format!("✗ Binary availability check failed: {e}"));
            has_failures = true;
        }
    }

    // SystemD service verification
    match verify_systemd_services(&mut messages).await {
        Ok(systemd_info) => {
            if !systemd_info
                .get("deployment_descriptor_loaded")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                messages.push(
                    "⚠ Deployment descriptor is missing; managed Sinex systemd unit verification was skipped".to_string(),
                );
                has_warnings = true;
            }
            details.insert("systemd_services", systemd_info);
        }
        Err(e) => {
            messages.push(format!("✗ SystemD service verification failed: {e}"));
            has_failures = true;
        }
    }

    if runtime_database_expected()? {
        match verify_postgresql_service(&mut messages).await {
            Ok(postgres_info) => {
                details.insert("postgresql", postgres_info);
            }
            Err(e) => {
                messages.push(format!("✗ PostgreSQL service verification failed: {e}"));
                has_failures = true;
            }
        }
    } else {
        details.insert(
            "postgresql",
            json!({
                "available": false,
                "skipped": true,
                "reason": "runtime database verification is disabled for this deployment"
            }),
        );
        messages.push(
            "ℹ PostgreSQL service verification skipped because this deployment does not expect a runtime database"
                .to_string(),
        );
    }

    // External dependencies verification
    match verify_external_dependencies(&mut messages).await {
        Ok(deps_info) => {
            details.insert("external_dependencies", deps_info);
        }
        Err(e) => {
            messages.push(format!("⚠ External dependencies verification warning: {e}"));
            has_warnings = true;
        }
    }

    // Service configuration checks
    match verify_service_configuration(&mut messages).await {
        Ok(config_info) => {
            details.insert("service_configuration", config_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Service configuration verification warning: {e}"));
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
    let descriptor = deployment_descriptor_result("service verification")?;
    let require_service_binaries = descriptor.is_none();

    let mut required_binaries = vec![("systemctl", "SystemD control", true)];
    if runtime_database_expected()? {
        required_binaries.push(("psql", "PostgreSQL client", true));
    }
    if require_service_binaries {
        required_binaries.splice(
            0..0,
            [
                ("sinex-ingestd", "Ingestion daemon", true),
                ("sinex-gateway", "API gateway", true),
                ("sinex-preflight", "Pre-flight verification service", true),
            ],
        );
    } else {
        messages.push(
            "ℹ Deployment descriptor loaded; skipping PATH-based Sinex service binary checks"
                .to_string(),
        );
    }

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
                    "✓ Required binary '{binary_name}' available at {}",
                    binary_data.path
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
                    messages.push(format!("✗ Required binary '{binary_name}' not found: {e}"));
                } else {
                    messages.push(format!("⚠ Optional binary '{binary_name}' not found: {e}"));
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

                messages.push(format!("✓ Optional binary '{binary_name}' available"));
            }
            Err(error) => {
                binary_info.insert(
                    binary_name.to_string(),
                    json!({
                        "available": false,
                        "description": description,
                        "required": false,
                        "error": error.to_string()
                    }),
                );

                messages.push(format!(
                    "ℹ Optional binary '{binary_name}' unavailable: {error}"
                ));
                debug!("Optional binary '{}' unavailable: {}", binary_name, error);
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
    let which_output = run_command_with_timeout("which", &[binary_name]).await?;

    if !which_output.status.success() {
        return Err(SinexError::processing(format!(
            "Binary '{binary_name}' not found in PATH"
        )));
    }

    let path = String::from_utf8_lossy(&which_output.stdout)
        .trim()
        .to_string();

    let version = get_binary_version(binary_name, &path).await;

    Ok(BinaryInfo { path, version })
}

async fn get_binary_version(binary_name: &str, _path: &str) -> Option<String> {
    let version_flags = ["--version", "-V", "version"];

    for flag in version_flags {
        if let Ok(output) = run_command_with_timeout(binary_name, &[flag]).await
            && output.status.success()
        {
            let version_output = String::from_utf8_lossy(&output.stdout);
            let first_line = version_output.lines().next().unwrap_or("").trim();
            if !first_line.is_empty() {
                return Some(first_line.to_string());
            }
        }
    }

    None
}

async fn verify_systemd_services(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut service_info = HashMap::new();
    let descriptor = deployment_descriptor_result("managed systemd verification")?;
    let descriptor_loaded = descriptor.is_some();
    let sinex_services = descriptor
        .as_ref()
        .filter(|value| !value.managed_units.is_empty())
        .map(|value| value.managed_units.clone());
    let enforce_declared_units = sinex_services.is_some();

    // System services that Sinex depends on
    let dependency_services = vec!["postgresql.service", "systemd-resolved.service"];
    let mut missing_declared_units = Vec::new();
    let mut notify_contract_violations = Vec::new();

    if let Some(sinex_services) = sinex_services {
        for service_name in sinex_services {
            match inspect_systemd_service(&service_name).await {
                Ok(service_data) => {
                    let service_json = service_data.to_json();
                    let is_available = service_data.is_loaded();
                    let load_state = service_data.load_state.as_str();
                    let contract_violations = service_data.notify_contract_violations();
                    service_info.insert(service_name.to_string(), service_json);

                    if service_name.starts_with("sinex-") && is_available {
                        if contract_violations.is_empty() {
                            messages.push(format!(
                                "✓ Sinex service '{service_name}' has a valid notify/watchdog contract"
                            ));
                        } else {
                            notify_contract_violations.push(format!(
                                "{service_name} ({})",
                                contract_violations.join(", ")
                            ));
                            messages.push(format!(
                                "✗ Sinex service '{service_name}' violates the notify/watchdog contract: {}",
                                contract_violations.join(", ")
                            ));
                        }
                    } else if service_name.starts_with("sinex-") {
                        if enforce_declared_units {
                            missing_declared_units
                                .push(format!("{service_name} (load state: {load_state})"));
                            messages.push(format!(
                                "✗ Declared Sinex service '{service_name}' is missing or unloaded (load state: {load_state})"
                            ));
                        } else {
                            messages.push(format!(
                                "ℹ Sinex service '{service_name}' not yet configured (load state: {load_state})"
                            ));
                        }
                    } else {
                        messages.push(format!("✓ Service '{service_name}' is available"));
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
                        if enforce_declared_units {
                            missing_declared_units.push(format!("{service_name} ({e})"));
                            messages.push(format!(
                                "✗ Declared Sinex service '{service_name}' could not be verified: {e}"
                            ));
                        } else {
                            messages.push(format!(
                                "ℹ Sinex service '{service_name}' not yet configured (expected)"
                            ));
                        }
                    } else {
                        messages.push(format!("⚠ Service '{service_name}' check failed: {e}"));
                    }
                }
            }
        }
    } else {
        messages.push(
            "⚠ No deployment descriptor loaded; skipping managed Sinex systemd unit checks"
                .to_string(),
        );
    }

    for service_name in dependency_services {
        match inspect_systemd_service(service_name).await {
            Ok(service_data) => {
                let service_json = service_data.to_json();
                let status_str = service_data.active_state.as_str();
                let status = ServiceStatus::from_str(status_str).unwrap_or(ServiceStatus::Unknown);
                service_info.insert(service_name.to_string(), service_json);
                if status.is_running() {
                    messages.push(format!("✓ Dependency service '{service_name}' is active"));
                } else {
                    messages.push(format!(
                        "⚠ Dependency service '{service_name}' status: {status}"
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
                    "⚠ Dependency service '{service_name}' check failed: {e}"
                ));
            }
        }
    }

    if !missing_declared_units.is_empty() {
        return Err(SinexError::processing(format!(
            "Declared managed units are missing or unloaded: {}",
            missing_declared_units.join(", ")
        )));
    }

    if !notify_contract_violations.is_empty() {
        return Err(SinexError::processing(format!(
            "Declared managed units violate the notify/watchdog contract: {}",
            notify_contract_violations.join(", ")
        )));
    }

    Ok(json!({
        "services": service_info,
        "deployment_descriptor_loaded": descriptor_loaded,
    }))
}

pub async fn inspect_systemd_service(service_name: &str) -> NodeResult<SystemdServiceDetails> {
    let status_output = run_command_with_timeout(
        "systemctl",
        &[
            "show",
            service_name,
            "--property=ActiveState,SubState,LoadState,Type,NotifyAccess,WatchdogUSec",
        ],
    )
    .await?;

    if !status_output.status.success() {
        return Err(SinexError::processing(format!(
            "Failed to get service status for {service_name}"
        )));
    }

    SystemdServiceDetails::from_show_output(&String::from_utf8_lossy(&status_output.stdout))
}

async fn verify_postgresql_service(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut postgres_info = HashMap::new();

    // Check PostgreSQL service status
    match inspect_systemd_service("postgresql.service").await {
        Ok(service_data) => {
            postgres_info.insert("service", service_data.to_json());

            let is_active = service_data.is_active();
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
                        messages.push(format!("✗ PostgreSQL connectivity failed: {e}"));
                        return Err(SinexError::processing(format!(
                            "PostgreSQL connectivity test failed: {e}"
                        )));
                    }
                }
            } else {
                let status = service_data.active_state.as_str();
                messages.push(format!(
                    "✗ PostgreSQL service is not active (status: {status})"
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

            messages.push(format!("✗ PostgreSQL service check failed: {e}"));
            return Err(SinexError::processing(format!(
                "PostgreSQL service verification failed: {e}"
            )));
        }
    }

    Ok(json!(postgres_info))
}

async fn test_postgresql_connectivity() -> NodeResult<Value> {
    let database_url = super::resolve_database_url()?;

    let test_output =
        run_command_with_timeout("psql", &[&database_url, "-c", "SELECT version();"]).await?;

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
            messages.push(format!("⚠ Git dependencies warning: {e}"));
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
                "Git binary not available: {e}"
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
        found_unit_files.extend(discover_unit_files_in_path(unit_path).await?);
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

async fn discover_unit_files_in_path(unit_path: &str) -> NodeResult<Vec<String>> {
    let mut entries = match tokio::fs::read_dir(unit_path).await {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(SinexError::processing(format!(
                "Failed to inspect systemd unit directory '{unit_path}': {error}"
            )));
        }
    };

    let mut found = Vec::new();
    loop {
        match entries.next_entry().await {
            Ok(Some(entry)) => {
                let file_name = entry.file_name();
                let file_name_str = file_name.to_string_lossy();
                let file_type = entry.file_type().await.map_err(|error| {
                    SinexError::processing(format!(
                        "Failed to inspect file type for systemd unit entry '{}/{}': {error}",
                        unit_path, file_name_str
                    ))
                })?;
                if file_type.is_file()
                    && file_name_str.starts_with("sinex-")
                    && file_name_str.ends_with(".service")
                {
                    found.push(format!("{unit_path}/{file_name_str}"));
                }
            }
            Ok(None) => return Ok(found),
            Err(error) => {
                return Err(SinexError::processing(format!(
                    "Failed to read entry from systemd unit directory '{unit_path}': {error}"
                )));
            }
        }
    }
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

#[cfg(test)]
mod tests {
    // Small inline tests are justified here because they exercise private
    // preflight helpers without widening the service-verification API surface.
    use super::{SystemdServiceDetails, discover_unit_files_in_path};
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn systemd_service_details_reject_invalid_watchdog_usec() -> TestResult<()> {
        let error = SystemdServiceDetails::from_show_output(
            "ActiveState=active\nSubState=running\nLoadState=loaded\nWatchdogUSec=not-a-number\n",
        )
        .expect_err("invalid WatchdogUSec should fail honestly");

        assert!(error.to_string().contains("WatchdogUSec"));
        assert!(error.to_string().contains("not-a-number"));
        Ok(())
    }

    #[sinex_test]
    async fn systemd_service_details_parse_watchdog_usec_when_valid() -> TestResult<()> {
        let details = SystemdServiceDetails::from_show_output(
            "ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=60000000\n",
        )?;

        assert_eq!(details.watchdog_usec, Some(60_000_000));
        assert_eq!(details.unit_type.as_deref(), Some("notify"));
        Ok(())
    }

    #[sinex_test]
    async fn discover_unit_files_in_path_reports_non_directory_paths() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bogus_path = temp.path().join("not-a-directory");
        std::fs::write(&bogus_path, "x")?;

        let error = discover_unit_files_in_path(bogus_path.to_str().expect("utf8 path"))
            .await
            .expect_err("non-directory path should fail honestly");

        assert!(error.to_string().contains("Failed to inspect systemd unit directory"));
        Ok(())
    }

    #[sinex_test]
    async fn discover_unit_files_in_path_finds_only_sinex_service_units() -> TestResult<()> {
        let temp = tempfile::tempdir()?;
        std::fs::write(temp.path().join("sinex-ingestd.service"), [])?;
        std::fs::write(temp.path().join("sinex-gateway.service"), [])?;
        std::fs::write(temp.path().join("postgresql.service"), [])?;
        std::fs::create_dir(temp.path().join("sinex-dir.service"))?;

        let mut found = discover_unit_files_in_path(temp.path().to_str().expect("utf8 path")).await?;
        found.sort();

        assert_eq!(
            found,
            vec![
                format!("{}/sinex-gateway.service", temp.path().display()),
                format!("{}/sinex-ingestd.service", temp.path().display()),
            ]
        );
        Ok(())
    }
}

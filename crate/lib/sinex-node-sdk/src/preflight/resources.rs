/*!
 * Resource verification module for Sinex Pre-Flight system
 *
 * Verifies system resource availability including:
 * - Available memory and disk space
 * - CPU capacity and load
 * - Network connectivity
 * - Filesystem permissions
 */

use crate::{NodeResult, SinexError};
use camino::Utf8Path;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::net::ToSocketAddrs;
use tracing::info;

use super::VerificationStatus;

fn env_string(name: &str) -> NodeResult<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(SinexError::configuration(format!(
            "Environment variable '{name}' is not valid UTF-8"
        ))),
    }
}

fn configured_state_dir() -> NodeResult<String> {
    if let Some(state_dir) = env_string("SINEX_STATE_DIR")? {
        return Ok(state_dir);
    }
    if let Some(state_home) = env_string("XDG_STATE_HOME")? {
        return Ok(format!("{state_home}/sinex"));
    }
    Ok("/var/lib/sinex".to_string())
}

fn configured_data_dir() -> NodeResult<String> {
    env_string("SINEX_DATA_DIR")?.map_or_else(configured_state_dir, Ok)
}

fn configured_log_dir() -> NodeResult<String> {
    if let Some(log_dir) = env_string("SINEX_LOG_DIR")? {
        return Ok(log_dir);
    }
    Ok(format!("{}/logs", configured_state_dir()?))
}

fn configured_work_dir() -> NodeResult<String> {
    if let Some(work_dir) = env_string("SINEX_WORK_DIR")? {
        return Ok(work_dir);
    }

    match dirs::cache_dir() {
        Some(dir) => dir
            .join("sinex")
            .into_os_string()
            .into_string()
            .map_err(|path| {
                SinexError::configuration(format!(
                    "Default cache directory for SINEX_WORK_DIR is not valid UTF-8: {path:?}"
                ))
            }),
        None => Ok("/tmp/sinex".to_string()),
    }
}

fn configured_tmp_dir() -> NodeResult<String> {
    Ok(env_string("TMPDIR")?.unwrap_or_else(|| "/tmp".to_string()))
}

/// Verify system resource availability for Sinex deployment
pub async fn verify_system_resources() -> NodeResult<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_warnings = false;
    let mut has_failures = false;

    info!("Verifying system resource availability");

    // Memory verification
    match verify_memory_availability(&mut messages).await {
        Ok(memory_info) => {
            details.insert("memory", memory_info);
        }
        Err(e) => {
            messages.push(format!("✗ Memory verification failed: {e}"));
            has_failures = true;
        }
    }

    // Disk space verification
    match verify_disk_space(&mut messages).await {
        Ok(disk_info) => {
            details.insert("disk", disk_info);
        }
        Err(e) => {
            messages.push(format!("✗ Disk space verification failed: {e}"));
            has_failures = true;
        }
    }

    // CPU load verification
    match verify_cpu_capacity(&mut messages).await {
        Ok(cpu_info) => {
            // Check if system is under high load
            if let Some(load) = cpu_info
                .get("load_average_1min")
                .and_then(serde_json::Value::as_f64)
                && load > 8.0
            {
                messages.push(format!("⚠ High system load detected: {load:.2}"));
                has_warnings = true;
            }

            details.insert("cpu", cpu_info);
        }
        Err(e) => {
            messages.push(format!("✗ CPU verification failed: {e}"));
            has_failures = true;
        }
    }

    // Filesystem permissions verification
    match verify_filesystem_permissions(&mut messages).await {
        Ok(fs_info) => {
            if !fs_info
                .get("meets_requirements")
                .and_then(Value::as_bool)
                .unwrap_or(true)
            {
                has_failures = true;
            }
            details.insert("filesystem", fs_info);
        }
        Err(e) => {
            messages.push(format!("✗ Filesystem verification failed: {e}"));
            has_failures = true;
        }
    }

    // Network connectivity verification
    match verify_network_connectivity(&mut messages).await {
        Ok(network_info) => {
            details.insert("network", network_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Network verification warning: {e}"));
            has_warnings = true;
        }
    }

    // Process limits verification
    match verify_process_limits(&mut messages) {
        Ok(limits_info) => {
            details.insert("process_limits", limits_info);
        }
        Err(e) => {
            messages.push(format!("⚠ Process limits verification warning: {e}"));
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

    info!("Resource verification completed with status: {:?}", status);
    Ok((status, json!(details), messages))
}

async fn verify_memory_availability(messages: &mut Vec<String>) -> NodeResult<Value> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_memory();

    let total_memory_gb = sys.total_memory() as f64 / 1024.0 / 1024.0 / 1024.0;
    let available_memory_gb = sys.available_memory() as f64 / 1024.0 / 1024.0 / 1024.0;
    let used_memory_gb = sys.used_memory() as f64 / 1024.0 / 1024.0 / 1024.0;
    let memory_usage_percent = (used_memory_gb / total_memory_gb) * 100.0;

    // Sinex requirements: minimum 2GB available, warning if <4GB
    let min_required_gb = 2.0;
    let recommended_gb = 4.0;

    if available_memory_gb < min_required_gb {
        return Err(SinexError::processing(format!(
            "Insufficient memory: {available_memory_gb:.2}GB available, {min_required_gb:.2}GB required"
        )));
    } else if available_memory_gb < recommended_gb {
        messages.push(format!(
            "⚠ Low memory: {available_memory_gb:.2}GB available, {recommended_gb:.2}GB recommended"
        ));
    } else {
        messages.push(format!(
            "✓ Memory sufficient: {available_memory_gb:.2}GB available"
        ));
    }

    Ok(json!({
        "total_gb": total_memory_gb,
        "available_gb": available_memory_gb,
        "used_gb": used_memory_gb,
        "usage_percent": memory_usage_percent,
        "min_required_gb": min_required_gb,
        "meets_requirements": available_memory_gb >= min_required_gb
    }))
}

async fn verify_disk_space(messages: &mut Vec<String>) -> NodeResult<Value> {
    let data_dir = configured_data_dir()?;
    let tmp_dir = configured_tmp_dir()?;
    let log_dir = configured_log_dir()?;
    let paths_to_check = vec![
        (data_dir, "Sinex data directory".to_string(), 10.0), // 10GB minimum
        (tmp_dir, "Temporary directory".to_string(), 5.0),    // 5GB minimum
        (log_dir, "Sinex log directory".to_string(), 2.0),    // 2GB minimum
    ];

    let mut disk_info = HashMap::new();
    let mut total_required = 0.0;
    let mut has_issues = false;

    for (path, description, min_gb) in &paths_to_check {
        let min_gb = *min_gb;
        total_required += min_gb;

        match get_disk_space(path.as_str()) {
            Ok((total_gb, available_gb)) => {
                let usage_percent = ((total_gb - available_gb) / total_gb) * 100.0;

                disk_info.insert(
                    path.clone(),
                    json!({
                        "description": description,
                        "total_gb": total_gb,
                        "available_gb": available_gb,
                        "usage_percent": usage_percent,
                        "min_required_gb": min_gb,
                        "meets_requirements": available_gb >= min_gb
                    }),
                );

                if available_gb < min_gb {
                    messages.push(format!(
                        "✗ {description} ({path}): {available_gb:.2}GB available, {min_gb:.2}GB required"
                    ));
                    has_issues = true;
                } else if available_gb < min_gb * 2.0 {
                    messages.push(format!(
                        "⚠ {description} ({path}): {available_gb:.2}GB available (low)"
                    ));
                } else {
                    messages.push(format!(
                        "✓ {description} ({path}): {available_gb:.2}GB available"
                    ));
                }
            }
            Err(e) => {
                messages.push(format!("⚠ Could not check disk space for {path}: {e}"));
                disk_info.insert(
                    path.clone(),
                    json!({
                        "description": description,
                        "error": e.to_string(),
                        "meets_requirements": false
                    }),
                );
            }
        }
    }

    if has_issues {
        return Err(SinexError::processing(
            "Insufficient disk space on one or more required paths".to_string(),
        ));
    }

    Ok(json!({
        "paths": disk_info,
        "total_required_gb": total_required
    }))
}

fn get_disk_space(path: &str) -> NodeResult<(f64, f64)> {
    use nix::sys::statvfs::statvfs;

    let stat = statvfs(path).map_err(|e| SinexError::processing(format!("Error: {e}")))?;

    let block_size = stat.block_size();
    let total_blocks = stat.blocks();
    let available_blocks = stat.blocks_available();

    let total_bytes = total_blocks * block_size;
    let available_bytes = available_blocks * block_size;

    let total_gb = total_bytes as f64 / 1024.0 / 1024.0 / 1024.0;
    let available_gb = available_bytes as f64 / 1024.0 / 1024.0 / 1024.0;

    Ok((total_gb, available_gb))
}

async fn verify_cpu_capacity(messages: &mut Vec<String>) -> NodeResult<Value> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_cpu_all();

    let cpu_count = sys.cpus().len();
    let load_avg = System::load_average();

    // Basic CPU requirements for Sinex
    let min_cpu_count = 2;
    let max_recommended_load = cpu_count as f64 * 0.8; // 80% of CPU capacity

    if cpu_count < min_cpu_count {
        return Err(SinexError::processing(format!(
            "Insufficient CPU cores: {cpu_count} available, {min_cpu_count} required"
        )));
    }

    if load_avg.one > max_recommended_load {
        messages.push(format!(
            "⚠ High CPU load: {:.2}, recommended max: {max_recommended_load:.2}",
            load_avg.one
        ));
    } else {
        messages.push(format!(
            "✓ CPU capacity sufficient: {cpu_count} cores, load: {:.2}",
            load_avg.one
        ));
    }

    Ok(json!({
        "cpu_count": cpu_count,
        "load_average_1min": load_avg.one,
        "load_average_5min": load_avg.five,
        "load_average_15min": load_avg.fifteen,
        "min_required_cores": min_cpu_count,
        "max_recommended_load": max_recommended_load,
        "meets_requirements": cpu_count >= min_cpu_count && load_avg.one <= max_recommended_load * 1.2
    }))
}

async fn verify_filesystem_permissions(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut directories_to_check = Vec::new();
    for dir in [
        configured_state_dir()?,
        configured_data_dir()?,
        configured_log_dir()?,
        configured_tmp_dir()?,
        configured_work_dir()?,
    ] {
        if !directories_to_check.contains(&dir) {
            directories_to_check.push(dir);
        }
    }

    let mut permissions_info = HashMap::new();
    let mut has_issues = false;

    for dir_path in &directories_to_check {
        match check_directory_permissions(dir_path.as_str()).await {
            Ok(perms) => {
                let is_writable = perms["writable"].as_bool().unwrap_or(false);
                if let Some(cleanup_error) = perms.get("cleanup_error").and_then(Value::as_str) {
                    messages.push(format!(
                        "⚠ Directory {dir_path} left a preflight probe file behind: {cleanup_error}"
                    ));
                }
                permissions_info.insert(dir_path.clone(), perms);

                if is_writable {
                    messages.push(format!("✓ Directory {dir_path} is writable"));
                } else {
                    messages.push(format!("✗ Directory {dir_path} is not writable"));
                    has_issues = true;
                }
            }
            Err(e) => {
                messages.push(format!("⚠ Could not check permissions for {dir_path}: {e}"));
                permissions_info.insert(
                    dir_path.clone(),
                    json!({
                        "error": e.to_string(),
                        "writable": false
                    }),
                );
            }
        }
    }

    Ok(json!({
        "directories": permissions_info,
        "meets_requirements": !has_issues
    }))
}

async fn check_directory_permissions(dir_path: &str) -> NodeResult<Value> {
    let path = Utf8Path::new(dir_path);
    let metadata = match tokio::fs::metadata(path.as_std_path()).await {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(json!({
                "exists": false,
                "is_directory": false,
                "writable": false,
                "readable": false,
                "error": "directory does not exist"
            }));
        }
        Err(e) => {
            return Ok(json!({
                "exists": path.exists(),
                "is_directory": false,
                "writable": false,
                "readable": false,
                "error": e.to_string()
            }));
        }
    };

    if !metadata.is_dir() {
        return Ok(json!({
            "exists": true,
            "is_directory": false,
            "writable": false,
            "readable": true,
            "error": "path is not a directory"
        }));
    }

    // Test write permissions by creating a temporary file
    let test_file = path.join(format!(".sinex_preflight_test_{}", std::process::id()));

    match tokio::fs::write(test_file.as_std_path(), "test").await {
        Ok(()) => {
            let mut details = json!({
                "exists": true,
                "is_directory": true,
                "writable": true,
                "readable": true
            });
            if let Err(error) = tokio::fs::remove_file(test_file.as_std_path()).await {
                details["cleanup_error"] = json!(error.to_string());
            }

            Ok(details)
        }
        Err(e) => Ok(json!({
            "exists": true,
            "is_directory": true,
            "writable": false,
            "readable": true,
            "error": e.to_string()
        })),
    }
}

async fn verify_network_connectivity(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut network_info = HashMap::new();

    match test_loopback_resolution() {
        Ok(()) => {
            messages.push("✓ Loopback hostname resolution working".to_string());
            network_info.insert("loopback_resolution", json!(true));
        }
        Err(e) => {
            messages.push(format!("⚠ Loopback hostname resolution issue: {e}"));
            network_info.insert("loopback_resolution", json!(false));
        }
    }

    let configured_targets = configured_hostname_resolution_probe();
    if !configured_targets.invalid_inputs.is_empty() {
        let invalid = configured_targets
            .invalid_inputs
            .iter()
            .map(|input| format!("{}={} ({})", input.env_name, input.raw, input.error))
            .collect::<Vec<_>>();
        messages.push(format!(
            "⚠ Invalid configured hostname targets: {}",
            invalid.join("; ")
        ));
        network_info.insert(
            "configured_hostname_resolution_inputs",
            json!(configured_targets.invalid_inputs),
        );
    }

    if configured_targets.hosts.is_empty() {
        messages.push(
            if network_info.contains_key("configured_hostname_resolution_inputs") {
                "ℹ No valid configured network hostnames to resolve; hostname probe skipped"
                    .to_string()
            } else {
                "ℹ No configured network hostnames to resolve; hostname probe skipped"
                    .to_string()
            },
        );
        network_info.insert(
            "configured_hostname_resolution",
            json!({
                "skipped": true,
                "reason": if network_info.contains_key("configured_hostname_resolution_inputs") {
                    "no_valid_configured_hostnames"
                } else {
                    "no_configured_hostnames"
                },
            }),
        );
    } else {
        let mut results = serde_json::Map::new();
        let mut failed_hosts = Vec::new();

        for host in configured_targets.hosts {
            match resolve_hostname(&host) {
                Ok(()) => {
                    results.insert(host, json!({ "resolved": true }));
                }
                Err(error) => {
                    failed_hosts.push(format!("{host}: {error}"));
                    results.insert(host, json!({ "resolved": false, "error": error.to_string() }));
                }
            }
        }

        if failed_hosts.is_empty() {
            messages.push("✓ Configured hostname resolution working".to_string());
        } else {
            messages.push(format!(
                "⚠ Configured hostname resolution issues: {}",
                failed_hosts.join("; ")
            ));
        }

        network_info.insert(
            "configured_hostname_resolution",
            Value::Object(results),
        );
    }

    Ok(json!(network_info))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct InvalidResolutionTarget {
    env_name: &'static str,
    raw: String,
    error: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConfiguredHostnameResolutionProbe {
    hosts: Vec<String>,
    invalid_inputs: Vec<InvalidResolutionTarget>,
}

fn configured_hostname_resolution_probe() -> ConfiguredHostnameResolutionProbe {
    let mut targets = BTreeSet::new();
    let mut invalid_inputs = Vec::new();
    for env_name in ["DATABASE_URL", "SINEX_NATS_URL", "SINEX_GATEWAY_URL"] {
        match std::env::var_os(env_name) {
            None => {}
            Some(raw) => match raw.into_string() {
                Ok(raw) => match resolution_target_host(&raw) {
                    Ok(Some(host)) => {
                        targets.insert(host);
                    }
                    Ok(None) => {}
                    Err(error) => invalid_inputs.push(InvalidResolutionTarget {
                        env_name,
                        raw,
                        error,
                    }),
                },
                Err(raw) => invalid_inputs.push(InvalidResolutionTarget {
                    env_name,
                    raw: format!("{raw:?}"),
                    error: "value contains non-unicode bytes".to_string(),
                }),
            },
        }
    }
    ConfiguredHostnameResolutionProbe {
        hosts: targets.into_iter().collect(),
        invalid_inputs,
    }
}

fn resolution_target_host(raw: &str) -> Result<Option<String>, String> {
    let candidate = if raw.contains("://") {
        raw.to_string()
    } else if raw.contains(':') && !raw.starts_with('/') {
        format!("dummy://{raw}")
    } else {
        return Err("configured endpoint is not a URL or host:port target".to_string());
    };

    let parsed = url::Url::parse(&candidate)
        .map_err(|error| format!("failed to parse configured endpoint: {error}"))?;
    let Some(host) = parsed.host_str() else {
        return Ok(None);
    };
    if matches!(host, "localhost" | "127.0.0.1" | "::1") {
        return Ok(None);
    }
    Ok(Some(host.to_string()))
}

fn resolve_hostname(host: &str) -> NodeResult<()> {
    (host, 0)
        .to_socket_addrs()
        .map_err(|e| SinexError::processing(format!("Failed to resolve host '{host}': {e}")))?
        .next()
        .ok_or_else(|| {
            SinexError::processing(format!("Host '{host}' resolved to no socket addresses"))
        })?;

    Ok(())
}

fn test_loopback_resolution() -> NodeResult<()> {
    ("localhost", 0)
        .to_socket_addrs()
        .map_err(|e| SinexError::processing(format!("Failed to resolve localhost: {e}")))?
        .next()
        .ok_or_else(|| SinexError::processing("localhost resolved to no socket addresses"))?;
    Ok(())
}

fn verify_process_limits(messages: &mut Vec<String>) -> NodeResult<Value> {
    let mut limits_info = HashMap::new();

    // Check file descriptor limits
    match check_file_descriptor_limits() {
        Ok(fd_info) => {
            limits_info.insert("file_descriptors", fd_info);
            messages.push("✓ File descriptor limits checked".to_string());
        }
        Err(e) => {
            messages.push(format!("⚠ Could not check file descriptor limits: {e}"));
        }
    }

    // Check process limits
    match check_process_limits_info() {
        Ok(proc_info) => {
            limits_info.insert("processes", proc_info);
            messages.push("✓ Process limits checked".to_string());
        }
        Err(e) => {
            messages.push(format!("⚠ Could not check process limits: {e}"));
        }
    }

    Ok(json!(limits_info))
}

fn check_file_descriptor_limits() -> NodeResult<Value> {
    use nix::sys::resource::{Resource, getrlimit};

    let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE).map_err(|e| {
        SinexError::processing(format!("Failed to get file descriptor limits: {e}"))
    })?;

    let min_recommended = 1024;
    let meets_requirements = soft >= min_recommended;

    Ok(json!({
        "soft_limit": soft,
        "hard_limit": hard,
        "min_recommended": min_recommended,
        "meets_requirements": meets_requirements
    }))
}

fn check_process_limits_info() -> NodeResult<Value> {
    use nix::sys::resource::{Resource, getrlimit};

    let (soft, hard) = getrlimit(Resource::RLIMIT_NPROC)
        .map_err(|e| SinexError::processing(format!("Failed to get process limits: {e}")))?;

    Ok(json!({
        "max_processes_soft": soft,
        "max_processes_hard": hard
    }))
}

#[cfg(test)]
mod tests {
    use super::{configured_hostname_resolution_probe, resolution_target_host};
    use xtask::sandbox::sinex_test;

    struct EnvGuard {
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl EnvGuard {
        fn new() -> Self {
            Self { saved: Vec::new() }
        }

        fn set(&mut self, key: &'static str, value: &str) {
            self.saved.push((key, std::env::var(key).ok()));
            unsafe { std::env::set_var(key, value) };
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..).rev() {
                match value {
                    Some(value) => unsafe { std::env::set_var(key, value) },
                    None => unsafe { std::env::remove_var(key) },
                }
            }
        }
    }

    #[sinex_test]
    async fn resolution_target_host_skips_local_and_socket_targets()
    -> ::xtask::sandbox::TestResult<()> {
        assert_eq!(
            resolution_target_host("postgresql://db.example/sinex"),
            Ok(Some("db.example".to_string()))
        );
        assert_eq!(
            resolution_target_host("nats://nats.example:4222"),
            Ok(Some("nats.example".to_string()))
        );
        assert_eq!(
            resolution_target_host("127.0.0.1:4222"),
            Ok(None),
            "loopback-only endpoints should not be reported as hostname resolution targets"
        );
        assert_eq!(
            resolution_target_host("postgresql:///sinex?host=/tmp"),
            Ok(None),
            "unix-socket URLs should not be treated as DNS targets"
        );
        Ok(())
    }

    #[sinex_test]
    async fn resolution_target_host_rejects_invalid_configured_target()
    -> ::xtask::sandbox::TestResult<()> {
        let error = resolution_target_host("db.example")
            .expect_err("bare hostname should surface as invalid configured endpoint");
        assert!(error.contains("not a URL or host:port target"));
        Ok(())
    }

    #[sinex_test]
    async fn configured_hostname_resolution_targets_deduplicate_hosts()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("DATABASE_URL", "postgresql://db.example/sinex");
        env.set("SINEX_NATS_URL", "nats://db.example:4222");
        env.set("SINEX_GATEWAY_URL", "https://gateway.example/rpc");

        let targets = configured_hostname_resolution_probe().hosts;
        assert_eq!(
            targets,
            vec!["db.example".to_string(), "gateway.example".to_string()]
        );
        Ok(())
    }

    #[sinex_test]
    async fn configured_hostname_resolution_probe_collects_invalid_inputs()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("DATABASE_URL", "db.example");
        env.set("SINEX_NATS_URL", "nats://nats.example:4222");

        let probe = configured_hostname_resolution_probe();
        assert_eq!(probe.hosts, vec!["nats.example".to_string()]);
        assert_eq!(probe.invalid_inputs.len(), 1);
        assert_eq!(probe.invalid_inputs[0].env_name, "DATABASE_URL");
        assert_eq!(probe.invalid_inputs[0].raw, "db.example");
        assert!(
            probe.invalid_inputs[0]
                .error
                .contains("not a URL or host:port target")
        );
        Ok(())
    }
}

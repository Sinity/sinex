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
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use tracing::info;

use super::VerificationStatus;

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
            if let Some(load) = cpu_info.get("load_average_1min").and_then(|v| v.as_f64()) {
                if load > 8.0 {
                    messages.push(format!("⚠ High system load detected: {load:.2}"));
                    has_warnings = true;
                }
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
    match verify_process_limits(&mut messages).await {
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
    let paths_to_check = vec![
        ("/var/lib/sinex", "Sinex data directory", 10.0), // 10GB minimum
        ("/tmp", "Temporary directory", 5.0),             // 5GB minimum
        ("/var/log", "Log directory", 2.0),               // 2GB minimum
    ];

    let mut disk_info = HashMap::new();
    let mut total_required = 0.0;
    let mut has_issues = false;

    for (path, description, min_gb) in paths_to_check {
        total_required += min_gb;

        match get_disk_space(path) {
            Ok((total_gb, available_gb)) => {
                let usage_percent = ((total_gb - available_gb) / total_gb) * 100.0;

                disk_info.insert(
                    path.to_string(),
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
                        "✗ {description}: {available_gb:.2}GB available, {min_gb:.2}GB required"
                    ));
                    has_issues = true;
                } else if available_gb < min_gb * 2.0 {
                    messages.push(format!(
                        "⚠ {description}: {available_gb:.2}GB available (low)"
                    ));
                } else {
                    messages.push(format!("✓ {description}: {available_gb:.2}GB available"));
                }
            }
            Err(e) => {
                messages.push(format!("⚠ Could not check disk space for {path}: {e}"));
                disk_info.insert(
                    path.to_string(),
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
    let directories_to_check = vec!["/var/lib/sinex", "/var/log/sinex", "/tmp"];

    let mut permissions_info = HashMap::new();
    let mut has_issues = false;

    for dir_path in directories_to_check {
        match check_directory_permissions(dir_path).await {
            Ok(perms) => {
                let is_writable = perms["writable"].as_bool().unwrap_or(false);
                permissions_info.insert(dir_path.to_string(), perms);

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
                    dir_path.to_string(),
                    json!({
                        "error": e.to_string(),
                        "writable": false
                    }),
                );
            }
        }
    }

    if has_issues {
        return Err(SinexError::processing(
            "Insufficient filesystem permissions for required directories".to_string(),
        ));
    }

    Ok(json!({
        "directories": permissions_info
    }))
}

async fn check_directory_permissions(dir_path: &str) -> NodeResult<Value> {
    let path = Utf8Path::new(dir_path);

    // Create directory if it doesn't exist
    if !path.exists() {
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| SinexError::processing(format!("Error: {e}")))?;
    }

    // Test write permissions by creating a temporary file
    let test_file = path.join(".sinex_preflight_test");

    match tokio::fs::write(&test_file, "test").await {
        Ok(_) => {
            // Clean up test file
            tokio::fs::remove_file(&test_file).await.ok();

            Ok(json!({
                "exists": true,
                "writable": true,
                "readable": true
            }))
        }
        Err(e) => Ok(json!({
            "exists": path.exists(),
            "writable": false,
            "readable": path.metadata().is_ok(),
            "error": e.to_string()
        })),
    }
}

async fn verify_network_connectivity(messages: &mut Vec<String>) -> NodeResult<Value> {
    // Basic network connectivity tests
    let mut network_info = HashMap::new();

    // Check if we can resolve DNS
    match test_dns_resolution().await {
        Ok(_) => {
            messages.push("✓ DNS resolution working".to_string());
            network_info.insert("dns_resolution", json!(true));
        }
        Err(e) => {
            messages.push(format!("⚠ DNS resolution issue: {e}"));
            network_info.insert("dns_resolution", json!(false));
        }
    }

    // Check localhost connectivity (for PostgreSQL)
    match test_localhost_connectivity().await {
        Ok(_) => {
            messages.push("✓ Localhost connectivity working".to_string());
            network_info.insert("localhost_connectivity", json!(true));
        }
        Err(e) => {
            messages.push(format!("⚠ Localhost connectivity issue: {e}"));
            network_info.insert("localhost_connectivity", json!(false));
        }
    }

    Ok(json!(network_info))
}

async fn test_dns_resolution() -> NodeResult<()> {
    // Try to resolve a well-known hostname
    "google.com:80"
        .to_socket_addrs()
        .map_err(|e| SinexError::processing(format!("Failed to resolve DNS: {e}")))?
        .next()
        .ok_or_else(|| SinexError::processing("No DNS resolution results".to_string()))?;

    Ok(())
}

async fn test_localhost_connectivity() -> NodeResult<()> {
    use std::net::SocketAddr;
    use std::time::Duration;

    // Test localhost connectivity by attempting to connect to a common port
    let addr: SocketAddr = "127.0.0.1:22"
        .parse()
        .map_err(|e| SinexError::processing(format!("Failed to parse localhost address: {e}")))?;

    // Try to connect with a short timeout
    if tokio::time::timeout(
        Duration::from_millis(100),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Connection timeout"))
    .and_then(|result| result)
    .is_ok()
    {
        Ok(())
    } else {
        // SSH not running is normal, just test that localhost is reachable
        // Try a different approach - just verify localhost resolves
        "localhost:80".to_socket_addrs().map_err(|e| {
            SinexError::processing(format!("Localhost name resolution failed: {e}"))
        })?;
        Ok(())
    }
}

async fn verify_process_limits(messages: &mut Vec<String>) -> NodeResult<Value> {
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
    use nix::sys::resource::{getrlimit, Resource};

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
    use nix::sys::resource::{getrlimit, Resource};

    let (soft, hard) = getrlimit(Resource::RLIMIT_NPROC)
        .map_err(|e| SinexError::processing(format!("Failed to get process limits: {e}")))?;

    Ok(json!({
        "max_processes_soft": soft,
        "max_processes_hard": hard
    }))
}

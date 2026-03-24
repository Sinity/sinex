//! Doctor command - health check for Postgres, NATS, tools, and TLS

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::infra::probe::{probe_nats, probe_postgres};
use crate::output::Status;
use crate::tools::{ToolInfo, ToolManager};
use color_eyre::eyre::{Result, WrapErr};
use console::style;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sinex_primitives::{
    DeploymentReadinessDescriptor, DeploymentReadinessMode, environment::SinexEnvironment,
    nats::NatsConnectionConfig,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const DEPLOYMENT_READY_TIMEOUT: Duration = Duration::from_secs(5);
const RECOMMENDED_INOTIFY_MAX_USER_WATCHES: u64 = 524_288;
const RUNTIME_LAG_WARN_THRESHOLD: f64 = 1000.0;
const RUNTIME_BATCH_LATENCY_WARN_THRESHOLD_MS: f64 = 5000.0;

#[derive(clap::Args)]
pub struct DoctorCommand {
    /// Run pipeline smoke tests in addition to health checks
    #[arg(long)]
    pub pipelines: bool,

    /// Auto-remediate: restart stale processes, invalidate stale preflight cache
    #[arg(long)]
    pub fix: bool,

    /// Check runtime health (ingestd heartbeat, consumer lag, batch latency)
    #[arg(long)]
    pub runtime: bool,

    /// Check deployment readiness (schema, services, permissions)
    #[arg(long)]
    pub deployment_readiness: bool,
}

/// Doctor report structures
#[derive(Debug, Serialize)]
pub(crate) struct DoctorReport {
    pub postgres: DoctorServiceCheck,
    pub nats: DoctorServiceCheck,
    pub tools: Vec<ToolCheck>,
    pub environment: Option<serde_json::Value>,
    pub tls: Option<TlsCheck>,
    pub postgres_extensions: Option<Vec<String>>,
    pub overall: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct DoctorServiceCheck {
    pub available: bool,
    pub message: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ToolCheck {
    pub name: String,
    pub available: bool,
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct TlsCheck {
    pub ca_exists: bool,
    pub server_cert_exists: bool,
    pub client_cert_exists: bool,
    /// Days until server cert expires (None if cert missing or unreadable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_expires_days: Option<i64>,
    /// Whether the server cert is expired
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_expired: Option<bool>,
    /// Whether the server cert's private key matches
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_matches: Option<bool>,
}

impl XtaskCommand for DoctorCommand {
    fn name(&self) -> &'static str {
        "doctor"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let mut result = execute_doctor(self.pipelines, ctx)?;

        if self.runtime {
            let runtime = execute_runtime_check(ctx).await?;
            let runtime_value = serde_json::to_value(&runtime)?;
            let existing_data = result.data.take();
            result.data = Some(match existing_data {
                Some(mut existing) => {
                    if let Some(map) = existing.as_object_mut() {
                        map.insert("runtime".to_string(), runtime_value);
                        existing
                    } else {
                        serde_json::json!({
                            "doctor": existing,
                            "runtime": runtime_value,
                        })
                    }
                }
                None => serde_json::json!({
                    "runtime": runtime_value,
                }),
            });

            if !runtime.overall && result.status == Status::Success {
                result.status = Status::Partial;
            }
            result.warnings.extend(runtime.warnings.clone());
        }

        if self.deployment_readiness {
            let readiness = execute_deployment_readiness(ctx).await?;
            let readiness_value = serde_json::to_value(&readiness)?;
            let existing_data = result.data.take();
            result.data = Some(match existing_data {
                Some(mut existing) => {
                    if let Some(map) = existing.as_object_mut() {
                        map.insert("deployment_readiness".to_string(), readiness_value);
                        existing
                    } else {
                        serde_json::json!({
                            "doctor": existing,
                            "deployment_readiness": readiness_value,
                        })
                    }
                }
                None => serde_json::json!({
                    "deployment_readiness": readiness_value,
                }),
            });

            if !readiness.overall && result.status == Status::Success {
                result.status = Status::Partial;
                result
                    .warnings
                    .push("Deployment readiness has failing checks".to_string());
            }
        }

        if self.fix {
            crate::preflight::invalidate_cache();
            if ctx.is_human() {
                println!("Invalidated preflight cache");
            }

            // Check infra status and restart if needed
            let pg_probe = probe_postgres();
            let nats_probe = probe_nats();

            if !pg_probe.ready() || !nats_probe.ready() {
                let stack_config = crate::infra::stack::StackConfig::for_current_checkout().ok();
                if let Some(cfg) = stack_config {
                    let verbose = ctx.is_human();
                    if !pg_probe.ready() {
                        let _ = crate::infra::stack::pg_start(&cfg, verbose);
                    }
                    if !nats_probe.ready() {
                        let _ = crate::infra::stack::nats_start(&cfg, verbose);
                    }
                }
            }
        }

        Ok(result)
    }

    fn metadata(&self) -> CommandMetadata {
        if self.fix {
            CommandMetadata::build()
        } else {
            CommandMetadata::diagnostics()
        }
    }
}

/// Run diagnostics (replaces 'stack doctor')
fn execute_doctor(pipelines: bool, ctx: &CommandContext) -> Result<CommandResult> {
    use crate::process::ProcessBuilder;

    let mut all_ok = true;

    // Check Postgres
    let pg_probe = probe_postgres();
    let pg_msg = if pg_probe.ready() {
        None
    } else {
        all_ok = false;
        Some(
            pg_probe
                .message
                .clone()
                .unwrap_or_else(|| "Postgres is not ready".to_string()),
        )
    };

    // Check NATS
    let nats_probe = probe_nats();
    let nats_msg = if nats_probe.ready() {
        None
    } else {
        all_ok = false;
        Some(
            nats_probe
                .message
                .clone()
                .unwrap_or_else(|| format!("Cannot connect to NATS on port {}", nats_probe.port)),
        )
    };

    // Check required tools
    let tools_to_check = [
        "rustc",
        "ast-grep",
        "repomix",
        "cargo-machete",
        "cargo-nextest",
    ];
    let mut tool_checks = Vec::new();
    for tool in tools_to_check {
        let check_result = ToolManager::check_tool(tool);
        let info = check_result.unwrap_or_else(|_| {
            all_ok = false;
            ToolInfo::unavailable(tool)
        });
        let available = info.is_available;
        let version = if info.is_available {
            Some(info.version)
        } else {
            None
        };
        let path = if info.is_available {
            Some(info.path.display().to_string())
        } else {
            None
        };
        tool_checks.push(ToolCheck {
            name: tool.to_string(),
            available,
            version,
            path,
        });
    }

    // Batch validation summary for missing tools
    let missing = ToolManager::check_required_tools(&tools_to_check);

    // Check Postgres extensions
    let mut pg_extensions = None;
    if pg_probe.ready() {
        let config = crate::infra::stack::StackConfig::for_current_checkout().ok();
        if let Some(cfg) = config {
            let output = std::process::Command::new("psql")
                .env("PGHOST", cfg.run_dir())
                .env("PGPORT", cfg.postgres.port.to_string())
                .args(["-tAc", "SELECT extname FROM pg_extension"])
                .output();

            if let Ok(o) = output {
                let exts: Vec<String> = String::from_utf8_lossy(&o.stdout)
                    .lines()
                    .map(ToString::to_string)
                    .collect();
                pg_extensions = Some(exts);
            }
        }
    }

    // Check TLS certificates from env vars or .sinex/tls/
    let tls_check = {
        let default_tls_dir = std::path::Path::new(".sinex/tls");
        let check = |dir: &std::path::Path, stem: &str| dir.join(format!("{stem}.pem")).exists();
        // If SINEX_GATEWAY_TLS_CERT is set, derive the directory from it
        let env_dir = std::env::var("SINEX_GATEWAY_TLS_CERT")
            .ok()
            .and_then(|p| std::path::Path::new(&p).parent().map(|d| d.to_path_buf()));
        let active_dir = if let Some(ref d) = env_dir {
            if d.exists() { Some(d.as_path()) } else { None }
        } else if default_tls_dir.exists() {
            Some(default_tls_dir as &std::path::Path)
        } else {
            None
        };
        active_dir.map(|dir| {
            let server_cert_path = dir.join("server.pem");
            let server_key_path = dir.join("server-key.pem");
            let server_cert_exists = check(dir, "server");

            // Attempt detailed cert validity check when server cert exists
            let (server_expires_days, server_expired, key_matches) = if server_cert_path.exists() {
                let opts = crate::tls::TlsCheckOptions {
                    cert_path: Some(server_cert_path),
                    key_path: server_key_path.exists().then_some(server_key_path),
                    ..Default::default()
                };
                if let Ok(result) = crate::tls::check_tls_config(&opts) {
                    let days = result.certificate.as_ref().map(|c| c.days_until_expiry);
                    let expired = result.certificate.as_ref().map(|c| c.is_expired);
                    (days, expired, result.key_matches)
                } else {
                    (None, None, None)
                }
            } else {
                (None, None, None)
            };

            TlsCheck {
                ca_exists: check(dir, "ca"),
                server_cert_exists,
                client_cert_exists: check(dir, "client"),
                server_expires_days,
                server_expired,
                key_matches,
            }
        })
    };

    // Collect environment configuration
    let cfg = config();
    let environment = Some(serde_json::json!({
        "hostname": cfg.hostname,
        "state_dir": cfg.state_dir.display().to_string(),
        "cache_dir": cfg.cache_dir.display().to_string(),
        "database_url": cfg.database_url,
        "nats_url": cfg.nats_url,
        "gateway_url": cfg.gateway_url,
        "test_results_dir": cfg.test_results_dir.as_ref().map(|p| p.display().to_string()),
        "toolchain": cfg.toolchain,
        "in_devenv": cfg.in_devenv,
    }));

    let report = DoctorReport {
        postgres: DoctorServiceCheck {
            available: pg_probe.ready(),
            message: pg_msg,
        },
        nats: DoctorServiceCheck {
            available: nats_probe.ready(),
            message: nats_msg,
        },
        tools: tool_checks,
        environment,
        tls: tls_check,
        postgres_extensions: pg_extensions,
        overall: all_ok,
    };

    if ctx.is_human() {
        println!("{}", style("━━━━━━━━━━ DOCTOR ━━━━━━━━━━").bold());
        println!();

        // Infrastructure
        println!("{}", style("Infrastructure:").bold());
        print_check(
            "Postgres",
            report.postgres.available,
            report.postgres.message.as_deref(),
        );
        print_check(
            "NATS",
            report.nats.available,
            report.nats.message.as_deref(),
        );

        // Tools
        println!("\n{}", style("Required Tools:").bold());
        for tool in &report.tools {
            let version_str = tool.version.as_deref().unwrap_or("");
            print_check(&tool.name, tool.available, Some(version_str));
        }

        // Installation guidance for missing tools
        if !missing.is_empty() {
            println!("\n{}", style("Installation Guidance:").bold().yellow());
            for (tool_name, guidance) in &missing {
                println!("  {} {tool_name}:", style("→").yellow());
                for line in guidance.lines() {
                    println!("    {line}");
                }
            }
        }

        // Environment
        if let Some(env_data) = &report.environment {
            println!("\n{}", style("Environment:").bold());
            print_env_field(env_data, "hostname", "Hostname:");
            print_env_field(env_data, "state_dir", "State dir:");
            print_env_field(env_data, "cache_dir", "Cache dir:");
            print_env_field(env_data, "database_url", "Database URL:");
            print_env_field(env_data, "nats_url", "NATS URL:");
            print_env_field(env_data, "gateway_url", "Gateway URL:");
            print_env_field(env_data, "test_results_dir", "Test results:");
            print_env_field(env_data, "toolchain", "Toolchain:");
            if let Some(in_devenv) = env_data
                .get("in_devenv")
                .and_then(serde_json::Value::as_bool)
            {
                println!(
                    "  {:<20} {}",
                    "In devenv:",
                    if in_devenv { "yes" } else { "no" }
                );
            }
        }

        // TLS
        if let Some(tls) = &report.tls {
            println!("\n{}", style("TLS Certificates:").bold());
            print_check("CA certificate", tls.ca_exists, None);
            print_check("Server certificate", tls.server_cert_exists, None);
            if let Some(days) = tls.server_expires_days {
                if tls.server_expired.unwrap_or(false) {
                    println!("  {} Server certificate is expired", style("✗").red());
                } else if days < 30 {
                    println!("  {} Expires in {} days", style("⚠").yellow(), days);
                } else {
                    println!("     Expires in {days} days");
                }
            }
            if let Some(matches) = tls.key_matches {
                print_check("Key/cert match", matches, None);
            }
            print_check("Client certificate", tls.client_cert_exists, None);
        }

        // Extensions
        if let Some(exts) = &report.postgres_extensions {
            println!("\n{}", style("Postgres Extensions:").bold());
            println!("  {}", exts.join(", "));
        }

        // Pipeline smoke tests
        if pipelines {
            println!("\n{}", style("Pipeline Smoke Test:").bold());
            let result = ProcessBuilder::cargo()
                .args(["run", "-p", "sinex-test-utils"])
                .run();
            match result {
                Ok(_) => println!("  {} Pipeline test passed", style("✓").green()),
                Err(e) => println!("  {} Pipeline test failed: {}", style("✗").red(), e),
            }
        }

        // Summary
        println!();
        if all_ok {
            println!("{}", style("✓ All checks passed").green().bold());
        } else {
            println!("{}", style("✗ Some checks failed").red().bold());
            println!(
                "{}",
                style("Tip: set SINEX_LOG=debug for verbose preflight and pool diagnostics.").dim()
            );
        }
    }

    let result = if all_ok {
        CommandResult::success()
    } else {
        CommandResult::partial().with_warning("Doctor detected failing checks")
    };

    Ok(result
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed()))
}

fn print_env_field(env_data: &serde_json::Value, key: &str, label: &str) {
    if let Some(val) = env_data.get(key) {
        let display = if val.is_null() {
            "(not set)"
        } else {
            val.as_str().unwrap_or("(not set)")
        };
        println!("  {label:<20} {display}");
    }
}

fn print_check(name: &str, ok: bool, detail: Option<&str>) {
    let status = if ok {
        style("✓").green()
    } else {
        style("✗").red()
    };
    let detail_str = detail.map(|d| format!(" ({d})")).unwrap_or_default();
    println!("  {} {:<20}{}", status, name, style(detail_str).dim());
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeCheckReport {
    overall: bool,
    metrics: crate::runtime_metrics::RuntimeMetrics,
    warnings: Vec<String>,
}

fn runtime_warnings(metrics: &crate::runtime_metrics::RuntimeMetrics) -> Vec<String> {
    use crate::runtime_metrics::IngestdStatus;

    let mut warnings = Vec::new();
    match metrics.ingestd_status {
        IngestdStatus::Healthy => {}
        IngestdStatus::Stale => warnings.push("Runtime health: ingestd heartbeat is stale".into()),
        IngestdStatus::Down => warnings.push("Runtime health: ingestd is down".into()),
        IngestdStatus::Unknown => warnings.push("Runtime health: ingestd status is unknown".into()),
    }

    if let Some(lag) = metrics.fresh_consumer_lag_pending()
        && lag > RUNTIME_LAG_WARN_THRESHOLD
    {
        warnings.push(format!(
            "Runtime health: consumer lag is high ({lag:.0} pending)"
        ));
    }
    if metrics.consumer_lag_is_stale() {
        warnings.push(format!(
            "Runtime health: consumer lag telemetry is stale ({}s old)",
            metrics.consumer_lag_age_secs.unwrap_or_default()
        ));
    }

    if let Some(latency) = metrics.fresh_batch_latency_ms()
        && latency > RUNTIME_BATCH_LATENCY_WARN_THRESHOLD_MS
    {
        warnings.push(format!(
            "Runtime health: batch latency is high ({latency:.0}ms)"
        ));
    }
    if metrics.batch_latency_is_stale() {
        warnings.push(format!(
            "Runtime health: batch latency telemetry is stale ({}s old)",
            metrics.last_batch_latency_age_secs.unwrap_or_default()
        ));
    }

    warnings
}

async fn execute_runtime_check(ctx: &CommandContext) -> Result<RuntimeCheckReport> {
    use crate::config::config;
    use crate::runtime_metrics::{IngestdStatus, query_runtime_metrics};

    let cfg = config();
    let db_url = match &cfg.database_url {
        Some(url) => url.clone(),
        None => {
            if ctx.is_human() {
                println!("\n{}", style("Runtime Check:").bold());
                println!(
                    "  {} DATABASE_URL not set, skipping runtime checks",
                    style("⚠").yellow()
                );
            }
            return Ok(RuntimeCheckReport {
                overall: true,
                metrics: crate::runtime_metrics::RuntimeMetrics {
                    ingestd_status: crate::runtime_metrics::IngestdStatus::Unknown,
                    last_heartbeat_age_secs: None,
                    consumer_lag_pending: None,
                    consumer_lag_age_secs: None,
                    last_batch_latency_ms: None,
                    last_batch_latency_age_secs: None,
                },
                warnings: vec!["Runtime health skipped: DATABASE_URL not set".into()],
            });
        }
    };

    let metrics = query_runtime_metrics(&db_url).await;
    let warnings = runtime_warnings(&metrics);

    if ctx.is_human() {
        println!("\n{}", style("Runtime Health:").bold());

        // Ingestd heartbeat
        let status_icon = match metrics.ingestd_status {
            IngestdStatus::Healthy => style("✓").green(),
            IngestdStatus::Stale => style("⚠").yellow(),
            IngestdStatus::Down => style("✗").red(),
            IngestdStatus::Unknown => style("?").dim(),
        };
        let age_str = metrics
            .last_heartbeat_age_secs
            .map(|a| format!("(last heartbeat {a}s ago)"))
            .unwrap_or_default();
        println!(
            "  {} {:<20} {}",
            status_icon,
            format!("ingestd: {}", metrics.ingestd_status),
            style(age_str).dim()
        );

        // Consumer lag
        if let Some(lag) = metrics.fresh_consumer_lag_pending() {
            let lag_icon = if lag > 1000.0 {
                style("⚠").yellow()
            } else {
                style("✓").green()
            };
            println!("  {} Consumer lag:       {:.0} pending", lag_icon, lag);
        } else if metrics.consumer_lag_is_stale() {
            println!(
                "  {} Consumer lag:       stale telemetry (last sample {}s ago)",
                style("⚠").yellow(),
                metrics.consumer_lag_age_secs.unwrap_or_default()
            );
        }

        // Batch latency
        if let Some(latency) = metrics.fresh_batch_latency_ms() {
            let lat_icon = if latency > 5000.0 {
                style("⚠").yellow()
            } else {
                style("✓").green()
            };
            println!("  {} Batch latency:      {:.0}ms", lat_icon, latency);
        } else if metrics.batch_latency_is_stale() {
            println!(
                "  {} Batch latency:      stale telemetry (last sample {}s ago)",
                style("⚠").yellow(),
                metrics.last_batch_latency_age_secs.unwrap_or_default()
            );
        }
    }

    Ok(RuntimeCheckReport {
        overall: warnings.is_empty(),
        metrics,
        warnings,
    })
}

/// Result of a single deployment readiness check.
#[derive(Debug, Serialize)]
pub struct DeploymentReadinessItem {
    pub name: String,
    /// `"pass"`, `"fail"`, or `"skip"`
    pub status: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct DeploymentReadinessReport {
    pub items: Vec<DeploymentReadinessItem>,
    pub overall: bool,
}

#[derive(Debug, Clone)]
struct TargetIdentity {
    user: String,
    uid: u32,
    home: PathBuf,
}

#[derive(Debug)]
struct GatewayProbeClient {
    client: reqwest::Client,
    client_identity_path: Option<(PathBuf, PathBuf)>,
}

impl DeploymentReadinessItem {
    fn pass(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "pass".into(),
            description: description.into(),
        }
    }

    fn fail(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "fail".into(),
            description: description.into(),
        }
    }

    fn skip(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: "skip".into(),
            description: description.into(),
        }
    }
}

fn normalize_gateway_base_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    trimmed.strip_suffix("/rpc").unwrap_or(trimmed).to_string()
}

fn env_truthy(key: &str) -> bool {
    std::env::var(key)
        .is_ok_and(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn path_from_env_or_default(env_key: &str, default_path: PathBuf) -> Option<PathBuf> {
    std::env::var(env_key)
        .ok()
        .map(PathBuf::from)
        .or_else(|| default_path.exists().then_some(default_path))
}

fn load_deployment_descriptor() -> (
    Option<DeploymentReadinessDescriptor>,
    DeploymentReadinessItem,
) {
    let configured_path = DeploymentReadinessDescriptor::configured_path();
    match DeploymentReadinessDescriptor::load() {
        Ok(Some(descriptor)) => {
            let source =
                configured_path.unwrap_or_else(DeploymentReadinessDescriptor::default_path);
            let mode = match descriptor.mode {
                DeploymentReadinessMode::Prepared => "prepared",
                DeploymentReadinessMode::Enabled => "enabled",
                DeploymentReadinessMode::Unknown => "unknown",
            };
            let declared_source = descriptor
                .source
                .clone()
                .unwrap_or_else(|| "deployment descriptor".to_string());
            (
                Some(descriptor),
                DeploymentReadinessItem::pass(
                    "deployment-descriptor",
                    format!(
                        "Loaded {declared_source} ({mode} mode) from {}",
                        source.display()
                    ),
                ),
            )
        }
        Ok(None) => (
            None,
            DeploymentReadinessItem::skip(
                "deployment-descriptor",
                "No deployment readiness descriptor found; using environment fallbacks",
            ),
        ),
        Err(error) => (
            None,
            DeploymentReadinessItem::fail("deployment-descriptor", error.to_string()),
        ),
    }
}

fn read_passwd_entry(username: &str) -> Result<Option<(u32, PathBuf)>> {
    let contents = match std::fs::read_to_string("/etc/passwd") {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).wrap_err("failed to read /etc/passwd"),
    };

    for line in contents.lines() {
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 7 || fields[0] != username {
            continue;
        }

        let uid = fields[2]
            .parse::<u32>()
            .wrap_err_with(|| format!("failed to parse UID for {username} from /etc/passwd"))?;
        return Ok(Some((uid, PathBuf::from(fields[5]))));
    }

    Ok(None)
}

fn command_output(command: &str, args: &[&str], description: &str) -> Result<String> {
    let output = Command::new(command)
        .args(args)
        .output()
        .wrap_err_with(|| {
            format!(
                "failed to run `{command} {}` for {description}",
                args.join(" ")
            )
        })?;
    if !output.status.success() {
        color_eyre::eyre::bail!(
            "`{command} {}` failed with status {} while resolving {description}",
            args.join(" "),
            output
                .status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string())
        );
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn resolve_target_identity(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<TargetIdentity> {
    let descriptor_target = descriptor.and_then(|value| value.target.as_ref());
    let explicit_target_user = descriptor_target
        .map(|target| target.user.clone())
        .or_else(|| std::env::var("SINEX_TARGET_USER").ok())
        .filter(|value| !value.trim().is_empty());
    let current_user = std::env::var("USER")
        .ok()
        .filter(|value| !value.trim().is_empty());

    let user = match explicit_target_user.clone().or(current_user.clone()) {
        Some(user) => user,
        None => command_output("id", &["-un"], "deployment readiness target user")?,
    };
    let passwd_entry = read_passwd_entry(&user)?;

    let uid = if let Some(uid) = descriptor_target.and_then(|target| target.uid) {
        uid
    } else if let Ok(uid) = std::env::var("SINEX_TARGET_UID") {
        uid.parse::<u32>()
            .wrap_err("failed to parse SINEX_TARGET_UID for deployment readiness")?
    } else if let Some((uid, _)) = passwd_entry.as_ref() {
        *uid
    } else if let Ok(uid) = std::env::var("UID") {
        uid.parse::<u32>()
            .wrap_err("failed to parse UID for deployment readiness")?
    } else {
        command_output("id", &["-u"], "deployment readiness target UID")?
            .parse::<u32>()
            .wrap_err("failed to parse `id -u` output")?
    };

    let home = if let Some(home) = descriptor_target.and_then(|target| target.home.clone()) {
        home
    } else if let Ok(home) = std::env::var("SINEX_TARGET_HOME") {
        PathBuf::from(home)
    } else if explicit_target_user.is_none() {
        dirs::home_dir()
            .or_else(|| passwd_entry.as_ref().map(|(_, home)| home.clone()))
            .unwrap_or_else(|| PathBuf::from(format!("/home/{user}")))
    } else {
        passwd_entry
            .as_ref()
            .map(|(_, home)| home.clone())
            .unwrap_or_else(|| PathBuf::from(format!("/home/{user}")))
    };

    Ok(TargetIdentity { user, uid, home })
}

fn terminal_source_candidates(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Vec<(String, PathBuf)> {
    if let Some(descriptor) = descriptor
        && !descriptor.terminal.history_sources.is_empty()
    {
        return descriptor
            .terminal
            .history_sources
            .iter()
            .map(|source| (source.shell.clone(), source.path.clone()))
            .collect();
    }

    vec![
        ("bash".to_string(), target.home.join(".bash_history")),
        ("zsh".to_string(), target.home.join(".zsh_history")),
        (
            "atuin".to_string(),
            target.home.join(".local/share/atuin/history.db"),
        ),
        (
            "fish".to_string(),
            target.home.join(".local/share/fish/fish_history"),
        ),
    ]
}

fn validate_atuin_history_db(path: &Path) -> Result<()> {
    use rusqlite::{Connection, OpenFlags};

    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .wrap_err_with(|| format!("failed to open Atuin database at {}", path.display()))?;
    let has_history_table: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='history')",
            [],
            |row| row.get(0),
        )
        .wrap_err_with(|| format!("failed to inspect Atuin schema at {}", path.display()))?;
    if !has_history_table {
        color_eyre::eyre::bail!("missing `history` table");
    }

    Ok(())
}

fn activitywatch_db_for_target(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> PathBuf {
    descriptor
        .and_then(|value| value.desktop.activitywatch_db_path.clone())
        .unwrap_or_else(|| {
            target
                .home
                .join(".local/share/activitywatch/aw-server-rust/sqlite.db")
        })
}

fn validate_activitywatch_db(path: &Path) -> Result<()> {
    use rusqlite::{Connection, OpenFlags};

    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).wrap_err_with(
        || {
            format!(
                "failed to open ActivityWatch database at {}",
                path.display()
            )
        },
    )?;
    let has_events_table: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='events')",
            [],
            |row| row.get(0),
        )
        .wrap_err_with(|| {
            format!(
                "failed to inspect ActivityWatch events table at {}",
                path.display()
            )
        })?;
    let has_buckets_table: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='buckets')",
            [],
            |row| row.get(0),
        )
        .wrap_err_with(|| {
            format!(
                "failed to inspect ActivityWatch buckets table at {}",
                path.display()
            )
        })?;
    if !has_events_table || !has_buckets_table {
        color_eyre::eyre::bail!("missing `events`/`buckets` tables");
    }

    Ok(())
}

fn runtime_dir_for_target(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> PathBuf {
    descriptor
        .and_then(|value| value.desktop.runtime_dir.clone())
        .or_else(|| {
            std::env::var("SINEX_HYPRLAND_RUNTIME_DIR")
                .ok()
                .map(PathBuf::from)
        })
        .or_else(|| std::env::var("XDG_RUNTIME_DIR").ok().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", target.uid)))
}

/// Check 1: deployment binaries exist in PATH.
fn check_node_binaries() -> DeploymentReadinessItem {
    let nodes = [
        "sinex-ingestd",
        "sinex-gateway",
        "sinex-fs-ingestor",
        "sinex-terminal-ingestor",
        "sinex-desktop-ingestor",
        "sinex-system-ingestor",
    ];
    let missing: Vec<&str> = nodes
        .iter()
        .copied()
        .filter(|bin| which::which(bin).is_err())
        .collect();

    if missing.is_empty() {
        DeploymentReadinessItem::pass("node-binaries", "All node binaries found on PATH")
    } else {
        DeploymentReadinessItem::fail(
            "node-binaries",
            format!("Missing node binaries: {}", missing.join(", ")),
        )
    }
}

/// Check 2: /realm is readable by the current user.
fn check_realm_accessible() -> DeploymentReadinessItem {
    let realm = std::path::Path::new("/realm");
    if realm.exists() {
        match std::fs::read_dir(realm) {
            Ok(_) => DeploymentReadinessItem::pass(
                "realm-accessible",
                "/realm is accessible by the current user",
            ),
            Err(e) => DeploymentReadinessItem::fail(
                "realm-accessible",
                format!("/realm exists but is not readable: {e}"),
            ),
        }
    } else {
        DeploymentReadinessItem::fail("realm-accessible", "/realm does not exist")
    }
}

/// Check 3: terminal history sources currently consumed by the node are readable.
fn check_terminal_sources(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.terminal.surface.enabled)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "terminal-sources",
            "Terminal ingestion is disabled in the deployment descriptor",
        );
    }

    let mut readable = Vec::new();
    let mut unreadable = Vec::new();

    for (label, path) in terminal_source_candidates(target, descriptor) {
        if !path.exists() {
            continue;
        }

        let check = match label.as_str() {
            "atuin" => validate_atuin_history_db(&path),
            _ => std::fs::File::open(&path)
                .map(|_| ())
                .wrap_err_with(|| format!("failed to open {}", path.display())),
        };

        match check {
            Ok(_) => readable.push(format!("{label}:{}", path.display())),
            Err(error) => unreadable.push(format!("{label}:{} ({error})", path.display())),
        }
    }

    if !unreadable.is_empty() {
        DeploymentReadinessItem::fail(
            "terminal-sources",
            format!(
                "Unreadable target-user history sources for {}: {}",
                target.user,
                unreadable.join(", ")
            ),
        )
    } else if !readable.is_empty() {
        DeploymentReadinessItem::pass(
            "terminal-sources",
            format!(
                "Readable target-user history sources for {}: {}",
                target.user,
                readable.join(", ")
            ),
        )
    } else {
        DeploymentReadinessItem::skip(
            "terminal-sources",
            format!(
                "No terminal history sources found under {} for target user {}",
                target.home.display(),
                target.user
            ),
        )
    }
}

/// Check 4: Hyprland sockets exist under the resolved runtime directory.
fn check_hyprland_socket(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.desktop.surface.enabled)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "hyprland-socket",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    if let Some(event_socket) = descriptor
        .and_then(|value| value.desktop.hyprland_event_socket.clone())
        .or_else(|| {
            std::env::var("SINEX_HYPRLAND_EVENT_SOCKET")
                .ok()
                .map(PathBuf::from)
        })
    {
        let command_socket = descriptor
            .and_then(|value| value.desktop.hyprland_command_socket.clone())
            .or_else(|| {
                std::env::var("SINEX_HYPRLAND_COMMAND_SOCKET")
                    .ok()
                    .map(PathBuf::from)
            });
        if event_socket.exists() {
            return DeploymentReadinessItem::pass(
                "hyprland-socket",
                format!(
                    "Configured Hyprland event socket {} is present (command socket present: {})",
                    event_socket.display(),
                    command_socket.as_ref().is_some_and(|path| path.exists())
                ),
            );
        }

        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "Configured Hyprland event socket {} is missing",
                event_socket.display()
            ),
        );
    }

    let hypr_dir = runtime_dir_for_target(target, descriptor).join("hypr");
    if !hypr_dir.exists() {
        return DeploymentReadinessItem::skip(
            "hyprland-socket",
            format!(
                "{} does not exist for target user {} (Hyprland not running or different UID)",
                hypr_dir.display(),
                target.user
            ),
        );
    }

    if let Some(signature) = descriptor
        .and_then(|value| value.desktop.hyprland_instance_signature.clone())
        .or_else(|| std::env::var("SINEX_HYPRLAND_INSTANCE_SIGNATURE").ok())
        .or_else(|| std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok())
    {
        let base = hypr_dir.join(&signature);
        let event_socket = base.join(".socket2.sock");
        let command_socket = base.join(".socket.sock");
        if event_socket.exists() {
            return DeploymentReadinessItem::pass(
                "hyprland-socket",
                format!(
                    "Resolved Hyprland sockets under {} (command socket present: {})",
                    base.display(),
                    command_socket.exists()
                ),
            );
        }

        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "Configured Hyprland instance {} under {} is missing .socket2.sock",
                signature,
                hypr_dir.display()
            ),
        );
    }

    match std::fs::read_dir(&hypr_dir) {
        Ok(entries) => {
            let candidates: Vec<PathBuf> = entries
                .filter_map(|entry| entry.ok().map(|value| value.path()))
                .filter(|path| path.join(".socket2.sock").exists())
                .collect();
            match candidates.as_slice() {
                [candidate] => DeploymentReadinessItem::pass(
                    "hyprland-socket",
                    format!("Found Hyprland event socket under {}", candidate.display()),
                ),
                [] => DeploymentReadinessItem::fail(
                    "hyprland-socket",
                    format!(
                        "{} exists but contains no Hyprland event sockets",
                        hypr_dir.display()
                    ),
                ),
                _ => DeploymentReadinessItem::fail(
                    "hyprland-socket",
                    format!(
                        "Multiple Hyprland instances found under {}; set SINEX_HYPRLAND_INSTANCE_SIGNATURE or SINEX_HYPRLAND_EVENT_SOCKET",
                        hypr_dir.display()
                    ),
                ),
            }
        }
        Err(e) => DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!("Could not read {}: {e}", hypr_dir.display()),
        ),
    }
}

fn check_activitywatch_db(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.desktop.surface.enabled)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "activitywatch-db",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    let path = activitywatch_db_for_target(target, descriptor);
    if !path.exists() {
        return DeploymentReadinessItem::skip(
            "activitywatch-db",
            format!(
                "No ActivityWatch SQLite database found at {} for target user {}",
                path.display(),
                target.user
            ),
        );
    }

    match validate_activitywatch_db(&path) {
        Ok(()) => DeploymentReadinessItem::pass(
            "activitywatch-db",
            format!(
                "ActivityWatch SQLite history is readable at {} for target user {}",
                path.display(),
                target.user
            ),
        ),
        Err(error) => DeploymentReadinessItem::fail(
            "activitywatch-db",
            format!(
                "Unreadable ActivityWatch history for {} at {} ({error})",
                target.user,
                path.display()
            ),
        ),
    }
}

/// Check 5: git-annex is on PATH.
fn check_git_annex() -> DeploymentReadinessItem {
    match which::which("git-annex") {
        Ok(path) => DeploymentReadinessItem::pass(
            "git-annex",
            format!("git-annex found at {}", path.display()),
        ),
        Err(_) => DeploymentReadinessItem::fail("git-annex", "git-annex not found on PATH"),
    }
}

/// Check 6: inotify watch limit is high enough for real filesystem deployment.
fn check_inotify_limit(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.filesystem.enabled)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "inotify-max-user-watches",
            "Filesystem ingestion is disabled in the deployment descriptor",
        );
    }

    let path = "/proc/sys/fs/inotify/max_user_watches";
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "inotify-max-user-watches",
                format!("Could not read {path}: {error}"),
            );
        }
    };

    let Ok(value) = contents.trim().parse::<u64>() else {
        return DeploymentReadinessItem::fail(
            "inotify-max-user-watches",
            format!("Could not parse {} as an integer", contents.trim()),
        );
    };

    if value >= RECOMMENDED_INOTIFY_MAX_USER_WATCHES {
        DeploymentReadinessItem::pass("inotify-max-user-watches", format!("Configured to {value}"))
    } else {
        DeploymentReadinessItem::fail(
            "inotify-max-user-watches",
            format!(
                "Configured to {value}; expected at least {RECOMMENDED_INOTIFY_MAX_USER_WATCHES}"
            ),
        )
    }
}

fn check_singleton_workstation_topology(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::skip(
            "singleton-workstation-topology",
            "No deployment descriptor available for planned instance validation",
        );
    };

    let surfaces = [
        ("filesystem", &descriptor.filesystem),
        ("terminal", &descriptor.terminal.surface),
        ("desktop", &descriptor.desktop.surface),
        ("system", &descriptor.system),
    ];
    let mut offenders = Vec::new();

    for (name, surface) in surfaces {
        let instances = surface.instances.unwrap_or(1);
        if surface.enabled && instances != 1 {
            offenders.push(format!("{name}={instances}"));
        }
    }

    if offenders.is_empty() {
        DeploymentReadinessItem::pass(
            "singleton-workstation-topology",
            "Workstation capture nodes are pinned to single-instance startup",
        )
    } else {
        DeploymentReadinessItem::fail(
            "singleton-workstation-topology",
            format!(
                "Workstation capture nodes must stay singleton for first enable: {}",
                offenders.join(", ")
            ),
        )
    }
}

/// Check 7: schema-apply readiness — connect to DB and run a simple query.
async fn check_schema_apply(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.expectations.schema_apply)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "schema-apply",
            "Schema bootstrap is not expected in the deployment descriptor",
        );
    }

    let Some(url) = database_url else {
        return DeploymentReadinessItem::skip(
            "schema-apply",
            "DATABASE_URL not set; skipping schema-apply check",
        );
    };

    let effective_url = match SinexEnvironment::current()
        .wrap_err("failed to resolve SINEX_ENVIRONMENT for schema-apply probe")
        .and_then(|env| {
            env.database_url(url)
                .wrap_err("failed to derive namespaced database URL for schema-apply probe")
        }) {
        Ok(url) => url,
        Err(error) => {
            return DeploymentReadinessItem::fail("schema-apply", error.to_string());
        }
    };

    use sqlx::Row;
    use sqlx::postgres::PgPoolOptions;

    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&effective_url)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                format!("Cannot connect to database: {e}"),
            );
        }
    };

    match sqlx::query("SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'core'")
        .fetch_one(&pool)
        .await
    {
        Ok(row) => {
            let count: i64 = row.get(0);
            if count > 0 {
                DeploymentReadinessItem::pass(
                    "schema-apply",
                    "Database reachable and 'core' schema exists",
                )
            } else {
                DeploymentReadinessItem::fail(
                    "schema-apply",
                    "Database reachable but 'core' schema is missing — schema-apply may not have run",
                )
            }
        }
        Err(e) => {
            DeploymentReadinessItem::fail("schema-apply", format!("Database query failed: {e}"))
        }
    }
}

fn required_nats_stream_names() -> Result<Vec<String>> {
    let env = SinexEnvironment::current()
        .wrap_err("failed to resolve SINEX_ENVIRONMENT for NATS readiness")?;
    Ok(vec![
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        env.nats_stream_name("SINEX_RAW_EVENTS_CONFIRMATIONS"),
        env.nats_stream_name("SOURCE_MATERIAL_BEGIN"),
        env.nats_stream_name("SOURCE_MATERIAL_SLICES"),
        env.nats_stream_name("SOURCE_MATERIAL_END"),
    ])
}

/// Check 8: NATS streams exist — connect and list streams.
async fn check_nats_streams(
    nats_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.expectations.nats_streams)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "nats-streams",
            "JetStream runtime is not expected in the deployment descriptor",
        );
    }

    use futures::StreamExt;

    let mut nats_config = NatsConnectionConfig::from_env();
    if nats_config.url == "nats://localhost:4222" {
        if let Some(url) = nats_url {
            nats_config.url = url.to_string();
        }
    }

    let client = match nats_config.connect().await {
        Ok(c) => c,
        Err(e) => {
            return DeploymentReadinessItem::fail(
                "nats-streams",
                format!("Cannot connect to NATS at {}: {e}", nats_config.url),
            );
        }
    };

    let jetstream = async_nats::jetstream::new(client);
    let mut streams = jetstream.streams();
    let mut names: Vec<String> = Vec::new();
    let mut list_error: Option<String> = None;
    while let Some(result) = streams.next().await {
        match result {
            Ok(stream) => names.push(stream.config.name.clone()),
            Err(e) => {
                list_error = Some(format!("Error listing NATS streams: {e}"));
                break;
            }
        }
    }

    if let Some(err) = list_error {
        return DeploymentReadinessItem::fail("nats-streams", err);
    }

    let required_streams = match required_nats_stream_names() {
        Ok(streams) => streams,
        Err(error) => {
            return DeploymentReadinessItem::fail("nats-streams", error.to_string());
        }
    };
    let available: BTreeSet<String> = names.iter().cloned().collect();
    let missing: Vec<String> = required_streams
        .iter()
        .filter(|name| !available.contains(name.as_str()))
        .cloned()
        .collect();

    if !missing.is_empty() {
        DeploymentReadinessItem::fail(
            "nats-streams",
            format!(
                "Connected to NATS at {}; missing required JetStream streams: {}; present: {}",
                nats_config.url,
                missing.join(", "),
                if names.is_empty() {
                    "<none>".to_string()
                } else {
                    names.join(", ")
                }
            ),
        )
    } else {
        DeploymentReadinessItem::pass(
            "nats-streams",
            format!(
                "Connected to NATS at {}; required streams present: {}",
                nats_config.url,
                names.join(", ")
            ),
        )
    }
}

fn check_secret_materials(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let default_tls_dir = Path::new(".sinex/tls");
    let descriptor_present = descriptor.is_some();
    let admin_token = descriptor
        .and_then(|value| value.secrets.gateway_admin_token_file.clone())
        .or_else(|| {
            path_from_env_or_default(
                "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
                PathBuf::from("/run/agenix/sinex-gateway-admin-token"),
            )
        });
    let db_password = descriptor
        .and_then(|value| value.secrets.database_password_file.clone())
        .or_else(|| {
            path_from_env_or_default(
                "SINEX_DATABASE_PASSWORD_FILE",
                PathBuf::from("/run/agenix/sinex-local-db"),
            )
        });
    let gateway_cert = descriptor
        .and_then(|value| value.secrets.gateway_tls_cert_file.clone())
        .or_else(|| {
            path_from_env_or_default("SINEX_GATEWAY_TLS_CERT", default_tls_dir.join("server.pem"))
        });
    let gateway_key = descriptor
        .and_then(|value| value.secrets.gateway_tls_key_file.clone())
        .or_else(|| {
            path_from_env_or_default(
                "SINEX_GATEWAY_TLS_KEY",
                default_tls_dir.join("server-key.pem"),
            )
        });
    let gateway_client_ca = descriptor
        .and_then(|value| value.secrets.gateway_tls_client_ca_file.clone())
        .or_else(|| {
            path_from_env_or_default(
                "SINEX_GATEWAY_TLS_CLIENT_CA",
                default_tls_dir.join("ca.pem"),
            )
        });

    let mtls_expected = descriptor
        .and_then(|value| value.secrets.gateway_tls_client_ca_file.as_ref())
        .is_some()
        || env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
        || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok();

    let mut missing = Vec::new();
    let mut present = Vec::new();

    if let Some(path) = admin_token {
        if path.is_file() {
            present.push(format!("gateway-admin-token={}", path.display()));
        } else {
            missing.push(format!(
                "gateway-admin-token unreadable: {}",
                path.display()
            ));
        }
    } else if !descriptor_present {
        missing.push(
            "gateway-admin-token missing (set SINEX_GATEWAY_ADMIN_TOKEN_FILE or provide /run/agenix/sinex-gateway-admin-token)"
                .to_string(),
        );
    }

    if let Some(path) = db_password {
        if path.is_file() {
            present.push(format!("database-password={}", path.display()));
        } else {
            missing.push(format!("database-password unreadable: {}", path.display()));
        }
    } else if !descriptor_present {
        missing.push(
            "database-password missing (set SINEX_DATABASE_PASSWORD_FILE or provide /run/agenix/sinex-local-db)"
                .to_string(),
        );
    }

    match (gateway_cert.as_ref(), gateway_key.as_ref()) {
        (Some(cert), Some(key)) if cert.is_file() && key.is_file() => {
            present.push(format!("gateway-tls={}/{}", cert.display(), key.display()));
        }
        (Some(cert), Some(key)) => missing.push(format!(
            "gateway-tls unreadable: cert={} key={}",
            cert.display(),
            key.display()
        )),
        (Some(cert), None) => {
            missing.push(format!(
                "gateway-tls missing key for cert {}",
                cert.display()
            ));
        }
        (None, Some(key)) => {
            missing.push(format!(
                "gateway-tls missing cert for key {}",
                key.display()
            ));
        }
        (None, None) => {
            if !descriptor_present {
                missing.push(
                    "gateway-tls missing (set SINEX_GATEWAY_TLS_CERT/SINEX_GATEWAY_TLS_KEY or provide .sinex/tls/server.pem + server-key.pem)"
                        .to_string(),
                );
            }
        }
    }

    if mtls_expected {
        match gateway_client_ca {
            Some(path) if path.is_file() => {
                present.push(format!("gateway-client-ca={}", path.display()));
            }
            Some(path) => {
                missing.push(format!("gateway-client-ca unreadable: {}", path.display()));
            }
            None => missing.push(
                "gateway-client-ca missing (set SINEX_GATEWAY_TLS_CLIENT_CA or provide .sinex/tls/ca.pem)"
                    .to_string(),
            ),
        }
    }

    if missing.is_empty() && present.is_empty() {
        DeploymentReadinessItem::skip(
            "secret-materials",
            "No deployment secret materials were declared for readiness validation",
        )
    } else if missing.is_empty() {
        DeploymentReadinessItem::pass(
            "secret-materials",
            format!("Deployment secret files available: {}", present.join(", ")),
        )
    } else {
        let description = if present.is_empty() {
            missing.join("; ")
        } else {
            format!("{}; present: {}", missing.join("; "), present.join(", "))
        };
        DeploymentReadinessItem::fail("secret-materials", description)
    }
}

async fn build_gateway_probe_client(base_url: &str) -> Result<GatewayProbeClient> {
    let mut builder = reqwest::Client::builder()
        .timeout(DEPLOYMENT_READY_TIMEOUT)
        .use_rustls_tls();
    let requires_tls = base_url.starts_with("https://");
    let default_tls_dir = Path::new(".sinex/tls");

    if requires_tls {
        let Some(ca_path) =
            path_from_env_or_default("SINEX_RPC_CA_CERT", default_tls_dir.join("ca.pem"))
        else {
            color_eyre::eyre::bail!(
                "gateway readiness over HTTPS requires a trusted CA; set SINEX_RPC_CA_CERT or provide .sinex/tls/ca.pem"
            );
        };
        let pem = tokio::fs::read(&ca_path).await.wrap_err_with(|| {
            format!(
                "failed to read RPC CA certificate from {}",
                ca_path.display()
            )
        })?;
        let cert = reqwest::Certificate::from_pem(&pem).wrap_err_with(|| {
            format!(
                "failed to parse RPC CA certificate at {}",
                ca_path.display()
            )
        })?;
        builder = builder.add_root_certificate(cert);
    }

    let client_cert =
        path_from_env_or_default("SINEX_RPC_CLIENT_CERT", default_tls_dir.join("client.pem"));
    let client_key = path_from_env_or_default(
        "SINEX_RPC_CLIENT_KEY",
        default_tls_dir.join("client-key.pem"),
    );
    let client_identity_path = match (client_cert, client_key) {
        (Some(cert_path), Some(key_path)) => {
            let mut pem = tokio::fs::read(&cert_path).await.wrap_err_with(|| {
                format!(
                    "failed to read RPC client certificate from {}",
                    cert_path.display()
                )
            })?;
            pem.extend_from_slice(&tokio::fs::read(&key_path).await.wrap_err_with(|| {
                format!("failed to read RPC client key from {}", key_path.display())
            })?);
            let identity = reqwest::Identity::from_pem(&pem).wrap_err_with(|| {
                format!(
                    "failed to parse client identity from {} and {}",
                    cert_path.display(),
                    key_path.display()
                )
            })?;
            builder = builder.identity(identity);
            Some((cert_path, key_path))
        }
        (Some(_), None) | (None, Some(_)) => {
            color_eyre::eyre::bail!(
                "SINEX_RPC_CLIENT_CERT and SINEX_RPC_CLIENT_KEY must both be set for gateway mTLS probing"
            );
        }
        (None, None) => None,
    };

    let client = builder
        .build()
        .wrap_err("failed to construct HTTP client for gateway readiness")?;
    Ok(GatewayProbeClient {
        client,
        client_identity_path,
    })
}

/// Check 9: gateway readiness endpoint responds and reports serving=true.
async fn check_gateway_ready(
    gateway_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    if descriptor
        .map(|value| !value.expectations.gateway_ready)
        .unwrap_or(false)
    {
        return DeploymentReadinessItem::skip(
            "gateway-ready",
            "Gateway runtime is not expected in the deployment descriptor",
        );
    }

    let base_url = normalize_gateway_base_url(gateway_url.unwrap_or("https://127.0.0.1:9999"));
    let ready_url = format!("{base_url}/ready");

    let mtls_expected = env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
        || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok();
    let probe_client = match build_gateway_probe_client(&base_url).await {
        Ok(client) => client,
        Err(error) => {
            return DeploymentReadinessItem::fail("gateway-ready", error.to_string());
        }
    };

    let response = match probe_client.client.get(&ready_url).send().await {
        Ok(response) => response,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "gateway-ready",
                if mtls_expected && probe_client.client_identity_path.is_none() {
                    format!(
                        "Cannot reach {ready_url}: {error}; gateway mTLS appears enabled, but no RPC client identity was available from SINEX_RPC_CLIENT_CERT/SINEX_RPC_CLIENT_KEY or .sinex/tls/client.pem + client-key.pem"
                    )
                } else {
                    format!("Cannot reach {ready_url}: {error}")
                },
            );
        }
    };

    let status = response.status();
    let body: Option<JsonValue> = response.json().await.ok();
    let serving = body
        .as_ref()
        .and_then(|json| json.get("serving"))
        .and_then(JsonValue::as_bool);
    let healthy = body
        .as_ref()
        .and_then(|json| json.get("healthy"))
        .and_then(JsonValue::as_bool);

    if status.is_success() && serving == Some(true) {
        DeploymentReadinessItem::pass(
            "gateway-ready",
            format!(
                "{ready_url} returned HTTP {status} (healthy={})",
                healthy.unwrap_or(false)
            ),
        )
    } else {
        DeploymentReadinessItem::fail(
            "gateway-ready",
            format!(
                "{ready_url} returned HTTP {status} (serving={:?}, healthy={:?})",
                serving, healthy
            ),
        )
    }
}

async fn execute_deployment_readiness(ctx: &CommandContext) -> Result<DeploymentReadinessReport> {
    let cfg = crate::config::config();
    let (descriptor, descriptor_item) = load_deployment_descriptor();

    let mut items = vec![
        descriptor_item,
        check_node_binaries(),
        check_realm_accessible(),
    ];

    match resolve_target_identity(descriptor.as_ref()) {
        Ok(target) => {
            let descriptor_suffix = descriptor
                .as_ref()
                .and_then(|value| value.source.as_deref())
                .map(|source| format!(" via {source}"))
                .unwrap_or_default();
            items.push(DeploymentReadinessItem::pass(
                "target-identity",
                format!(
                    "Using target user {} (uid {}, home {}) for terminal/desktop checks{}",
                    target.user,
                    target.uid,
                    target.home.display(),
                    descriptor_suffix
                ),
            ));
            items.push(check_terminal_sources(&target, descriptor.as_ref()));
            items.push(check_hyprland_socket(&target, descriptor.as_ref()));
            items.push(check_activitywatch_db(&target, descriptor.as_ref()));
        }
        Err(error) => {
            items.push(DeploymentReadinessItem::fail(
                "target-identity",
                format!("Could not resolve deployment target identity: {error}"),
            ));
            items.push(DeploymentReadinessItem::skip(
                "terminal-sources",
                "Skipped because target identity resolution failed",
            ));
            items.push(DeploymentReadinessItem::skip(
                "hyprland-socket",
                "Skipped because target identity resolution failed",
            ));
            items.push(DeploymentReadinessItem::skip(
                "activitywatch-db",
                "Skipped because target identity resolution failed",
            ));
        }
    }

    items.push(check_git_annex());
    items.push(check_singleton_workstation_topology(descriptor.as_ref()));
    items.push(check_inotify_limit(descriptor.as_ref()));
    items.push(check_secret_materials(descriptor.as_ref()));
    items.push(check_schema_apply(cfg.database_url.as_deref(), descriptor.as_ref()).await);
    items.push(check_nats_streams(cfg.nats_url.as_deref(), descriptor.as_ref()).await);
    items.push(check_gateway_ready(cfg.gateway_url.as_deref(), descriptor.as_ref()).await);

    let overall_pass = items.iter().all(|i| i.status != "fail");

    if ctx.is_human() {
        println!("\n{}", style("Deployment Readiness:").bold());
        for item in &items {
            let (icon, styled_status) = match item.status.as_str() {
                "pass" => (style("✓").green(), style("PASS").green()),
                "fail" => (style("✗").red(), style("FAIL").red()),
                _ => (style("–").dim(), style("SKIP").dim()),
            };
            println!(
                "  {} [{styled_status}] {:<25} {}",
                icon,
                item.name,
                style(&item.description).dim()
            );
        }
        println!();
        if overall_pass {
            println!(
                "{}",
                style("✓ Deployment readiness: all checks passed or skipped").green()
            );
        } else {
            println!(
                "{}",
                style("✗ Deployment readiness: some checks failed").red()
            );
        }
    }

    Ok(DeploymentReadinessReport {
        items,
        overall: overall_pass,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use ::xtask::sandbox::EnvGuard;

    fn sample_descriptor() -> DeploymentReadinessDescriptor {
        DeploymentReadinessDescriptor {
            version: 1,
            mode: DeploymentReadinessMode::Prepared,
            source: Some("test".to_string()),
            target: Some(sinex_primitives::DeploymentTarget {
                user: "probe-user".to_string(),
                uid: Some(4242),
                home: Some(PathBuf::from("/tmp/probe-home")),
            }),
            ..Default::default()
        }
    }

    #[sinex_test]
    async fn test_doctor_report_json_shape() -> ::xtask::sandbox::TestResult<()> {
        let report = DoctorReport {
            postgres: DoctorServiceCheck {
                available: true,
                message: None,
            },
            nats: DoctorServiceCheck {
                available: false,
                message: Some("Cannot connect to NATS on port 4222".into()),
            },
            tools: vec![
                ToolCheck {
                    name: "rustc".into(),
                    available: true,
                    version: Some("1.95.0-nightly".into()),
                    path: Some("/nix/store/.../rustc".into()),
                },
                ToolCheck {
                    name: "ast-grep".into(),
                    available: false,
                    version: None,
                    path: None,
                },
            ],
            environment: Some(serde_json::json!({
                "hostname": "testhost",
                "in_devenv": true,
            })),
            tls: Some(TlsCheck {
                ca_exists: true,
                server_cert_exists: true,
                client_cert_exists: false,
                server_expires_days: None,
                server_expired: None,
                key_matches: None,
            }),
            postgres_extensions: Some(vec!["pgvector".into(), "timescaledb".into()]),
            overall: false,
        };

        let json = serde_json::to_value(&report)?;

        // Postgres/NATS (agents use: .data.postgres.available, .data.nats.available)
        assert_eq!(json["postgres"]["available"], true);
        assert!(json["postgres"]["message"].is_null());
        assert_eq!(json["nats"]["available"], false);
        assert!(json["nats"]["message"].is_string());

        // Tools (agents use: .data.tools[].name, .available, .version)
        assert!(json["tools"].is_array());
        assert_eq!(json["tools"][0]["name"], "rustc");
        assert_eq!(json["tools"][0]["available"], true);
        assert!(json["tools"][0]["version"].is_string());
        assert_eq!(json["tools"][1]["available"], false);
        // Unavailable tool should have null version and no path
        assert!(json["tools"][1]["version"].is_null());
        assert!(json["tools"][1].get("path").is_none() || json["tools"][1]["path"].is_null());

        // Overall (agents use: .data.overall)
        assert_eq!(json["overall"], false);

        // TLS (agents use: .data.tls.ca_exists, etc.)
        assert_eq!(json["tls"]["ca_exists"], true);
        assert_eq!(json["tls"]["client_cert_exists"], false);

        // Extensions (agents use: .data.postgres_extensions[])
        assert!(json["postgres_extensions"].is_array());
        assert_eq!(json["postgres_extensions"][0], "pgvector");
        Ok(())
    }

    #[sinex_test]
    async fn test_doctor_service_check_serialization() -> ::xtask::sandbox::TestResult<()> {
        let check = DoctorServiceCheck {
            available: false,
            message: Some("Connection refused".into()),
        };
        let json = serde_json::to_value(&check)?;
        assert_eq!(json["available"], false);
        assert_eq!(json["message"], "Connection refused");

        // When available, message is typically None
        let check_ok = DoctorServiceCheck {
            available: true,
            message: None,
        };
        let json_ok = serde_json::to_value(&check_ok)?;
        assert_eq!(json_ok["available"], true);
        assert!(json_ok["message"].is_null());
        Ok(())
    }

    #[sinex_test]
    async fn test_tls_check_serialization() -> ::xtask::sandbox::TestResult<()> {
        let check = TlsCheck {
            ca_exists: true,
            server_cert_exists: false,
            client_cert_exists: false,
            server_expires_days: None,
            server_expired: None,
            key_matches: None,
        };
        let json = serde_json::to_value(&check)?;
        assert_eq!(json["ca_exists"], true);
        assert_eq!(json["server_cert_exists"], false);
        assert_eq!(json["client_cert_exists"], false);
        Ok(())
    }

    #[sinex_test]
    async fn test_normalize_gateway_base_url_strips_rpc_suffix() -> ::xtask::sandbox::TestResult<()>
    {
        assert_eq!(
            normalize_gateway_base_url("https://127.0.0.1:9999/rpc"),
            "https://127.0.0.1:9999"
        );
        assert_eq!(
            normalize_gateway_base_url("https://127.0.0.1:9999/"),
            "https://127.0.0.1:9999"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_deployment_readiness_report_serialization() -> ::xtask::sandbox::TestResult<()> {
        let report = DeploymentReadinessReport {
            items: vec![
                DeploymentReadinessItem::pass("gateway-ready", "ready"),
                DeploymentReadinessItem::fail("inotify-max-user-watches", "too low"),
            ],
            overall: false,
        };

        let json = serde_json::to_value(&report)?;
        assert_eq!(json["overall"], false);
        assert_eq!(json["items"][0]["name"], "gateway-ready");
        assert_eq!(json["items"][1]["status"], "fail");
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_warnings_capture_degraded_signals()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = crate::runtime_metrics::RuntimeMetrics {
            ingestd_status: crate::runtime_metrics::IngestdStatus::Stale,
            last_heartbeat_age_secs: Some(300),
            consumer_lag_pending: Some(1500.0),
            consumer_lag_age_secs: Some(10),
            last_batch_latency_ms: Some(6000.0),
            last_batch_latency_age_secs: Some(10),
        };

        let warnings = runtime_warnings(&metrics);
        assert!(
            warnings.iter().any(|warning| warning.contains("ingestd heartbeat is stale"))
        );
        assert!(warnings.iter().any(|warning| warning.contains("consumer lag is high")));
        assert!(warnings.iter().any(|warning| warning.contains("batch latency is high")));
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_warnings_capture_stale_telemetry()
    -> ::xtask::sandbox::TestResult<()> {
        let metrics = crate::runtime_metrics::RuntimeMetrics {
            ingestd_status: crate::runtime_metrics::IngestdStatus::Healthy,
            last_heartbeat_age_secs: Some(5),
            consumer_lag_pending: Some(42.0),
            consumer_lag_age_secs: Some(600),
            last_batch_latency_ms: Some(125.0),
            last_batch_latency_age_secs: Some(600),
        };

        let warnings = runtime_warnings(&metrics);
        assert!(
            warnings.iter().any(|warning| warning.contains("consumer lag telemetry is stale"))
        );
        assert!(
            warnings.iter().any(|warning| warning.contains("batch latency telemetry is stale"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_target_identity_prefers_explicit_target_env()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_TARGET_USER", "probe-user");
        env.set("SINEX_TARGET_UID", "4242");
        env.set("SINEX_TARGET_HOME", "/tmp/probe-home");
        env.set("USER", "current-user");
        env.set("UID", "1000");
        env.set("HOME", "/tmp/current-home");

        let identity = resolve_target_identity(None)?;
        assert_eq!(identity.user, "probe-user");
        assert_eq!(identity.uid, 4242);
        assert_eq!(identity.home, PathBuf::from("/tmp/probe-home"));
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_target_identity_prefers_descriptor_target()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_TARGET_USER", "env-user");
        env.set("SINEX_TARGET_UID", "1000");
        env.set("SINEX_TARGET_HOME", "/tmp/env-home");

        let identity = resolve_target_identity(Some(&sample_descriptor()))?;
        assert_eq!(identity.user, "probe-user");
        assert_eq!(identity.uid, 4242);
        assert_eq!(identity.home, PathBuf::from("/tmp/probe-home"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_terminal_sources_accepts_atuin_sqlite_history()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(home.join(".local/share/atuin"))?;
        std::fs::write(home.join(".bash_history"), "echo hello\n")?;

        let atuin_db = home.join(".local/share/atuin/history.db");
        let conn = rusqlite::Connection::open(&atuin_db)?;
        conn.execute(
            "CREATE TABLE history (
                id TEXT PRIMARY KEY,
                timestamp INTEGER NOT NULL,
                command TEXT NOT NULL,
                cwd TEXT NOT NULL,
                session TEXT NOT NULL,
                hostname TEXT NOT NULL,
                exit INTEGER NOT NULL,
                duration INTEGER NOT NULL,
                deleted_at INTEGER
            )",
            [],
        )?;

        let item = check_terminal_sources(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            None,
        );
        assert_eq!(item.status, "pass");
        assert!(item.description.contains("atuin:"));
        assert!(item.description.contains("bash:"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_activitywatch_db_accepts_valid_sqlite_history()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        let aw_dir = home.join(".local/share/activitywatch/aw-server-rust");
        std::fs::create_dir_all(&aw_dir)?;

        let aw_db = aw_dir.join("sqlite.db");
        let conn = rusqlite::Connection::open(&aw_db)?;
        conn.execute(
            "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            [],
        )?;
        conn.execute(
            "CREATE TABLE events (
                bucketrow INTEGER NOT NULL,
                starttime INTEGER NOT NULL,
                endtime INTEGER NOT NULL,
                data TEXT
            )",
            [],
        )?;

        let item = check_activitywatch_db(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            None,
        );
        assert_eq!(item.status, "pass");
        assert!(item.description.contains("ActivityWatch SQLite history"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_hyprland_socket_rejects_multiple_instances_without_signature()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let runtime_dir = temp.path();
        let hypr_dir = runtime_dir.join("hypr");
        std::fs::create_dir_all(hypr_dir.join("one"))?;
        std::fs::create_dir_all(hypr_dir.join("two"))?;
        std::fs::write(hypr_dir.join("one/.socket2.sock"), "")?;
        std::fs::write(hypr_dir.join("two/.socket2.sock"), "")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_HYPRLAND_RUNTIME_DIR",
            runtime_dir.display().to_string(),
        );
        env.clear("SINEX_HYPRLAND_INSTANCE_SIGNATURE");
        env.clear("HYPRLAND_INSTANCE_SIGNATURE");

        let item = check_hyprland_socket(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home: runtime_dir.to_path_buf(),
            },
            None,
        );
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("Multiple Hyprland instances"));
        Ok(())
    }

    #[sinex_test]
    async fn test_build_gateway_probe_client_allows_http_without_ca()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_RPC_CA_CERT");
        env.clear("SINEX_RPC_CLIENT_CERT");
        env.clear("SINEX_RPC_CLIENT_KEY");

        let _client = build_gateway_probe_client("http://127.0.0.1:9999").await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_build_gateway_probe_client_requires_readable_ca_for_https()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let missing_ca = temp.path().join("missing-ca.pem");

        let mut env = EnvGuard::new();
        env.set("SINEX_RPC_CA_CERT", missing_ca.display().to_string());
        env.clear("SINEX_RPC_CLIENT_CERT");
        env.clear("SINEX_RPC_CLIENT_KEY");

        let error = build_gateway_probe_client("https://127.0.0.1:9999")
            .await
            .expect_err("HTTPS readiness probing should fail without a readable CA");
        assert!(
            error
                .to_string()
                .contains("failed to read RPC CA certificate")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_required_nats_stream_names_follow_environment() -> ::xtask::sandbox::TestResult<()>
    {
        let mut env = EnvGuard::new();
        env.set("SINEX_ENVIRONMENT", "prod");

        let streams = required_nats_stream_names()?;
        assert!(streams.iter().all(|stream| stream.starts_with("PROD_")));
        assert!(streams.contains(&"PROD_SINEX_RAW_EVENTS".to_string()));
        assert!(streams.contains(&"PROD_SOURCE_MATERIAL_SLICES".to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_secret_materials_requires_gateway_admin_token()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let cert = temp.path().join("server.pem");
        let key = temp.path().join("server-key.pem");
        let db = temp.path().join("db-password");
        std::fs::write(&cert, "cert")?;
        std::fs::write(&key, "key")?;
        std::fs::write(&db, "password")?;

        let mut env = EnvGuard::new();
        env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());
        env.set("SINEX_GATEWAY_TLS_KEY", key.display().to_string());
        env.set("SINEX_DATABASE_PASSWORD_FILE", db.display().to_string());
        env.clear("SINEX_GATEWAY_ADMIN_TOKEN_FILE");

        let item = check_secret_materials(None);
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("gateway-admin-token"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_secret_materials_respects_descriptor_declared_paths_only()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let db = temp.path().join("db-password");
        std::fs::write(&db, "password")?;

        let descriptor = DeploymentReadinessDescriptor {
            secrets: sinex_primitives::DeploymentSecrets {
                database_password_file: Some(db),
                gateway_admin_token_file: None,
                gateway_tls_cert_file: None,
                gateway_tls_key_file: None,
                gateway_tls_client_ca_file: None,
            },
            ..Default::default()
        };

        let item = check_secret_materials(Some(&descriptor));
        assert_eq!(item.status, "pass");
        assert!(item.description.contains("database-password"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_singleton_workstation_topology_flags_fanout()
    -> ::xtask::sandbox::TestResult<()> {
        let descriptor = DeploymentReadinessDescriptor {
            filesystem: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: Some(2),
            },
            terminal: sinex_primitives::TerminalDeploymentSurface {
                surface: sinex_primitives::DeploymentSurface {
                    enabled: true,
                    instances: Some(1),
                },
                history_sources: Vec::new(),
            },
            ..Default::default()
        };

        let item = check_singleton_workstation_topology(Some(&descriptor));
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("filesystem=2"));
        Ok(())
    }
}

//! Doctor command - health check for Postgres, NATS, tools, and TLS

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::infra::probe::{probe_nats, probe_postgres};
use crate::output::Status;
use crate::tools::{ToolInfo, ToolManager};
use color_eyre::eyre::{Result, WrapErr, eyre};
use console::style;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sinex_node_sdk::preflight::configuration::{
    validate_activitywatch_db, validate_terminal_history_source,
};
use sinex_node_sdk::preflight::services::inspect_systemd_service;
use sinex_primitives::{
    DeploymentDatabaseRuntime, DeploymentReadinessDescriptor, DeploymentReadinessMode,
    environment::SinexEnvironment, nats::NatsConnectionConfig,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

const DEPLOYMENT_READY_TIMEOUT: Duration = Duration::from_secs(5);
const RECOMMENDED_INOTIFY_MAX_USER_WATCHES: u64 = 524_288;

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
    /// Error reported by TLS validation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TlsCheck {
    fn is_healthy(&self) -> bool {
        self.error.is_none()
            && !self.server_expired.unwrap_or(false)
            && self.key_matches.unwrap_or(true)
    }
}

fn resolve_tls_artifact(dir: &Path, candidates: &[&str]) -> Option<PathBuf> {
    candidates
        .iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

fn detect_tls_check() -> Option<TlsCheck> {
    let default_tls_dir = Path::new(".sinex/tls");
    let env_dir = std::env::var("SINEX_GATEWAY_TLS_CERT")
        .ok()
        .and_then(|p| Path::new(&p).parent().map(Path::to_path_buf));
    let active_dir = if let Some(ref dir) = env_dir {
        dir.exists().then_some(dir.as_path())
    } else if default_tls_dir.exists() {
        Some(default_tls_dir)
    } else {
        None
    }?;

    let server_cert_path = resolve_tls_artifact(active_dir, &["server.pem", "gateway.crt"]);
    let server_key_path = resolve_tls_artifact(active_dir, &["server-key.pem", "gateway.key"]);
    let client_cert_exists = resolve_tls_artifact(active_dir, &["client.pem"]).is_some();
    let ca_exists = resolve_tls_artifact(active_dir, &["ca.pem"]).is_some();

    let (server_expires_days, server_expired, key_matches, error) =
        if let Some(cert_path) = server_cert_path.as_ref() {
            let opts = crate::tls::TlsCheckOptions {
                cert_path: Some(cert_path.clone()),
                key_path: server_key_path.clone(),
                ..Default::default()
            };
            match crate::tls::check_tls_config(&opts) {
                Ok(result) => {
                    let days = result.certificate.as_ref().map(|c| c.days_until_expiry);
                    let expired = result.certificate.as_ref().map(|c| c.is_expired);
                    let error = (!result.valid && !result.issues.is_empty())
                        .then(|| result.issues.join("; "));
                    (days, expired, result.key_matches, error)
                }
                Err(error) => (None, None, None, Some(error.to_string())),
            }
        } else {
            (None, None, None, None)
        };

    Some(TlsCheck {
        ca_exists,
        server_cert_exists: server_cert_path.is_some(),
        client_cert_exists,
        server_expires_days,
        server_expired,
        key_matches,
        error,
    })
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
    let tls_check = detect_tls_check();
    if tls_check.as_ref().is_some_and(|check| !check.is_healthy()) {
        all_ok = false;
    }

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
            if let Some(error) = tls.error.as_deref() {
                println!("  {} TLS validation failed: {error}", style("✗").red());
            }
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
    skipped: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_reason: Option<String>,
    metrics: crate::runtime_metrics::RuntimeMetrics,
    assessment: crate::runtime_metrics::RuntimeAssessment,
    warnings: Vec<String>,
}

async fn execute_runtime_check(ctx: &CommandContext) -> Result<RuntimeCheckReport> {
    use crate::config::config;
    use crate::runtime_metrics::{IngestdStatus, RuntimeAssessment, RuntimeHealthStatus};

    let cfg = config();
    let descriptor = match DeploymentReadinessDescriptor::load() {
        Ok(descriptor) => descriptor,
        Err(error) => {
            let metrics = crate::runtime_metrics::RuntimeMetrics::query_failure(error.to_string());
            let assessment = metrics.assessment();
            let warnings = assessment.warnings.clone();
            return Ok(RuntimeCheckReport {
                overall: false,
                skipped: false,
                skip_reason: None,
                metrics,
                assessment,
                warnings,
            });
        }
    };
    let db_url = match resolve_effective_database_probe_url(
        cfg.database_url.as_deref(),
        descriptor.as_ref(),
        "runtime health check",
    ) {
        Ok(Some((url, _source))) => url,
        Ok(None) => {
            if ctx.is_human() {
                println!("\n{}", style("Runtime Check:").bold());
                println!(
                    "  {} runtime database target not configured, skipping runtime checks",
                    style("⚠").yellow()
                );
            }
            let warnings =
                vec!["Runtime health skipped: no runtime database target configured".into()];
            let assessment = RuntimeAssessment {
                status: RuntimeHealthStatus::Unavailable,
                warnings: warnings.clone(),
            };
            return Ok(RuntimeCheckReport {
                overall: false,
                skipped: true,
                skip_reason: Some("runtime database target not configured".into()),
                metrics: crate::runtime_metrics::RuntimeMetrics::unavailable(),
                assessment,
                warnings,
            });
        }
        Err(error) => {
            let metrics = crate::runtime_metrics::RuntimeMetrics::query_failure(error.to_string());
            let assessment = metrics.assessment();
            let warnings = assessment.warnings.clone();
            return Ok(RuntimeCheckReport {
                overall: false,
                skipped: false,
                skip_reason: None,
                metrics,
                assessment,
                warnings,
            });
        }
    };

    let metrics = crate::runtime_metrics::query_runtime_metrics(&db_url).await;
    let assessment = metrics.assessment();
    let warnings = assessment.warnings.clone();

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

        if let Some(error) = metrics.query_error.as_deref() {
            println!(
                "  {} Runtime query:      {}",
                style("✗").red(),
                style(error).dim()
            );
        }
    }

    Ok(RuntimeCheckReport {
        overall: matches!(assessment.status, RuntimeHealthStatus::Healthy),
        skipped: false,
        skip_reason: None,
        metrics,
        assessment,
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

fn descriptor_secret_path(
    descriptor: Option<&DeploymentReadinessDescriptor>,
    selector: impl FnOnce(&DeploymentReadinessDescriptor) -> Option<PathBuf>,
    env_key: &str,
    default_path: PathBuf,
) -> Option<PathBuf> {
    if let Some(descriptor) = descriptor {
        selector(descriptor)
    } else {
        path_from_env_or_default(env_key, default_path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GatewayProbeTlsPaths {
    trust_anchor: Option<PathBuf>,
    client_cert: Option<PathBuf>,
    client_key: Option<PathBuf>,
}

fn resolve_gateway_probe_tls_paths(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> GatewayProbeTlsPaths {
    let default_tls_dir = Path::new(".sinex/tls");
    GatewayProbeTlsPaths {
        trust_anchor: descriptor_secret_path(
            descriptor,
            |value| value.secrets.gateway_tls_trust_anchor_file.clone(),
            "SINEX_RPC_CA_CERT",
            default_tls_dir.join("ca.pem"),
        ),
        client_cert: path_from_env_or_default(
            "SINEX_RPC_CLIENT_CERT",
            default_tls_dir.join("client.pem"),
        ),
        client_key: path_from_env_or_default(
            "SINEX_RPC_CLIENT_KEY",
            default_tls_dir.join("client-key.pem"),
        ),
    }
}

fn apply_descriptor_nats_overrides(
    mut config: NatsConnectionConfig,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> NatsConnectionConfig {
    let Some(descriptor) = descriptor else {
        return config;
    };

    if config.url == "nats://localhost:4222"
        && let Some(url) = descriptor.nats.servers.first()
    {
        config.url = url.clone();
    }

    if config.ca_cert.is_none() {
        config.ca_cert = descriptor.secrets.nats_ca_cert_file.clone();
    }
    if config.client_cert.is_none() {
        config.client_cert = descriptor.secrets.nats_client_cert_file.clone();
    }
    if config.client_key.is_none() {
        config.client_key = descriptor.secrets.nats_client_key_file.clone();
    }
    if config.token_file.is_none() {
        config.token_file = descriptor.secrets.nats_token_file.clone();
    }
    if config.creds_file.is_none() {
        config.creds_file = descriptor.secrets.nats_creds_file.clone();
    }
    if config.nkey_seed_file.is_none() {
        config.nkey_seed_file = descriptor.secrets.nats_nkey_seed_file.clone();
    }

    config
}
fn descriptor_gateway_base_url(descriptor: Option<&DeploymentReadinessDescriptor>) -> Option<&str> {
    descriptor.and_then(|value| value.gateway.base_url.as_deref())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DatabaseProbeTarget {
    database_url: String,
    password_file: Option<PathBuf>,
    password_required: bool,
    source: String,
}

fn descriptor_database_url(database: &DeploymentDatabaseRuntime) -> Result<Option<String>> {
    if !database.enabled {
        return Ok(None);
    }

    let Some(user) = database.user.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.user is missing"
        ));
    };
    let Some(host) = database.host.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.host is missing"
        ));
    };
    let Some(port) = database.port else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.port is missing"
        ));
    };
    let Some(name) = database.name.as_deref() else {
        return Err(eyre!(
            "deployment descriptor database runtime is enabled but database.name is missing"
        ));
    };

    Ok(Some(format!("postgresql://{user}@{host}:{port}/{name}")))
}

fn resolve_database_probe_target(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<Option<DatabaseProbeTarget>> {
    if let Some(url) = database_url {
        return Ok(Some(DatabaseProbeTarget {
            database_url: url.to_string(),
            password_file: descriptor
                .and_then(|value| value.secrets.database_password_file.clone()),
            password_required: descriptor
                .map(|value| value.database.password_required)
                .unwrap_or(false),
            source: "DATABASE_URL".to_string(),
        }));
    }

    let Some(descriptor) = descriptor else {
        return Ok(None);
    };
    let Some(url) = descriptor_database_url(&descriptor.database)? else {
        return Ok(None);
    };

    Ok(Some(DatabaseProbeTarget {
        database_url: url,
        password_file: descriptor.secrets.database_password_file.clone(),
        password_required: descriptor.database.password_required,
        source: descriptor
            .source
            .clone()
            .unwrap_or_else(|| "deployment descriptor".to_string()),
    }))
}

pub(crate) fn resolve_effective_database_probe_url(
    database_url: Option<&str>,
    descriptor: Option<&DeploymentReadinessDescriptor>,
    purpose: &str,
) -> Result<Option<(String, String)>> {
    let Some(probe_target) = resolve_database_probe_target(database_url, descriptor)? else {
        return Ok(None);
    };

    let mut effective_url = SinexEnvironment::current()
        .wrap_err_with(|| format!("failed to resolve SINEX_ENVIRONMENT for {purpose}"))
        .and_then(|env| {
            env.database_url(probe_target.database_url.as_str())
                .wrap_err_with(|| format!("failed to derive namespaced database URL for {purpose}"))
        })?;

    if let Some(password_file) = probe_target.password_file.as_deref() {
        let password = read_database_password(password_file)?;
        let mut parsed = url::Url::parse(&effective_url).wrap_err_with(|| {
            format!(
                "resolved {} for {purpose} but failed to parse it as a database URL",
                probe_target.source
            )
        })?;
        parsed
            .set_password(Some(&password))
            .map_err(|_| eyre!("failed to apply database password for {purpose}"))?;
        effective_url = parsed.to_string();
    } else if probe_target.password_required && !database_url_has_password(&effective_url) {
        return Err(eyre!(
            "{purpose} requires password authentication, but {} does not provide a password and deployment secret material is missing",
            probe_target.source
        ));
    }

    Ok(Some((effective_url, probe_target.source)))
}

fn database_url_has_password(database_url: &str) -> bool {
    url::Url::parse(database_url)
        .ok()
        .and_then(|value| value.password().map(str::to_string))
        .is_some()
}

fn read_database_password(password_file: &Path) -> Result<String> {
    let password = std::fs::read_to_string(password_file).map_err(|error| {
        eyre!(
            "failed to read database password file {}: {error}",
            password_file.display()
        )
    })?;
    Ok(password.trim_end_matches(['\n', '\r']).to_string())
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
            DeploymentReadinessItem::fail(
                "deployment-descriptor",
                "No deployment readiness descriptor found; deployment readiness requires a config-derived descriptor from /etc/sinex/deployment-readiness.json or SINEX_DEPLOYMENT_READINESS_CONFIG",
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
    let env_target_user = std::env::var("SINEX_TARGET_USER")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let explicit_target_user = descriptor_target
        .map(|target| target.user.clone())
        .or_else(|| env_target_user.clone());

    if descriptor.is_some() && descriptor_target.is_none() && env_target_user.is_none() {
        color_eyre::eyre::bail!(
            "deployment descriptor is present but does not declare target.user; set SINEX_TARGET_USER or fix the descriptor"
        );
    }
    let Some(user) = explicit_target_user.clone() else {
        color_eyre::eyre::bail!(
            "deployment readiness refuses to guess the target user; set SINEX_TARGET_USER or provide a deployment descriptor with target.user"
        );
    };
    let passwd_entry = read_passwd_entry(&user)?;
    let explicit_uid = if let Some(uid) = descriptor_target.and_then(|target| target.uid) {
        Some(uid)
    } else if let Some(uid) = std::env::var("SINEX_TARGET_UID")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        Some(
            uid.parse::<u32>()
                .wrap_err("failed to parse SINEX_TARGET_UID for deployment readiness")?,
        )
    } else {
        None
    };
    let explicit_home = descriptor_target
        .and_then(|target| target.home.clone())
        .or_else(|| {
            std::env::var("SINEX_TARGET_HOME")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
        });

    if passwd_entry.is_none() && (explicit_uid.is_none() || explicit_home.is_none()) {
        color_eyre::eyre::bail!(
            "deployment target user '{user}' is missing from /etc/passwd; declare target.uid and target.home (or SINEX_TARGET_UID/SINEX_TARGET_HOME) explicitly"
        );
    }

    let uid = explicit_uid
        .or_else(|| passwd_entry.as_ref().map(|(uid, _)| *uid))
        .ok_or_else(|| {
            eyre!(
                "deployment target user '{user}' has no resolvable UID; declare target.uid explicitly"
            )
        })?;

    let home = explicit_home
        .or_else(|| passwd_entry.as_ref().map(|(_, home)| home.clone()))
        .ok_or_else(|| {
            eyre!(
                "deployment target user '{user}' has no resolvable home; declare target.home explicitly"
            )
        })?;

    Ok(TargetIdentity { user, uid, home })
}

fn terminal_source_candidates(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Vec<(String, PathBuf)> {
    if let Some(descriptor) = descriptor {
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
    ]
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

fn runtime_dir_for_target(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> PathBuf {
    if let Some(descriptor) = descriptor {
        return descriptor
            .desktop
            .runtime_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", target.uid)));
    }

    std::env::var("SINEX_HYPRLAND_RUNTIME_DIR")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            current_process_uid()
                .filter(|uid| *uid == target.uid)
                .and_then(|_| std::env::var("XDG_RUNTIME_DIR").ok().map(PathBuf::from))
        })
        .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", target.uid)))
}

fn configured_hyprland_sockets(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> (Option<PathBuf>, Option<PathBuf>) {
    if let Some(descriptor) = descriptor {
        return (
            descriptor.desktop.hyprland_event_socket.clone(),
            descriptor.desktop.hyprland_command_socket.clone(),
        );
    }

    (
        std::env::var("SINEX_HYPRLAND_EVENT_SOCKET")
            .ok()
            .map(PathBuf::from),
        std::env::var("SINEX_HYPRLAND_COMMAND_SOCKET")
            .ok()
            .map(PathBuf::from),
    )
}

fn configured_hyprland_instance_signature(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Option<String> {
    if let Some(descriptor) = descriptor {
        return descriptor.desktop.hyprland_instance_signature.clone();
    }

    std::env::var("SINEX_HYPRLAND_INSTANCE_SIGNATURE")
        .ok()
        .or_else(|| std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok())
}

fn current_process_uid() -> Option<u32> {
    std::env::var("UID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            command_output("id", &["-u"], "current process UID")
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
        })
}

async fn check_node_entrypoints(
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let Some(descriptor) = descriptor else {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            "Deployment readiness requires a descriptor-declared managed unit set",
        );
    };
    let units = &descriptor.managed_units;

    if units.is_empty() {
        return match descriptor.mode {
            DeploymentReadinessMode::Prepared => DeploymentReadinessItem::skip(
                "node-entrypoints",
                "Prepared deployment descriptor does not declare any managed units yet",
            ),
            _ => DeploymentReadinessItem::fail(
                "node-entrypoints",
                "Deployment descriptor does not declare managed units",
            ),
        };
    }

    let mut unavailable = Vec::new();
    let mut notify_contract_violations = Vec::new();

    for unit in units {
        let service_data = match inspect_systemd_service(unit).await {
            Ok(service_data) => service_data,
            Err(error) => {
                return DeploymentReadinessItem::fail(
                    "node-entrypoints",
                    format!("Could not query systemd for {unit}: {error}"),
                );
            }
        };

        if !service_data.is_loaded() {
            unavailable.push(unit.clone());
            continue;
        }

        let contract_violations = service_data.notify_contract_violations();
        if !contract_violations.is_empty() {
            notify_contract_violations.push(format!("{unit} {}", contract_violations.join(", ")));
        }
    }

    if !unavailable.is_empty() {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            format!(
                "Managed Sinex units are missing or not loaded: {}",
                unavailable.join(", ")
            ),
        );
    }

    if !notify_contract_violations.is_empty() {
        return DeploymentReadinessItem::fail(
            "node-entrypoints",
            format!(
                "Managed units violate the notify service contract: {}",
                notify_contract_violations.join(", ")
            ),
        );
    }

    DeploymentReadinessItem::pass(
        "node-entrypoints",
        format!(
            "Managed Sinex units are present in systemd with notify/watchdog contract intact: {}",
            units.join(", ")
        ),
    )
}

/// Check 2: /realm is readable by the resolved deployment principal.
fn check_realm_accessible(target: &TargetIdentity) -> DeploymentReadinessItem {
    let realm = std::path::Path::new("/realm");
    if !realm.exists() {
        return DeploymentReadinessItem::fail("realm-accessible", "/realm does not exist");
    }

    let Some(current_uid) = current_process_uid() else {
        return DeploymentReadinessItem::skip(
            "realm-accessible",
            format!(
                "Could not determine the current principal; rerun as {} or root to validate /realm access honestly",
                target.user
            ),
        );
    };

    if current_uid != target.uid && current_uid != 0 {
        return DeploymentReadinessItem::skip(
            "realm-accessible",
            format!(
                "Current principal uid {} differs from target uid {}; rerun as {} or root to validate /realm access",
                current_uid, target.uid, target.user
            ),
        );
    }

    match std::fs::read_dir(realm) {
        Ok(_) => DeploymentReadinessItem::pass(
            "realm-accessible",
            format!("/realm is readable for deployment target {}", target.user),
        ),
        Err(error) => DeploymentReadinessItem::fail(
            "realm-accessible",
            format!(
                "/realm exists but is not readable for deployment target {}: {error}",
                target.user
            ),
        ),
    }
}

/// Check 3: terminal history sources currently consumed by the node are readable.
fn check_terminal_sources(
    target: &TargetIdentity,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> DeploymentReadinessItem {
    let terminal_enabled = descriptor
        .map(|value| value.terminal.surface.enabled)
        .unwrap_or(true);
    if !terminal_enabled {
        return DeploymentReadinessItem::skip(
            "terminal-sources",
            "Terminal ingestion is disabled in the deployment descriptor",
        );
    }

    let candidates = terminal_source_candidates(target, descriptor);
    if descriptor.is_some() && candidates.is_empty() {
        return DeploymentReadinessItem::fail(
            "terminal-sources",
            "Terminal ingestion is enabled in the deployment descriptor but terminal.history_sources is empty",
        );
    }

    let mut readable = Vec::new();
    let mut unreadable = Vec::new();

    for (label, path) in candidates {
        if !path.exists() {
            continue;
        }

        let check = validate_terminal_history_source(&label, &path);

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
        DeploymentReadinessItem::fail(
            "terminal-sources",
            format!(
                "No readable terminal history sources found under {} for target user {}",
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
    let desktop_enabled = descriptor
        .map(|value| value.desktop.surface.enabled)
        .unwrap_or(true);
    if !desktop_enabled {
        return DeploymentReadinessItem::skip(
            "hyprland-socket",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    let (configured_event_socket, configured_command_socket) =
        configured_hyprland_sockets(descriptor);
    if let Some(event_socket) = configured_event_socket {
        let command_socket = configured_command_socket;
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
        return DeploymentReadinessItem::fail(
            "hyprland-socket",
            format!(
                "{} does not exist for target user {} (Hyprland runtime is unavailable)",
                hypr_dir.display(),
                target.user
            ),
        );
    }

    if let Some(signature) = configured_hyprland_instance_signature(descriptor) {
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
    let desktop_enabled = descriptor
        .map(|value| value.desktop.surface.enabled)
        .unwrap_or(true);
    if !desktop_enabled {
        return DeploymentReadinessItem::skip(
            "activitywatch-db",
            "Desktop ingestion is disabled in the deployment descriptor",
        );
    }

    if descriptor.is_some()
        && descriptor
            .and_then(|value| value.desktop.activitywatch_db_path.as_ref())
            .is_none()
    {
        return DeploymentReadinessItem::fail(
            "activitywatch-db",
            "Desktop ingestion is enabled in the deployment descriptor but desktop.activitywatch_db_path is unset",
        );
    }

    let path = activitywatch_db_for_target(target, descriptor);
    if !path.exists() {
        return DeploymentReadinessItem::fail(
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

    if descriptor.mode == DeploymentReadinessMode::Prepared && descriptor.target.is_none() {
        return DeploymentReadinessItem::skip(
            "singleton-workstation-topology",
            "Prepared descriptor does not declare a workstation target yet; singleton defaults are not expected until target wiring exists",
        );
    }

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

    let (effective_url, source) = match resolve_effective_database_probe_url(
        database_url,
        descriptor,
        "schema-apply probe",
    ) {
        Ok(Some(result)) => result,
        Ok(None) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                "Schema bootstrap is expected but neither DATABASE_URL nor deployment descriptor database runtime is available",
            );
        }
        Err(error) => {
            return DeploymentReadinessItem::fail("schema-apply", error.to_string());
        }
    };

    use sqlx::Row;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

    let connect_options: PgConnectOptions = match effective_url.parse() {
        Ok(options) => options,
        Err(error) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                format!("Resolved {source} for schema-apply but failed to parse it: {error}",),
            );
        }
    };

    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(connect_options)
        .await
    {
        Ok(p) => p,
        Err(e) => {
            return DeploymentReadinessItem::fail(
                "schema-apply",
                format!("Cannot connect to database via {source}: {e}"),
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

    let mut nats_config =
        apply_descriptor_nats_overrides(NatsConnectionConfig::from_env(), descriptor);
    if nats_config.url == "nats://localhost:4222"
        && let Some(url) = nats_url
    {
        nats_config.url = url.to_string();
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
    let admin_token = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_admin_token_file.clone(),
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        PathBuf::from("/run/agenix/sinex-gateway-admin-token"),
    );
    let db_password = descriptor_secret_path(
        descriptor,
        |value| value.secrets.database_password_file.clone(),
        "SINEX_DATABASE_PASSWORD_FILE",
        PathBuf::from("/run/agenix/sinex-local-db"),
    );
    let gateway_cert = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_cert_file.clone(),
        "SINEX_GATEWAY_TLS_CERT",
        default_tls_dir.join("server.pem"),
    );
    let gateway_key = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_key_file.clone(),
        "SINEX_GATEWAY_TLS_KEY",
        default_tls_dir.join("server-key.pem"),
    );
    let gateway_trust_anchor = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_trust_anchor_file.clone(),
        "SINEX_RPC_CA_CERT",
        default_tls_dir.join("ca.pem"),
    );
    let gateway_client_ca = descriptor_secret_path(
        descriptor,
        |value| value.secrets.gateway_tls_client_ca_file.clone(),
        "SINEX_GATEWAY_TLS_CLIENT_CA",
        default_tls_dir.join("ca.pem"),
    );
    let nats_ca = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_ca_cert_file.clone(),
        "SINEX_NATS_CA_CERT",
        PathBuf::from("/run/agenix/sinex-nats-ca"),
    );
    let nats_client_cert = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_client_cert_file.clone(),
        "SINEX_NATS_CLIENT_CERT",
        PathBuf::from("/run/agenix/sinex-nats-client-cert"),
    );
    let nats_client_key = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_client_key_file.clone(),
        "SINEX_NATS_CLIENT_KEY",
        PathBuf::from("/run/agenix/sinex-nats-client-key"),
    );
    let nats_token = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_token_file.clone(),
        "SINEX_NATS_TOKEN_FILE",
        PathBuf::from("/run/agenix/sinex-nats-token"),
    );
    let nats_creds = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_creds_file.clone(),
        "SINEX_NATS_CREDS_FILE",
        PathBuf::from("/run/agenix/sinex-nats-client-creds"),
    );
    let nats_nkey = descriptor_secret_path(
        descriptor,
        |value| value.secrets.nats_nkey_seed_file.clone(),
        "SINEX_NATS_NKEY_SEED_FILE",
        PathBuf::from("/run/agenix/sinex-nats-client-nkey"),
    );

    let mtls_expected = descriptor
        .map(|value| {
            value.gateway.require_client_tls
                || value.secrets.gateway_tls_client_ca_file.as_ref().is_some()
        })
        .unwrap_or_else(|| {
            env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
                || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok()
        });
    let database_password_expected = descriptor
        .map(|value| value.database.password_required)
        .unwrap_or(!descriptor_present);

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
    } else if database_password_expected {
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

    if let Some(path) = gateway_trust_anchor
        && gateway_cert.as_ref() != Some(&path)
    {
        if path.is_file() {
            present.push(format!("gateway-trust-anchor={}", path.display()));
        } else {
            missing.push(format!(
                "gateway-trust-anchor unreadable: {}",
                path.display()
            ));
        }
    }

    if let Some(path) = nats_ca {
        if path.is_file() {
            present.push(format!("nats-ca={}", path.display()));
        } else {
            missing.push(format!("nats-ca unreadable: {}", path.display()));
        }
    }

    match (nats_client_cert.as_ref(), nats_client_key.as_ref()) {
        (Some(cert), Some(key)) if cert.is_file() && key.is_file() => {
            present.push(format!(
                "nats-client-mtls={}/{}",
                cert.display(),
                key.display()
            ));
        }
        (Some(cert), Some(key)) => missing.push(format!(
            "nats-client-mtls unreadable: cert={} key={}",
            cert.display(),
            key.display()
        )),
        (Some(cert), None) => {
            missing.push(format!(
                "nats-client-mtls missing key for cert {}",
                cert.display()
            ));
        }
        (None, Some(key)) => {
            missing.push(format!(
                "nats-client-mtls missing cert for key {}",
                key.display()
            ));
        }
        (None, None) => {}
    }

    let nats_auth_candidates = [nats_token, nats_creds, nats_nkey];
    let declared_nats_auth = nats_auth_candidates
        .iter()
        .filter(|path| path.is_some())
        .count();
    if declared_nats_auth > 1 {
        missing
            .push("NATS auth is ambiguous; declare only one of token, creds, or nkey".to_string());
    } else {
        for (label, path) in [
            ("nats-token", nats_auth_candidates[0].as_ref()),
            ("nats-creds", nats_auth_candidates[1].as_ref()),
            ("nats-nkey", nats_auth_candidates[2].as_ref()),
        ] {
            if let Some(path) = path {
                if path.is_file() {
                    present.push(format!("{label}={}", path.display()));
                } else {
                    missing.push(format!("{label} unreadable: {}", path.display()));
                }
            }
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

async fn build_gateway_probe_client(
    base_url: &str,
    descriptor: Option<&DeploymentReadinessDescriptor>,
) -> Result<GatewayProbeClient> {
    let mut builder = reqwest::Client::builder()
        .timeout(DEPLOYMENT_READY_TIMEOUT)
        .use_rustls_tls();
    let requires_tls = base_url.starts_with("https://");
    let tls_paths = resolve_gateway_probe_tls_paths(descriptor);

    if requires_tls && let Some(ca_path) = tls_paths.trust_anchor.as_ref() {
        let pem = tokio::fs::read(ca_path).await.wrap_err_with(|| {
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

    let client_identity_path = match (tls_paths.client_cert, tls_paths.client_key) {
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

    let base_url = normalize_gateway_base_url(
        descriptor_gateway_base_url(descriptor)
            .or(gateway_url)
            .unwrap_or("https://127.0.0.1:9999"),
    );
    let ready_url = format!("{base_url}/ready");

    let mtls_expected = descriptor
        .map(|value| value.gateway.require_client_tls)
        .unwrap_or(false)
        || env_truthy("SINEX_GATEWAY_REQUIRE_CLIENT_TLS")
        || std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA").is_ok();
    let probe_client = match build_gateway_probe_client(&base_url, descriptor).await {
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

    let mut items = vec![descriptor_item, check_node_entrypoints(descriptor.as_ref()).await];

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
            items.push(check_realm_accessible(&target));
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
                "realm-accessible",
                "Skipped because target identity resolution failed",
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
    use crate::command::CommandContext;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::sinex_test;
    use ::xtask::sandbox::EnvGuard;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;

    fn write_executable_script(
        path: &std::path::Path,
        body: &str,
    ) -> ::xtask::sandbox::TestResult<()> {
        fs::write(path, body)?;
        let mut permissions = fs::metadata(path)?.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)?;
        Ok(())
    }

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

    fn sample_nixos_descriptor() -> DeploymentReadinessDescriptor {
        DeploymentReadinessDescriptor {
            source: Some("nixos".to_string()),
            managed_units: vec![
                "sinex-ingestd.service".to_string(),
                "sinex-gateway.service".to_string(),
                "sinex-filesystem-1.service".to_string(),
                "sinex-terminal-1.service".to_string(),
                "sinex-terminal-2.service".to_string(),
                "sinex-system-1.service".to_string(),
                "sinex-health-automaton.service".to_string(),
            ],
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
                error: None,
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
            error: None,
        };
        let json = serde_json::to_value(&check)?;
        assert_eq!(json["ca_exists"], true);
        assert_eq!(json["server_cert_exists"], false);
        assert_eq!(json["client_cert_exists"], false);
        Ok(())
    }

    #[sinex_test]
    async fn test_detect_tls_check_accepts_gateway_cert_names() -> ::xtask::sandbox::TestResult<()>
    {
        let temp = tempfile::tempdir()?;
        let cert = temp.path().join("gateway.crt");
        let key = temp.path().join("gateway.key");
        std::fs::write(&cert, "not-a-real-cert")?;
        std::fs::write(&key, "not-a-real-key")?;

        let mut env = EnvGuard::new();
        env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());

        let check = detect_tls_check().expect("TLS check should resolve active directory");
        assert!(check.server_cert_exists);
        Ok(())
    }

    #[sinex_test]
    async fn test_detect_tls_check_reports_validation_errors() -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let cert = temp.path().join("gateway.crt");
        let key = temp.path().join("gateway.key");
        std::fs::write(&cert, "not-a-real-cert")?;
        std::fs::write(&key, "not-a-real-key")?;

        let mut env = EnvGuard::new();
        env.set("SINEX_GATEWAY_TLS_CERT", cert.display().to_string());

        let check = detect_tls_check().expect("TLS check should resolve active directory");
        assert!(check.server_cert_exists);
        assert!(check.error.is_some());
        assert!(!check.is_healthy());
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
    async fn test_runtime_assessment_capture_degraded_signals() -> ::xtask::sandbox::TestResult<()>
    {
        let metrics = crate::runtime_metrics::RuntimeMetrics {
            ingestd_status: crate::runtime_metrics::IngestdStatus::Stale,
            last_heartbeat_age_secs: Some(300),
            consumer_lag_pending: Some(1500.0),
            consumer_lag_age_secs: Some(10),
            last_batch_latency_ms: Some(6000.0),
            last_batch_latency_age_secs: Some(10),
            query_error: None,
        };

        let warnings = metrics.assessment().warnings;
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("ingestd heartbeat is stale"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("consumer lag is high"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("batch latency is high"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_assessment_capture_stale_telemetry() -> ::xtask::sandbox::TestResult<()> {
        let metrics = crate::runtime_metrics::RuntimeMetrics {
            ingestd_status: crate::runtime_metrics::IngestdStatus::Healthy,
            last_heartbeat_age_secs: Some(5),
            consumer_lag_pending: Some(42.0),
            consumer_lag_age_secs: Some(600),
            last_batch_latency_ms: Some(125.0),
            last_batch_latency_age_secs: Some(600),
            query_error: None,
        };

        let warnings = metrics.assessment().warnings;
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("consumer lag telemetry is stale"))
        );
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("batch latency telemetry is stale"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_check_skips_honestly_without_database_url()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("DATABASE_URL");
        env.clear("SINEX_DEPLOYMENT_READINESS_CONFIG");
        let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Json), false, None, "doctor");

        let report = execute_runtime_check(&ctx).await?;
        assert!(!report.overall);
        assert!(report.skipped);
        assert_eq!(
            report.skip_reason.as_deref(),
            Some("runtime database target not configured")
        );
        assert_eq!(
            report.assessment.status,
            crate::runtime_metrics::RuntimeHealthStatus::Unavailable
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
    async fn test_resolve_target_identity_rejects_implicit_shell_user()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_TARGET_USER");
        env.clear("SINEX_TARGET_UID");
        env.clear("SINEX_TARGET_HOME");
        env.set("USER", "current-user");
        env.set("UID", "1000");
        env.set("HOME", "/tmp/current-home");

        let error = resolve_target_identity(None)
            .expect_err("deployment readiness should not guess the shell user");
        assert!(
            error
                .to_string()
                .contains("refuses to guess the target user")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_target_identity_rejects_unknown_target_without_explicit_uid_home()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_TARGET_USER",
            "sinex-target-user-that-should-not-exist-for-tests",
        );
        env.clear("SINEX_TARGET_UID");
        env.clear("SINEX_TARGET_HOME");

        let error = resolve_target_identity(None)
            .expect_err("missing passwd target should not fall back to the current process");
        assert!(error.to_string().contains("missing from /etc/passwd"));
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
    async fn test_check_terminal_sources_ignores_native_fish_history_when_not_configured()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(home.join(".local/share/fish"))?;
        std::fs::write(home.join(".bash_history"), "echo hello\n")?;
        std::fs::write(
            home.join(".local/share/fish/fish_history"),
            "- cmd: echo fish\n  when: 1234567890\n",
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
        assert!(item.description.contains("bash:"));
        assert!(!item.description.contains("fish:"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_terminal_sources_fails_when_enabled_sources_are_missing()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home)?;

        let item = check_terminal_sources(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                terminal: sinex_primitives::TerminalDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    kitty_enabled: false,
                    history_sources: vec![sinex_primitives::TerminalHistorySource {
                        path: PathBuf::from("/tmp/probe-home/.bash_history"),
                        shell: "bash".to_string(),
                    }],
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(
            item.description
                .contains("No readable terminal history sources")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_check_terminal_sources_refuses_descriptor_without_declared_sources()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home)?;
        std::fs::write(home.join(".bash_history"), "echo hidden default\n")?;

        let item = check_terminal_sources(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                terminal: sinex_primitives::TerminalDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    kitty_enabled: false,
                    history_sources: Vec::new(),
                },
                ..Default::default()
            }),
        );

        assert_eq!(item.status, "fail");
        assert!(
            item.description
                .contains("terminal.history_sources is empty")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_check_terminal_sources_rejects_descriptor_declared_native_fish_history()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(home.join(".local/share/fish"))?;
        let fish_history = home.join(".local/share/fish/fish_history");
        std::fs::write(&fish_history, "- cmd: echo fish\n  when: 1234567890\n")?;

        let item = check_terminal_sources(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                terminal: sinex_primitives::TerminalDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    kitty_enabled: false,
                    history_sources: vec![sinex_primitives::TerminalHistorySource {
                        path: fish_history.clone(),
                        shell: "fish".to_string(),
                    }],
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("native Fish YAML history is unsupported"));
        assert!(item.description.contains(&fish_history.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_terminal_sources_rejects_descriptor_declared_elvish_history()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(home.join(".config/elvish"))?;
        let elvish_db = home.join(".config/elvish/db");
        std::fs::write(&elvish_db, "not-supported")?;

        let item = check_terminal_sources(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                terminal: sinex_primitives::TerminalDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    kitty_enabled: false,
                    history_sources: vec![sinex_primitives::TerminalHistorySource {
                        path: elvish_db.clone(),
                        shell: "elvish".to_string(),
                    }],
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("native Elvish history database is unsupported"));
        assert!(item.description.contains(&elvish_db.display().to_string()));
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
    async fn test_check_activitywatch_db_fails_when_enabled_path_is_missing()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home)?;
        let missing_db = home.join(".local/share/activitywatch/aw-server-rust/sqlite.db");

        let item = check_activitywatch_db(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                desktop: sinex_primitives::DesktopDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    activitywatch_db_path: Some(missing_db.clone()),
                    ..Default::default()
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(item.description.contains(&missing_db.display().to_string()));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_activitywatch_db_refuses_descriptor_without_declared_path()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let home = temp.path().join("home");
        std::fs::create_dir_all(&home)?;

        let item = check_activitywatch_db(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home,
            },
            Some(&DeploymentReadinessDescriptor {
                desktop: sinex_primitives::DesktopDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    activitywatch_db_path: None,
                    ..Default::default()
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(
            item.description
                .contains("desktop.activitywatch_db_path is unset")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_check_schema_apply_requires_database_url_when_expected()
    -> ::xtask::sandbox::TestResult<()> {
        let item = check_schema_apply(
            None,
            Some(&DeploymentReadinessDescriptor {
                expectations: sinex_primitives::DeploymentExpectations {
                    schema_apply: true,
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .await;
        assert_eq!(item.status, "fail");
        assert!(
            item.description
                .contains("deployment descriptor database runtime")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_database_probe_target_uses_descriptor_runtime()
    -> ::xtask::sandbox::TestResult<()> {
        let descriptor = DeploymentReadinessDescriptor {
            source: Some("nixos".to_string()),
            database: sinex_primitives::DeploymentDatabaseRuntime {
                enabled: true,
                host: Some("127.0.0.1".to_string()),
                port: Some(5432),
                name: Some("sinex_prod".to_string()),
                user: Some("sinex".to_string()),
                local_auth: Some("scram-sha-256".to_string()),
                password_required: true,
            },
            secrets: sinex_primitives::DeploymentSecrets {
                database_password_file: Some(PathBuf::from("/run/agenix/sinex-local-db")),
                ..Default::default()
            },
            ..Default::default()
        };

        let target = resolve_database_probe_target(None, Some(&descriptor))?
            .expect("descriptor runtime should produce a database probe target");
        assert_eq!(
            target.database_url,
            "postgresql://sinex@127.0.0.1:5432/sinex_prod"
        );
        assert_eq!(
            target.password_file,
            Some(PathBuf::from("/run/agenix/sinex-local-db"))
        );
        assert!(target.password_required);
        assert_eq!(target.source, "nixos");
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
    async fn test_runtime_dir_for_target_ignores_current_xdg_runtime_for_other_uid()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("UID", "4242");
        env.set("XDG_RUNTIME_DIR", "/run/user/4242");

        let runtime_dir = runtime_dir_for_target(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home: PathBuf::from("/home/probe-user"),
            },
            None,
        );

        assert_eq!(runtime_dir, PathBuf::from("/run/user/1000"));
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_dir_for_target_ignores_ambient_env_when_descriptor_present()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_HYPRLAND_RUNTIME_DIR", "/tmp/ambient-runtime");
        env.set("UID", "4242");
        env.set("XDG_RUNTIME_DIR", "/run/user/4242");

        let runtime_dir = runtime_dir_for_target(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home: PathBuf::from("/home/probe-user"),
            },
            Some(&DeploymentReadinessDescriptor {
                target: Some(sinex_primitives::DeploymentTarget {
                    user: "probe-user".to_string(),
                    uid: Some(1000),
                    home: Some(PathBuf::from("/home/probe-user")),
                }),
                desktop: sinex_primitives::DesktopDeploymentSurface::default(),
                ..Default::default()
            }),
        );

        assert_eq!(runtime_dir, PathBuf::from("/run/user/1000"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_hyprland_socket_fails_when_enabled_runtime_is_missing()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let runtime_dir = temp.path().join("missing-runtime");

        let item = check_hyprland_socket(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home: temp.path().to_path_buf(),
            },
            Some(&DeploymentReadinessDescriptor {
                desktop: sinex_primitives::DesktopDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    runtime_dir: Some(runtime_dir),
                    ..Default::default()
                },
                ..Default::default()
            }),
        );
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("Hyprland runtime is unavailable"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_hyprland_socket_ignores_ambient_env_when_descriptor_present()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let ambient_runtime = temp.path().join("ambient-runtime");
        let ambient_socket = ambient_runtime.join("ambient/.socket2.sock");
        std::fs::create_dir_all(ambient_socket.parent().expect("socket parent"))?;
        std::fs::write(&ambient_socket, "")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_HYPRLAND_EVENT_SOCKET",
            ambient_socket.display().to_string(),
        );

        let item = check_hyprland_socket(
            &TargetIdentity {
                user: "probe-user".to_string(),
                uid: 1000,
                home: temp.path().to_path_buf(),
            },
            Some(&DeploymentReadinessDescriptor {
                desktop: sinex_primitives::DesktopDeploymentSurface {
                    surface: sinex_primitives::DeploymentSurface {
                        enabled: true,
                        instances: Some(1),
                    },
                    runtime_dir: Some(temp.path().join("missing-runtime")),
                    ..Default::default()
                },
                ..Default::default()
            }),
        );

        assert_eq!(item.status, "fail");
        assert!(item.description.contains("Hyprland runtime is unavailable"));
        Ok(())
    }

    #[sinex_test]
    async fn test_build_gateway_probe_client_allows_http_without_ca()
    -> ::xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_RPC_CA_CERT");
        env.clear("SINEX_RPC_CLIENT_CERT");
        env.clear("SINEX_RPC_CLIENT_KEY");

        let _client = build_gateway_probe_client("http://127.0.0.1:9999", None).await?;
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

        let error = build_gateway_probe_client("https://127.0.0.1:9999", None)
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
    async fn test_build_gateway_probe_client_uses_descriptor_trust_anchor()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        crate::tls::generate_dev_certs(&crate::tls::CertConfig {
            output_dir: temp.path().to_path_buf(),
            san: vec!["127.0.0.1".to_string(), "localhost".to_string()],
            ca_name: "Doctor Test CA".to_string(),
            validity_days: 30,
            force: true,
        })?;

        let mut env = EnvGuard::new();
        env.clear("SINEX_RPC_CA_CERT");
        env.clear("SINEX_RPC_CLIENT_CERT");
        env.clear("SINEX_RPC_CLIENT_KEY");

        let descriptor = DeploymentReadinessDescriptor {
            secrets: sinex_primitives::DeploymentSecrets {
                gateway_tls_trust_anchor_file: Some(temp.path().join("ca.pem")),
                ..Default::default()
            },
            ..Default::default()
        };

        let _client =
            build_gateway_probe_client("https://127.0.0.1:9999", Some(&descriptor)).await?;
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_gateway_probe_tls_paths_prefers_descriptor_trust_anchor()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let descriptor_ca = temp.path().join("descriptor-ca.pem");
        let env_ca = temp.path().join("env-ca.pem");
        std::fs::write(&descriptor_ca, "descriptor")?;
        std::fs::write(&env_ca, "env")?;

        let mut env = EnvGuard::new();
        env.set("SINEX_RPC_CA_CERT", env_ca.display().to_string());

        let descriptor = DeploymentReadinessDescriptor {
            secrets: sinex_primitives::DeploymentSecrets {
                gateway_tls_trust_anchor_file: Some(descriptor_ca.clone()),
                ..Default::default()
            },
            ..Default::default()
        };

        let paths = resolve_gateway_probe_tls_paths(Some(&descriptor));
        assert_eq!(paths.trust_anchor, Some(descriptor_ca));
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

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_GATEWAY_TLS_CLIENT_CA",
            temp.path().join("ambient-client-ca.pem").display().to_string(),
        );
        env.set("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", "1");

        let descriptor = DeploymentReadinessDescriptor {
            secrets: sinex_primitives::DeploymentSecrets {
                database_password_file: Some(db),
                gateway_admin_token_file: None,
                gateway_tls_cert_file: None,
                gateway_tls_key_file: None,
                gateway_tls_trust_anchor_file: None,
                gateway_tls_client_ca_file: None,
                nats_ca_cert_file: None,
                nats_client_cert_file: None,
                nats_client_key_file: None,
                nats_token_file: None,
                nats_creds_file: None,
                nats_nkey_seed_file: None,
            },
            ..Default::default()
        };

        let item = check_secret_materials(Some(&descriptor));
        assert_eq!(item.status, "pass");
        assert!(item.description.contains("database-password"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_secret_materials_requires_descriptor_database_password_when_auth_required()
    -> ::xtask::sandbox::TestResult<()> {
        let descriptor = DeploymentReadinessDescriptor {
            database: sinex_primitives::DeploymentDatabaseRuntime {
                enabled: true,
                host: Some("127.0.0.1".to_string()),
                port: Some(5432),
                name: Some("sinex_prod".to_string()),
                user: Some("sinex".to_string()),
                local_auth: Some("scram-sha-256".to_string()),
                password_required: true,
            },
            ..Default::default()
        };

        let item = check_secret_materials(Some(&descriptor));
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("database-password missing"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_secret_materials_reports_descriptor_declared_nats_secret()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let token = temp.path().join("nats-token");
        std::fs::write(&token, "token")?;

        let mut env = EnvGuard::new();
        env.set(
            "SINEX_GATEWAY_TLS_CLIENT_CA",
            temp.path().join("ambient-client-ca.pem").display().to_string(),
        );
        env.set("SINEX_GATEWAY_REQUIRE_CLIENT_TLS", "1");

        let descriptor = DeploymentReadinessDescriptor {
            secrets: sinex_primitives::DeploymentSecrets {
                nats_token_file: Some(token),
                ..Default::default()
            },
            ..Default::default()
        };

        let item = check_secret_materials(Some(&descriptor));
        assert_eq!(item.status, "pass");
        assert!(item.description.contains("nats-token"));
        Ok(())
    }

    #[sinex_test]
    async fn test_descriptor_nats_secrets_backfill_connection_config()
    -> ::xtask::sandbox::TestResult<()> {
        let descriptor = DeploymentReadinessDescriptor {
            nats: sinex_primitives::DeploymentNatsRuntime {
                servers: vec!["tls://nats.example:4223".to_string()],
            },
            secrets: sinex_primitives::DeploymentSecrets {
                nats_ca_cert_file: Some(PathBuf::from("/run/agenix/sinex-nats-ca")),
                nats_token_file: Some(PathBuf::from("/run/agenix/sinex-nats-token")),
                ..Default::default()
            },
            ..Default::default()
        };

        let config =
            apply_descriptor_nats_overrides(NatsConnectionConfig::default(), Some(&descriptor));
        assert_eq!(
            config.ca_cert,
            Some(PathBuf::from("/run/agenix/sinex-nats-ca"))
        );
        assert_eq!(config.url, "tls://nats.example:4223");
        assert_eq!(
            config.token_file,
            Some(PathBuf::from("/run/agenix/sinex-nats-token"))
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_check_node_entrypoints_skips_empty_prepared_descriptor_units()
    -> ::xtask::sandbox::TestResult<()> {
        let item = check_node_entrypoints(Some(&DeploymentReadinessDescriptor {
            mode: DeploymentReadinessMode::Prepared,
            managed_units: Vec::new(),
            ..Default::default()
        }))
        .await;
        assert_eq!(item.status, "skip");
        assert!(item.description.contains("managed units"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_node_entrypoints_requires_watchdog_contract()
    -> ::xtask::sandbox::TestResult<()> {
        let temp = tempfile::tempdir()?;
        let bin_dir = temp.path().join("bin");
        fs::create_dir_all(&bin_dir)?;

        write_executable_script(
            &bin_dir.join("systemctl"),
            r#"#!/bin/sh
if [ "$1" = "show" ] && [ "$2" = "sinex-ingestd.service" ]; then
  printf 'ActiveState=active\nSubState=running\nLoadState=loaded\nType=notify\nNotifyAccess=main\nWatchdogUSec=0\n'
  exit 0
fi
printf 'unexpected invocation: %s\n' "$*" >&2
exit 1
"#,
        )?;

        let original_path = std::env::var("PATH").unwrap_or_default();
        let mut env = EnvGuard::new();
        env.set("PATH", bin_dir.display().to_string());

        let item = check_node_entrypoints(Some(&DeploymentReadinessDescriptor {
            mode: DeploymentReadinessMode::Prepared,
            managed_units: vec!["sinex-ingestd.service".to_string()],
            ..Default::default()
        }))
        .await;

        drop(env);
        assert_eq!(std::env::var("PATH").unwrap_or_default(), original_path);
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("watchdog_usec=0"));
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
                kitty_enabled: false,
                history_sources: Vec::new(),
            },
            ..Default::default()
        };

        let item = check_singleton_workstation_topology(Some(&descriptor));
        assert_eq!(item.status, "fail");
        assert!(item.description.contains("filesystem=2"));
        Ok(())
    }

    #[sinex_test]
    async fn test_check_singleton_workstation_topology_skips_prepared_descriptor_without_target()
    -> ::xtask::sandbox::TestResult<()> {
        let item = check_singleton_workstation_topology(Some(&DeploymentReadinessDescriptor {
            mode: DeploymentReadinessMode::Prepared,
            filesystem: sinex_primitives::DeploymentSurface {
                enabled: true,
                instances: Some(2),
            },
            ..Default::default()
        }));

        assert_eq!(item.status, "skip");
        assert!(
            item.description
                .contains("singleton defaults are not expected")
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_nixos_descriptor_managed_units_are_consumed_directly()
    -> ::xtask::sandbox::TestResult<()> {
        let units = sample_nixos_descriptor().managed_units;
        assert!(units.contains(&"sinex-ingestd.service".to_string()));
        assert!(units.contains(&"sinex-gateway.service".to_string()));
        assert!(units.contains(&"sinex-filesystem-1.service".to_string()));
        assert!(units.contains(&"sinex-terminal-1.service".to_string()));
        assert!(units.contains(&"sinex-terminal-2.service".to_string()));
        assert!(units.contains(&"sinex-system-1.service".to_string()));
        assert!(units.contains(&"sinex-health-automaton.service".to_string()));
        assert!(!units.iter().any(|unit| unit == "sinex-desktop-1.service"));
        Ok(())
    }
}

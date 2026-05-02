//! Doctor command - health check for Postgres, NATS, tools, and TLS

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::infra::probe::{probe_nats, probe_postgres};
use crate::output::Status;
use crate::tools::{ToolInfo, ToolManager};
use color_eyre::eyre::{Result, WrapErr, eyre};
use console::style;
use serde::Serialize;
use sinex_primitives::DeploymentReadinessDescriptor;
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEPLOYMENT_READY_TIMEOUT: Duration = Duration::from_secs(5);
const RECOMMENDED_INOTIFY_MAX_USER_WATCHES: u64 = 524_288;

/// Probe developer-environment health and deployment readiness.
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_extensions_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pipeline_smoke: Option<DoctorServiceCheck>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
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

fn workspace_tls_dir() -> PathBuf {
    crate::config::workspace_root().join(".sinex/tls")
}

fn detect_tls_check() -> Option<TlsCheck> {
    let default_tls_dir = workspace_tls_dir();
    let env_dir = std::env::var("SINEX_GATEWAY_TLS_CERT")
        .ok()
        .and_then(|p| Path::new(&p).parent().map(Path::to_path_buf));
    let active_dir = if let Some(dir) = env_dir.as_deref() {
        dir.exists().then_some(dir)
    } else if default_tls_dir.exists() {
        Some(default_tls_dir.as_path())
    } else {
        None
    }?;

    let server_cert_path = resolve_tls_artifact(active_dir, &["server.pem"]);
    let server_key_path = resolve_tls_artifact(active_dir, &["server-key.pem"]);
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PostgresExtensionsProbe {
    extensions: Option<Vec<String>>,
    error: Option<String>,
}

fn summarize_command_probe_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn probe_postgres_extensions<T, RunPsql>(
    pg_ready: bool,
    stack_config: std::result::Result<T, String>,
    mut run_psql: RunPsql,
) -> PostgresExtensionsProbe
where
    RunPsql: FnMut(&T) -> std::io::Result<std::process::Output>,
{
    if !pg_ready {
        return PostgresExtensionsProbe {
            extensions: None,
            error: None,
        };
    }

    let cfg = match stack_config {
        Ok(cfg) => cfg,
        Err(error) => {
            return PostgresExtensionsProbe {
                extensions: None,
                error: Some(format!(
                    "Postgres is reachable, but doctor could not load stack config to probe extensions: {error}"
                )),
            };
        }
    };

    let output = match run_psql(&cfg) {
        Ok(output) => output,
        Err(error) => {
            return PostgresExtensionsProbe {
                extensions: None,
                error: Some(format!(
                    "Postgres is reachable, but doctor failed to run extension probe via psql: {error}"
                )),
            };
        }
    };

    if !output.status.success() {
        return PostgresExtensionsProbe {
            extensions: None,
            error: Some(format!(
                "Postgres is reachable, but doctor failed to probe extensions: {}",
                summarize_command_probe_output(&output)
            )),
        };
    }

    let extensions = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToString::to_string)
        .collect();

    PostgresExtensionsProbe {
        extensions: Some(extensions),
        error: None,
    }
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

            let remediation_warnings = remediate_stack_services(
                pg_probe.ready(),
                nats_probe.ready(),
                crate::infra::stack::StackConfig::for_current_checkout()
                    .map_err(|error| error.to_string()),
                ctx.is_human(),
                crate::infra::stack::pg_start,
                crate::infra::stack::nats_start,
            );
            if !remediation_warnings.is_empty() {
                if result.status == Status::Success {
                    result.status = Status::Partial;
                }
                result.warnings.extend(remediation_warnings);
            }

            // Re-run diagnostics to reflect the post-remediation state.  The
            // initial `result` was captured before any fixes were applied; without
            // this refresh, callers see stale `overall: false` even after a
            // successful infra restart.
            if let Ok(fresh) = execute_doctor(self.pipelines, ctx) {
                result.data = fresh.data;
                result.status = fresh.status;
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

/// Run diagnostics.
fn execute_doctor(pipelines: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let mut all_ok = true;

    let pg_probe = probe_postgres();
    let pg_msg = service_readiness_message(
        pg_probe.ready(),
        pg_probe.message.as_deref(),
        || "Postgres is not ready".to_string(),
        &mut all_ok,
    );

    let nats_probe = probe_nats();
    let nats_msg = service_readiness_message(
        nats_probe.ready(),
        nats_probe.message.as_deref(),
        || format!("Cannot connect to NATS on port {}", nats_probe.port),
        &mut all_ok,
    );

    let tools_to_check = [
        "rustc",
        "ast-grep",
        "repomix",
        "cargo-machete",
        "cargo-nextest",
    ];
    let tool_checks = collect_tool_checks(&tools_to_check, &mut all_ok);

    // Batch validation summary for missing tools
    let missing = ToolManager::check_required_tools(&tools_to_check);

    // Check Postgres extensions
    let postgres_extensions_probe = probe_postgres_extensions(
        pg_probe.ready(),
        crate::infra::stack::StackConfig::for_current_checkout().map_err(|error| error.to_string()),
        |cfg| {
            std::process::Command::new("psql")
                .env("PGHOST", cfg.run_dir())
                .env("PGPORT", cfg.postgres.port.to_string())
                .args(["-tAc", "SELECT extname FROM pg_extension"])
                .output()
        },
    );
    if postgres_extensions_probe.error.is_some() {
        all_ok = false;
    }

    // Check TLS certificates from env vars or .sinex/tls/
    let tls_check = detect_tls_check();
    if tls_check.as_ref().is_some_and(|check| !check.is_healthy()) {
        all_ok = false;
    }

    let pipeline_smoke = pipeline_smoke_check(pipelines, &mut all_ok);

    // Collect environment configuration
    let cfg = config();
    let environment = Some(serde_json::json!({
        "hostname": cfg.hostname,
        "state_dir": cfg.state_dir.display().to_string(),
        "cache_dir": cfg.cache_dir.display().to_string(),
        "database_url": cfg.database_url.as_deref().map(redact_database_url_password),
        "nats_url": cfg.nats_url,
        "gateway_url": cfg.gateway_url,
        "test_results_dir": cfg.test_results_dir.as_ref().map(|p| p.display().to_string()),
        "toolchain": cfg.toolchain,
        "in_dev_shell": cfg.in_dev_shell,
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
        postgres_extensions: postgres_extensions_probe.extensions,
        postgres_extensions_error: postgres_extensions_probe.error,
        pipeline_smoke,
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
            let detail = tool_check_detail(tool);
            print_check(&tool.name, tool.available, detail.as_deref());
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
            if let Some(in_dev_shell) = env_data
                .get("in_dev_shell")
                .and_then(serde_json::Value::as_bool)
            {
                println!(
                    "  {:<20} {}",
                    "In devShell:",
                    if in_dev_shell { "yes" } else { "no" }
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
        } else if let Some(error) = &report.postgres_extensions_error {
            println!("\n{}", style("Postgres Extensions:").bold());
            println!("  {} {error}", style("✗").red());
        }

        // Pipeline smoke tests
        if let Some(pipeline_smoke) = &report.pipeline_smoke {
            println!("\n{}", style("Pipeline Smoke Test:").bold());
            print_check(
                "Pipeline smoke",
                pipeline_smoke.available,
                pipeline_smoke.message.as_deref(),
            );
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

fn pipeline_smoke_invocation(
    xtask_program: impl Into<String>,
) -> (String, [&'static str; 5], String) {
    (
        xtask_program.into(),
        ["test", "--debug", "-p", "sinex-ingestd", "-E"],
        "test(test_pipeline_smoke)".to_string(),
    )
}

fn run_pipeline_smoke_test() -> Result<()> {
    use crate::process::ProcessBuilder;

    let xtask_program = std::env::current_exe()
        .wrap_err("failed to resolve current xtask executable for pipeline smoke")?;
    let (program, args, filter) = pipeline_smoke_invocation(xtask_program.display().to_string());
    let output = ProcessBuilder::new(program)
        .args(args)
        .arg(&filter)
        .with_description("pipeline smoke test")
        .run_capture()?;
    if output.success() {
        return Ok(());
    }

    let combined = output.combined().trim().to_string();
    let detail = if combined.is_empty() {
        String::new()
    } else {
        format!("\n{combined}")
    };
    Err(eyre!(
        "pipeline smoke test failed with exit code {}{}",
        output.exit_code,
        detail
    ))
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

fn tool_check_detail(tool: &ToolCheck) -> Option<String> {
    match (tool.version.as_deref(), tool.message.as_deref()) {
        (Some(version), Some(message)) => Some(format!("{version}; {message}")),
        (Some(version), None) => Some(version.to_string()),
        (None, Some(message)) => Some(message.to_string()),
        (None, None) => None,
    }
}

fn service_readiness_message(
    ready: bool,
    message: Option<&str>,
    fallback: impl FnOnce() -> String,
    all_ok: &mut bool,
) -> Option<String> {
    if ready {
        return None;
    }
    *all_ok = false;
    Some(message.map_or_else(fallback, str::to_owned))
}

fn collect_tool_checks(tools_to_check: &[&str], all_ok: &mut bool) -> Vec<ToolCheck> {
    let mut tool_checks = Vec::with_capacity(tools_to_check.len());
    for tool in tools_to_check {
        let tool_check = build_tool_check(tool, ToolManager::check_tool(tool));
        if !tool_check.available {
            *all_ok = false;
        }
        tool_checks.push(tool_check);
    }
    tool_checks
}

fn pipeline_smoke_check(pipelines: bool, all_ok: &mut bool) -> Option<DoctorServiceCheck> {
    if !pipelines {
        return None;
    }

    match run_pipeline_smoke_test() {
        Ok(()) => Some(DoctorServiceCheck {
            available: true,
            message: None,
        }),
        Err(error) => {
            *all_ok = false;
            Some(DoctorServiceCheck {
                available: false,
                message: Some(format!("{error:#}")),
            })
        }
    }
}

fn build_tool_check(name: &str, result: Result<ToolInfo>) -> ToolCheck {
    match result {
        Ok(info) => ToolCheck {
            name: name.to_string(),
            available: info.probe_issue.is_none(),
            version: Some(info.version),
            path: Some(info.path.display().to_string()),
            message: info.probe_issue,
        },
        Err(error) => ToolCheck {
            name: name.to_string(),
            available: false,
            version: None,
            path: None,
            message: Some(error.to_string()),
        },
    }
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
            println!("  {lag_icon} Consumer lag:       {lag:.0} pending");
        } else if let Some(note) = metrics.consumer_lag_stale_note() {
            println!(
                "  {} Consumer lag:       stale telemetry ({})",
                style("⚠").yellow(),
                note
            );
        }

        // Batch latency
        if let Some(latency) = metrics.fresh_batch_latency_ms() {
            let lat_icon = if latency > 5000.0 {
                style("⚠").yellow()
            } else {
                style("✓").green()
            };
            println!("  {lat_icon} Batch latency:      {latency:.0}ms");
        } else if let Some(note) = metrics.batch_latency_stale_note() {
            println!(
                "  {} Batch latency:      stale telemetry ({})",
                style("⚠").yellow(),
                note
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

fn remediate_stack_services<T, PgStart, NatsStart>(
    pg_ready: bool,
    nats_ready: bool,
    stack_config: std::result::Result<T, String>,
    verbose: bool,
    mut pg_start: PgStart,
    mut nats_start: NatsStart,
) -> Vec<String>
where
    PgStart: FnMut(&T, bool) -> Result<()>,
    NatsStart: FnMut(&T, bool) -> Result<()>,
{
    if pg_ready && nats_ready {
        return Vec::new();
    }

    let cfg = match stack_config {
        Ok(cfg) => cfg,
        Err(error) => {
            return vec![format!(
                "Doctor --fix could not load stack config for infra remediation: {error}"
            )];
        }
    };

    let mut warnings = Vec::new();
    if !pg_ready && let Err(error) = pg_start(&cfg, verbose) {
        warnings.push(format!(
            "Doctor --fix failed to start Postgres during infra remediation: {error}"
        ));
    }
    if !nats_ready && let Err(error) = nats_start(&cfg, verbose) {
        warnings.push(format!(
            "Doctor --fix failed to start NATS during infra remediation: {error}"
        ));
    }

    warnings
}

mod deployment;

#[cfg(test)]
use deployment::*;
pub(crate) use deployment::{DeploymentReadinessItem, check_gateway_ready};
use deployment::{
    execute_deployment_readiness, redact_database_url_password,
    resolve_effective_database_probe_url,
};

#[cfg(test)]
mod tests;

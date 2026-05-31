//! Doctor command - health check for Postgres, NATS, tools, and TLS

use crate::cargo_diagnostics::CompilerDiagnostic;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::infra::probe::{probe_nats, probe_postgres};
use crate::output::Status;
use crate::tools::{ToolInfo, ToolManager};
use color_eyre::eyre::{Result, WrapErr, eyre};
use console::style;
use serde::Serialize;
use sinex_primitives::{DeploymentReadinessDescriptor, privacy::load_private_mode_state};
use std::path::{Path, PathBuf};
use std::time::Duration;
use walkdir::WalkDir;

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

    /// Reclaim stale target-dir artifacts (cargo-sweep + incremental/ prune)
    #[arg(long)]
    pub reclaim: bool,

    /// Inspect managed test database footprint and /dev/shm headroom
    #[arg(long)]
    pub test_db: bool,

    /// Inspect rust-analyzer process footprint and local config
    #[arg(long)]
    pub rust_analyzer: bool,
}

/// Diagnose rust-analyzer process footprint and local workspace contract.
#[derive(clap::Args)]
pub struct RaDiagnoseCommand {
    /// Also run rust-analyzer's batch diagnostics subcommand.
    #[arg(long)]
    pub collect_diagnostics: bool,

    /// Minimum severity for --collect-diagnostics.
    #[arg(long, default_value = "warning")]
    pub severity: String,
}

/// Doctor report structures
#[derive(Debug, Serialize)]
pub(crate) struct DoctorReport {
    pub postgres: DoctorServiceCheck,
    pub nats: DoctorServiceCheck,
    pub private_mode: DoctorServiceCheck,
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

#[derive(Debug, Serialize)]
struct TestDbDoctorReport {
    footprint: crate::sandbox::db::pool::TestDatabaseFootprintReport,
    dev_shm: Option<DevShmSnapshot>,
}

#[derive(Debug, Serialize)]
struct DevShmSnapshot {
    used_mb: f64,
    free_mb: f64,
}

#[derive(Debug, Serialize)]
struct RustAnalyzerDoctorReport {
    config_path: String,
    config_present: bool,
    diagnostic_role: &'static str,
    proof_authority: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<RustAnalyzerConfigSummary>,
    target_dir: String,
    process_count: usize,
    total_rss_mb: f64,
    processes: Vec<RustAnalyzerProcess>,
    workspace_contract: RustAnalyzerWorkspaceContractSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    cli_diagnostics: Option<RustAnalyzerCliDiagnosticScan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    history_recorded_diagnostics: Option<usize>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RustAnalyzerConfigSummary {
    parse_ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    parse_error: Option<String>,
    num_threads: Option<i64>,
    cargo_all_targets: Option<bool>,
    cache_priming_enable: Option<bool>,
    check_workspace: Option<bool>,
    lru_capacity: Option<i64>,
    proc_macro_enable: Option<bool>,
    proc_macro_attributes_enable: Option<bool>,
    files_exclude_dirs: Vec<String>,
    diagnostics_disabled: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RustAnalyzerProcess {
    pid: u32,
    rss_mb: f64,
    command: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RustAnalyzerWorkspaceContractSummary {
    xtask_dev_dependency_count: usize,
    xtask_dev_dependency_packages: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RustAnalyzerCliDiagnosticScan {
    command: Vec<String>,
    exit_code: Option<i32>,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr_summary: Option<RustAnalyzerCliStderrSummary>,
    diagnostics: Vec<RustAnalyzerCliDiagnostic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stderr: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RustAnalyzerCliStderrSummary {
    categories: Vec<&'static str>,
    remediation_actions: Vec<&'static str>,
    cyclic_dependency_warnings: usize,
    cyclic_dependency_edges: Vec<String>,
    self_cycle_edges: Vec<String>,
    xtask_cycle_edges: Vec<String>,
    workspace_cycle_edges: Vec<String>,
    internal_errors: usize,
    internal_error_kinds: Vec<&'static str>,
    other_warnings: usize,
    other_warning_samples: Vec<String>,
    other_errors: usize,
    other_error_samples: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
struct RustAnalyzerCliDiagnostic {
    crate_name: String,
    file: String,
    severity: String,
    diagnostic_kind: String,
    line: u32,
    col: u32,
    end_line: u32,
    end_col: u32,
    message: String,
}

const RA_ACTION_EXTRACT_XTASK_SANDBOX: &str = "extract shared test/sandbox helpers out of xtask or remove workspace crates' dev-dependency on xtask";
const RA_ACTION_BREAK_WORKSPACE_CYCLES: &str =
    "break non-xtask workspace crate cycles before treating rust-analyzer as clean";
const RA_ACTION_INSPECT_SELF_CYCLES: &str =
    "inspect rust-analyzer self-cycle reports against crate target/dev-dependency metadata";
const RA_ACTION_CAPTURE_UPSTREAM_REPRO: &str =
    "capture an upstream rust-analyzer repro after local cycle pressure is reduced";
const RA_ACTION_CLASSIFY_UNCATEGORIZED_STDERR: &str =
    "classify uncategorized rust-analyzer stderr samples";

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
    let env_dir = std::env::var("SINEX_API_TLS_CERT")
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

        if self.test_db {
            let test_db = execute_test_db_check(ctx).await?;
            let test_db_value = serde_json::to_value(&test_db)?;
            merge_result_data(&mut result, "test_db", test_db_value);
            if ctx.is_human() {
                print_test_db_report(&test_db);
            }
        }

        if self.rust_analyzer {
            match execute_rust_analyzer_check(false, "warning") {
                Ok(rust_analyzer) => {
                    let rust_analyzer_value = serde_json::to_value(&rust_analyzer)?;
                    merge_result_data(&mut result, "rust_analyzer", rust_analyzer_value);
                    if !rust_analyzer.warnings.is_empty() && result.status == Status::Success {
                        result.status = Status::Partial;
                    }
                    result.warnings.extend(rust_analyzer.warnings.clone());
                    if ctx.is_human() {
                        print_rust_analyzer_report(&rust_analyzer);
                    }
                }
                Err(error) => {
                    if ctx.is_human() {
                        eprintln!("  rust-analyzer: unavailable ({error})");
                    }
                    merge_result_data(
                        &mut result,
                        "rust_analyzer",
                        serde_json::json!({
                            "available": false,
                            "error": format!("{error:#}"),
                            "diagnostic_role": "advisory",
                            "proof_authority": false,
                        }),
                    );
                }
            }
        }

        if self.reclaim {
            let target_dir = std::env::var("CARGO_TARGET_DIR").map_or_else(
                |_| crate::config::workspace_root().join("target"),
                std::path::PathBuf::from,
            );
            if ctx.is_human() {
                println!(
                    "Reclaiming stale artifacts from {}...",
                    target_dir.display()
                );
            }
            match crate::cache_hygiene::reclaim(&target_dir) {
                Ok(report) => {
                    let total_reclaimed =
                        report.cargo_sweep_reclaimed_bytes + report.incremental_bytes_reclaimed;
                    if ctx.is_human() {
                        println!(
                            "Reclaimed {:.2} GB (cargo-sweep: {:.2} GB; incremental keep-3: {} dirs / {:.2} GB).",
                            total_reclaimed as f64 / 1e9,
                            report.cargo_sweep_reclaimed_bytes as f64 / 1e9,
                            report.incremental_dirs_deleted,
                            report.incremental_bytes_reclaimed as f64 / 1e9,
                        );
                        if !report.cargo_sweep_ran {
                            println!(
                                "  (cargo-sweep not in PATH — install via flake.nix devshell)"
                            );
                        }
                        if let (Some(before), Some(after)) = (&report.before, &report.after) {
                            println!(
                                "  Disk usage on {}: {:.1}% -> {:.1}% ({} GB free)",
                                before.mount,
                                before.percent_used,
                                after.percent_used,
                                after.free_gb as u64,
                            );
                        }
                    }
                }
                Err(error) => {
                    if ctx.is_human() {
                        eprintln!("Reclaim failed: {error}");
                    }
                    result.warnings.push(format!("--reclaim failed: {error}"));
                }
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

impl XtaskCommand for RaDiagnoseCommand {
    fn name(&self) -> &'static str {
        "ra-diagnose"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let mut report = match execute_rust_analyzer_check(self.collect_diagnostics, &self.severity)
        {
            Ok(report) => report,
            Err(error) => {
                let message = format!("rust-analyzer unavailable: {error}");
                if ctx.is_human() {
                    eprintln!("  rust-analyzer: unavailable ({error})");
                }
                return Ok(CommandResult::success()
                    .with_message("rust-analyzer unavailable")
                    .with_detail(&message)
                    .with_data(serde_json::json!({
                        "ra_available": false,
                        "ra_error": format!("{error:#}"),
                        "diagnostic_role": "advisory",
                        "proof_authority": false,
                    }))
                    .with_duration(ctx.elapsed()));
            }
        };
        if let Some(scan) = &report.cli_diagnostics {
            let diagnostics = scan
                .diagnostics
                .iter()
                .map(rust_analyzer_diagnostic_to_compiler_diagnostic)
                .collect::<Vec<_>>();
            ctx.record_advisory_diagnostics(&diagnostics)?;
            report.history_recorded_diagnostics = Some(diagnostics.len());
        }
        if ctx.is_human() {
            print_rust_analyzer_report(&report);
        }

        let status = if report.warnings.is_empty() {
            CommandResult::success()
        } else {
            CommandResult::partial()
        };
        Ok(status
            .with_warnings(report.warnings.clone())
            .with_data(serde_json::to_value(&report)?)
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
    }
}

fn merge_result_data(result: &mut CommandResult, key: &str, value: serde_json::Value) {
    let existing_data = result.data.take();
    result.data = Some(match existing_data {
        Some(mut existing) => {
            if let Some(map) = existing.as_object_mut() {
                map.insert(key.to_string(), value);
                existing
            } else {
                serde_json::json!({
                    "doctor": existing,
                    key: value,
                })
            }
        }
        None => serde_json::json!({
            key: value,
        }),
    });
}

async fn execute_test_db_check(_ctx: &CommandContext) -> Result<TestDbDoctorReport> {
    let footprint = crate::sandbox::db::pool::inspect_test_database_footprint()
        .await
        .map_err(|error| eyre!("failed to inspect test database footprint: {error:#}"))?;
    let dev_shm = crate::process::shm_usage_mb()
        .map(|(used_mb, free_mb)| DevShmSnapshot { used_mb, free_mb });
    Ok(TestDbDoctorReport { footprint, dev_shm })
}

fn execute_rust_analyzer_check(
    collect_diagnostics: bool,
    severity: &str,
) -> Result<RustAnalyzerDoctorReport> {
    let root = crate::config::workspace_root();
    let config_path = root.join("rust-analyzer.toml");
    let config = analyze_rust_analyzer_config(&config_path)?;
    let workspace_contract = summarize_rust_analyzer_workspace_contract(&root)?;
    let target_dir = std::env::var("CARGO_TARGET_DIR").map_or_else(
        |_| crate::config::workspace_target_dir_for(&root),
        PathBuf::from,
    );
    let processes = collect_rust_analyzer_processes()?;
    let total_rss_mb = if processes.is_empty() {
        0.0
    } else {
        processes.iter().map(|process| process.rss_mb).sum()
    };
    let mut warnings = Vec::new();
    if processes.len() > 1 {
        warnings.push(format!(
            "{} rust-analyzer processes are running for this user/session",
            processes.len()
        ));
    }
    if total_rss_mb > 2048.0 {
        warnings.push(format!(
            "rust-analyzer RSS is {total_rss_mb:.0} MB; consider checking editor duplicate sessions"
        ));
    }
    match config.as_ref() {
        Some(config) => warnings.extend(config.warnings.clone()),
        None => warnings.push(
            "rust-analyzer.toml is missing; rust-analyzer may index the full workspace".to_string(),
        ),
    }
    let cli_diagnostics = if collect_diagnostics {
        let scan = run_rust_analyzer_cli_diagnostics(&root, severity)?;
        if scan.exit_code != Some(0) {
            let categories = scan
                .stderr_summary
                .as_ref()
                .map(|summary| format!(" ({})", summary.categories.join(", ")))
                .unwrap_or_default();
            warnings.push(format!(
                "rust-analyzer diagnostics exited with {:?}{categories}; see cli_diagnostics.stderr",
                scan.exit_code,
            ));
        }
        Some(scan)
    } else {
        None
    };

    Ok(RustAnalyzerDoctorReport {
        config_path: config_path.display().to_string(),
        config_present: config_path.is_file(),
        diagnostic_role: "advisory",
        proof_authority: false,
        config,
        target_dir: target_dir.display().to_string(),
        process_count: processes.len(),
        total_rss_mb,
        processes,
        workspace_contract,
        cli_diagnostics,
        history_recorded_diagnostics: None,
        warnings,
    })
}

fn analyze_rust_analyzer_config(path: &Path) -> Result<Option<RustAnalyzerConfigSummary>> {
    if !path.is_file() {
        return Ok(None);
    }

    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let parsed = match toml::from_str::<toml::Value>(&contents) {
        Ok(value) => value,
        Err(error) => {
            return Ok(Some(RustAnalyzerConfigSummary {
                parse_ok: false,
                parse_error: Some(error.to_string()),
                num_threads: None,
                cargo_all_targets: None,
                cache_priming_enable: None,
                check_workspace: None,
                lru_capacity: None,
                proc_macro_enable: None,
                proc_macro_attributes_enable: None,
                files_exclude_dirs: Vec::new(),
                diagnostics_disabled: Vec::new(),
                warnings: vec![format!(
                    "rust-analyzer.toml is malformed; rust-analyzer will ignore the local contract: {error}"
                )],
            }));
        }
    };

    Ok(Some(summarize_rust_analyzer_config(&parsed)))
}

fn summarize_rust_analyzer_config(value: &toml::Value) -> RustAnalyzerConfigSummary {
    let num_threads = toml_i64(value, &["numThreads"]);
    let cargo_all_targets = toml_bool(value, &["cargo", "allTargets"]);
    let cache_priming_enable = toml_bool(value, &["cachePriming", "enable"]);
    let check_workspace = toml_bool(value, &["check", "workspace"]);
    let lru_capacity = toml_i64(value, &["lru", "capacity"]);
    let proc_macro_enable = toml_bool(value, &["procMacro", "enable"]);
    let proc_macro_attributes_enable = toml_bool(value, &["procMacro", "attributes", "enable"]);
    let files_exclude_dirs = toml_string_array(value, &["files", "excludeDirs"]);
    let diagnostics_disabled = toml_string_array(value, &["diagnostics", "disabled"]);

    let mut warnings = Vec::new();
    if !matches!(num_threads, Some(1..=8)) {
        warnings.push("rust-analyzer numThreads should be set between 1 and 8".to_string());
    }
    if cargo_all_targets != Some(false) {
        warnings.push("rust-analyzer cargo.allTargets should stay false".to_string());
    }
    if cache_priming_enable != Some(false) {
        warnings.push("rust-analyzer cachePriming.enable should stay false".to_string());
    }
    if check_workspace != Some(false) {
        warnings.push("rust-analyzer check.workspace should stay false".to_string());
    }
    if !matches!(lru_capacity, Some(1..=2048)) {
        warnings.push("rust-analyzer lru.capacity should be capped at 2048 or lower".to_string());
    }
    if proc_macro_enable != Some(true) {
        warnings
            .push("rust-analyzer procMacro.enable should stay true for derives/sqlx".to_string());
    }
    if proc_macro_attributes_enable != Some(false) {
        warnings.push("rust-analyzer procMacro.attributes.enable should stay false".to_string());
    }
    for required in [".sinex", "target", ".direnv"] {
        if !files_exclude_dirs.iter().any(|dir| dir == required) {
            warnings.push(format!(
                "rust-analyzer files.excludeDirs should include {required}"
            ));
        }
    }
    if !diagnostics_disabled
        .iter()
        .any(|disabled| disabled == "unresolved-proc-macro")
    {
        warnings.push(
            "rust-analyzer diagnostics.disabled should include unresolved-proc-macro".to_string(),
        );
    }

    RustAnalyzerConfigSummary {
        parse_ok: true,
        parse_error: None,
        num_threads,
        cargo_all_targets,
        cache_priming_enable,
        check_workspace,
        lru_capacity,
        proc_macro_enable,
        proc_macro_attributes_enable,
        files_exclude_dirs,
        diagnostics_disabled,
        warnings,
    }
}

fn toml_at<'a>(value: &'a toml::Value, path: &[&str]) -> Option<&'a toml::Value> {
    let mut current = value;
    for segment in path {
        current = current.get(*segment)?;
    }
    Some(current)
}

fn toml_bool(value: &toml::Value, path: &[&str]) -> Option<bool> {
    toml_at(value, path).and_then(toml::Value::as_bool)
}

fn toml_i64(value: &toml::Value, path: &[&str]) -> Option<i64> {
    toml_at(value, path).and_then(toml::Value::as_integer)
}

fn toml_string_array(value: &toml::Value, path: &[&str]) -> Vec<String> {
    toml_at(value, path)
        .and_then(toml::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(toml::Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn collect_rust_analyzer_processes() -> Result<Vec<RustAnalyzerProcess>> {
    let mut processes = Vec::new();
    let entries = match std::fs::read_dir("/proc") {
        Ok(entries) => entries,
        Err(error) => {
            return Err(error).wrap_err("failed to read /proc for rust-analyzer processes");
        }
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(pid) = file_name.to_str().and_then(|name| name.parse::<u32>().ok()) else {
            continue;
        };
        let proc_dir = entry.path();
        let comm = std::fs::read_to_string(proc_dir.join("comm")).unwrap_or_default();
        let command = read_proc_cmdline(&proc_dir).unwrap_or_else(|| comm.clone());
        if !is_rust_analyzer_process(&comm, &command) {
            continue;
        }
        let rss_mb = read_proc_rss_mb(&proc_dir).unwrap_or(0.0);
        processes.push(RustAnalyzerProcess {
            pid,
            rss_mb,
            command: command.trim().to_string(),
        });
    }

    processes.sort_by_key(|process| process.pid);
    Ok(processes)
}

fn summarize_rust_analyzer_workspace_contract(
    root: &Path,
) -> Result<RustAnalyzerWorkspaceContractSummary> {
    let mut xtask_dev_dependency_packages = Vec::new();

    for entry in WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_entry(|entry| rust_analyzer_manifest_walk_entry(root, entry.path()))
    {
        let entry = entry?;
        if entry.file_type().is_file() && entry.file_name() == "Cargo.toml" {
            let manifest_path = entry.path();
            let contents = std::fs::read_to_string(manifest_path)
                .with_context(|| format!("failed to read {}", manifest_path.display()))?;
            let parsed = toml::from_str::<toml::Value>(&contents)
                .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
            if manifest_has_xtask_dev_dependency(&parsed) {
                let package = toml_at(&parsed, &["package", "name"])
                    .and_then(toml::Value::as_str)
                    .map_or_else(|| manifest_path.display().to_string(), ToString::to_string);
                if !xtask_dev_dependency_packages.contains(&package) {
                    xtask_dev_dependency_packages.push(package);
                }
            }
        }
    }

    xtask_dev_dependency_packages.sort();
    Ok(RustAnalyzerWorkspaceContractSummary {
        xtask_dev_dependency_count: xtask_dev_dependency_packages.len(),
        xtask_dev_dependency_packages,
    })
}

fn rust_analyzer_manifest_walk_entry(root: &Path, path: &Path) -> bool {
    if path == root {
        return true;
    }
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return true;
    };
    !matches!(
        name,
        ".git" | ".direnv" | ".sinex" | "target" | "node_modules"
    ) && !name.starts_with("result")
}

fn manifest_has_xtask_dev_dependency(manifest: &toml::Value) -> bool {
    toml_at(manifest, &["dev-dependencies", "xtask"]).is_some()
        || manifest
            .get("target")
            .and_then(toml::Value::as_table)
            .is_some_and(|targets| {
                targets
                    .values()
                    .any(|target| toml_at(target, &["dev-dependencies", "xtask"]).is_some())
            })
}

fn is_rust_analyzer_process(comm: &str, command: &str) -> bool {
    if comm.trim() == "rust-analyzer" {
        return true;
    }
    command
        .split_whitespace()
        .next()
        .and_then(|arg0| Path::new(arg0).file_name())
        .and_then(|name| name.to_str())
        == Some("rust-analyzer")
}

fn read_proc_cmdline(proc_dir: &Path) -> Option<String> {
    let raw = std::fs::read(proc_dir.join("cmdline")).ok()?;
    if raw.is_empty() {
        return None;
    }
    Some(
        raw.split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part))
            .collect::<Vec<_>>()
            .join(" "),
    )
}

fn read_proc_rss_mb(proc_dir: &Path) -> Option<f64> {
    let raw = std::fs::read_to_string(proc_dir.join("status")).ok()?;
    let line = raw.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kb = line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse::<f64>().ok())?;
    Some(kb / 1024.0)
}

fn run_rust_analyzer_cli_diagnostics(
    root: &Path,
    severity: &str,
) -> Result<RustAnalyzerCliDiagnosticScan> {
    let command = vec![
        "rust-analyzer".to_string(),
        "diagnostics".to_string(),
        root.display().to_string(),
        "--disable-build-scripts".to_string(),
        "--severity".to_string(),
        severity.to_string(),
    ];
    let output = std::process::Command::new("rust-analyzer")
        .args([
            "diagnostics",
            &root.display().to_string(),
            "--disable-build-scripts",
            "--severity",
            severity,
        ])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .wrap_err("spawn rust-analyzer diagnostics")?;
    Ok(build_rust_analyzer_cli_diagnostic_scan(
        command,
        output.status.code(),
        &String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr).trim(),
    ))
}

fn build_rust_analyzer_cli_diagnostic_scan(
    command: Vec<String>,
    exit_code: Option<i32>,
    stdout: &str,
    raw_stderr: &str,
) -> RustAnalyzerCliDiagnosticScan {
    let diagnostics = parse_rust_analyzer_cli_diagnostics(stdout);
    let status = rust_analyzer_cli_scan_status(exit_code, diagnostics.len());
    let stderr_summary = summarize_rust_analyzer_cli_stderr(raw_stderr);
    let stderr = truncate_report_text(raw_stderr, 4096);
    RustAnalyzerCliDiagnosticScan {
        command,
        exit_code,
        status,
        stderr_summary,
        diagnostics,
        stderr: (!stderr.is_empty()).then_some(stderr),
    }
}

const fn rust_analyzer_cli_scan_status(
    exit_code: Option<i32>,
    diagnostic_count: usize,
) -> &'static str {
    match (exit_code, diagnostic_count) {
        (Some(0), 0) => "clean",
        (Some(0), _) => "diagnostics",
        (_, 0) => "failed",
        (_, _) => "partial",
    }
}

fn summarize_rust_analyzer_cli_stderr(stderr: &str) -> Option<RustAnalyzerCliStderrSummary> {
    let mut cyclic_dependency_warnings = 0;
    let mut cyclic_dependency_edges = Vec::new();
    let mut internal_errors = 0;
    let mut internal_error_kinds = Vec::new();
    let mut other_warnings = 0;
    let mut other_warning_samples = Vec::new();
    let mut other_errors = 0;
    let mut other_error_samples = Vec::new();

    for line in stderr.lines() {
        if line.contains(" WARN cyclic deps:") {
            cyclic_dependency_warnings += 1;
            if let Some(edge) = parse_rust_analyzer_cyclic_dependency_edge(line) {
                push_unique_capped(&mut cyclic_dependency_edges, edge, 16);
            }
        } else if line.contains(" WARN ") {
            other_warnings += 1;
            push_unique_capped(&mut other_warning_samples, line.to_string(), 3);
        } else if line.contains(" ERROR ") && rust_analyzer_internal_error_line(line) {
            internal_errors += 1;
            if let Some(kind) = rust_analyzer_internal_error_kind(line)
                && !internal_error_kinds.contains(&kind)
            {
                internal_error_kinds.push(kind);
            }
        } else if line.contains(" ERROR ") {
            other_errors += 1;
            push_unique_capped(&mut other_error_samples, line.to_string(), 3);
        }
    }

    cyclic_dependency_edges.sort();
    let self_cycle_edges =
        classify_rust_analyzer_cycle_edges(&cyclic_dependency_edges, |from, to| from == to);
    let xtask_cycle_edges =
        classify_rust_analyzer_cycle_edges(&cyclic_dependency_edges, |from, to| {
            from == "xtask" || to == "xtask"
        });
    let workspace_cycle_edges =
        classify_rust_analyzer_cycle_edges(&cyclic_dependency_edges, |from, to| {
            from != to && from != "xtask" && to != "xtask"
        });
    internal_error_kinds.sort_unstable();

    let mut categories = Vec::new();
    if cyclic_dependency_warnings > 0 {
        categories.push("cyclic_dependencies");
    }
    if internal_errors > 0 {
        categories.push("rust_analyzer_internal_errors");
    }
    if other_warnings > 0 {
        categories.push("other_warnings");
    }
    if other_errors > 0 {
        categories.push("other_errors");
    }
    let remediation_actions = rust_analyzer_remediation_actions(
        &self_cycle_edges,
        &xtask_cycle_edges,
        &workspace_cycle_edges,
        internal_errors,
        other_warnings,
        other_errors,
    );

    (!categories.is_empty()).then_some(RustAnalyzerCliStderrSummary {
        categories,
        remediation_actions,
        cyclic_dependency_warnings,
        cyclic_dependency_edges,
        self_cycle_edges,
        xtask_cycle_edges,
        workspace_cycle_edges,
        internal_errors,
        internal_error_kinds,
        other_warnings,
        other_warning_samples,
        other_errors,
        other_error_samples,
    })
}

fn rust_analyzer_remediation_actions(
    self_cycle_edges: &[String],
    xtask_cycle_edges: &[String],
    workspace_cycle_edges: &[String],
    internal_errors: usize,
    other_warnings: usize,
    other_errors: usize,
) -> Vec<&'static str> {
    let mut actions = Vec::new();
    if !xtask_cycle_edges.is_empty() {
        actions.push(RA_ACTION_EXTRACT_XTASK_SANDBOX);
    }
    if !workspace_cycle_edges.is_empty() {
        actions.push(RA_ACTION_BREAK_WORKSPACE_CYCLES);
    }
    if !self_cycle_edges.is_empty() {
        actions.push(RA_ACTION_INSPECT_SELF_CYCLES);
    }
    if internal_errors > 0 {
        actions.push(RA_ACTION_CAPTURE_UPSTREAM_REPRO);
    }
    if other_warnings > 0 || other_errors > 0 {
        actions.push(RA_ACTION_CLASSIFY_UNCATEGORIZED_STDERR);
    }
    actions
}

fn classify_rust_analyzer_cycle_edges(
    edges: &[String],
    include: impl Fn(&str, &str) -> bool,
) -> Vec<String> {
    edges
        .iter()
        .filter_map(|edge| {
            let (from, to) = edge.split_once("->")?;
            include(from, to).then(|| edge.clone())
        })
        .collect()
}

fn rust_analyzer_internal_error_line(line: &str) -> bool {
    rust_analyzer_internal_error_kind(line).is_some()
}

fn rust_analyzer_internal_error_kind(line: &str) -> Option<&'static str> {
    if line.contains("pattern has unexpected type") {
        Some("unexpected_pattern_type")
    } else if line.contains("Overloaded deref on type") {
        Some("overloaded_deref")
    } else {
        None
    }
}

fn parse_rust_analyzer_cyclic_dependency_edge(line: &str) -> Option<String> {
    let (_, raw_edge) = line.split_once(" WARN cyclic deps: ")?;
    let (from, to) = raw_edge.split_once(" -> ")?;
    Some(format!(
        "{}->{}",
        rust_analyzer_crate_name_from_cycle_node(from)?,
        rust_analyzer_crate_name_from_cycle_node(to)?
    ))
}

fn rust_analyzer_crate_name_from_cycle_node(node: &str) -> Option<&str> {
    let name = node.split_once('(').map_or(node, |(name, _)| name).trim();
    (!name.is_empty()).then_some(name)
}

fn push_unique_capped<T>(items: &mut Vec<T>, item: T, cap: usize)
where
    T: PartialEq,
{
    if items.len() < cap && !items.contains(&item) {
        items.push(item);
    }
}

fn parse_rust_analyzer_cli_diagnostics(output: &str) -> Vec<RustAnalyzerCliDiagnostic> {
    output
        .lines()
        .filter_map(parse_rust_analyzer_cli_diagnostic_line)
        .collect()
}

fn parse_rust_analyzer_cli_diagnostic_line(line: &str) -> Option<RustAnalyzerCliDiagnostic> {
    let line = line.trim_matches(|ch: char| ch.is_control() || ch.is_whitespace());
    let line = line.strip_prefix("at crate ")?;
    let (crate_name, rest) = line.split_once(", file ")?;
    let (file, rest) = rest.split_once(": ")?;
    let (severity, rest) = rest.split_once(' ')?;
    let (diagnostic_kind, rest) = rest.split_once(" from LineCol { ")?;
    let (start, rest) = rest.split_once(" } to LineCol { ")?;
    let (end, message) = rest.split_once(" }: ")?;
    let (line, col) = parse_line_col(start)?;
    let (end_line, end_col) = parse_line_col(end)?;

    Some(RustAnalyzerCliDiagnostic {
        crate_name: crate_name.to_string(),
        file: file.to_string(),
        severity: severity.to_string(),
        diagnostic_kind: diagnostic_kind.to_string(),
        line,
        col,
        end_line,
        end_col,
        message: message.to_string(),
    })
}

fn parse_line_col(value: &str) -> Option<(u32, u32)> {
    let mut line = None;
    let mut col = None;
    for part in value.split(',') {
        let (key, raw_value) = part.trim().split_once(": ")?;
        match key {
            "line" => line = raw_value.parse().ok(),
            "col" => col = raw_value.parse().ok(),
            _ => {}
        }
    }
    Some((line?, col?))
}

fn rust_analyzer_diagnostic_to_compiler_diagnostic(
    diagnostic: &RustAnalyzerCliDiagnostic,
) -> CompilerDiagnostic {
    CompilerDiagnostic {
        level: diagnostic.severity.to_lowercase(),
        code: Some(diagnostic.diagnostic_kind.clone()),
        message: diagnostic.message.clone(),
        file_path: Some(diagnostic.file.clone()),
        line: Some(diagnostic.line + 1),
        column: Some(diagnostic.col + 1),
        rendered: Some(format!(
            "{}:{}:{}: {}: {} [{}]",
            diagnostic.file,
            diagnostic.line + 1,
            diagnostic.col + 1,
            diagnostic.severity.to_lowercase(),
            diagnostic.message,
            diagnostic.diagnostic_kind
        )),
        package: Some(diagnostic.crate_name.clone()),
        ..CompilerDiagnostic::default()
    }
}

fn truncate_report_text(value: &str, max_len: usize) -> String {
    if value.len() <= max_len {
        return value.to_string();
    }

    let mut end = 0;
    for (idx, _) in value.char_indices() {
        if idx > max_len {
            break;
        }
        end = idx;
    }
    format!("{}... [truncated]", &value[..end])
}

/// Run diagnostics.
fn execute_doctor(pipelines: bool, ctx: &CommandContext) -> Result<CommandResult> {
    let mut all_ok = true;
    let cfg = config();

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

    let private_mode = private_mode_check(&cfg.state_dir, &mut all_ok);

    // Collect environment configuration
    let environment = Some(serde_json::json!({
        "hostname": cfg.hostname,
        "state_dir": cfg.state_dir.display().to_string(),
        "cache_dir": cfg.cache_dir.display().to_string(),
        "database_url": cfg.database_url.as_deref().map(redact_database_url_password),
        "nats_url": cfg.nats_url,
        "gateway_url": cfg.gateway_url,
        "test_results_dir": cfg.test_results_dir.as_ref().map(|p| p.display().to_string()),
        "test_tmp_dir": cfg.test_tmp_dir.as_ref().map(|p| p.display().to_string()),
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
        private_mode,
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
        print_check(
            "Private mode",
            report.private_mode.available,
            report.private_mode.message.as_deref(),
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
            print_env_field(env_data, "test_tmp_dir", "Test temp:");
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

fn private_mode_check(state_dir: &Path, all_ok: &mut bool) -> DoctorServiceCheck {
    match load_private_mode_state(state_dir) {
        Ok(state) if state.enabled => {
            let scope = if state.affected_source_classes.is_empty() {
                "all source classes".to_string()
            } else {
                state.affected_source_classes.join(",")
            };
            DoctorServiceCheck {
                available: true,
                message: Some(format!(
                    "enabled for {scope}; actor={}, reason={}",
                    state.actor, state.reason_class
                )),
            }
        }
        Ok(_) => DoctorServiceCheck {
            available: true,
            message: Some("disabled".to_string()),
        },
        Err(error) => {
            *all_ok = false;
            DoctorServiceCheck {
                available: false,
                message: Some(format!("private-mode state unavailable: {error}")),
            }
        }
    }
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

fn print_test_db_report(report: &TestDbDoctorReport) {
    let totals = &report.footprint.totals;
    println!("\n{}", style("Test Database Footprint:").bold());
    println!(
        "  Configured pool:     {} slots × {} conns (+{} admin conns)",
        report.footprint.configured_pool_size,
        report.footprint.slot_max_connections,
        report.footprint.admin_max_connections,
    );
    println!(
        "  Existing databases:  {} total ({} pool slots, {} shared templates, {} adhoc templates, {} stale/legacy)",
        totals.database_count,
        totals.pool_slot_count,
        totals.shared_template_count,
        totals.adhoc_template_count,
        totals.stale_or_legacy_count,
    );
    println!(
        "  Disk footprint:      {:.1} MB total ({:.1} MB pool, {:.1} MB templates)",
        totals.total_size_bytes as f64 / 1_048_576.0,
        totals.pool_slot_size_bytes as f64 / 1_048_576.0,
        totals.template_size_bytes as f64 / 1_048_576.0,
    );
    println!(
        "  Process-local pool:  {} initialized slots, {} open conns",
        report.footprint.process_pool_slots, report.footprint.process_pool_stats.total_connections,
    );
    if let Some(shm) = &report.dev_shm {
        println!(
            "  /dev/shm now:        {:.0} MB used, {:.0} MB free",
            shm.used_mb, shm.free_mb
        );
    } else {
        println!("  /dev/shm now:        unavailable");
    }
    if totals.stale_or_legacy_count > 0 {
        println!(
            "  {} {} stale/legacy sinex_test_* databases are visible; inspect before dropping.",
            style("⚠").yellow(),
            totals.stale_or_legacy_count
        );
    }
}

fn print_rust_analyzer_report(report: &RustAnalyzerDoctorReport) {
    println!("\n{}", style("Rust Analyzer:").bold());
    println!(
        "  Authority:          {} ({})",
        report.diagnostic_role,
        if report.proof_authority {
            "proof-producing"
        } else {
            "not proof"
        }
    );
    println!(
        "  Config:             {} ({})",
        report.config_path,
        if report.config_present {
            "present"
        } else {
            "missing"
        }
    );
    println!("  Target dir:         {}", report.target_dir);
    if let Some(config) = &report.config {
        if config.parse_ok {
            println!(
                "  Contract:           numThreads={:?}, allTargets={:?}, check.workspace={:?}, lru={:?}",
                config.num_threads,
                config.cargo_all_targets,
                config.check_workspace,
                config.lru_capacity
            );
        } else if let Some(error) = config.parse_error.as_deref() {
            println!("  Contract:           parse failed ({error})");
        }
    }
    println!(
        "  Processes:          {} ({:.0} MB RSS total)",
        report.process_count, report.total_rss_mb
    );
    println!(
        "  Xtask dev-deps:     {} package(s){}",
        report.workspace_contract.xtask_dev_dependency_count,
        if report
            .workspace_contract
            .xtask_dev_dependency_packages
            .is_empty()
        {
            String::new()
        } else {
            format!(
                " [{}]",
                report
                    .workspace_contract
                    .xtask_dev_dependency_packages
                    .join(", ")
            )
        }
    );
    for process in &report.processes {
        println!(
            "    pid {:<8} {:>7.0} MB  {}",
            process.pid, process.rss_mb, process.command
        );
    }
    if let Some(scan) = &report.cli_diagnostics {
        println!(
            "  CLI diagnostics:   {} diagnostic(s), exit {:?}, {}",
            scan.diagnostics.len(),
            scan.exit_code,
            scan.status
        );
        if let Some(summary) = &scan.stderr_summary {
            println!(
                "  RA stderr:         categories [{}], cyclic {}, internal {}, other warn {}, other error {}",
                summary.categories.join(", "),
                summary.cyclic_dependency_warnings,
                summary.internal_errors,
                summary.other_warnings,
                summary.other_errors
            );
            if !summary.cyclic_dependency_edges.is_empty() {
                println!(
                    "  RA cycle edges:    {}",
                    summary.cyclic_dependency_edges.join(", ")
                );
                println!(
                    "  RA cycle buckets:  self [{}], xtask [{}], workspace [{}]",
                    summary.self_cycle_edges.join(", "),
                    summary.xtask_cycle_edges.join(", "),
                    summary.workspace_cycle_edges.join(", ")
                );
            }
            if !summary.internal_error_kinds.is_empty() {
                println!(
                    "  RA internal kinds: {}",
                    summary.internal_error_kinds.join(", ")
                );
            }
            if !summary.remediation_actions.is_empty() {
                println!(
                    "  RA actions:        {}",
                    summary.remediation_actions.join("; ")
                );
            }
        }
        for diagnostic in scan.diagnostics.iter().take(5) {
            println!(
                "    {}:{}:{} {} {}",
                diagnostic.file,
                diagnostic.line + 1,
                diagnostic.col + 1,
                diagnostic.severity,
                diagnostic.message
            );
        }
        if let Some(recorded) = report.history_recorded_diagnostics {
            println!("  History:           recorded {recorded} RA diagnostic(s)");
        }
    }
    for warning in &report.warnings {
        println!("  {} {warning}", style("⚠").yellow());
    }
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

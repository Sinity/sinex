//! Doctor command - health check for Postgres, NATS, tools, and TLS

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::tools::{ToolInfo, ToolManager};
use color_eyre::eyre::Result;
use console::style;
use serde::Serialize;

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
        let result = execute_doctor(self.pipelines, ctx)?;

        if self.runtime {
            execute_runtime_check(ctx).await?;
        }

        if self.fix {
            crate::preflight::invalidate_cache();
            if ctx.is_human() {
                println!("Invalidated preflight cache");
            }

            // Check infra status and restart if needed
            let pg_ready = std::process::Command::new("pg_isready")
                .arg("-q")
                .status()
                .is_ok_and(|s| s.success());
            let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(4222);
            let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();

            if !pg_ready || !nats_ready {
                let stack_config = crate::infra::stack::StackConfig::for_current_checkout().ok();
                if let Some(cfg) = stack_config {
                    let verbose = ctx.is_human();
                    if !pg_ready {
                        let _ = crate::infra::stack::pg_start(&cfg, verbose);
                    }
                    if !nats_ready {
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
    let pg_ready = std::process::Command::new("pg_isready")
        .arg("-q")
        .status()
        .is_ok_and(|s| s.success());
    let pg_msg = if pg_ready {
        None
    } else {
        all_ok = false;
        Some("pg_isready failed - is Postgres running?".to_string())
    };

    // Check NATS
    let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
        .ok()
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(4222);
    let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{nats_port}")).is_ok();
    let nats_msg = if nats_ready {
        None
    } else {
        all_ok = false;
        Some(format!("Cannot connect to NATS on port {nats_port}"))
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
    if pg_ready {
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
        "test_results_dir": cfg.test_results_dir.as_ref().map(|p| p.display().to_string()),
        "toolchain": cfg.toolchain,
        "in_devenv": cfg.in_devenv,
    }));

    let report = DoctorReport {
        postgres: DoctorServiceCheck {
            available: pg_ready,
            message: pg_msg,
        },
        nats: DoctorServiceCheck {
            available: nats_ready,
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

    Ok(CommandResult::success()
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

async fn execute_runtime_check(ctx: &CommandContext) -> Result<()> {
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
            return Ok(());
        }
    };

    let metrics = query_runtime_metrics(&db_url).await;

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
        if let Some(lag) = metrics.consumer_lag_pending {
            let lag_icon = if lag > 1000.0 {
                style("⚠").yellow()
            } else {
                style("✓").green()
            };
            println!("  {} Consumer lag:       {:.0} pending", lag_icon, lag);
        }

        // Batch latency
        if let Some(latency) = metrics.last_batch_latency_ms {
            let lat_icon = if latency > 5000.0 {
                style("⚠").yellow()
            } else {
                style("✓").green()
            };
            println!("  {} Batch latency:      {:.0}ms", lat_icon, latency);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

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
}

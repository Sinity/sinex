/*!
 * Sinex Pre-Flight Verification System
 *
 * Comprehensive system-level verification that must pass before any service deployment.
 * This implements the Pre-Flight Verification Model for zero-downtime, safe deployments.
 */

use camino::Utf8PathBuf;
use clap::{Parser, Subcommand};
use color_eyre::eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use sinex_core::coordination::kv_client::{CoordinationKvClient, InstanceMetadata};
use sinex_core::nats::NatsConnectionConfig;
use sinex_core::types::domain::EventSource;
use sinex_core::types::Seconds;
use sinex_core::DbPoolExt;
use sinex_core::{Event, JsonValue};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use uuid::Uuid;

use sinex_node_sdk::preflight::{
    configuration, database, resources, services, verification, VerificationStatus,
};

#[derive(Parser)]
#[command(name = "sinex-preflight")]
#[command(about = "Sinex Pre-Flight Verification System")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Configuration file path
    #[arg(short, long)]
    config: Option<Utf8PathBuf>,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Output format (json, text)
    #[arg(short, long, default_value = "text")]
    output: OutputFormat,
}

#[derive(Subcommand)]
enum Commands {
    /// Run complete system verification
    Verify {
        /// Timeout for verification (seconds)
        #[arg(short, long, default_value = "120")]
        timeout: Seconds,

        /// Skip specific verification phases
        #[arg(short, long)]
        skip: Vec<VerificationPhase>,
    },

    /// Run migration dry-run only
    MigrationDryRun,

    /// Check database extensions only
    ExtensionCheck,

    /// Verify resource capacity only
    ResourceCheck,

    /// Generate verification report
    Report {
        /// Include detailed diagnostics
        #[arg(short, long)]
        detailed: bool,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
enum OutputFormat {
    Json,
    Text,
}

#[derive(Clone, Debug, Serialize, Deserialize, clap::ValueEnum, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
enum VerificationPhase {
    Database,
    Extensions,
    Migrations,
    Resources,
    Configuration,
    Services,
    Integration,
}

#[derive(Debug, Serialize)]
struct VerificationReport {
    overall_status: VerificationStatus,
    verification_id: Uuid,
    started_at: chrono::DateTime<chrono::Utc>,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
    duration_ms: Option<u64>,
    phases: HashMap<VerificationPhase, PhaseResult>,
    system_info: SystemInfo,
    warnings: Vec<String>,
    errors: Vec<String>,
}

// Using VerificationStatus from sinex_node_sdk::preflight module

#[derive(Debug, Serialize)]
struct PhaseResult {
    status: VerificationStatus,
    duration_ms: u64,
    details: serde_json::Value,
    messages: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SystemInfo {
    hostname: String,
    uptime_seconds: u64,
    available_memory_gb: f64,
    available_disk_gb: f64,
    cpu_count: usize,
    load_average: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(format!("sinex_preflight={log_level}"))
        .with_target(false)
        .json()
        .init();

    info!("Sinex Pre-Flight Verification System starting");

    let result = match cli.command {
        Commands::Verify { timeout, skip } => {
            run_complete_verification(timeout, skip, cli.output).await
        }
        Commands::MigrationDryRun => run_migration_dry_run(cli.output).await,
        Commands::ExtensionCheck => run_extension_check(cli.output).await,
        Commands::ResourceCheck => run_resource_check(cli.output).await,
        Commands::Report { detailed } => generate_verification_report(detailed, cli.output).await,
    };

    match result {
        Ok(status) => {
            if matches!(status, VerificationStatus::Pass) {
                info!("✓ Pre-flight verification PASSED");
                std::process::exit(0);
            } else {
                error!("✗ Pre-flight verification FAILED");
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("Pre-flight verification error: {}", e);
            std::process::exit(2);
        }
    }
}

async fn run_complete_verification(
    timeout_secs: Seconds,
    skip_phases: Vec<VerificationPhase>,
    output_format: OutputFormat,
) -> Result<VerificationStatus> {
    let start_time = Instant::now();
    let timeout = Duration::from_secs(timeout_secs.as_secs());

    let mut report = VerificationReport {
        overall_status: VerificationStatus::Pass, // Initial state
        verification_id: Uuid::new_v4(),
        started_at: chrono::Utc::now(),
        completed_at: None,
        duration_ms: None,
        phases: HashMap::new(),
        system_info: collect_system_info().await?,
        warnings: Vec::new(),
        errors: Vec::new(),
    };

    info!(
        "Starting comprehensive system verification (ID: {})",
        report.verification_id
    );

    // Define verification phases in dependency order
    let phases = vec![
        VerificationPhase::Database,
        VerificationPhase::Extensions,
        VerificationPhase::Migrations,
        VerificationPhase::Configuration,
        VerificationPhase::Resources,
        VerificationPhase::Services,
        VerificationPhase::Integration,
    ];

    let mut overall_status = VerificationStatus::Pass;
    let deadline = start_time + timeout;

    for phase in phases {
        if skip_phases.contains(&phase) {
            info!("Skipping verification phase: {:?}", phase);
            continue;
        }

        let now = Instant::now();
        if now >= deadline {
            report
                .errors
                .push("Verification timeout exceeded".to_string());
            overall_status = VerificationStatus::Fail;
            break;
        }

        let phase_start = Instant::now();
        info!("Running verification phase: {:?}", phase);
        let remaining = deadline.saturating_duration_since(now);
        let phase_result = match tokio::time::timeout(remaining, run_verification_phase(&phase))
            .await
        {
            Ok(outcome) => match outcome {
                Ok(result) => result,
                Err(e) => {
                    error!("Phase {:?} failed: {}", phase, e);
                    report.errors.push(format!("Phase {:?}: {}", phase, e));
                    PhaseResult {
                        status: VerificationStatus::Fail,
                        duration_ms: phase_start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        details: serde_json::json!({"error": e.to_string()}),
                        messages: vec![e.to_string()],
                    }
                }
            },
            Err(_) => {
                let timeout_message = format!("Phase {:?} timed out after {:?}", phase, remaining);
                error!(timeout_message);
                report.errors.push(timeout_message.clone());
                PhaseResult {
                    status: VerificationStatus::Fail,
                    duration_ms: phase_start.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    details: serde_json::json!({"error": "timeout"}),
                    messages: vec!["Phase exceeded allotted time".to_string(), timeout_message],
                }
            }
        };

        // Update overall status based on phase result
        match phase_result.status {
            VerificationStatus::Fail => overall_status = VerificationStatus::Fail,
            VerificationStatus::Warning if matches!(overall_status, VerificationStatus::Pass) => {
                overall_status = VerificationStatus::Warning;
            }
            _ => {}
        }

        report.phases.insert(phase, phase_result);

        // Fail fast on critical failures
        if matches!(overall_status, VerificationStatus::Fail) {
            error!("Critical verification failure, aborting remaining phases");
            break;
        }
    }

    report.overall_status = overall_status.clone();
    report.completed_at = Some(chrono::Utc::now());
    report.duration_ms = Some(start_time.elapsed().as_millis().min(u64::MAX as u128) as u64);

    // Output report
    output_report(&report, output_format).await?;

    // Record verification in database for monitoring
    if let Err(e) = record_verification_result(&report).await {
        warn!("Failed to record verification result: {}", e);
    }

    Ok(overall_status)
}

async fn run_verification_phase(phase: &VerificationPhase) -> Result<PhaseResult> {
    let start = Instant::now();

    let (status, details, messages) = match phase {
        VerificationPhase::Database => database::verify_database_connectivity().await?,
        VerificationPhase::Extensions => database::verify_postgresql_extensions().await?,
        VerificationPhase::Migrations => database::verify_migration_readiness().await?,
        VerificationPhase::Configuration => {
            configuration::verify_configuration_generation().await?
        }
        VerificationPhase::Resources => resources::verify_system_resources().await?,
        VerificationPhase::Services => services::verify_service_dependencies().await?,
        VerificationPhase::Integration => verification::verify_end_to_end_integration().await?,
    };

    Ok(PhaseResult {
        status,
        duration_ms: start.elapsed().as_millis().min(u64::MAX as u128) as u64,
        details,
        messages,
    })
}

async fn collect_system_info() -> Result<SystemInfo> {
    use sysinfo::System;

    let mut sys = System::new_all();
    sys.refresh_all();

    Ok(SystemInfo {
        hostname: gethostname::gethostname().to_string_lossy().to_string(),
        uptime_seconds: System::uptime(),
        available_memory_gb: sys.available_memory() as f64 / 1024.0 / 1024.0 / 1024.0,
        available_disk_gb: get_available_disk_space()?,
        cpu_count: sys.cpus().len(),
        load_average: System::load_average().one,
    })
}

fn get_available_disk_space() -> Result<f64> {
    use nix::sys::statvfs::statvfs;
    use std::env;

    // Allow configurable data directory for disk space checks
    let data_dir = env::var("SINEX_DATA_DIR")
        .or_else(|_| env::var("XDG_DATA_HOME").map(|d| format!("{}/sinex", d)))
        .unwrap_or_else(|_| "/var/lib/sinex".to_string());

    let stat = statvfs(data_dir.as_str())?;
    let available_bytes = stat.blocks_available() * stat.block_size();
    Ok(available_bytes as f64 / 1024.0 / 1024.0 / 1024.0)
}

async fn output_report(report: &VerificationReport, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
        OutputFormat::Text => {
            println!("\n=== SINEX PRE-FLIGHT VERIFICATION REPORT ===");
            println!("Verification ID: {}", report.verification_id);
            println!("Overall Status: {:?}", report.overall_status);

            if let Some(duration) = report.duration_ms {
                println!("Duration: {}ms", duration);
            }

            println!("\nSystem Information:");
            println!("  Hostname: {}", report.system_info.hostname);
            println!(
                "  Available Memory: {:.2} GB",
                report.system_info.available_memory_gb
            );
            println!(
                "  Available Disk: {:.2} GB",
                report.system_info.available_disk_gb
            );
            println!("  CPU Count: {}", report.system_info.cpu_count);
            println!("  Load Average: {:.2}", report.system_info.load_average);

            println!("\nVerification Phases:");
            for (phase, result) in &report.phases {
                println!(
                    "  {:?}: {:?} ({}ms)",
                    phase, result.status, result.duration_ms
                );
                for message in &result.messages {
                    println!("    {}", message);
                }
            }

            if !report.warnings.is_empty() {
                println!("\nWarnings:");
                for warning in &report.warnings {
                    println!("  ⚠ {}", warning);
                }
            }

            if !report.errors.is_empty() {
                println!("\nErrors:");
                for error in &report.errors {
                    println!("  ✗ {}", error);
                }
            }
        }
    }

    Ok(())
}

async fn record_verification_result(report: &VerificationReport) -> Result<()> {
    let status_str = match report.overall_status {
        VerificationStatus::Pass => "healthy",
        VerificationStatus::Fail => "failed",
        VerificationStatus::Warning => "degraded",
    };

    let nats_config = NatsConnectionConfig::from_env();
    let nats_client = nats_config
        .connect()
        .await
        .wrap_err("Failed to connect to NATS for verification recording")?;
    let js = async_nats::jetstream::new(nats_client);
    ensure_coordination_buckets(&js).await?;

    let kv_client = CoordinationKvClient::new(js, "sinex-preflight".to_string());
    let instance_id = Uuid::new_v4().to_string();
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let now = chrono::Utc::now().timestamp();
    let metadata = InstanceMetadata {
        instance_id: instance_id.clone(),
        hostname,
        version: env!("CARGO_PKG_VERSION").to_string(),
        started_at: now,
        last_heartbeat: now,
    };

    if kv_client.acquire_leadership(&instance_id).await? {
        kv_client
            .register_instance(&metadata)
            .await
            .wrap_err("Failed to register verification metadata in KV")?;
        kv_client.heartbeat(&instance_id, &metadata).await.ok(); // heartbeat best-effort

        info!(
            service_name = "sinex-preflight",
            instance_id = %instance_id,
            verification_status = %status_str,
            errors_count = report.errors.len(),
            warnings_count = report.warnings.len(),
            verification_result = ?report.overall_status,
            "System preflight verification completed via KV coordination"
        );

        kv_client.release_leadership(&instance_id).await?;
    } else {
        warn!(
            "Another preflight verification is already running - skipping duplicate verification"
        );
    }

    Ok(())
}

async fn run_migration_dry_run(output_format: OutputFormat) -> Result<VerificationStatus> {
    info!("Running migration dry-run verification");

    let (status, details, messages) = database::verify_migration_readiness().await?;

    let report = serde_json::json!({
        "phase": "migration_dry_run",
        "status": status,
        "details": details,
        "messages": messages
    });

    match output_format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Text => {
            println!("Migration Dry-Run: {:?}", status);
            for message in messages {
                println!("  {}", message);
            }
        }
    }

    Ok(status)
}

async fn run_extension_check(output_format: OutputFormat) -> Result<VerificationStatus> {
    info!("Running PostgreSQL extension verification");

    let (status, details, messages) = database::verify_postgresql_extensions().await?;

    let report = serde_json::json!({
        "phase": "extension_check",
        "status": status,
        "details": details,
        "messages": messages
    });

    match output_format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Text => {
            println!("Extension Check: {:?}", status);
            for message in messages {
                println!("  {}", message);
            }
        }
    }

    Ok(status)
}

async fn run_resource_check(output_format: OutputFormat) -> Result<VerificationStatus> {
    info!("Running system resource verification");

    let (status, details, messages) = resources::verify_system_resources().await?;

    let report = serde_json::json!({
        "phase": "resource_check",
        "status": status,
        "details": details,
        "messages": messages
    });

    match output_format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Text => {
            println!("Resource Check: {:?}", status);
            for message in messages {
                println!("  {}", message);
            }
        }
    }

    Ok(status)
}

async fn generate_verification_report(
    detailed: bool,
    output_format: OutputFormat,
) -> Result<VerificationStatus> {
    info!("Generating verification report");

    // This would query the database for recent verification results
    // and generate a comprehensive report
    let database_url = sinex_node_sdk::preflight::resolve_database_url()?;

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .wrap_err("Failed to connect to database")?;

    let end_time = chrono::Utc::now();
    let start_time = end_time - chrono::Duration::hours(24);

    let recent_verifications: Vec<Event<JsonValue>> = pool
        .events()
        .get_process_heartbeats(&EventSource::new("sinex-preflight"), start_time, end_time)
        .await
        .wrap_err("Failed to fetch verification history")?;

    let report = if detailed {
        serde_json::json!({
            "verification_count": recent_verifications.len(),
            "latest_status": recent_verifications.first().map(|v| v.payload.get("health_status")),
            "system_info": collect_system_info().await?
        })
    } else {
        serde_json::json!({
            "verification_count": recent_verifications.len(),
            "latest_status": recent_verifications.first().map(|v| v.payload.get("health_status"))
        })
    };

    match output_format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Text => {
            println!("Recent Verification History:");
            for verification in &recent_verifications {
                let ts_str = verification
                    .ts_orig
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "UNKNOWN_TIME".to_string());
                println!(
                    "  {} - {} ({})",
                    ts_str,
                    verification
                        .payload
                        .get("health_status")
                        .and_then(|v| v.as_str())
                        .unwrap_or("UNKNOWN"),
                    verification
                        .id
                        .as_ref()
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "NO_ID".to_string())
                );
            }
        }
    }

    Ok(VerificationStatus::Pass)
}

async fn ensure_coordination_buckets(js: &async_nats::jetstream::Context) -> Result<()> {
    const LEADERSHIP_TTL_SECS: Seconds = Seconds::from_secs(15);

    let _ = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "KV_sinex_instances".to_string(),
            history: 1,
            ..Default::default()
        })
        .await
        .ok();

    let _ = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "KV_sinex_leadership".to_string(),
            history: 5,
            max_age: Duration::from_secs(LEADERSHIP_TTL_SECS.as_secs()),
            ..Default::default()
        })
        .await
        .ok();

    Ok(())
}

//! Schema management tool for EventPayload types
//!
//! This tool manages JSON schemas for EventPayload types:
//! - Generates schemas from Rust types
//! - Syncs schemas to the database
//! - Validates schema compatibility
//! - Exports schemas for external use

use color_eyre::eyre::Result;
use clap::{Parser, Subcommand};
use schemars::schema_for;
use serde_json::Value;
use sinex_types::events::payloads::*;
use sinex_types::events::EventPayload;
use sinex_types::Ulid;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Database URL
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate schemas from EventPayload types
    Generate {
        /// Output directory for schema files
        #[arg(short, long, default_value = "schemas/v1")]
        output: String,

        /// Also sync to database
        #[arg(short, long)]
        sync: bool,
    },

    /// Sync schemas to database
    Sync {
        /// Directory containing schema files
        #[arg(short, long, default_value = "schemas/v1")]
        input: String,
    },

    /// List all schemas in database
    List {
        /// Show only active schemas
        #[arg(short, long)]
        active_only: bool,
    },

    /// Validate schema compatibility
    Validate {
        /// From schema name
        from: String,

        /// To schema name
        to: String,
    },
}

// Macro to register all payload types
macro_rules! register_payloads {
    ($($module:ident :: $payload:ident),* $(,)?) => {{
        let mut schemas = HashMap::new();
        $(
            let schema = schema_for!($module::$payload);
            let source_const = <$module::$payload as EventPayload>::SOURCE;
            let event_type_const = <$module::$payload as EventPayload>::EVENT_TYPE;
            let source = source_const.as_str();
            let event_type = event_type_const.as_str();
            let schema_name = format!("{}.{}", source, event_type);
            schemas.insert(schema_name, serde_json::to_value(schema)?);
        )*
        schemas
    }};
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let pool = PgPool::connect(&cli.database_url).await?;

    match cli.command {
        Commands::Generate { output, sync } => {
            generate_schemas(&pool, &output, sync).await?;
        }
        Commands::Sync { input } => {
            sync_schemas(&pool, &input).await?;
        }
        Commands::List { active_only } => {
            list_schemas(&pool, active_only).await?;
        }
        Commands::Validate { from, to } => {
            validate_compatibility(&pool, &from, &to).await?;
        }
    }

    Ok(())
}

async fn generate_schemas(pool: &PgPool, output_dir: &str, sync: bool) -> Result<()> {
    info!("Generating schemas for EventPayload types...");

    // Register all payload types
    let schemas = register_payloads![
        // Filesystem payloads
        filesystem::FileCreatedPayload,
        filesystem::FileModifiedPayload,
        filesystem::FileDeletedPayload,
        filesystem::FileMovedPayload,
        filesystem::DirCreatedPayload,
        filesystem::DirDeletedPayload,
        filesystem::FileDiscoveredPayload,
        filesystem::DirDiscoveredPayload,
        // Shell payloads
        shell::KittyCommandExecutedPayload,
        shell::KittyCommandCompletedPayload,
        shell::KittySessionStartedPayload,
        shell::KittySessionEndedPayload,
        shell::KittyProcessChangedPayload,
        shell::KittyTabFocusedPayload,
        shell::KittyContentStreamedPayload,
        shell::AtuinCommandExecutedPayload,
        shell::AtuinCommandCompletedPayload,
        shell::AtuinEntryPayload,
        shell::HistoryCommandImportedPayload,
        shell::CommandImportedPayload,
        shell::BashHistoryEntryPayload,
        shell::BashHistoricalCommandPayload,
        shell::ZshHistoricalCommandPayload,
        shell::FishHistoricalCommandPayload,
        shell::TerminalMonitoringStartedPayload,
        shell::TerminalCommandHistoricalPayload,
        shell::TerminalHistoryHistoricalPayload,
        shell::TerminalSnapshotPayload,
        shell::CanonicalCommandPayload,
        shell::ShellOutputCapturedPayload,
        shell::AsciinemaSessionStartedPayload,
        shell::AsciinemaSessionEndedPayload,
        // Clipboard payloads
        clipboard::ClipboardCopiedPayload,
        clipboard::ClipboardSelectedPayload,
        // Window payloads
        window::HyprlandWindowOpenedPayload,
        window::HyprlandWindowClosedPayload,
        window::HyprlandWindowFocusedPayload,
        window::HyprlandWorkspaceSwitchedPayload,
        window::HyprlandWindowMovedPayload,
        window::HyprlandMonitorFocusedPayload,
        window::HyprlandStateCapturedPayload,
        // Desktop payloads
        desktop::DesktopMonitoringStartedPayload,
        desktop::DesktopSnapshotPayload,
        desktop::ClipboardHistoricalPayload,
        desktop::WindowManagerHistoricalPayload,
        // System payloads
        system::ScanStartedPayload,
        system::ScanCompletedPayload,
        system::JournalEntryPayload,
        system::JournalSyncCompletedPayload,
        system::JournalEntryWrittenPayload,
        system::DbusSignalPayload,
        system::DbusMethodCalledPayload,
        system::DbusNotificationSentPayload,
        system::DbusMediaStateChangedPayload,
        system::DbusPowerStateChangedPayload,
        system::DbusDeviceConnectedPayload,
        system::DbusBluetoothDeviceChangedPayload,
        system::DbusNetworkStateChangedPayload,
        system::DbusMountEventPayload,
        system::SystemdUnitStartedPayload,
        system::SystemdUnitStoppedPayload,
        system::SystemdUnitStatusPayload,
        system::SystemdUnitFailedPayload,
        system::SystemdUnitReloadedPayload,
        system::SystemdTimerTriggeredPayload,
        system::SystemdUnitStartingPayload,
        system::SystemdUnitStoppingPayload,
        system::SystemdUnitStateChangedPayload,
        system::UdevDeviceAddedPayload,
        system::UdevDeviceRemovedPayload,
        system::UdevDeviceConnectedPayload,
        system::UdevDeviceDisconnectedPayload,
        system::UdevDeviceChangedPayload,
        system::UdevDeviceDriverChangedPayload,
        system::UdevDeviceOtherPayload,
        system::LogLinePayload,
        system::SystemHealthSummaryPayload,
        system::SatelliteHeartbeatPayload,
        system::SystemMonitoringStartedPayload,
        system::SystemSnapshotPayload,
        system::JournaldHistoricalPayload,
        system::SystemdUnitsHistoricalPayload,
        system::UdevDeviceHistoricalPayload,
        // Process payloads
        process::ProcessStartedPayload,
        process::ProcessHeartbeatPayload,
        process::ProcessShutdownPayload,
        process::AutomatonErrorPayload,
        process::SensorActivatedPayload,
        process::SensorDeactivatedPayload,
        // Telemetry payloads
        telemetry::EventsProcessedPayload,
        telemetry::ErrorsSummaryPayload,
        telemetry::SystemResourcesPayload,
        telemetry::OperationPerformancePayload,
        telemetry::ComponentResourceUsagePayload,
        // Blob payloads
        blob::BlobStoredPayload,
        blob::BlobRetrievedPayload,
        blob::BlobDeletedPayload,
        blob::BlobIngestedPayload,
        blob::BlobVerifiedPayload,
        blob::StorageStatisticsPayload,
        // Document payloads
        document::DocumentIngestedPayload,
        // RPC payloads
        rpc::RpcContentResponsePayload,
        rpc::RpcPkmResponsePayload,
    ];

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    // Write schemas to files
    for (schema_name, schema) in &schemas {
        let file_path = format!("{}/{}.json", output_dir, schema_name);
        let pretty_json = serde_json::to_string_pretty(&schema)?;
        std::fs::write(&file_path, pretty_json)?;
        info!("Generated schema: {}", file_path);
    }

    // Write registry file
    let registry = serde_json::json!({
        "version": "v1",
        "schemas": schemas.keys().cloned().collect::<Vec<_>>(),
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "total_schemas": schemas.len(),
    });
    let registry_path = format!("{}/registry.json", output_dir);
    std::fs::write(registry_path, serde_json::to_string_pretty(&registry)?)?;

    info!("Generated {} schemas", schemas.len());

    if sync {
        sync_schemas_to_db(pool, schemas).await?;
    }

    Ok(())
}

async fn sync_schemas_to_db(pool: &PgPool, schemas: HashMap<String, Value>) -> Result<()> {
    info!("Syncing schemas to database...");

    for (schema_name, schema_content) in schemas {
        // Extract event types from schema name
        let event_types = vec![schema_name.clone()];

        // Parse source and event_type from schema_name (format: "source.event_type")
        let (source, event_type) = if let Some(dot_pos) = schema_name.find('.') {
            (&schema_name[..dot_pos], &schema_name[dot_pos + 1..])
        } else {
            ("unknown", schema_name.as_str())
        };

        // Compute content hash
        let schema_text = serde_json::to_string(&schema_content)?;
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&schema_text);
        let content_hash = format!("{:x}", hasher.finalize());

        // Insert or update schema
        let id = sqlx::query_scalar!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas
                (schema_name, schema_version, schema_content, event_types, source, event_type, content_hash)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (source, event_type, schema_version) DO UPDATE
            SET schema_content = EXCLUDED.schema_content,
                event_types = EXCLUDED.event_types,
                content_hash = EXCLUDED.content_hash,
                updated_at = NOW()
            RETURNING id as "id: Ulid"
            "#,
            &schema_name,
            "v1",
            &schema_content,
            &event_types as &[String],
            source,
            event_type,
            &content_hash
        )
        .fetch_one(pool)
        .await?;

        info!("Synced schema {} with ID {}", schema_name, id);
    }

    Ok(())
}

async fn sync_schemas(pool: &PgPool, input_dir: &str) -> Result<()> {
    info!("Syncing schemas from directory: {}", input_dir);

    let mut schemas = HashMap::new();

    // Read all JSON files from directory
    for entry in std::fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            if path.file_name().and_then(|s| s.to_str()) == Some("registry.json") {
                continue; // Skip registry file
            }

            let schema_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap()
                .to_string();

            let content = std::fs::read_to_string(&path)?;
            let schema: Value = serde_json::from_str(&content)?;

            schemas.insert(schema_name, schema);
        }
    }

    sync_schemas_to_db(pool, schemas).await?;

    Ok(())
}

async fn list_schemas(pool: &PgPool, active_only: bool) -> Result<()> {
    let query = if active_only {
        "SELECT id, schema_name, schema_version, event_types, created_at
         FROM sinex_schemas.event_payload_schemas
         WHERE is_active = true
         ORDER BY schema_name, schema_version"
    } else {
        "SELECT id, schema_name, schema_version, event_types, created_at, is_active
         FROM sinex_schemas.event_payload_schemas
         ORDER BY schema_name, schema_version"
    };

    let rows = sqlx::query(query).fetch_all(pool).await?;

    println!("Schemas in database:");
    println!("{:-<80}", "");

    for row in rows {
        let id: Ulid = row.get("id");
        let schema_name: String = row.get("schema_name");
        let schema_version: String = row.get("schema_version");
        let event_types: Vec<String> = row.get("event_types");
        let created_at: chrono::DateTime<chrono::Utc> = row.get("created_at");

        println!("ID: {}", id);
        println!("Name: {} ({})", schema_name, schema_version);
        println!("Event Types: {}", event_types.join(", "));
        println!("Created: {}", created_at.format("%Y-%m-%d %H:%M:%S"));

        if !active_only {
            let is_active: bool = row.get("is_active");
            println!("Active: {}", is_active);
        }

        println!("{:-<80}", "");
    }

    Ok(())
}

async fn validate_compatibility(_pool: &PgPool, from: &str, to: &str) -> Result<()> {
    warn!("Schema compatibility validation not yet implemented");
    info!("Would validate compatibility from {} to {}", from, to);

    // TODO: Implement schema compatibility validation
    // 1. Load both schemas from database
    // 2. Compare for breaking changes:
    //    - Removed required fields
    //    - Changed field types
    //    - Removed enum values
    // 3. Store compatibility result in schema_compatibility table

    Ok(())
}

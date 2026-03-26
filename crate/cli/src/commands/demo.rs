//! Demo seeder command — populates the database with deterministic fake events.
//!
//! Connects directly to the database (bypassing the gateway) and bulk-inserts
//! N semantically valid events using `insert_stream_batch`, which auto-routes
//! to COPY protocol for large batches.

use std::env;
use std::time::Instant;

use clap::Parser;
use color_eyre::Result;
use rand::{RngExt, SeedableRng};
use rand::rngs::SmallRng;
use serde_json::json;
use sinex_db::repositories::{SourceMaterial, StreamBatchRow};
use sinex_db::{DbPoolExt, create_pool};
use sinex_primitives::Id;
use sinex_primitives::Timestamp;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType, HostName};
use sinex_primitives::events::SourceMaterial as SourceMaterialMarker;

const DEMO_SOURCE: &str = "sinexctl-demo";
const BATCH_SIZE: usize = 500;

/// Seed the database with deterministic fake events for testing and demos
#[derive(Debug, Parser)]
#[command(after_help = "\
EXAMPLES:
    # Seed 10 000 events with default seed
    sinexctl demo

    # Deterministic seed — same args always produce same events
    sinexctl demo --seed 42

    # Smaller run
    sinexctl demo --count 5000

    # Wipe previous demo data first, then seed
    sinexctl demo --count 5000 --clear
")]
pub struct DemoCommand {
    /// RNG seed for deterministic event generation.
    /// Same seed + same count always produces identical events.
    #[arg(long, default_value = "0")]
    pub seed: u64,

    /// Number of events to insert
    #[arg(long, default_value = "10000")]
    pub count: usize,

    /// Delete all existing demo events (source == "sinexctl-demo") before seeding
    #[arg(long)]
    pub clear: bool,
}

type PayloadFn = fn(&mut SmallRng, usize) -> serde_json::Value;

static EVENT_TYPES: &[(&'static str, PayloadFn)] = &[
    ("file.created", gen_file_created),
    ("window.focused", gen_window_focused),
    ("shell.command", gen_shell_command),
    ("process.started", gen_process_started),
    ("network.connection", gen_network_connection),
];

impl DemoCommand {
    pub async fn execute(&self) -> Result<()> {
        let database_url = env::var("DATABASE_URL").map_err(|_| {
            color_eyre::eyre::eyre!(
                "DATABASE_URL not set. Set it in your environment or use the gateway commands instead."
            )
        })?;

        let pool = create_pool(&database_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to connect to database: {}", e))?;

        if self.clear {
            let rows: Vec<i64> =
                sqlx::query_scalar("DELETE FROM core.events WHERE source = $1 RETURNING 1")
                    .bind(DEMO_SOURCE)
                    .fetch_all(&pool)
                    .await
                    .unwrap_or_default();
            println!("Cleared {} existing demo events.", rows.len());
        }

        // Register a single source material used by all demo events.
        let material =
            SourceMaterial::stream(format!("sinexctl-demo://seed={}", self.seed));
        let material_record = pool
            .source_materials()
            .register_material(material)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to register source material: {}", e))?;
        let material_id: Id<SourceMaterialMarker> =
            Id::from_uuid(material_record.id);

        println!("Seeding {} events (seed={})...", self.count, self.seed);
        let started = Instant::now();

        let mut rng = SmallRng::seed_from_u64(self.seed);
        let host = HostName::from_static("sinexctl-demo-host");
        let source = EventSource::from_static(DEMO_SOURCE);

        let mut total_inserted: usize = 0;

        for batch_start in (0..self.count).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(self.count);
            let batch_len = batch_end - batch_start;

            let mut batch = Vec::with_capacity(batch_len);
            for i in batch_start..batch_end {
                let type_idx = rng.random_range(0..EVENT_TYPES.len());
                let (event_type_str, gen_payload) = EVENT_TYPES[type_idx];
                let payload = gen_payload(&mut rng, i);

                let row = StreamBatchRow {
                    id: Uuid::now_v7(),
                    source: source.clone(),
                    event_type: EventType::from_static(event_type_str),
                    ts_orig: Timestamp::now(),
                    host: host.clone(),
                    payload,
                    source_material_id: Some(material_id.clone()),
                    anchor_byte: Some(i as i64),
                    offset_start: Some(i as i64),
                    offset_end: Some((i + 1) as i64),
                    offset_kind: Some("byte".to_string()),
                    source_event_ids: None,
                    payload_schema_id: None,
                    node_run_id: None,
                    associated_blob_ids: None,
                    temporal_policy: None,
                    semantics_version: None,
                    scope_key: None,
                    equivalence_key: None,
                    created_by_operation_id: None,
                    node_model: None,
                };
                batch.push(row);
            }

            let result = pool
                .events()
                .insert_stream_batch(&batch)
                .await
                .map_err(|e| color_eyre::eyre::eyre!("Batch insert failed: {}", e))?;

            total_inserted += result.inserted_count;
        }

        let elapsed = started.elapsed();
        println!(
            "Done. Inserted {} events in {:.2}s.",
            total_inserted,
            elapsed.as_secs_f64()
        );

        Ok(())
    }
}

fn gen_file_created(rng: &mut SmallRng, i: usize) -> serde_json::Value {
    json!({
        "path": format!("/home/user/docs/file_{i}.txt"),
        "size": rng.random_range(100u64..1_000_000u64),
        "mime_type": "text/plain"
    })
}

fn gen_window_focused(rng: &mut SmallRng, i: usize) -> serde_json::Value {
    let apps = ["code", "firefox", "kitty", "sinex", "obsidian"];
    let app = apps[rng.random_range(0..apps.len())];
    json!({
        "app": app,
        "title": format!("file_{i}.rs - {app}"),
        "duration_ms": rng.random_range(1_000u64..60_000u64)
    })
}

fn gen_shell_command(rng: &mut SmallRng, _i: usize) -> serde_json::Value {
    let commands = [
        "git status",
        "xtask check",
        "ls -la",
        "cargo build",
        "grep -r pattern .",
    ];
    let cmd = commands[rng.random_range(0..commands.len())];
    json!({
        "command": cmd,
        "exit_code": 0,
        "duration_ms": rng.random_range(10u64..5_000u64)
    })
}

fn gen_process_started(rng: &mut SmallRng, _i: usize) -> serde_json::Value {
    let procs = ["sinex-ingestd", "sinex-gateway", "postgres", "nats-server"];
    let proc = procs[rng.random_range(0..procs.len())];
    json!({
        "name": proc,
        "pid": rng.random_range(1u32..65535u32),
        "uid": 1000
    })
}

fn gen_network_connection(rng: &mut SmallRng, _i: usize) -> serde_json::Value {
    let ports = [80u16, 443, 5432, 4222, 8080];
    let port = ports[rng.random_range(0..ports.len())];
    json!({
        "remote_host": format!("192.168.1.{}", rng.random_range(1u8..254u8)),
        "remote_port": port,
        "bytes_sent": rng.random_range(100u64..100_000u64)
    })
}

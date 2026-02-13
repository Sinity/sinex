//! The command-line interface for the Sinex database migrator and schema manager.
//!
//! This binary entry point provides two main functionalities:
//! 1. Database migrations via `sea-orm-cli` (up, down, status, etc.)
//! 2. Event payload schema synchronization (`sync` command)

use color_eyre::eyre::{bail, Context, Result};
use sea_orm_migration::prelude::*;
use serde::Deserialize;
use sinex_schema::Migrator;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let args: Vec<String> = env::args().collect();
    if let Some(cmd) = args.get(1) {
        match cmd.as_str() {
            "generate" => {
                bail!(
                    "The 'generate' command for event payload schemas is not yet implemented.\n\
                     This feature will generate JSON schemas from Rust event payload types.\n\
                     For now, schemas must be created manually in the schemas/v1 directory."
                );
            }
            "sync" => {
                return run_sync(&args[2..]).await;
            }
            _ => {}
        }
    }

    // For all other commands, delegate to SeaORM migration CLI
    // This handles: up, down, status, fresh, reset, etc.
    cli::run_cli(Migrator).await;

    Ok(())
}

// =============================================================================
// Schema Sync Implementation
// =============================================================================

/// Registry entry from `schemas/v1/registry.json`.
#[derive(Debug, Deserialize)]
struct RegistryEntry {
    source: String,
    event_type: String,
    version: String,
    path: String,
    #[allow(dead_code)] // Pre-computed by generate; we recompute for DB consistency
    content_hash: String,
}

/// Top-level registry file structure.
#[derive(Debug, Deserialize)]
struct Registry {
    #[allow(dead_code)]
    version: String,
    entries: Vec<RegistryEntry>,
}

/// Existing schema record from the database.
struct ExistingSchema {
    content_hash: Option<String>,
}

/// Result of the sync operation.
struct SyncResult {
    discovered: usize,
    created: usize,
    updated: usize,
    unchanged: usize,
}

/// Run the `sync` subcommand: read registry.json, load schemas, sync to database.
async fn run_sync(args: &[String]) -> Result<()> {
    let (input_dir, dry_run, database_url) = parse_sync_args(args)?;

    // Read registry.json
    let registry_path = input_dir.join("registry.json");
    let registry_content = fs::read_to_string(&registry_path)
        .with_context(|| format!("failed to read {}", registry_path.display()))?;
    let registry: Registry = serde_json::from_str(&registry_content)
        .with_context(|| format!("failed to parse {}", registry_path.display()))?;

    eprintln!(
        "Discovered {} schema entries in {}",
        registry.entries.len(),
        registry_path.display()
    );

    // Load all schema files and compute content hashes
    let mut candidates: Vec<SchemaCandidate> = Vec::with_capacity(registry.entries.len());
    let mut skipped = 0usize;
    for entry in &registry.entries {
        let schema_path = input_dir.join(&entry.path);

        // Skip entries whose schema files are missing (registry can drift from filesystem)
        let schema_content = match fs::read_to_string(&schema_path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                eprintln!(
                    "  ⚠ skipping {}/{}: schema file not found at {}",
                    entry.source,
                    entry.event_type,
                    schema_path.display()
                );
                skipped += 1;
                continue;
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("failed to read schema file {}", schema_path.display())
                });
            }
        };

        let schema_json: serde_json::Value = serde_json::from_str(&schema_content)
            .with_context(|| format!("failed to parse schema JSON {}", schema_path.display()))?;

        let content_hash = compute_content_hash(
            &entry.source,
            &entry.event_type,
            &entry.version,
            &schema_json,
        )?;

        candidates.push(SchemaCandidate {
            source: entry.source.clone(),
            event_type: entry.event_type.clone(),
            version: entry.version.clone(),
            schema_content: schema_json,
            content_hash,
        });
    }

    if skipped > 0 {
        eprintln!("  ({skipped} entries skipped due to missing schema files)");
    }

    // Connect to database
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .with_context(|| "failed to connect to database")?;

    // Perform sync
    let result = sync_schemas(&pool, &candidates, dry_run).await?;

    // Report results
    let mode = if dry_run { " (DRY RUN)" } else { "" };
    eprintln!(
        "Schema sync complete{mode}: {} discovered, {} created, {} updated, {} unchanged",
        result.discovered, result.created, result.updated, result.unchanged
    );

    Ok(())
}

/// Parse sync subcommand arguments.
fn parse_sync_args(args: &[String]) -> Result<(PathBuf, bool, String)> {
    let mut input_dir = None;
    let mut dry_run = false;
    let mut database_url = None;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--input" => {
                i += 1;
                input_dir =
                    Some(PathBuf::from(args.get(i).ok_or_else(|| {
                        color_eyre::eyre::eyre!("--input requires a value")
                    })?));
            }
            "--dry-run" => {
                dry_run = true;
            }
            "--database-url" => {
                i += 1;
                database_url = Some(
                    args.get(i)
                        .ok_or_else(|| color_eyre::eyre::eyre!("--database-url requires a value"))?
                        .clone(),
                );
            }
            other => {
                bail!("Unknown sync argument: {other}");
            }
        }
        i += 1;
    }

    let input_dir = input_dir
        .ok_or_else(|| color_eyre::eyre::eyre!("--input <dir> is required for sync command"))?;

    let database_url = database_url
        .or_else(|| env::var("DATABASE_URL").ok())
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "DATABASE_URL is required (pass --database-url or set DATABASE_URL env var)"
            )
        })?;

    Ok((input_dir, dry_run, database_url))
}

/// Compute content hash using the same blake3 formula as `NewEventSchema::calculate_content_hash()`
/// in sinex-db. The format is: `blake3(source + ":" + event_type + ":" + version + ":" + json_bytes)`.
fn compute_content_hash(
    source: &str,
    event_type: &str,
    version: &str,
    schema_content: &serde_json::Value,
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(source.as_bytes());
    hasher.update(b":");
    hasher.update(event_type.as_bytes());
    hasher.update(b":");
    hasher.update(version.as_bytes());
    hasher.update(b":");
    let serialized = serde_json::to_vec(schema_content)
        .with_context(|| "failed to serialize schema content for hashing")?;
    hasher.update(&serialized);
    Ok(hasher.finalize().to_hex().to_string())
}

struct SchemaCandidate {
    source: String,
    event_type: String,
    version: String,
    schema_content: serde_json::Value,
    content_hash: String,
}

/// Synchronize schema candidates with the database.
async fn sync_schemas(
    pool: &sqlx::PgPool,
    candidates: &[SchemaCandidate],
    dry_run: bool,
) -> Result<SyncResult> {
    // Load existing active schemas from DB
    let existing = load_active_schemas(pool).await?;

    let mut created = 0usize;
    let mut updated = 0usize;
    let mut unchanged = 0usize;

    for candidate in candidates {
        let key = (
            candidate.source.as_str(),
            candidate.event_type.as_str(),
            candidate.version.as_str(),
        );

        if let Some(existing_schema) = existing.get(&(
            candidate.source.clone(),
            candidate.event_type.clone(),
            candidate.version.clone(),
        )) {
            if existing_schema
                .content_hash
                .as_ref()
                .is_some_and(|hash| hash == &candidate.content_hash)
            {
                unchanged += 1;
            } else {
                if !dry_run {
                    update_schema(pool, &key, candidate).await?;
                }
                eprintln!(
                    "  {} {}/{} v{}",
                    if dry_run { "would update" } else { "updated" },
                    candidate.source,
                    candidate.event_type,
                    candidate.version
                );
                updated += 1;
            }
        } else {
            if !dry_run {
                insert_schema(pool, candidate).await?;
            }
            eprintln!(
                "  {} {}/{} v{}",
                if dry_run { "would create" } else { "created" },
                candidate.source,
                candidate.event_type,
                candidate.version
            );
            created += 1;
        }
    }

    Ok(SyncResult {
        discovered: candidates.len(),
        created,
        updated,
        unchanged,
    })
}

/// Load all active schemas from the database, keyed by (source, event_type, version).
async fn load_active_schemas(
    pool: &sqlx::PgPool,
) -> Result<HashMap<(String, String, String), ExistingSchema>> {
    let rows = sqlx::query_as::<_, (String, String, String, Option<String>)>(
        r#"
        SELECT source, event_type, schema_version, content_hash
        FROM sinex_schemas.event_payload_schemas
        WHERE is_active = true
        "#,
    )
    .fetch_all(pool)
    .await
    .with_context(|| "failed to load active schemas from database")?;

    let mut map = HashMap::with_capacity(rows.len());
    for (source, event_type, version, content_hash) in rows {
        map.insert(
            (source, event_type, version),
            ExistingSchema { content_hash },
        );
    }

    Ok(map)
}

/// Update an existing schema's content and hash.
async fn update_schema(
    pool: &sqlx::PgPool,
    key: &(&str, &str, &str),
    candidate: &SchemaCandidate,
) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE sinex_schemas.event_payload_schemas
        SET schema_content = $1,
            content_hash = $2,
            updated_at = NOW()
        WHERE source = $3
          AND event_type = $4
          AND schema_version = $5
          AND is_active = true
        "#,
    )
    .bind(&candidate.schema_content)
    .bind(&candidate.content_hash)
    .bind(key.0)
    .bind(key.1)
    .bind(key.2)
    .execute(pool)
    .await
    .with_context(|| format!("failed to update schema {}/{} v{}", key.0, key.1, key.2))?;

    Ok(())
}

/// Insert a new schema into the database.
async fn insert_schema(pool: &sqlx::PgPool, candidate: &SchemaCandidate) -> Result<()> {
    // Deactivate any existing schemas for this source/event_type first
    sqlx::query(
        r#"
        UPDATE sinex_schemas.event_payload_schemas
        SET is_active = false, updated_at = NOW()
        WHERE source = $1
          AND event_type = $2
          AND is_active = true
        "#,
    )
    .bind(&candidate.source)
    .bind(&candidate.event_type)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to deactivate old schemas for {}/{}",
            candidate.source, candidate.event_type
        )
    })?;

    // Insert with gen_ulid() default for ID
    sqlx::query(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (
            source, event_type, schema_version, schema_content,
            content_hash, is_active
        ) VALUES ($1, $2, $3, $4, $5, true)
        "#,
    )
    .bind(&candidate.source)
    .bind(&candidate.event_type)
    .bind(&candidate.version)
    .bind(&candidate.schema_content)
    .bind(&candidate.content_hash)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to insert schema {}/{} v{}",
            candidate.source, candidate.event_type, candidate.version
        )
    })?;

    Ok(())
}

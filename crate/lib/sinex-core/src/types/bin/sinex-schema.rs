//! Schema management tool for EventPayload types
//!
//! This tool manages JSON schemas for EventPayload types:
//! - Generates schemas from Rust types
//! - Syncs schemas to the database
//! - Validates schema compatibility
//! - Exports schemas for external use

use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_core::types::events::schema_registry::generate_all_schemas;
use sinex_core::types::Ulid;
use sqlx::postgres::PgPool;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use tracing::info;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Database URL (required for sync/list operations)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

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

#[derive(Debug, Clone)]
struct DiscoveredSchema {
    source: String,
    event_type: String,
    version: String,
    content: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RegistryFile {
    version: String,
    generated_at: String,
    entries: Vec<RegistryEntry>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct RegistryEntry {
    source: String,
    event_type: String,
    version: String,
    path: String,
    content_hash: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    let needs_db = matches!(
        cli.command,
        Commands::Generate { sync: true, .. } | Commands::Sync { .. } | Commands::List { .. }
    );

    let pool = if needs_db {
        let base_url = cli
            .database_url
            .as_deref()
            .ok_or_else(|| eyre!("DATABASE_URL is required for this command"))?;
        let namespaced_url = sinex_core::environment().database_url(base_url)?;
        Some(PgPool::connect(&namespaced_url).await?)
    } else {
        None
    };

    match cli.command {
        Commands::Generate { output, sync } => {
            generate_schemas(pool.as_ref(), &output, sync).await?;
        }
        Commands::Sync { input } => {
            let pool = pool
                .as_ref()
                .ok_or_else(|| eyre!("DATABASE_URL is required for sync"))?;
            sync_schemas(pool, &input).await?;
        }
        Commands::List { active_only } => {
            let pool = pool
                .as_ref()
                .ok_or_else(|| eyre!("DATABASE_URL is required for list"))?;
            list_schemas(pool, active_only).await?;
        }
        Commands::Validate { from, to } => {
            validate_compatibility(&from, &to)?;
        }
    }

    Ok(())
}

async fn generate_schemas(pool: Option<&PgPool>, output_dir: &str, sync: bool) -> Result<()> {
    info!("Generating schemas for EventPayload types...");

    let schemas = generate_all_schemas();
    tokio::fs::create_dir_all(output_dir).await?;

    let mut registry_entries = Vec::new();
    let mut discovered = Vec::new();

    for ((source, event_type, version), schema) in schemas {
        let safe_source = sanitize_component(&source);
        let safe_event = sanitize_component(&event_type);
        let dir_path = Path::new(output_dir).join(&safe_source);
        tokio::fs::create_dir_all(&dir_path).await?;
        let file_path = dir_path.join(format!("{safe_event}.json"));

        let pretty_json = serde_json::to_string_pretty(&schema)?;
        tokio::fs::write(&file_path, pretty_json).await?;

        let relative_path = file_path
            .strip_prefix(output_dir)
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|_| file_path.to_string_lossy().to_string());

        let content_hash = compute_content_hash(&schema)?;

        registry_entries.push(RegistryEntry {
            source: source.clone(),
            event_type: event_type.clone(),
            version: version.clone(),
            path: relative_path,
            content_hash: content_hash.clone(),
        });

        discovered.push(DiscoveredSchema {
            source,
            event_type,
            version,
            content: schema,
        });
    }

    registry_entries.sort_by(|a, b| {
        (a.source.as_str(), a.event_type.as_str(), a.version.as_str()).cmp(&(
            b.source.as_str(),
            b.event_type.as_str(),
            b.version.as_str(),
        ))
    });

    let registry_path = Path::new(output_dir).join("registry.json");
    let generated_at = match tokio::fs::read_to_string(&registry_path).await {
        Ok(contents) => {
            if let Ok(existing) = serde_json::from_str::<RegistryFile>(&contents) {
                if existing.entries == registry_entries {
                    existing.generated_at
                } else {
                    chrono::Utc::now().to_rfc3339()
                }
            } else {
                chrono::Utc::now().to_rfc3339()
            }
        }
        Err(_) => chrono::Utc::now().to_rfc3339(),
    };

    let registry = RegistryFile {
        version: "v1".to_string(),
        generated_at,
        entries: registry_entries,
    };
    tokio::fs::write(registry_path, serde_json::to_string_pretty(&registry)?).await?;

    info!(count = registry.entries.len(), "Generated schemas");

    if sync && !discovered.is_empty() {
        let pool = pool.ok_or_else(|| eyre!("--sync requires DATABASE_URL"))?;
        sync_schemas_to_db(pool, &discovered).await?;
    }

    Ok(())
}

async fn sync_schemas_to_db(pool: &PgPool, schemas: &[DiscoveredSchema]) -> Result<()> {
    info!(count = schemas.len(), "Syncing schemas to database");

    for schema in schemas {
        let content_hash = compute_content_hash(&schema.content)?;

        let row = sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.event_payload_schemas
                (source, event_type, schema_version, schema_content, content_hash)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (source, event_type, schema_version) DO UPDATE
            SET schema_content = EXCLUDED.schema_content,
                content_hash = EXCLUDED.content_hash,
                updated_at = NOW()
            RETURNING id::uuid as "id!"
            "#,
            schema.source,
            schema.event_type,
            schema.version,
            schema.content,
            content_hash
        )
        .fetch_one(pool)
        .await?;

        let id: Ulid = Ulid::from(row.id);

        info!(
            "Synced schema {}.{} v{} ({})",
            schema.source, schema.event_type, schema.version, id
        );
    }

    Ok(())
}

async fn sync_schemas(pool: &PgPool, input_dir: &str) -> Result<()> {
    info!("Syncing schemas from directory: {}", input_dir);

    let registry_path = Path::new(input_dir).join("registry.json");
    let schemas = if registry_path.exists() {
        load_schemas_from_registry(input_dir, &registry_path)?
    } else {
        load_schemas_by_walking(input_dir)?
    };

    if schemas.is_empty() {
        info!("No schemas discovered on disk");
        return Ok(());
    }

    sync_schemas_to_db(pool, &schemas).await
}

fn load_schemas_from_registry(
    input_dir: &str,
    registry_path: &Path,
) -> Result<Vec<DiscoveredSchema>> {
    let contents = fs::read_to_string(registry_path)
        .with_context(|| format!("Failed to read {}", registry_path.display()))?;
    let registry: RegistryFile = serde_json::from_str(&contents)
        .with_context(|| format!("Invalid registry JSON at {}", registry_path.display()))?;

    registry
        .entries
        .into_iter()
        .map(|entry| {
            let schema_path = Path::new(input_dir).join(&entry.path);
            let schema_text = fs::read_to_string(&schema_path)
                .with_context(|| format!("Failed to read schema {}", schema_path.display()))?;
            let content: Value = serde_json::from_str(&schema_text)
                .with_context(|| format!("Invalid JSON in {}", schema_path.display()))?;

            Ok(DiscoveredSchema {
                source: entry.source,
                event_type: entry.event_type,
                version: entry.version,
                content,
            })
        })
        .collect()
}

fn load_schemas_by_walking(input_dir: &str) -> Result<Vec<DiscoveredSchema>> {
    let root = Path::new(input_dir);
    let mut files = Vec::new();
    collect_json_files(root, &mut files)?;

    let mut schemas = Vec::new();
    for path in files {
        let relative_parent = path
            .parent()
            .and_then(|dir| dir.strip_prefix(root).ok())
            .and_then(|p| {
                let s = p.to_string_lossy().replace('\\', "/");
                if s.is_empty() {
                    None
                } else {
                    Some(s)
                }
            })
            .unwrap_or_else(|| "unknown".to_string());

        let source = relative_parent.replace('/', ".");
        let event_type = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string();
        let schema_text = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read schema {}", path.display()))?;
        let content: Value = serde_json::from_str(&schema_text)
            .with_context(|| format!("Invalid JSON in {}", path.display()))?;

        schemas.push(DiscoveredSchema {
            source,
            event_type,
            version: "1.0.0".to_string(),
            content,
        });
    }

    Ok(schemas)
}

fn collect_json_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(&path, files)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("json") {
            if path.file_name().and_then(|s| s.to_str()) == Some("registry.json") {
                continue;
            }
            files.push(path);
        }
    }

    Ok(())
}

fn sanitize_component(raw: &str) -> String {
    let cleaned = raw
        .chars()
        .map(|c| match c {
            '/' | '\\' => '-',
            ' ' => '_',
            _ => c,
        })
        .collect::<String>();

    if cleaned.is_empty() {
        "component".to_string()
    } else {
        cleaned
    }
}

fn compute_content_hash(value: &Value) -> Result<String> {
    use sha2::{Digest, Sha256};

    let serialized = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(&serialized);
    Ok(format!("{:x}", hasher.finalize()))
}

async fn list_schemas(pool: &PgPool, active_only: bool) -> Result<()> {
    println!("Schemas in database:");
    println!("{:-<80}", "");

    if active_only {
        let rows = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!",
                source,
                event_type,
                schema_version,
                content_hash,
                updated_at
            FROM sinex_schemas.event_payload_schemas
            WHERE is_active = true
            ORDER BY source, event_type, schema_version
            "#
        )
        .fetch_all(pool)
        .await?;

        for row in rows {
            let id: Ulid = Ulid::from(row.id);
            let updated_at = row.updated_at;

            println!("ID: {}", id);
            println!("Source: {}", row.source);
            println!("Event: {}", row.event_type);
            println!("Version: {}", row.schema_version);
            println!("Hash: {}", row.content_hash);
            println!("Updated: {}", updated_at.format("%Y-%m-%d %H:%M:%S"));
            println!("{:-<80}", "");
        }
    } else {
        let rows = sqlx::query!(
            r#"
            SELECT 
                id::uuid as "id!",
                source,
                event_type,
                schema_version,
                content_hash,
                updated_at,
                is_active
            FROM sinex_schemas.event_payload_schemas
            ORDER BY source, event_type, schema_version
            "#
        )
        .fetch_all(pool)
        .await?;

        for row in rows {
            let id: Ulid = Ulid::from(row.id);
            let updated_at = row.updated_at;

            println!("ID: {}", id);
            println!("Source: {}", row.source);
            println!("Event: {}", row.event_type);
            println!("Version: {}", row.schema_version);
            println!("Hash: {}", row.content_hash);
            println!("Updated: {}", updated_at.format("%Y-%m-%d %H:%M:%S"));
            println!("Active: {}", row.is_active);
            println!("{:-<80}", "");
        }
    }

    Ok(())
}

fn validate_compatibility(from: &str, to: &str) -> Result<()> {
    let from_schema = load_schema_from_identifier(from)?;
    let to_schema = load_schema_from_identifier(to)?;

    let issues = compare_schemas(&from_schema, &to_schema);
    if issues.is_empty() {
        println!("Schemas {from} → {to} are compatible.");
        return Ok(());
    }

    println!(
        "Found {} compatibility issue(s) between {from} and {to}:",
        issues.len()
    );
    for (idx, issue) in issues.iter().enumerate() {
        println!("  {}. {}", idx + 1, issue);
    }

    Err(eyre!(
        "{} compatibility issue(s) detected between {from} and {to}",
        issues.len()
    ))
}

fn load_schema_from_identifier(identifier: &str) -> Result<Value> {
    let candidates = candidate_schema_paths(identifier);
    for path in candidates {
        if path.exists() {
            let contents = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read schema from {}", path.display()))?;
            return serde_json::from_str(&contents)
                .with_context(|| format!("Invalid JSON in schema {}", path.display()));
        }
    }

    Err(eyre!(
        "Could not locate schema {identifier}. Provide a path or a schemas/v1 relative name."
    ))
}

fn candidate_schema_paths(identifier: &str) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let id_path = PathBuf::from(identifier);
    push_candidates(&id_path, &mut paths);

    let schemas_root = Path::new("schemas/v1");
    push_candidates(&schemas_root.join(identifier), &mut paths);

    if !identifier.ends_with(".json") {
        push_candidates(&id_path.with_extension("json"), &mut paths);
        push_candidates(
            &schemas_root.join(identifier).with_extension("json"),
            &mut paths,
        );
    }

    paths
}

fn push_candidates(path: &Path, paths: &mut Vec<PathBuf>) {
    if !paths.iter().any(|existing| existing == path) {
        paths.push(path.to_path_buf());
    }
}

fn compare_schemas(from: &Value, to: &Value) -> Vec<String> {
    let mut issues = Vec::new();

    let required_from = extract_required(from);
    let required_to = extract_required(to);
    for field in required_from.difference(&required_to) {
        issues.push(format!("Required property `{}` was removed", field));
    }

    let props_from = extract_properties(from);
    let props_to = extract_properties(to);

    for (name, from_prop) in props_from {
        match props_to.get(&name) {
            None => issues.push(format!("Property `{}` was removed", name)),
            Some(to_prop) => {
                compare_property_types(&name, from_prop, to_prop, &mut issues);
                compare_property_enums(&name, from_prop, to_prop, &mut issues);
            }
        }
    }

    issues
}

fn extract_required(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(|value| value.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|value| value.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn extract_properties<'a>(schema: &'a Value) -> BTreeMap<String, &'a Value> {
    schema
        .get("properties")
        .and_then(|props| props.as_object())
        .map(|object| {
            object
                .iter()
                .map(|(key, value)| (key.clone(), value))
                .collect()
        })
        .unwrap_or_default()
}

fn compare_property_types(name: &str, from: &Value, to: &Value, issues: &mut Vec<String>) {
    let from_types = extract_type_set(from);
    if from_types.is_empty() {
        return;
    }

    let to_types = extract_type_set(to);
    if to_types.is_empty() {
        issues.push(format!("Property `{}` no longer declares a type", name));
        return;
    }

    for ty in from_types.difference(&to_types) {
        issues.push(format!(
            "Property `{}` previously allowed type `{}` which is missing now",
            name, ty
        ));
    }
}

fn compare_property_enums(name: &str, from: &Value, to: &Value, issues: &mut Vec<String>) {
    if let Some(from_enum) = extract_enum_set(from) {
        match extract_enum_set(to) {
            Some(to_enum) => {
                for variant in from_enum.difference(&to_enum) {
                    issues.push(format!(
                        "Enum variant `{}` for property `{}` was removed",
                        variant, name
                    ));
                }
            }
            None => issues.push(format!("Property `{}` no longer defines enum values", name)),
        }
    }
}

fn extract_type_set(value: &Value) -> BTreeSet<String> {
    match value.get("type") {
        Some(Value::String(single)) => {
            let mut set = BTreeSet::new();
            set.insert(single.clone());
            set
        }
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        _ => BTreeSet::new(),
    }
}

fn extract_enum_set(value: &Value) -> Option<BTreeSet<String>> {
    value
        .get("enum")
        .and_then(|value| value.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    if let Some(s) = v.as_str() {
                        Some(s.to_string())
                    } else if let Some(n) = v.as_i64() {
                        Some(n.to_string())
                    } else {
                        None
                    }
                })
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn detect_missing_required_fields() -> color_eyre::Result<()> {
        let from = json!({
            "required": ["important"],
            "properties": {
                "important": {"type": "string"},
                "optional": {"type": "integer"}
            }
        });
        let to = json!({
            "required": [],
            "properties": {
                "optional": {"type": "integer"}
            }
        });

        let issues = compare_schemas(&from, &to);
        assert!(
            issues
                .iter()
                .any(|issue| issue.contains("Required property `important`")),
            "Missing required field should be reported"
        );
        Ok(())
    }

    #[sinex_test]
    fn detect_enum_regressions() -> color_eyre::Result<()> {
        let from = json!({
            "properties": {
                "state": {"enum": ["queued", "running", "done"]}
            }
        });

        let to = json!({
            "properties": {
                "state": {"enum": ["queued", "running"]}
            }
        });

        let issues = compare_schemas(&from, &to);
        assert!(
            issues.iter().any(|issue| issue.contains("variant `done`")),
            "Removed enum entries should be reported"
        );
        Ok(())
    }
}

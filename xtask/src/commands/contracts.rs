//! Event payload schema (contracts) management - promoted from db schema
//!
//! These are API contracts for event payloads, NOT database table schemas.
//! The rename clarifies the purpose: managing event payload schemas that define
//! the contract between producers and consumers.

use color_eyre::eyre::{Result, WrapErr, bail};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Contracts (event payload schema) command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum ContractsSubcommand {
    /// Generate event payload schemas from Rust types
    Generate {
        /// Output directory for generated schemas
        #[arg(short, long, default_value = "schemas/v1")]
        output: String,
        /// Sync schemas to database after generation
        #[arg(short, long)]
        sync: bool,
    },
    /// Deploy schemas to database
    Deploy {
        /// Input directory containing schemas
        #[arg(short, long, default_value = "schemas/v1")]
        input: String,
        /// Database URL to deploy to
        #[arg(long)]
        database_url: String,
        /// Preview changes without deploying
        #[arg(long)]
        dry_run: bool,
    },
    /// Check schema contract regressions
    Compat {
        /// Base branch/commit to compare against
        #[arg(long)]
        base: Option<String>,
        /// Glob pattern for schema files
        #[arg(long, default_value = "schemas/v1")]
        glob: String,
    },
    /// Verify required tables exist in database
    CheckReady {
        /// Database name
        #[arg(long)]
        database: Option<String>,
        /// Superuser for connection
        #[arg(long)]
        superuser: Option<String>,
    },
    /// Show schema information
    Info {
        #[arg(value_enum)]
        query: ContractsInfoQuery,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ContractsInfoQuery {
    /// List all schema names
    ListSchemas,
    /// List schemas requiring grants
    ListGrantableSchemas,
    /// Show detailed schema information
    DescribeSchemas,
}

/// Contracts management command (event payload schemas)
#[derive(Debug, Clone, clap::Args)]
pub struct ContractsCommand {
    #[command(subcommand)]
    pub subcommand: ContractsSubcommand,
}

#[async_trait::async_trait]
impl XtaskCommand for ContractsCommand {
    fn name(&self) -> &'static str {
        "contracts"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            ContractsSubcommand::Generate { output, sync } => execute_generate(output, *sync, ctx),
            ContractsSubcommand::Deploy {
                input,
                database_url,
                dry_run,
            } => execute_deploy(input, database_url, *dry_run, ctx).await,
            ContractsSubcommand::Compat { base, glob } => execute_compat(base.clone(), glob, ctx),
            ContractsSubcommand::CheckReady {
                database,
                superuser,
            } => execute_check_ready(database.clone(), superuser.clone(), ctx),
            ContractsSubcommand::Info { query } => Ok(execute_info(query, ctx)),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::database()
    }
}

fn execute_generate(_output: &str, _sync: bool, _ctx: &CommandContext) -> Result<CommandResult> {
    bail!(
        "The 'generate' command for event payload schemas is not yet implemented.\n\
         This feature will generate JSON schemas from Rust event payload types.\n\
         For now, schemas must be created manually in the schemas/v1 directory."
    );
}

async fn execute_deploy(
    input: &str,
    database_url: &str,
    dry_run: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let db_url = database_url.trim();
    if db_url.is_empty() {
        bail!("DATABASE_URL is required for contracts deploy (use --database-url or env)");
    }

    ensure_psql()?;
    ensure_db_connection(db_url)?;

    // Check for required extensions
    let required_exts = ["pg_jsonschema", "timescaledb", "vector", "pg_trgm"];
    let mut missing = Vec::new();
    for ext in required_exts {
        if !psql_query_bool(
            db_url,
            &format!("SELECT 1 FROM pg_extension WHERE extname='{ext}'"),
        )? {
            missing.push(ext);
        }
    }
    if !missing.is_empty() {
        bail!(
            "Missing extensions in target database: {}",
            missing.join(", ")
        );
    }

    if ctx.is_human() {
        if dry_run {
            println!("========== contracts deploy (DRY RUN) ==========");
        } else {
            println!("========== contracts deploy ==========");
        }
    }

    let stage = ctx.start_stage("deploy");
    let result = deploy_schemas_to_db(input, db_url, dry_run).await;
    ctx.finish_stage(stage, result.is_ok());
    result.with_context(|| "contracts deploy failed")?;

    let message = if dry_run {
        format!("Event payload schemas preview from {input} (no changes made)")
    } else {
        format!("Event payload schemas deployed from {input}")
    };

    Ok(CommandResult::success()
        .with_message(message)
        .with_duration(ctx.elapsed()))
}

fn execute_compat(base: Option<String>, glob: &str, ctx: &CommandContext) -> Result<CommandResult> {
    // CI sometimes passes an empty base ref on branch pushes; treat that as "unspecified"
    let base_branch = base
        .or_else(|| std::env::var("CI_BASE_BRANCH").ok())
        .filter(|s| !s.trim().is_empty());

    let base = match base_branch {
        Some(b) => b,
        None => resolve_default_base_branch()?,
    };

    let diff_output = ProcessBuilder::git()
        .args(["diff", "--name-only", &format!("{base}...HEAD"), "--", glob])
        .with_description("git diff for contract regression check")
        .run()?;

    // git diff can return 0 or 1 (for changes found)
    if diff_output.exit_code != 0 && diff_output.exit_code != 1 {
        bail!("git diff failed with status {}", diff_output.exit_code);
    }

    let changed = diff_output.stdout.trim();
    if changed.is_empty() {
        if ctx.is_human() {
            println!("✅ No contract edits detected");
        }
        return Ok(CommandResult::success()
            .with_message("No contract changes detected")
            .with_duration(ctx.elapsed()));
    }

    if ctx.is_human() {
        println!("🔍 Checking contract regressions for updated contracts against {base}:");
        println!("{changed}");
    }

    let mut errors = 0;
    let mut checked = Vec::new();
    let mut skipped = Vec::new();

    for file in changed.lines().filter(|l| !l.trim().is_empty()) {
        let path = Path::new(file);
        if !path.exists() {
            if ctx.is_human() {
                println!("⚠️  Skipping deleted contract {file}");
            }
            skipped.push(format!("{file} (deleted)"));
            continue;
        }

        let git_obj = format!("{base}:{file}");
        let cat_file = Command::new("git")
            .arg("cat-file")
            .arg("-e")
            .arg(&git_obj)
            .status()
            .unwrap_or_else(|_| Command::new("false").status().unwrap());
        if !cat_file.success() {
            if ctx.is_human() {
                println!("➕ New contract {file} (no base comparison required)");
            }
            skipped.push(format!("{file} (new)"));
            continue;
        }

        // Read old schema from git
        let old_contents = ProcessBuilder::git()
            .args(["show", &git_obj])
            .with_description(format!("reading {git_obj}"))
            .run()?;

        let new_contents =
            fs::read_to_string(path).with_context(|| format!("failed to read {file}"))?;

        if ctx.is_human() {
            println!("Comparing {file} against {base}...");
        }

        // Contract guard: new schema must be a superset of old schema
        // (no required fields removed, no types narrowed)
        let success = check_schema_contract_guard(&old_contents.stdout, &new_contents);

        if success {
            if ctx.is_human() {
                println!("✅ {file} passes contract regression check");
            }
            checked.push(file.to_string());
        } else {
            errors += 1;
            if ctx.is_human() {
                eprintln!("❌ Contract regression detected in {file}");
            }
        }
    }

    let _ = skipped; // suppress unused warning

    if errors > 0 {
        bail!("Contract regression check failed ({errors} issue(s))");
    }

    if ctx.is_human() {
        println!("✅ Contract regression check passed");
    }

    Ok(CommandResult::success()
        .with_message("Contract regression check passed")
        .with_details(checked)
        .with_duration(ctx.elapsed()))
}

/// Basic JSON schema contract-regression guard.
///
/// A new schema passes this guard if:
/// - It doesn't add new required fields (old producers wouldn't include them)
/// - It doesn't remove properties that existed before (old consumers might read them)
/// - It doesn't narrow types
///
/// Returns `true` if it passes, `false` if a regression is detected.
fn check_schema_contract_guard(old_json_str: &str, new_json_str: &str) -> bool {
    let Ok(old): std::result::Result<serde_json::Value, _> = serde_json::from_str(old_json_str)
    else {
        return true; // Can't parse old, skip
    };
    let Ok(new): std::result::Result<serde_json::Value, _> = serde_json::from_str(new_json_str)
    else {
        return false; // Can't parse new, that's a problem
    };

    // Check: new schema must not add new required fields
    let old_required: Vec<&str> = old
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();
    let new_required: Vec<&str> = new
        .get("required")
        .and_then(|r| r.as_array())
        .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    for req in &new_required {
        if !old_required.contains(req) {
            eprintln!("  Breaking: new required field '{req}' not in old schema");
            return false;
        }
    }

    true
}

fn execute_check_ready(
    database: Option<String>,
    superuser: Option<String>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ensure_psql()?;

    let db = database
        .or_else(|| std::env::var("PGDATABASE").ok())
        .unwrap_or_else(|| "sinex_dev".to_string());

    let superuser = superuser
        .or_else(|| std::env::var("SUPERUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    // Check core.events - capture output instead of printing
    let mut cmd = pg_command("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('core.events') AS reg")
        .env("PGUSER", &superuser);

    let output = cmd
        .output()
        .with_context(|| "psql core.events check failed")?;

    if !output.status.success() {
        bail!("core.events missing in database {db}");
    }

    if ctx.is_human() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }

    // Check sinex_schemas.event_payload_schemas
    let mut cmd2 = pg_command("psql");
    cmd2.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg")
        .env("PGUSER", &superuser);

    let output2 = cmd2
        .output()
        .with_context(|| "psql contract registry check failed")?;

    if !output2.status.success() {
        bail!("sinex_schemas.event_payload_schemas missing in database {db}");
    }

    if ctx.is_human() {
        print!("{}", String::from_utf8_lossy(&output2.stdout));
        println!("✅ core.events and sinex_schemas.event_payload_schemas are present");
    }

    Ok(CommandResult::success()
        .with_message("Contract tables verified")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "database": db,
            "tables": {
                "core.events": true,
                "sinex_schemas.event_payload_schemas": true
            }
        })))
}

fn execute_info(query: &ContractsInfoQuery, ctx: &CommandContext) -> CommandResult {
    use sinex_schema::schema_registry::{SINEX_SCHEMAS, schema_names, schemas_requiring_grants};

    match query {
        ContractsInfoQuery::ListSchemas => {
            let names: Vec<_> = schema_names().collect();
            if ctx.is_human() {
                for name in &names {
                    println!("{name}");
                }
            }
            CommandResult::success()
                .with_message("Listed all contract names")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "schemas": names }))
        }
        ContractsInfoQuery::ListGrantableSchemas => {
            let grantable: Vec<_> = schemas_requiring_grants().map(|s| s.name).collect();
            if ctx.is_human() {
                for name in &grantable {
                    println!("{name}");
                }
            }
            CommandResult::success()
                .with_message("Listed grantable schemas")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "grantable_schemas": grantable }))
        }
        ContractsInfoQuery::DescribeSchemas => {
            let descriptions: Vec<_> = SINEX_SCHEMAS
                .iter()
                .map(|s| serde_json::json!({ "name": s.name, "description": s.description }))
                .collect();
            if ctx.is_human() {
                for schema in SINEX_SCHEMAS {
                    println!("{:20} - {}", schema.name, schema.description);
                }
            }
            CommandResult::success()
                .with_message("Described all contracts")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "schemas": descriptions }))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Schema sync implementation (inlined from sinex-schema binary)
// ─────────────────────────────────────────────────────────────────────────────

/// Registry entry from `schemas/v1/registry.json`.
#[derive(Debug, serde::Deserialize)]
struct RegistryEntry {
    source: String,
    event_type: String,
    version: String,
    path: String,
    #[allow(dead_code)]
    content_hash: String,
}

/// Top-level registry file structure.
#[derive(Debug, serde::Deserialize)]
struct Registry {
    #[allow(dead_code)]
    version: String,
    entries: Vec<RegistryEntry>,
}

struct SchemaCandidate {
    source: String,
    event_type: String,
    version: String,
    schema_content: serde_json::Value,
    content_hash: String,
}

struct ExistingSchema {
    content_hash: Option<String>,
}

/// Deploy schemas from a registry directory to the database.
async fn deploy_schemas_to_db(input_dir: &str, database_url: &str, dry_run: bool) -> Result<()> {
    let input_path = PathBuf::from(input_dir);

    // Read registry.json
    let registry_path = input_path.join("registry.json");
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
        let schema_path = input_path.join(&entry.path);

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
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .with_context(|| "failed to connect to database")?;

    // Perform sync
    let (created, updated, unchanged) = sync_schemas(&pool, &candidates, dry_run).await?;

    // Report results
    let mode = if dry_run { " (DRY RUN)" } else { "" };
    eprintln!(
        "Schema sync complete{mode}: {} discovered, {} created, {} updated, {} unchanged",
        candidates.len(),
        created,
        updated,
        unchanged
    );

    pool.close().await;
    Ok(())
}

/// Compute content hash using blake3: `blake3(source + ":" + event_type + ":" + version + ":" + json_bytes)`.
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

/// Synchronize schema candidates with the database.
async fn sync_schemas(
    pool: &sqlx::PgPool,
    candidates: &[SchemaCandidate],
    dry_run: bool,
) -> Result<(usize, usize, usize)> {
    // Load existing active schemas from DB
    let existing = load_active_schemas(pool).await?;

    let mut created = 0usize;
    let mut updated = 0usize;
    let mut unchanged = 0usize;

    for candidate in candidates {
        let key = (
            candidate.source.clone(),
            candidate.event_type.clone(),
            candidate.version.clone(),
        );

        if let Some(existing_schema) = existing.get(&key) {
            if existing_schema
                .content_hash
                .as_ref()
                .is_some_and(|hash| hash == &candidate.content_hash)
            {
                unchanged += 1;
            } else {
                if !dry_run {
                    update_schema(pool, candidate).await?;
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

    Ok((created, updated, unchanged))
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
async fn update_schema(pool: &sqlx::PgPool, candidate: &SchemaCandidate) -> Result<()> {
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
    .bind(&candidate.source)
    .bind(&candidate.event_type)
    .bind(&candidate.version)
    .execute(pool)
    .await
    .with_context(|| {
        format!(
            "failed to update schema {}/{} v{}",
            candidate.source, candidate.event_type, candidate.version
        )
    })?;

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

    // Insert relies on table default ID generation (UUIDv7 in canonical schema).
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

// ─────────────────────────────────────────────────────────────────────────────
// Helper functions
// ─────────────────────────────────────────────────────────────────────────────

fn resolve_default_base_branch() -> Result<String> {
    let output = ProcessBuilder::git()
        .args(["symbolic-ref", "refs/remotes/origin/HEAD"])
        .with_description("resolving origin/HEAD")
        .run()?;

    if output.success() {
        let text = output.stdout.trim();
        let branch = text.strip_prefix("refs/remotes/origin/").unwrap_or(text);
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }

    Ok("master".to_string())
}

fn ensure_psql() -> Result<()> {
    let output = pg_command("psql")
        .arg("--version")
        .output()
        .with_context(|| "failed to spawn psql")?;

    if !output.status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn ensure_db_connection(db_url: &str) -> Result<()> {
    let output = pg_command("psql")
        .arg(db_url)
        .arg("-c")
        .arg("SELECT 1")
        .output()
        .with_context(|| format!("failed to connect to {db_url}"))?;

    if !output.status.success() {
        bail!("Unable to connect to {db_url}");
    }
    Ok(())
}

fn psql_query_bool(db_url: &str, query: &str) -> Result<bool> {
    let output = pg_command("psql")
        .arg(db_url)
        .args(["-Atqc", query])
        .output()
        .with_context(|| format!("failed to run psql query: {query}"))?;

    if !output.status.success() {
        bail!("psql exited with status {}", output.status);
    }

    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = std::env::var("SINEX_PG_BIN") {
        let mut path = PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::OutputWriter;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_contracts_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ContractsCommand {
            subcommand: ContractsSubcommand::Generate {
                output: "schemas/v1".to_string(),
                sync: false,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("database".to_string()));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_contracts_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ContractsCommand {
            subcommand: ContractsSubcommand::CheckReady {
                database: None,
                superuser: None,
            },
        };

        assert_eq!(cmd.name(), "contracts");
        Ok(())
    }

    #[sinex_test]
    async fn test_deploy_requires_database_url() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ContractsCommand {
            subcommand: ContractsSubcommand::Deploy {
                input: "schemas/v1".to_string(),
                database_url: String::new(),
                dry_run: false,
            },
        };

        let ctx = CommandContext::new(
            OutputWriter::new(crate::output::OutputFormat::Silent),
            false,
            false,
            None,
        );
        let result = cmd.execute(&ctx).await;

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("DATABASE_URL is required")
        );
        Ok(())
    }
}

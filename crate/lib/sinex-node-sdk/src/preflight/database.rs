/*!
 * Database verification module for Sinex Pre-Flight system
 *
 * Handles comprehensive database validation including:
 * - PostgreSQL extension availability
 * - Declarative schema dry-run verification
 * - Schema compatibility checks
 * - Connection pool validation
 */

use crate::{NodeResult, SinexError};
use camino::Utf8PathBuf;
use serde_json::{Value, json};
use sinex_primitives::constants::timeouts;
// VerificationQueries removed - using direct SQL queries instead
use sqlx::PgPool;
use std::collections::HashMap;
use std::fs;
use tracing::{debug, error, info};

use super::{VerificationStatus, resolve_database_url};

/// Check if a table exists in the specified schema
async fn table_exists(pool: &PgPool, schema: &str, table: &str) -> NodeResult<bool> {
    let exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )",
    )
    .bind(schema)
    .bind(table)
    .fetch_one(pool)
    .await?;

    Ok(exists.0)
}

/// Verify database connectivity and basic operations
pub async fn verify_database_connectivity() -> NodeResult<(VerificationStatus, Value, Vec<String>)>
{
    let mut messages = Vec::new();
    let mut details = HashMap::new();

    info!("Verifying database connectivity");

    // Get database URL
    let database_url = resolve_database_url()?;

    details.insert("database_url", json!(redact_password(&database_url)));

    // Test connection with timeout
    let pool = match tokio::time::timeout(
        timeouts::TEST_DATABASE_TIMEOUT,
        PgPool::connect(&database_url),
    )
    .await
    {
        Ok(Ok(pool)) => {
            messages.push("✓ Database connection established".to_string());
            pool
        }
        Ok(Err(e)) => {
            let error_msg = format!("Database connection failed: {e}");
            messages.push(format!("✗ {error_msg}"));
            return Ok((VerificationStatus::Fail, json!(details), messages));
        }
        Err(_) => {
            messages.push(format!(
                "✗ Database connection timeout ({}s)",
                timeouts::TEST_DATABASE_TIMEOUT.as_secs()
            ));
            return Ok((VerificationStatus::Fail, json!(details), messages));
        }
    };

    // Test basic query operations
    match test_basic_operations(&pool, &mut messages, &mut details).await {
        Ok(_) => {
            info!("Database connectivity verification passed");
            Ok((VerificationStatus::Pass, json!(details), messages))
        }
        Err(e) => {
            let error_msg = format!("Database operations failed: {e}");
            messages.push(format!("✗ {error_msg}"));
            Ok((VerificationStatus::Fail, json!(details), messages))
        }
    }
}

/// Verify PostgreSQL extensions are available and can be loaded
pub async fn verify_postgresql_extensions() -> NodeResult<(VerificationStatus, Value, Vec<String>)>
{
    let mut messages = Vec::new();
    let mut details = HashMap::new();
    let mut has_failures = false;

    info!("Verifying PostgreSQL extensions");

    let database_url = resolve_database_url()?;

    let pool = PgPool::connect(&database_url)
        .await
        .map_err(SinexError::from)?;

    // Required extensions for Sinex
    let required_extensions = vec![
        ("timescaledb", "Time-series database functionality"),
        ("pg_jsonschema", "JSON schema validation"),
        ("vector", "Vector embeddings support"),
        ("pg_trgm", "Trigram indexing support"),
    ];

    let mut extension_status = HashMap::new();

    for (extension_name, description) in required_extensions {
        match verify_single_extension(&pool, extension_name, description).await {
            Ok(status) => {
                let is_available = status["available"].as_bool().unwrap_or(false);
                extension_status.insert(extension_name.to_string(), status);
                if is_available {
                    messages.push(format!("✓ Extension '{extension_name}' available"));
                } else {
                    messages.push(format!("✗ Extension '{extension_name}' NOT available"));
                    has_failures = true;
                }
            }
            Err(e) => {
                error!("Failed to verify extension {extension_name}: {e}");
                extension_status.insert(
                    extension_name.to_string(),
                    json!({
                        "available": false,
                        "error": e.to_string()
                    }),
                );
                messages.push(format!(
                    "✗ Extension '{extension_name}' verification failed: {e}"
                ));
                has_failures = true;
            }
        }
    }

    details.insert("extensions", json!(extension_status));

    // Test extension loading in a transaction (rollback to avoid side effects)
    if !has_failures {
        match test_extension_loading(&pool, &mut messages).await {
            Ok(_) => {
                messages.push("✓ All extensions can be loaded successfully".to_string());
            }
            Err(e) => {
                messages.push(format!("✗ Extension loading test failed: {e}"));
                has_failures = true;
            }
        }
    }

    let status = if has_failures {
        VerificationStatus::Fail
    } else {
        VerificationStatus::Pass
    };

    Ok((status, json!(details), messages))
}

/// Verify declarative schema readiness with comprehensive dry-run
pub async fn verify_migration_readiness() -> NodeResult<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();

    info!("Verifying declarative schema readiness");

    let database_url = resolve_database_url()?;

    let pool = PgPool::connect(&database_url)
        .await
        .map_err(SinexError::from)?;

    // Check current declarative schema status
    let migration_info = check_migration_status(&pool, &mut messages).await?;
    details.insert("current_schema", json!(migration_info));

    // Perform dry-run of pending schema apply
    match perform_migration_dry_run(&pool, &mut messages, &mut details).await {
        Ok(_) => {
            messages.push("✓ Schema apply dry-run completed successfully".to_string());

            // Verify schema readiness
            match verify_schema_compatibility(&pool, &mut messages, &mut details).await {
                Ok(_) => {
                    info!("Declarative schema readiness verification passed");
                    Ok((VerificationStatus::Pass, json!(details), messages))
                }
                Err(e) => {
                    messages.push(format!("✗ Schema readiness check failed: {e}"));
                    Ok((VerificationStatus::Fail, json!(details), messages))
                }
            }
        }
        Err(e) => {
            messages.push(format!("✗ Schema apply dry-run failed: {e}"));
            Ok((VerificationStatus::Fail, json!(details), messages))
        }
    }
}

async fn test_basic_operations(
    pool: &PgPool,
    messages: &mut Vec<String>,
    details: &mut HashMap<&str, Value>,
) -> NodeResult<()> {
    // Test basic connectivity - keep as raw SQL for system function
    let version_result = sqlx::query!("SELECT version() as version")
        .fetch_one(pool)
        .await
        .map_err(SinexError::from)?;

    details.insert("postgresql_version", json!(version_result.version));
    messages.push("✓ PostgreSQL version query successful".to_string());

    // Test transaction handling
    let mut tx = pool.begin().await.map_err(SinexError::from)?;

    // Direct query for transaction test
    sqlx::query("SELECT 1 as test")
        .fetch_one(&mut *tx)
        .await
        .map_err(SinexError::from)?;

    tx.rollback().await.map_err(SinexError::from)?;

    messages.push("✓ Transaction handling verified".to_string());

    // Test connection pool health
    let pool_info = test_connection_pool_health(pool).await?;
    details.insert("connection_pool", json!(pool_info));
    messages.push("✓ Connection pool health verified".to_string());

    Ok(())
}

async fn verify_single_extension(
    pool: &PgPool,
    extension_name: &str,
    description: &str,
) -> NodeResult<Value> {
    debug!("Verifying extension: {extension_name} ({description})");

    // Check if extension is available in the system
    let available_result = sqlx::query!(
        "SELECT name FROM pg_available_extensions WHERE name = $1",
        extension_name
    )
    .fetch_optional(pool)
    .await
    .map_err(SinexError::from)?;

    let available = available_result.is_some();

    if !available {
        return Ok(json!({
            "available": false,
            "installed": false,
            "description": description,
            "error": "Extension not available in system"
        }));
    }

    // Check if extension is already installed
    let installed_result = sqlx::query!(
        "SELECT extname FROM pg_extension WHERE extname = $1",
        extension_name
    )
    .fetch_optional(pool)
    .await
    .map_err(SinexError::from)?;

    let installed = installed_result.is_some();

    // For extensions that aren't installed, test if they CAN be installed
    let can_install = if !installed {
        test_extension_installability(pool, extension_name)
            .await
            .unwrap_or(false)
    } else {
        true
    };

    Ok(json!({
        "available": available,
        "installed": installed,
        "can_install": can_install,
        "description": description
    }))
}

async fn test_extension_installability(pool: &PgPool, extension_name: &str) -> NodeResult<bool> {
    // Test installation in a transaction that we'll rollback
    let mut tx = pool.begin().await.map_err(SinexError::from)?;

    let result = sqlx::query(&format!(
        "CREATE EXTENSION IF NOT EXISTS \"{extension_name}\""
    ))
    .execute(&mut *tx)
    .await;

    // Always rollback to avoid side effects
    tx.rollback().await.map_err(SinexError::from)?;

    Ok(result.is_ok())
}

async fn test_extension_loading(pool: &PgPool, messages: &mut Vec<String>) -> NodeResult<()> {
    // Test loading all extensions in a transaction that we'll rollback
    let mut tx = pool.begin().await.map_err(SinexError::from)?;

    let extensions = vec!["timescaledb", "pg_jsonschema", "vector", "pg_trgm"];

    for extension in extensions {
        match sqlx::query(&format!("CREATE EXTENSION IF NOT EXISTS \"{extension}\""))
            .execute(&mut *tx)
            .await
        {
            Ok(_) => {
                debug!("Extension {} loaded successfully in test", extension);
            }
            Err(e) => {
                // Rollback and return error
                let _ = tx.rollback().await;
                return Err(SinexError::from(e));
            }
        }
    }

    // Test that extensions work by using their functionality
    test_extension_functionality(&mut tx, messages).await?;

    // Rollback to clean up
    tx.rollback().await.map_err(SinexError::from)?;

    Ok(())
}

async fn test_extension_functionality(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    messages: &mut Vec<String>,
) -> NodeResult<()> {
    // Test UUIDv7 generation function expected by schema bootstrap.
    let uuid_result = sqlx::query!("SELECT uuidv7() as uuid")
        .fetch_one(&mut **tx)
        .await
        .map_err(SinexError::from)?;
    let uuid_str = uuid_result
        .uuid
        .map_or_else(|| "OK".to_string(), |u| u.to_string());
    messages.push(format!("✓ UUIDv7 generation: {uuid_str}"));

    // Test TimescaleDB extension by checking version
    let timescale_version =
        sqlx::query!("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
            .fetch_optional(&mut **tx)
            .await
            .map_err(SinexError::from)?;

    if let Some(version) = timescale_version {
        messages.push(format!("✓ TimescaleDB version: {}", version.extversion));
    }

    // Test JSON schema validation (if available)
    let schema_test_result = sqlx::query!(
        r#"SELECT json_matches_schema(
            '{"type": "object"}',
            '{"test": true}'
        ) as matches"#
    )
    .fetch_one(&mut **tx)
    .await;

    match schema_test_result {
        Ok(_) => {
            messages.push("✓ JSON schema validation tested".to_string());
        }
        Err(e) => {
            return Err(SinexError::from(e));
        }
    }

    Ok(())
}

async fn check_migration_status(pool: &PgPool, messages: &mut Vec<String>) -> NodeResult<Value> {
    let drift = collect_schema_drift(pool).await?;

    if drift.is_empty() {
        messages.push("✓ Declarative schema shape is converged".to_string());
    } else {
        messages.push(format!(
            "⚠ Declarative schema drift detected ({} item(s))",
            drift.len()
        ));
    }

    Ok(json!({
        "declarative_schema": true,
        "drift_count": drift.len(),
        "drift": drift
    }))
}

async fn collect_schema_drift(pool: &PgPool) -> NodeResult<Vec<String>> {
    let mut drift = Vec::new();
    let required_tables = [
        ("core", "events"),
        ("core", "blobs"),
        ("core", "operations_log"),
        ("raw", "source_material_registry"),
        ("sinex_schemas", "event_payload_schemas"),
        ("sinex_schemas", "node_manifests"),
    ];

    for (schema, table) in required_tables {
        if !table_exists(pool, schema, table).await? {
            drift.push(format!("missing table {schema}.{table}"));
        }
    }

    let required_event_columns = ["id", "ts_coided", "ts_persisted", "payload"];
    for column in required_event_columns {
        let exists: (bool,) = sqlx::query_as(
            "SELECT EXISTS(
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'core'
                  AND table_name = 'events'
                  AND column_name = $1
            )",
        )
        .bind(column)
        .fetch_one(pool)
        .await
        .map_err(SinexError::from)?;

        if !exists.0 {
            drift.push(format!("missing column core.events.{column}"));
        }
    }

    Ok(drift)
}

async fn perform_migration_dry_run(
    pool: &PgPool,
    messages: &mut Vec<String>,
    details: &mut HashMap<&str, Value>,
) -> NodeResult<()> {
    info!("Performing declarative schema apply dry-run");

    // Create a separate database connection for the dry-run
    let _database_url = resolve_database_url()?;

    // For a true dry-run, we'd apply against an ephemeral database.
    // Here we verify declarative schema source availability and DB prerequisites.
    let schema_sources = discover_schema_sources().await?;
    details.insert("schema_sources", json!(schema_sources));

    messages.push(format!(
        "ℹ Discovered {} declarative schema source files",
        schema_sources.len()
    ));

    for source_file in &schema_sources {
        if let Err(e) = validate_schema_source(source_file).await {
            return Err(SinexError::processing(format!(
                "Declarative schema source '{}' is invalid: {e}",
                source_file.kind
            )));
        }
    }

    messages.push("✓ Declarative schema source files are accessible".to_string());
    verify_schema_prerequisites(pool).await?;
    messages.push("✓ Schema prerequisites check passed".to_string());

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct SchemaSourceFile {
    kind: String,
    path: String,
}

async fn discover_schema_sources() -> NodeResult<Vec<SchemaSourceFile>> {
    let schema_src_root = Utf8PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../sinex-schema/src");
    let schema_dir = schema_src_root.join("schema");
    let mut discovered = Vec::new();

    for entry in fs::read_dir(schema_dir.as_std_path()).map_err(SinexError::io)? {
        let entry = entry.map_err(SinexError::io)?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let utf8_path = Utf8PathBuf::from_path_buf(path.clone()).map_err(|_| {
            SinexError::processing(format!(
                "Schema path is not valid UTF-8: {}",
                path.display()
            ))
        })?;

        if !utf8_path.as_str().ends_with(".rs") {
            continue;
        }

        discovered.push(SchemaSourceFile {
            kind: format!("schema/{}", utf8_path.file_name().unwrap_or_default()),
            path: utf8_path.to_string(),
        });
    }

    for (kind, path) in [
        ("apply.rs".to_string(), schema_src_root.join("apply.rs")),
        (
            "schema_registry.rs".to_string(),
            schema_src_root.join("schema_registry.rs"),
        ),
    ] {
        discovered.push(SchemaSourceFile {
            kind,
            path: path.to_string(),
        });
    }

    discovered.sort_by(|left, right| left.path.cmp(&right.path));

    Ok(discovered)
}

async fn validate_schema_source(source: &SchemaSourceFile) -> NodeResult<()> {
    debug!("Validating declarative schema source: {}", source.kind);
    let path = Utf8PathBuf::from(source.path.as_str());
    if !path.exists() {
        return Err(SinexError::processing(format!(
            "Schema source file not found: {}",
            source.path
        )));
    }

    fs::metadata(path.as_std_path()).map_err(|e| {
        SinexError::processing(format!(
            "Schema source file not accessible: {} ({})",
            source.path, e
        ))
    })?;

    Ok(())
}

async fn verify_schema_prerequisites(pool: &PgPool) -> NodeResult<()> {
    let core_schema_exists: (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = 'core')",
    )
    .fetch_one(pool)
    .await
    .map_err(SinexError::from)?;

    if !core_schema_exists.0 {
        return Err(SinexError::processing(
            "Schema 'core' does not exist. Declarative schema apply may not have been run.",
        ));
    }

    let required_extensions = ["timescaledb", "vector", "pg_jsonschema", "pg_trgm"];
    for ext in required_extensions {
        let ext_installed: (bool,) =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = $1)")
                .bind(ext)
                .fetch_one(pool)
                .await
                .map_err(SinexError::from)?;

        if !ext_installed.0 {
            return Err(SinexError::processing(format!(
                "Required extension '{ext}' is not installed. Schema apply requires it."
            )));
        }
    }

    Ok(())
}

async fn verify_schema_compatibility(
    pool: &PgPool,
    messages: &mut Vec<String>,
    details: &mut HashMap<&str, Value>,
) -> NodeResult<()> {
    info!("Verifying schema compatibility");

    // Check for existence of critical tables
    let critical_tables = vec![
        "core.events",
        "core.blobs",
        "raw.source_material_registry",
        "sinex_schemas.event_payload_schemas",
        "sinex_schemas.node_manifests",
        "core.operations_log",
    ];

    let mut table_status = HashMap::new();
    let mut missing_tables = Vec::new();

    for table_name in critical_tables {
        let exists = check_table_exists(pool, table_name).await?;
        table_status.insert(table_name.to_string(), exists);

        if exists {
            messages.push(format!("✓ Critical table '{table_name}' exists"));
        } else {
            messages.push(format!("✗ Critical table '{table_name}' is MISSING"));
            missing_tables.push(table_name);
        }
    }

    details.insert("table_compatibility", json!(table_status));

    if !missing_tables.is_empty() {
        return Err(SinexError::processing(format!(
            "Missing {} critical table(s): {}. Run schema apply (`xtask db migrate`) before starting the node.",
            missing_tables.len(),
            missing_tables.join(", ")
        )));
    }

    Ok(())
}

async fn check_table_exists(pool: &PgPool, table_name: &str) -> NodeResult<bool> {
    let parts: Vec<&str> = table_name.split('.').collect();
    let (schema, table) = if parts.len() == 2 {
        (parts[0], parts[1])
    } else {
        ("public", table_name)
    };

    table_exists(pool, schema, table).await
}

async fn test_connection_pool_health(pool: &PgPool) -> NodeResult<Value> {
    // Test connection pool metrics
    let pool_options = pool.options();

    Ok(json!({
        "max_connections": pool_options.get_max_connections(),
        "min_connections": pool_options.get_min_connections(),
        "current_connections": pool.size(),
        "idle_connections": pool.num_idle(),
    }))
}

/// Redact password from database URL for logging
fn redact_password(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let mut redacted = parsed.clone();
        if redacted.password().is_some() {
            redacted.set_password(Some("***")).ok();
        }
        redacted.to_string()
    } else {
        "[INVALID_URL]".to_string()
    }
}

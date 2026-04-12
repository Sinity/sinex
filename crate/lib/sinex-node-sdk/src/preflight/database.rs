/*!
 * Database verification module for Sinex Pre-Flight system
 *
 * Handles comprehensive database validation including:
 * - `PostgreSQL` extension availability
 * - Declarative schema dry-run verification
 * - Schema integrity checks
 * - Connection pool validation
 */

use crate::{NodeResult, SinexError};
use serde_json::{Value, json};
use sinex_primitives::constants::timeouts;
use sqlx::PgPool;
use std::collections::HashMap;
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

    details.insert(
        "database_url_source",
        json!("DATABASE_URL (effective env-scoped)"),
    );
    details.insert("database_url", json!(redact_password(&database_url)));

    // Test connection with timeout
    let pool = match tokio::time::timeout(
        timeouts::PREFLIGHT_DATABASE_TIMEOUT,
        PgPool::connect(&database_url),
    )
    .await
    {
        Ok(Ok(pool)) => {
            messages.push("✓ Database connection established".to_string());
            pool
        }
        Ok(Err(e)) => {
            let error_msg =
                format!("Database connection failed against effective runtime database: {e}");
            messages.push(format!("✗ {error_msg}"));
            return Ok((VerificationStatus::Fail, json!(details), messages));
        }
        Err(_) => {
            messages.push(format!(
                "✗ Database connection timeout against effective runtime database ({}s)",
                timeouts::PREFLIGHT_DATABASE_TIMEOUT.as_secs()
            ));
            return Ok((VerificationStatus::Fail, json!(details), messages));
        }
    };

    // Test basic query operations
    match test_basic_operations(&pool, &mut messages, &mut details).await {
        Ok(()) => {
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

/// Verify `PostgreSQL` extensions are available and usable without mutating state.
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

    // Exercise installed extension functionality using read-only queries only.
    if !has_failures {
        match test_installed_extension_functionality(&pool, &mut messages).await {
            Ok(()) => {
                messages.push(
                    "✓ Installed extensions respond correctly in a read-only transaction"
                        .to_string(),
                );
            }
            Err(e) => {
                messages.push(format!("✗ Extension functionality test failed: {e}"));
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

/// Verify declarative schema readiness with comprehensive read-only probing.
pub async fn verify_schema_readiness() -> NodeResult<(VerificationStatus, Value, Vec<String>)> {
    let mut messages = Vec::new();
    let mut details = HashMap::new();

    info!("Verifying declarative schema readiness");

    let database_url = resolve_database_url()?;

    let pool = PgPool::connect(&database_url)
        .await
        .map_err(SinexError::from)?;

    // Check current declarative schema status
    let schema_info = check_schema_status(&pool, &mut messages).await?;
    details.insert("current_schema", json!(schema_info));

    // Probe declarative schema readiness without applying anything.
    match perform_schema_readiness_probe(&pool, &mut messages, &mut details).await {
        Ok(()) => {
            messages.push("✓ Schema readiness probe completed successfully".to_string());

            // Verify schema readiness
            match verify_schema_integrity(&pool, &mut messages, &mut details).await {
                Ok(()) => {
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
            messages.push(format!("✗ Schema readiness probe failed: {e}"));
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
        "SELECT name, default_version FROM pg_available_extensions WHERE name = $1",
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
        "SELECT extname, extversion FROM pg_extension WHERE extname = $1",
        extension_name
    )
    .fetch_optional(pool)
    .await
    .map_err(SinexError::from)?;

    let installed = installed_result.is_some();

    let default_version = available_result.and_then(|row| row.default_version);
    let installed_version = installed_result.map(|row| row.extversion);

    Ok(json!({
        "available": available,
        "installed": installed,
        "default_version": default_version,
        "installed_version": installed_version,
        "description": description
    }))
}

async fn test_installed_extension_functionality(
    pool: &PgPool,
    messages: &mut Vec<String>,
) -> NodeResult<()> {
    // Preflight must remain read-only. Use an explicit read-only transaction so any
    // accidental write fails immediately instead of mutating the runtime database.
    let mut tx = pool.begin().await.map_err(SinexError::from)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(SinexError::from)?;

    // Test that installed extensions work by using their functionality.
    test_extension_functionality(&mut tx, messages).await?;
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

async fn check_schema_status(pool: &PgPool, messages: &mut Vec<String>) -> NodeResult<Value> {
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
        ("core", "node_manifests"),
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

async fn perform_schema_readiness_probe(
    pool: &PgPool,
    messages: &mut Vec<String>,
    details: &mut HashMap<&str, Value>,
) -> NodeResult<()> {
    info!("Performing declarative schema readiness probe");

    // Resolve the runtime database URL for consistent diagnostics.
    let _database_url = resolve_database_url()?;

    // This path is intentionally read-only. It verifies declarative schema source
    // availability and DB prerequisites without applying anything.
    let schema_sources = discover_schema_sources();
    details.insert("schema_sources", json!(schema_sources));

    messages.push(format!(
        "ℹ Discovered {} declarative schema source files",
        schema_sources.len()
    ));

    for source_file in &schema_sources {
        if let Err(e) = validate_schema_source(source_file) {
            return Err(SinexError::processing(format!(
                "Declarative schema source '{}' is invalid: {e}",
                source_file.kind
            )));
        }
    }

    messages.push("✓ Embedded declarative schema manifest is populated".to_string());
    verify_schema_prerequisites(pool).await?;
    messages.push("✓ Schema prerequisites check passed".to_string());

    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct SchemaSourceFile {
    kind: &'static str,
    path: &'static str,
    embedded: bool,
    bytes: usize,
    #[serde(skip_serializing)]
    contents: &'static str,
}

fn discover_schema_sources() -> Vec<SchemaSourceFile> {
    let mut discovered = vec![
        schema_source(
            "apply.rs",
            "crate/lib/sinex-schema/src/apply.rs",
            include_str!("../../../sinex-schema/src/apply.rs"),
        ),
        schema_source(
            "schema_registry.rs",
            "crate/lib/sinex-schema/src/schema_registry.rs",
            include_str!("../../../sinex-schema/src/schema_registry.rs"),
        ),
        schema_source(
            "schema/annotations.rs",
            "crate/lib/sinex-schema/src/schema/annotations.rs",
            include_str!("../../../sinex-schema/src/schema/annotations.rs"),
        ),
        schema_source(
            "schema/blobs.rs",
            "crate/lib/sinex-schema/src/schema/blobs.rs",
            include_str!("../../../sinex-schema/src/schema/blobs.rs"),
        ),
        schema_source(
            "schema/embeddings.rs",
            "crate/lib/sinex-schema/src/schema/embeddings.rs",
            include_str!("../../../sinex-schema/src/schema/embeddings.rs"),
        ),
        schema_source(
            "schema/entities.rs",
            "crate/lib/sinex-schema/src/schema/entities.rs",
            include_str!("../../../sinex-schema/src/schema/entities.rs"),
        ),
        schema_source(
            "schema/events.rs",
            "crate/lib/sinex-schema/src/schema/events.rs",
            include_str!("../../../sinex-schema/src/schema/events.rs"),
        ),
        schema_source(
            "schema/mod.rs",
            "crate/lib/sinex-schema/src/schema/mod.rs",
            include_str!("../../../sinex-schema/src/schema/mod.rs"),
        ),
        schema_source(
            "schema/operations.rs",
            "crate/lib/sinex-schema/src/schema/operations.rs",
            include_str!("../../../sinex-schema/src/schema/operations.rs"),
        ),
        schema_source(
            "schema/sinex_schemas.rs",
            "crate/lib/sinex-schema/src/schema/sinex_schemas.rs",
            include_str!("../../../sinex-schema/src/schema/sinex_schemas.rs"),
        ),
        schema_source(
            "schema/source_materials.rs",
            "crate/lib/sinex-schema/src/schema/source_materials.rs",
            include_str!("../../../sinex-schema/src/schema/source_materials.rs"),
        ),
        schema_source(
            "schema/temporal_ledger.rs",
            "crate/lib/sinex-schema/src/schema/temporal_ledger.rs",
            include_str!("../../../sinex-schema/src/schema/temporal_ledger.rs"),
        ),
    ];

    discovered.sort_by(|left, right| left.path.cmp(right.path));
    discovered
}

fn schema_source(
    kind: &'static str,
    path: &'static str,
    contents: &'static str,
) -> SchemaSourceFile {
    SchemaSourceFile {
        kind,
        path,
        embedded: true,
        bytes: contents.len(),
        contents,
    }
}

fn validate_schema_source(source: &SchemaSourceFile) -> NodeResult<()> {
    debug!("Validating declarative schema source: {}", source.kind);
    if source.contents.trim().is_empty() {
        return Err(SinexError::processing(format!(
            "Embedded schema source is empty: {}",
            source.path
        )));
    }

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

async fn verify_schema_integrity(
    pool: &PgPool,
    messages: &mut Vec<String>,
    details: &mut HashMap<&str, Value>,
) -> NodeResult<()> {
    info!("Verifying schema integrity");

    // Check for existence of critical tables
    let critical_tables = vec![
        "core.events",
        "core.blobs",
        "raw.source_material_registry",
        "sinex_schemas.event_payload_schemas",
        "core.node_manifests",
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

    details.insert("table_integrity", json!(table_status));

    if !missing_tables.is_empty() {
        return Err(SinexError::processing(format!(
            "Missing {} critical table(s): {}. Run schema apply (`xtask infra schema-apply`) before starting the node.",
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

#[cfg(test)]
mod tests {
    use super::{discover_schema_sources, validate_schema_source};

    #[test]
    fn schema_source_manifest_is_embedded() {
        let schema_sources = discover_schema_sources();
        assert!(!schema_sources.is_empty());
        assert!(schema_sources.iter().all(|source| source.embedded));
        assert!(schema_sources.iter().all(|source| source.bytes > 0));
        assert!(
            schema_sources
                .iter()
                .all(|source| source.path.starts_with("crate/lib/sinex-schema/src/"))
        );
    }

    #[test]
    fn embedded_schema_sources_validate_without_filesystem_access() {
        for source in discover_schema_sources() {
            validate_schema_source(&source).expect("embedded schema source should validate");
        }
    }
}

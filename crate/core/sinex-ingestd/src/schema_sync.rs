#![doc = include_str!("../doc/schema_sync.md")]

//! Helpers that ensure ingestd stays in sync with schema metadata.

use crate::IngestdResult;
use sinex_core::types::events::schema_registry::{generate_all_schemas, get_all_payloads};
use sinex_core::types::ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{debug, info};

/// Schema metadata for synchronization
#[derive(Debug)]
struct SchemaMetadata {
    source: String,
    event_type: String,
    version: String,
    content: serde_json::Value,
    content_hash: String,
}

/// Synchronize all discovered payload schemas with the database
pub async fn synchronize_schemas(pool: &PgPool) -> IngestdResult<SyncResult> {
    info!("Starting schema synchronization");

    // Get all schemas from the Rust codebase
    let discovered_schemas = generate_all_schemas();
    let discovered_count = discovered_schemas.len();

    debug!("Found {} payload schemas in codebase", discovered_count);

    // Load existing schemas from database
    let existing_schemas = load_existing_schemas(pool).await?;

    // Process each discovered schema using iterator methods
    // Pre-compute hashes and metadata to avoid repeated computation
    let (created, updated, unchanged) = {
        let mut results = Vec::new();

        for ((source, event_type, version), schema_content) in discovered_schemas {
            // Pre-compute the content hash once
            let content_hash = compute_content_hash(&schema_content);

            let metadata = SchemaMetadata {
                source: source.clone(),
                event_type: event_type.clone(),
                version: version.clone(),
                content: schema_content.clone(),
                content_hash,
            };

            results.push(process_schema(pool, &metadata, &existing_schemas).await?);
        }

        // Use iterator methods to count results
        results.iter().fold(
            (0, 0, 0),
            |(created, updated, unchanged), action| match action {
                SchemaAction::Created => (created + 1, updated, unchanged),
                SchemaAction::Updated => (created, updated + 1, unchanged),
                SchemaAction::Unchanged => (created, updated, unchanged + 1),
            },
        )
    };

    let result = SyncResult {
        discovered: discovered_count,
        created,
        updated,
        unchanged,
    };

    info!(?result, "Schema synchronization completed");
    Ok(result)
}

/// Result of schema synchronization
#[derive(Debug)]
pub struct SyncResult {
    pub discovered: usize,
    pub created: usize,
    pub updated: usize,
    pub unchanged: usize,
}

#[derive(Debug)]
enum SchemaAction {
    Created,
    Updated,
    Unchanged,
}

/// Load existing schemas from database
async fn load_existing_schemas(
    pool: &PgPool,
) -> IngestdResult<HashMap<(String, String, String), SchemaRecord>> {
    let rows = sqlx::query!(
        r#"
        SELECT 
            id as "id: Ulid",
            source,
            event_type,
            schema_version,
            content_hash
        FROM sinex_schemas.event_payload_schemas
        WHERE is_active = true
        "#
    )
    .fetch_all(pool)
    .await?;

    let mut schemas = HashMap::new();
    for row in rows {
        // Key now includes version to support multiple versions
        let key = (row.source, row.event_type, row.schema_version.clone());
        schemas.insert(
            key,
            SchemaRecord {
                id: row.id,
                content_hash: Some(row.content_hash),
            },
        );
    }

    Ok(schemas)
}

#[derive(Debug)]
struct SchemaRecord {
    id: Ulid,
    content_hash: Option<String>,
}

/// Process a single schema
async fn process_schema(
    pool: &PgPool,
    metadata: &SchemaMetadata,
    existing_schemas: &HashMap<(String, String, String), SchemaRecord>,
) -> IngestdResult<SchemaAction> {
    let key = (
        metadata.source.clone(),
        metadata.event_type.clone(),
        metadata.version.clone(),
    );

    // Check if this exact version exists
    if let Some(existing) = existing_schemas.get(&key) {
        // Check if content has changed
        if existing.content_hash.as_ref() == Some(&metadata.content_hash) {
            debug!(
                source = %metadata.source,
                event_type = %metadata.event_type,
                version = %metadata.version,
                "Schema unchanged"
            );
            return Ok(SchemaAction::Unchanged);
        }

        // Update existing schema version (rare - usually content doesn't change for a version)
        update_schema(pool, existing.id, metadata).await?;
        Ok(SchemaAction::Updated)
    } else {
        // Create new schema version
        create_schema(pool, metadata).await?;
        Ok(SchemaAction::Created)
    }
}

/// Create a new schema in the database
async fn create_schema(pool: &PgPool, metadata: &SchemaMetadata) -> IngestdResult<Ulid> {
    let id = Ulid::new();
    let _schema_name = format!("{}.{}", metadata.source, metadata.event_type);

    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.event_payload_schemas (
            id, source, event_type, schema_version, schema_content, 
            content_hash, is_active, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, true, NOW()
        )
        "#,
        &id as &Ulid,
        &metadata.source,
        &metadata.event_type,
        &metadata.version,
        &metadata.content,
        &metadata.content_hash,
    )
    .execute(pool)
    .await?;

    info!(
        source = %metadata.source,
        event_type = %metadata.event_type,
        version = %metadata.version,
        schema_id = %id,
        "Created new schema"
    );

    Ok(id)
}

/// Update an existing schema in the database
async fn update_schema(pool: &PgPool, id: Ulid, metadata: &SchemaMetadata) -> IngestdResult<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.event_payload_schemas
        SET 
            schema_content = $2,
            content_hash = $3,
            schema_version = $4,
            updated_at = NOW()
        WHERE id = $1
        "#,
        &id as &Ulid,
        &metadata.content,
        &metadata.content_hash,
        &metadata.version,
    )
    .execute(pool)
    .await?;

    info!(
        source = %metadata.source,
        event_type = %metadata.event_type,
        version = %metadata.version,
        schema_id = %id,
        "Updated schema"
    );

    Ok(())
}

/// Compute SHA-256 hash of schema content
fn compute_content_hash(content: &serde_json::Value) -> String {
    use sha2::{Digest, Sha256};

    // Serialize to canonical JSON
    // serde_json::Value is always serializable
    let canonical = serde_json::to_string(content).expect("serialize schema content");

    // Compute hash
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();

    // Convert to hex string
    hex::encode(result)
}

/// List all discovered payload information
pub fn list_discovered_payloads() {
    info!("Listing all discovered EventPayload types:");

    for (i, payload) in get_all_payloads().enumerate() {
        info!(
            index = i,
            type_name = %payload.type_name,
            source = %payload.source,
            event_type = %payload.event_type,
            version = %payload.version,
            "Discovered payload"
        );
    }
}

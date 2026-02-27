#![doc = include_str!("../docs/schema_sync.md")]

//! Helpers that ensure ingestd stays in sync with schema metadata.

use crate::IngestdResult;
use sinex_db::repositories::schema_management::{
    NewEventSchema, SchemaManagementRepository, SchemaSyncResult,
};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::schema_registry::{generate_all_schemas, get_all_payloads};
use sqlx::PgPool;
use tracing::info;

/// Synchronize all discovered payload schemas with the database
pub async fn synchronize_schemas(pool: &PgPool) -> IngestdResult<SchemaSyncResult> {
    info!("Starting schema synchronization");

    let discovered_schemas = generate_all_schemas();
    let repo = SchemaManagementRepository::new(pool);
    let result = repo.sync_discovered_schemas(discovered_schemas).await?;

    info!(?result, "Schema synchronization completed");
    Ok(result)
}

/// Compute the canonical content hash for a schema (exposed for tests)
pub fn compute_content_hash_for_testing(
    content: &serde_json::Value,
) -> Result<String, sinex_primitives::error::SinexError> {
    let schema = NewEventSchema {
        source: EventSource::new("test-source"),
        event_type: EventType::new("test-event"),
        schema_version: "1.0.0".to_string(),
        schema_content: content.clone(),
    };
    schema.calculate_content_hash()
}

/// Convenience helper for manual inspection of discovered payloads
pub fn list_discovered_payloads() {
    info!("Listing all discovered EventPayload types:");

    for (i, payload) in get_all_payloads().enumerate() {
        info!(
            index = i,
            source = payload.source,
            event_type = payload.event_type,
            version = payload.version,
            type_name = payload.type_name,
            "payload"
        );
    }
}

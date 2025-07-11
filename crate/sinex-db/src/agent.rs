//! Agent operations following the clean API pattern
//!
//! This module provides agent-related database operations with proper error handling
//! and clean API design, following the exact same pattern as existing *_correct.rs files.

use crate::DbPoolRef;
use crate::JsonValue;
use anyhow::Result;

/// Parameters for upserting an agent manifest
pub struct AgentManifestParams {
    pub agent_name: String,
    pub version: String,
    pub description: Option<String>,
    pub agent_type: String,
    pub config_template_json: JsonValue,
    pub produces_event_types: JsonValue,
    pub subscribes_to_event_types: JsonValue,
    pub required_capabilities: JsonValue,
}

/// Upsert an agent manifest following the exact same pattern as existing correct functions
pub async fn upsert_agent_manifest(pool: DbPoolRef<'_>, params: AgentManifestParams) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests 
            (agent_name, version, description, agent_type, config_template_json, 
             produces_event_types, subscribes_to_event_types, required_capabilities, 
             last_heartbeat_ts)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (agent_name) DO UPDATE SET
            version = EXCLUDED.version,
            description = EXCLUDED.description,
            agent_type = EXCLUDED.agent_type,
            config_template_json = EXCLUDED.config_template_json,
            produces_event_types = EXCLUDED.produces_event_types,
            subscribes_to_event_types = EXCLUDED.subscribes_to_event_types,
            required_capabilities = EXCLUDED.required_capabilities,
            last_heartbeat_ts = EXCLUDED.last_heartbeat_ts,
            updated_at = NOW()
        "#,
        params.agent_name,
        params.version,
        params.description,
        params.agent_type,
        params.config_template_json,
        params.produces_event_types,
        params.subscribes_to_event_types,
        params.required_capabilities
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Update agent heartbeat following the exact same pattern as existing correct functions
pub async fn update_agent_heartbeat(pool: DbPoolRef<'_>, agent_name: &str) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests
        SET last_heartbeat_ts = NOW()
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

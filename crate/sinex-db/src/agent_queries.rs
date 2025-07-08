//! Agent management database operations with clean API
//!
//! This module provides domain-specific agent operations following the
//! *_correct.rs pattern for clean API and proper error handling.

use crate::models::AgentManifest;
use crate::query_helpers::{ulid_to_uuid, uuid_to_ulid};
use crate::DbPoolRef;
use sinex_core::{Result, CoreError, JsonValue};
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use chrono::Utc;

/// Input for creating or updating an agent manifest
#[derive(Debug)]
pub struct UpsertAgentManifestInput {
    pub agent_name: String,
    pub version: String,
    pub status: String,
    pub agent_type: String,
    pub description: Option<String>,
    pub config_template_json: Option<JsonValue>,
    pub produces_event_types: Option<JsonValue>,
    pub subscribes_to_event_types: Option<JsonValue>,
    pub required_capabilities: Option<JsonValue>,
    pub llm_dependencies: Option<JsonValue>,
    pub repo_url: Option<String>,
}

/// Create or update an agent manifest
pub async fn upsert_agent_manifest(
    pool: DbPoolRef<'_>,
    input: UpsertAgentManifestInput,
) -> Result<AgentManifest> {
    let record = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.agent_manifests (
            agent_name, version, status, agent_type, description, 
            config_template_json, produces_event_types, subscribes_to_event_types,
            required_capabilities, llm_dependencies, repo_url
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (agent_name) DO UPDATE SET
            version = EXCLUDED.version,
            status = EXCLUDED.status,
            agent_type = EXCLUDED.agent_type,
            description = EXCLUDED.description,
            config_template_json = EXCLUDED.config_template_json,
            produces_event_types = EXCLUDED.produces_event_types,
            subscribes_to_event_types = EXCLUDED.subscribes_to_event_types,
            required_capabilities = EXCLUDED.required_capabilities,
            llm_dependencies = EXCLUDED.llm_dependencies,
            repo_url = EXCLUDED.repo_url,
            updated_at = NOW()
        RETURNING 
            agent_name as "agent_name!",
            description,
            version as "version!",
            status as "status!",
            agent_type as "agent_type!",
            config_template_json,
            produces_event_types,
            subscribes_to_event_types,
            required_capabilities,
            llm_dependencies,
            repo_url,
            last_heartbeat_ts,
            last_error_ts,
            last_error_summary,
            registered_at as "registered_at!",
            updated_at as "updated_at!"
        "#,
        input.agent_name,
        input.version,
        input.status,
        input.agent_type,
        input.description,
        input.config_template_json,
        input.produces_event_types,
        input.subscribes_to_event_types,
        input.required_capabilities,
        input.llm_dependencies,
        input.repo_url
    )
    .fetch_one(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to upsert agent manifest")
            .with_context("agent_name", &input.agent_name)
            .with_context("version", &input.version)
            .with_context("status", &input.status)
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(AgentManifest {
        agent_name: record.agent_name,
        description: record.description,
        version: record.version,
        status: record.status,
        agent_type: record.agent_type,
        config_template_json: record.config_template_json,
        produces_event_types: record.produces_event_types,
        subscribes_to_event_types: record.subscribes_to_event_types,
        required_capabilities: record.required_capabilities,
        llm_dependencies: record.llm_dependencies,
        repo_url: record.repo_url,
        last_heartbeat_ts: record.last_heartbeat_ts,
        last_error_ts: record.last_error_ts,
        last_error_summary: record.last_error_summary,
        registered_at: record.registered_at,
        updated_at: record.updated_at,
    })
}

/// Update agent heartbeat
pub async fn update_agent_heartbeat(pool: DbPoolRef<'_>, agent_name: &str) -> Result<()> {
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests 
        SET 
            last_heartbeat_ts = NOW(),
            status = 'running'
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to update agent heartbeat")
            .with_context("agent_name", agent_name)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("Agent manifest", agent_name));
    }
    
    Ok(())
}

/// Get agent manifest by name
pub async fn get_agent_manifest(pool: DbPoolRef<'_>, agent_name: &str) -> Result<AgentManifest> {
    let record = sqlx::query!(
        r#"
        SELECT 
            agent_name as "agent_name!",
            description,
            version as "version!",
            status as "status!",
            agent_type as "agent_type!",
            config_template_json,
            produces_event_types,
            subscribes_to_event_types,
            required_capabilities,
            llm_dependencies,
            repo_url,
            last_heartbeat_ts,
            last_error_ts,
            last_error_summary,
            registered_at as "registered_at!",
            updated_at as "updated_at!"
        FROM sinex_schemas.agent_manifests
        WHERE agent_name = $1
        "#,
        agent_name
    )
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get agent manifest")
            .with_context("agent_name", agent_name)
            .with_source(e.to_string())
            .build()
    })?;
    
    match record {
        Some(record) => Ok(AgentManifest {
            agent_name: record.agent_name,
            description: record.description,
            version: record.version,
            status: record.status,
            agent_type: record.agent_type,
            config_template_json: record.config_template_json,
            produces_event_types: record.produces_event_types,
            subscribes_to_event_types: record.subscribes_to_event_types,
            required_capabilities: record.required_capabilities,
            llm_dependencies: record.llm_dependencies,
            repo_url: record.repo_url,
            last_heartbeat_ts: record.last_heartbeat_ts,
            last_error_ts: record.last_error_ts,
            last_error_summary: record.last_error_summary,
            registered_at: record.registered_at,
            updated_at: record.updated_at,
        }),
        None => Err(CoreError::not_found("Agent manifest", agent_name)),
    }
}

/// Get all agent manifests
pub async fn get_all_agent_manifests(pool: DbPoolRef<'_>) -> Result<Vec<AgentManifest>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            agent_name as "agent_name!",
            version as "version!",
            capabilities as "capabilities!",
            configuration as "configuration!",
            status as "status!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            last_heartbeat_at
        FROM sinex_schemas.agent_manifests
        ORDER BY agent_name
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get all agent manifests")
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| AgentManifest {
            id: uuid_to_ulid(record.id),
            agent_name: record.agent_name,
            version: record.version,
            capabilities: record.capabilities,
            configuration: record.configuration,
            status: record.status,
            metadata: record.metadata,
            created_at: record.created_at,
            updated_at: record.updated_at,
            last_heartbeat_at: record.last_heartbeat_at,
        })
        .collect())
}

/// Get active agents (heartbeat within last 5 minutes)
pub async fn get_active_agents(pool: DbPoolRef<'_>) -> Result<Vec<AgentManifest>> {
    let records = sqlx::query!(
        r#"
        SELECT 
            id::uuid as "id!",
            agent_name as "agent_name!",
            version as "version!",
            capabilities as "capabilities!",
            configuration as "configuration!",
            status as "status!",
            metadata as "metadata!",
            created_at as "created_at!",
            updated_at as "updated_at!",
            last_heartbeat_at
        FROM sinex_schemas.agent_manifests
        WHERE last_heartbeat_at > NOW() - INTERVAL '5 minutes'
        ORDER BY last_heartbeat_at DESC
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to get active agents")
            .with_source(e.to_string())
            .build()
    })?;
    
    Ok(records
        .into_iter()
        .map(|record| AgentManifest {
            id: uuid_to_ulid(record.id),
            agent_name: record.agent_name,
            version: record.version,
            capabilities: record.capabilities,
            configuration: record.configuration,
            status: record.status,
            metadata: record.metadata,
            created_at: record.created_at,
            updated_at: record.updated_at,
            last_heartbeat_at: record.last_heartbeat_at,
        })
        .collect())
}

/// Update agent status
pub async fn update_agent_status(
    pool: DbPoolRef<'_>,
    agent_name: &str,
    status: &str,
) -> Result<()> {
    let result = sqlx::query!(
        r#"
        UPDATE sinex_schemas.agent_manifests 
        SET 
            status = $2,
            updated_at = NOW()
        WHERE agent_name = $1
        "#,
        agent_name,
        status
    )
    .execute(pool)
    .await
    .map_err(|e| {
        CoreError::database("Failed to update agent status")
            .with_context("agent_name", agent_name)
            .with_context("status", status)
            .with_source(e.to_string())
            .build()
    })?;
    
    if result.rows_affected() == 0 {
        return Err(CoreError::not_found("Agent manifest", agent_name));
    }
    
    Ok(())
}
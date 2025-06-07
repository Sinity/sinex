use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::info;

use crate::Ulid;

/// Agent manifest for self-registration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentManifest {
    pub agent_name: String,
    pub description: String,
    pub version: String,
    pub status: AgentManifestStatus,
    pub config_schema_id: Option<Ulid>,
    pub produces_event_types: HashMap<String, Vec<String>>, // source -> [event_types]
    pub repo_url: Option<String>,
    pub last_heartbeat_ts: Option<DateTime<Utc>>,
    pub registered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentManifestStatus {
    Development,
    Stable,
    Deprecated,
}

impl Default for AgentManifestStatus {
    fn default() -> Self {
        Self::Development
    }
}

/// Agent manifest manager for database operations
pub struct ManifestManager {
    pool: PgPool,
}

impl ManifestManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Register or update an agent manifest
    pub async fn register_agent(&self, manifest: &AgentManifest) -> Result<()> {
        let produces_json = serde_json::to_value(&manifest.produces_event_types)?;
        
        sqlx::query!(
            r#"
            INSERT INTO sinex_schemas.agent_manifests 
                (agent_name, description, version, status, config_schema_id, 
                 produces_event_types, repo_url, last_heartbeat_ts)
            VALUES ($1, $2, $3, $4, $5::uuid::ulid, $6, $7, $8)
            ON CONFLICT (agent_name) DO UPDATE SET
                description = EXCLUDED.description,
                version = EXCLUDED.version,
                status = EXCLUDED.status,
                config_schema_id = EXCLUDED.config_schema_id,
                produces_event_types = EXCLUDED.produces_event_types,
                repo_url = EXCLUDED.repo_url,
                last_heartbeat_ts = COALESCE(EXCLUDED.last_heartbeat_ts, agent_manifests.last_heartbeat_ts)
            "#,
            manifest.agent_name,
            manifest.description,
            manifest.version,
            format!("{:?}", manifest.status).to_lowercase(),
            manifest.config_schema_id.map(|id| uuid::Uuid::from_bytes(id.to_bytes())),
            produces_json,
            manifest.repo_url,
            manifest.last_heartbeat_ts
        )
        .execute(&self.pool)
        .await
        .context("Failed to register agent manifest")?;

        info!("Registered/updated agent manifest: {}", manifest.agent_name);
        Ok(())
    }

    /// Update agent heartbeat timestamp
    pub async fn update_heartbeat(&self, agent_name: &str) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE sinex_schemas.agent_manifests 
            SET last_heartbeat_ts = NOW()
            WHERE agent_name = $1
            "#,
            agent_name
        )
        .execute(&self.pool)
        .await
        .context("Failed to update agent heartbeat")?;

        Ok(())
    }

    /// Get agent manifest by name
    pub async fn get_agent(&self, agent_name: &str) -> Result<Option<AgentManifest>> {
        let row = sqlx::query!(
            r#"
            SELECT agent_name, description, version, status, 
                   config_schema_id::uuid as "config_schema_id?",
                   produces_event_types, repo_url, last_heartbeat_ts, registered_at
            FROM sinex_schemas.agent_manifests
            WHERE agent_name = $1
            "#,
            agent_name
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let config_schema_id: Option<Ulid> = row.config_schema_id
                .map(|uuid| Ulid::from_uuid(uuid));
            let manifest = AgentManifest {
                agent_name: row.agent_name,
                description: row.description.unwrap_or_default(),
                version: row.version,
                status: match row.status.as_str() {
                    "stable" => AgentManifestStatus::Stable,
                    "deprecated" => AgentManifestStatus::Deprecated,
                    _ => AgentManifestStatus::Development,
                },
                config_schema_id,
                produces_event_types: serde_json::from_value(row.produces_event_types.unwrap_or(JsonValue::Object(Default::default())))
                    .unwrap_or_default(),
                repo_url: row.repo_url,
                last_heartbeat_ts: row.last_heartbeat_ts,
                registered_at: Some(row.registered_at),
            };
            Ok(Some(manifest))
        } else {
            Ok(None)
        }
    }

    /// List all registered agents
    pub async fn list_agents(&self) -> Result<Vec<AgentManifest>> {
        let rows = sqlx::query!(
            r#"
            SELECT agent_name, description, version, status, 
                   config_schema_id::uuid as "config_schema_id?",
                   produces_event_types, repo_url, last_heartbeat_ts, registered_at
            FROM sinex_schemas.agent_manifests
            ORDER BY agent_name
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let manifests = rows
            .into_iter()
            .map(|row| {
                let config_schema_id: Option<Ulid> = row.config_schema_id
                    .map(|uuid| Ulid::from_uuid(uuid));
                AgentManifest {
                    agent_name: row.agent_name,
                    description: row.description.unwrap_or_default(),
                    version: row.version,
                    status: match row.status.as_str() {
                        "stable" => AgentManifestStatus::Stable,
                        "deprecated" => AgentManifestStatus::Deprecated,
                        _ => AgentManifestStatus::Development,
                    },
                    config_schema_id,
                produces_event_types: serde_json::from_value(row.produces_event_types.unwrap_or(JsonValue::Object(Default::default())))
                    .unwrap_or_default(),
                    repo_url: row.repo_url,
                    last_heartbeat_ts: row.last_heartbeat_ts,
                    registered_at: Some(row.registered_at),
                }
            })
            .collect();

        Ok(manifests)
    }
}

/// Helper to create a standard agent manifest
pub fn create_agent_manifest(
    agent_name: impl Into<String>,
    description: impl Into<String>,
    version: impl Into<String>,
    produces: HashMap<String, Vec<String>>,
) -> AgentManifest {
    AgentManifest {
        agent_name: agent_name.into(),
        description: description.into(),
        version: version.into(),
        status: AgentManifestStatus::Development,
        config_schema_id: None,
        produces_event_types: produces,
        repo_url: Some("https://github.com/sinity/sinex".to_string()),
        last_heartbeat_ts: None,
        registered_at: None,
    }
}
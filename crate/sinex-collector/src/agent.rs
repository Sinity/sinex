use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sinex_core::{JsonValue, Timestamp, OptionalTimestamp, EventSender, RawEventBuilder, sources, event_type_constants, RawEvent};
use sinex_ulid::Ulid;
use sinex_db::DbPool;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Agent status enum
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentStatus {
    Running,
    Degraded,
    Erroring,
}

/// Agent heartbeat payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHeartbeat {
    pub agent_name: String,
    pub status: AgentStatus,
    pub uptime_seconds: u64,
    pub events_processed_session: u64,
    pub dlq_size: u64,
    pub version: String,
}

/// Agent error severity
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSeverity {
    Warning,
    Error,
    Critical,
}

/// Agent error payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentError {
    pub agent_name: String,
    pub error_message: String,
    pub error_context: String,
    pub severity: ErrorSeverity,
    pub original_event_id_if_related: Option<String>,
}

/// DLQ event written payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEventWritten {
    pub agent_name: String,
    pub failed_event_source: String,
    pub failed_event_type: String,
    pub dlq_file_path: String,
    pub failure_reason: String,
}

/// Agent manifest status
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
    pub last_heartbeat_ts: OptionalTimestamp,
    pub registered_at: OptionalTimestamp,
}

/// Agent metrics tracker
#[derive(Debug, Clone)]
pub struct AgentMetrics {
    pub start_time: Timestamp,
    pub events_processed: u64,
    pub dlq_count: u64,
    agent_name: String,
    version: String,
}

impl AgentMetrics {
    pub fn new(agent_name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            start_time: Utc::now(),
            events_processed: 0,
            dlq_count: 0,
            agent_name: agent_name.into(),
            version: version.into(),
        }
    }

    pub fn increment_processed(&mut self) {
        self.events_processed += 1;
    }

    pub fn increment_dlq(&mut self) {
        self.dlq_count += 1;
    }

    pub fn uptime_seconds(&self) -> u64 {
        (Utc::now() - self.start_time).num_seconds() as u64
    }

    pub fn create_heartbeat(&self, status: AgentStatus) -> AgentHeartbeat {
        AgentHeartbeat {
            agent_name: self.agent_name.clone(),
            status,
            uptime_seconds: self.uptime_seconds(),
            events_processed_session: self.events_processed,
            dlq_size: self.dlq_count,
            version: self.version.clone(),
        }
    }
}

/// Agent manifest manager for database operations
#[derive(Debug, Clone)]
pub struct ManifestManager {
    pool: DbPool,
}

impl ManifestManager {
    pub fn new(pool: DbPool) -> Self {
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
            &manifest.agent_name,
            &manifest.description,
            &manifest.version,
            format!("{:?}", manifest.status).to_lowercase(),
            manifest.config_schema_id.map(|id| uuid::Uuid::from(id)),
            produces_json,
            manifest.repo_url.as_deref(),
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
            SELECT agent_name as "agent_name!", 
                   description, 
                   version as "version!", 
                   status as "status!", 
                   config_schema_id::uuid,
                   produces_event_types, 
                   repo_url, 
                   last_heartbeat_ts, 
                   registered_at as "registered_at!"
            FROM sinex_schemas.agent_manifests
            WHERE agent_name = $1
            "#,
            agent_name
        )
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            let config_schema_id: Option<Ulid> = row.config_schema_id
                .map(|uuid| uuid.into());
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
            SELECT agent_name as "agent_name!", 
                   description, 
                   version as "version!", 
                   status as "status!", 
                   config_schema_id::uuid,
                   produces_event_types, 
                   repo_url, 
                   last_heartbeat_ts, 
                   registered_at as "registered_at!"
            FROM sinex_schemas.agent_manifests
            ORDER BY agent_name
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut manifests = Vec::new();
        for row in rows {
            let config_schema_id: Option<Ulid> = row.config_schema_id
                .map(|uuid| uuid.into());
            manifests.push(AgentManifest {
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
            });
        }

        Ok(manifests)
    }
}

/// Agent lifecycle manager combining metrics, heartbeats, and manifest management
pub struct AgentLifecycle {
    metrics: AgentMetrics,
    manifest_manager: Option<ManifestManager>,
    heartbeat_interval: std::time::Duration,
    event_tx: Option<EventSender>,
}

impl AgentLifecycle {
    pub fn new(
        agent_name: impl Into<String>, 
        version: impl Into<String>,
        db_pool: Option<DbPool>,
        heartbeat_interval: Option<std::time::Duration>,
    ) -> Self {
        let metrics = AgentMetrics::new(agent_name, version);
        let manifest_manager = db_pool.map(ManifestManager::new);
        let heartbeat_interval = heartbeat_interval.unwrap_or(std::time::Duration::from_secs(30));
        
        Self {
            metrics,
            manifest_manager,
            heartbeat_interval,
            event_tx: None,
        }
    }
    
    /// Register agent manifest and start heartbeat loop
    pub async fn start(&mut self, manifest: AgentManifest, event_tx: EventSender) -> Result<()> {
        // Register manifest if we have a database connection
        if let Some(manager) = &self.manifest_manager {
            manager.register_agent(&manifest).await?;
        }
        
        self.event_tx = Some(event_tx.clone());
        
        // Start heartbeat loop
        let agent_name = manifest.agent_name.clone();
        let manifest_manager = self.manifest_manager.clone();
        let mut interval = tokio::time::interval(self.heartbeat_interval);
        let metrics_clone = self.metrics.clone();
        
        tokio::spawn(async move {
            loop {
                interval.tick().await;
                
                let heartbeat = metrics_clone.create_heartbeat(AgentStatus::Running);
                let heartbeat_event = create_heartbeat_event(heartbeat);
                
                if let Err(e) = event_tx.send(heartbeat_event).await {
                    warn!("Failed to send heartbeat event: {}", e);
                    break;
                }
                
                // Update manifest heartbeat timestamp
                if let Some(manager) = &manifest_manager {
                    if let Err(e) = manager.update_heartbeat(&agent_name).await {
                        warn!("Failed to update heartbeat timestamp: {}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Record successful event processing
    pub fn record_event_processed(&mut self) {
        self.metrics.increment_processed();
    }
    
    /// Record DLQ event and send notification
    pub async fn record_dlq_event(&mut self, dlq_event: DlqEventWritten) -> Result<()> {
        self.metrics.increment_dlq();
        
        if let Some(tx) = &self.event_tx {
            let event = create_dlq_event(dlq_event);
            tx.send(event).await.context("Failed to send DLQ event")?;
        }
        
        Ok(())
    }
    
    /// Send agent error event
    pub async fn report_error(&self, error: AgentError) -> Result<()> {
        if let Some(tx) = &self.event_tx {
            let event = create_error_event(error);
            tx.send(event).await.context("Failed to send error event")?;
        }
        
        Ok(())
    }
    
    pub fn get_metrics(&self) -> &AgentMetrics {
        &self.metrics
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

/// Helper functions to create agent events
pub fn create_heartbeat_event(heartbeat: AgentHeartbeat) -> RawEvent {
    use sinex_core::{event_type_constants, sources, RawEventBuilder};
    
    RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_HEARTBEAT,
        serde_json::to_value(heartbeat).unwrap(),
    ).build()
}

pub fn create_error_event(error: AgentError) -> RawEvent {
    use sinex_core::{event_type_constants, sources, RawEventBuilder};
    
    RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_ERROR,
        serde_json::to_value(error).unwrap(),
    ).build()
}

pub fn create_dlq_event(dlq: DlqEventWritten) -> RawEvent {
    use sinex_core::{event_type_constants, sources, RawEventBuilder};
    
    RawEventBuilder::new(
        sources::SINEX,
        event_type_constants::sinex::AGENT_DLQ_EVENT_WRITTEN,
        serde_json::to_value(dlq).unwrap(),
    ).build()
}
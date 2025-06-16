use anyhow::Result;
use sinex_db::models::{AgentManifest, RawEvent};
use sinex_ulid::Ulid;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::{debug, info, warn};

pub mod scanner;

pub use scanner::{EventScanner, ScannerConfig};

/// Represents a subscription rule for an agent
#[derive(Debug, Clone)]
pub struct EventSubscription {
    pub agent_name: String,
    pub source: String,
    pub event_types: Vec<String>,
}

/// Core work logic that determines which agents should process an event
pub struct WorkRouter {
    subscriptions: HashMap<String, Vec<EventSubscription>>,
}

impl WorkRouter {
    /// Create a new router from agent manifests
    pub fn from_manifests(manifests: Vec<AgentManifest>) -> Self {
        let mut subscriptions: HashMap<String, Vec<EventSubscription>> = HashMap::new();
        
        for manifest in manifests {
            if manifest.status != "running" {
                continue;
            }
            
            if let Some(subs) = manifest.subscribes_to_event_types {
                // Parse subscription JSON like:
                // { "desktop.hyprland.plugin": ["window_focused", "workspace_activated"] }
                if let Some(obj) = subs.as_object() {
                    for (source, event_types) in obj {
                        if let Some(types_array) = event_types.as_array() {
                            let event_types: Vec<String> = types_array
                                .iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect();
                            
                            if !event_types.is_empty() {
                                let sub = EventSubscription {
                                    agent_name: manifest.agent_name.clone(),
                                    source: source.clone(),
                                    event_types,
                                };
                                
                                subscriptions
                                    .entry(source.clone())
                                    .or_insert_with(Vec::new)
                                    .push(sub);
                            }
                        }
                    }
                }
            }
        }
        
        Self { subscriptions }
    }
    
    /// Determine which agents should process a given event
    pub fn route_event(&self, event: &RawEvent) -> Vec<String> {
        let mut target_agents = Vec::new();
        
        if let Some(subs) = self.subscriptions.get(&event.source) {
            for sub in subs {
                if sub.event_types.contains(&event.event_type) {
                    target_agents.push(sub.agent_name.clone());
                }
            }
        }
        
        // Also check for wildcard subscriptions (source "*")
        if let Some(wildcard_subs) = self.subscriptions.get("*") {
            for sub in wildcard_subs {
                if sub.event_types.contains(&event.event_type) || sub.event_types.contains(&"*".to_string()) {
                    if !target_agents.contains(&sub.agent_name) {
                        target_agents.push(sub.agent_name.clone());
                    }
                }
            }
        }
        
        target_agents
    }
}

/// Create work queue entries for new events
pub async fn create_work_entries(
    pool: &PgPool,
    events: Vec<RawEvent>,
    router: &WorkRouter,
) -> Result<usize> {
    let mut total_created = 0;
    
    for event in events {
        let target_agents = router.route_event(&event);
        
        if target_agents.is_empty() {
            debug!(
                event_id = %event.id,
                source = %event.source,
                event_type = %event.event_type,
                "No agents subscribed to event"
            );
            continue;
        }
        
        for agent_name in target_agents {
            match insert_work_queue_entry(pool, event.id, &agent_name).await {
                Ok(inserted) => {
                    if inserted {
                        total_created += 1;
                        debug!(
                            event_id = %event.id,
                            agent = %agent_name,
                            "Created work queue entry"
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        event_id = %event.id,
                        agent = %agent_name,
                        error = %e,
                        "Failed to create work queue entry"
                    );
                }
            }
        }
    }
    
    if total_created > 0 {
        info!(count = total_created, "Created work queue entries");
    }
    
    Ok(total_created)
}

/// Insert a single work queue entry
async fn insert_work_queue_entry(
    pool: &PgPool,
    event_id: Ulid,
    agent_name: &str,
) -> Result<bool> {
    let result = sqlx::query!(
        r#"
        INSERT INTO sinex_schemas.work_queue (raw_event_id, target_agent_name)
        VALUES ($1::uuid::ulid, $2)
        ON CONFLICT (raw_event_id, target_agent_name) DO NOTHING
        RETURNING queue_id::uuid as "queue_id!"
        "#,
        event_id.to_uuid(),
        agent_name
    )
    .fetch_optional(pool)
    .await?;
    
    Ok(result.is_some())
}

/// Get all active agent manifests
pub async fn get_active_manifests(pool: &PgPool) -> Result<Vec<AgentManifest>> {
    let records = sqlx::query!(
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
        WHERE status = 'running'
        "#
    )
    .fetch_all(pool)
    .await?;
    
    let manifests = records
        .into_iter()
        .map(|r| AgentManifest {
            agent_name: r.agent_name,
            description: r.description,
            version: r.version,
            status: r.status,
            agent_type: r.agent_type,
            config_template_json: r.config_template_json,
            produces_event_types: r.produces_event_types,
            subscribes_to_event_types: r.subscribes_to_event_types,
            required_capabilities: r.required_capabilities,
            llm_dependencies: r.llm_dependencies,
            repo_url: r.repo_url,
            last_heartbeat_ts: r.last_heartbeat_ts,
            last_error_ts: r.last_error_ts,
            last_error_summary: r.last_error_summary,
            registered_at: r.registered_at,
            updated_at: r.updated_at,
        })
        .collect();
    
    Ok(manifests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_promotion_router_basic() {
        let manifests = vec![
            AgentManifest {
                agent_name: "test-agent".to_string(),
                status: "running".to_string(),
                subscribes_to_event_types: Some(json!({
                    "test.source": ["event1", "event2"],
                    "other.source": ["event3"]
                })),
                // Other fields omitted for brevity
                description: None,
                version: "1.0.0".to_string(),
                agent_type: "promoter".to_string(),
                config_template_json: None,
                produces_event_types: None,
                required_capabilities: None,
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: None,
                last_error_ts: None,
                last_error_summary: None,
                registered_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ];
        
        let router = WorkRouter::from_manifests(manifests);
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "event1".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        };
        
        let agents = router.route_event(&event);
        assert_eq!(agents, vec!["test-agent"]);
    }
    
    #[test]
    fn test_promotion_router_wildcard() {
        let manifests = vec![
            AgentManifest {
                agent_name: "wildcard-agent".to_string(),
                status: "running".to_string(),
                subscribes_to_event_types: Some(json!({
                    "*": ["*"]
                })),
                description: None,
                version: "1.0.0".to_string(),
                agent_type: "promoter".to_string(),
                config_template_json: None,
                produces_event_types: None,
                required_capabilities: None,
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: None,
                last_error_ts: None,
                last_error_summary: None,
                registered_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ];
        
        let router = WorkRouter::from_manifests(manifests);
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "any.source".to_string(),
            event_type: "any.event".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        };
        
        let agents = router.route_event(&event);
        assert_eq!(agents, vec!["wildcard-agent"]);
    }
    
    #[test]
    fn test_promotion_router_no_match() {
        let manifests = vec![
            AgentManifest {
                agent_name: "test-agent".to_string(),
                status: "running".to_string(),
                subscribes_to_event_types: Some(json!({
                    "test.source": ["event1"]
                })),
                description: None,
                version: "1.0.0".to_string(),
                agent_type: "promoter".to_string(),
                config_template_json: None,
                produces_event_types: None,
                required_capabilities: None,
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: None,
                last_error_ts: None,
                last_error_summary: None,
                registered_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ];
        
        let router = WorkRouter::from_manifests(manifests);
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "other.source".to_string(),
            event_type: "unknown".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        };
        
        let agents = router.route_event(&event);
        assert!(agents.is_empty());
    }
    
    #[test]
    fn test_promotion_router_ignores_inactive() {
        let manifests = vec![
            AgentManifest {
                agent_name: "stopped-agent".to_string(),
                status: "stopped".to_string(),
                subscribes_to_event_types: Some(json!({
                    "test.source": ["event1"]
                })),
                description: None,
                version: "1.0.0".to_string(),
                agent_type: "promoter".to_string(),
                config_template_json: None,
                produces_event_types: None,
                required_capabilities: None,
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: None,
                last_error_ts: None,
                last_error_summary: None,
                registered_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            },
        ];
        
        let router = WorkRouter::from_manifests(manifests);
        
        let event = RawEvent {
            id: Ulid::new(),
            source: "test.source".to_string(),
            event_type: "event1".to_string(),
            ts_ingest: chrono::Utc::now(),
            ts_orig: None,
            host: "test-host".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({}),
        };
        
        let agents = router.route_event(&event);
        assert!(agents.is_empty());
    }
}
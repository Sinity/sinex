//! Agent integration tests
//!
//! This module contains integration tests for agent functionality,
//! including agent registration, heartbeat monitoring, manifest management,
//! and agent lifecycle operations.
//!
//! # Test Coverage
//! - Agent manifest registration and updates
//! - Heartbeat monitoring and health checks
//! - Agent lifecycle management
//! - Inter-agent communication patterns

/// Agent manifest registration and management tests
pub mod agent_manifest_tests;

/// Agent heartbeat and health monitoring tests
pub mod heartbeat_tests;

/// Common utilities for agent testing
pub mod utils {
    use crate::common::prelude::*;
    use crate::common::event_builders::EventBuilder;
    
    /// Create a test agent manifest
    pub fn create_test_agent_manifest(name: &str) -> serde_json::Value {
        json!({
            "agent_name": name,
            "description": format!("Test agent {}", name),
            "version": "1.0.0",
            "status": "development",
            "agent_type": "test",
            "produces_event_types": ["test.event"],
            "subscribes_to_event_types": ["test.trigger"]
        })
    }
    
    /// Create agent heartbeat event
    pub fn create_heartbeat_event(agent_name: &str) -> RawEvent {
        EventBuilder::agent()
            .name(agent_name)
            .heartbeat()
            .uptime_seconds(3600)
            .events_processed(42)
            .build()
    }
    
    /// Create agent startup event
    pub fn create_startup_event(agent_name: &str, version: &str) -> RawEvent {
        EventBuilder::agent()
            .name(agent_name)
            .startup()
            .version(version)
            .build()
    }
    
    /// Create agent error event
    pub fn create_error_event(agent_name: &str, error_msg: &str) -> RawEvent {
        EventBuilder::agent()
            .name(agent_name)
            .error(error_msg)
            .build()
    }
    
    /// Wait for agent to be registered
    pub async fn wait_for_agent_registration(
        pool: &PgPool,
        agent_name: &str,
        timeout_secs: u64
    ) -> Result<(), anyhow::Error> {
        crate::common::timing_optimization::wait_helpers::wait_for_condition(
            move || {
                let pool = pool.clone();
                let name = agent_name.to_string();
                async move {
                    let exists = sqlx::query_scalar!(
                        "SELECT EXISTS(SELECT 1 FROM sinex_schemas.agent_manifests WHERE agent_name = $1)",
                        name
                    )
                    .fetch_one(&pool)
                    .await?
                    .unwrap_or(false);
                    
                    Ok(exists)
                }
            },
            timeout_secs
        ).await
    }
}
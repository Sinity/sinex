use crate::common::prelude::*;
use crate::common::create_test_db_pool;
use crate::common::events;
use sinex_db::{queries, models::{RawEvent, AgentManifest}};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use futures::future::join_all;
use chrono::Utc;
use serde_json::json;

#[tokio::test]
async fn test_agent_registering_from_multiple_instances() {
    let pool = create_test_db_pool().await.unwrap();
    
    let agent_name = "chaos-agent";
    let successful_registrations = Arc::new(AtomicU64::new(0));
    let failed_registrations = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    // 10 instances try to register the same agent simultaneously
    for instance_id in 0..10 {
        let pool_clone = pool.clone();
        let success_count = successful_registrations.clone();
        let fail_count = failed_registrations.clone();
        
        let handle = tokio::spawn(async move {
            let manifest = AgentManifest {
                agent_name: agent_name.to_string(),
                description: Some(format!("Chaos agent instance {}", instance_id)),
                version: format!("1.0.{}", instance_id), // Slightly different versions
                status: "running".to_string(),
                agent_type: "filesystem".to_string(),
                config_template_json: Some(json!({
                    "type": "object",
                    "properties": {
                        "paths": {"type": "array"}
                    }
                })),
                produces_event_types: Some(json!(["file.created", "file.modified"])),
                subscribes_to_event_types: None,
                required_capabilities: Some(json!(["read", "write"])),
                llm_dependencies: None,
                repo_url: None,
                last_heartbeat_ts: Some(Utc::now()),
                last_error_ts: None,
                last_error_summary: None,
                registered_at: Utc::now(),
                updated_at: Utc::now(),
            };
            
            match queries::upsert_agent_manifest(
                &pool_clone,
                &manifest.agent_name,
                &manifest.version,
                &manifest.status,
                &manifest.agent_type,
                manifest.description.as_deref(),
                manifest.produces_event_types.clone(),
                manifest.subscribes_to_event_types.clone(),
            ).await {
                Ok(_) => {
                    println!("Instance {} successfully registered agent {}", instance_id, agent_name);
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!("Instance {} failed to register agent {}: {}", instance_id, agent_name, e);
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    let successes = successful_registrations.load(Ordering::SeqCst);
    let failures = failed_registrations.load(Ordering::SeqCst);
    
    println!("Agent registration chaos results:");
    println!("- Successful registrations: {}", successes);
    println!("- Failed registrations: {}", failures);
    
    // Check database state
    let agents = sqlx::query_as!(
        AgentManifest,
        r#"
        SELECT 
            agent_name,
            description,
            version,
            status,
            agent_type,
            config_template_json,
            produces_event_types,
            subscribes_to_event_types,
            required_capabilities,
            llm_dependencies,
            repo_url,
            last_heartbeat_ts,
            last_error_ts,
            last_error_summary,
            registered_at,
            updated_at
        FROM sinex_schemas.agent_manifests 
        WHERE agent_name = $1
        "#,
        agent_name
    ).fetch_all(&pool).await.unwrap();
    
    println!("- Agents in database: {}", agents.len());
    
    // Should be exactly 1 agent, but race conditions might create duplicates or corruption
    if agents.len() != 1 {
        println!("CHAOS DETECTED: Expected 1 agent, found {}", agents.len());
    }
    
    if successes > 1 {
        println!("RACE CONDITION: Multiple instances succeeded registration");
    }
}

#[tokio::test]
async fn test_heartbeat_from_unregistered_agent() {
    let pool = create_test_db_pool().await.unwrap();
    
    let phantom_agent = "phantom-agent";
    
    // Send heartbeat without registration
    let heartbeat_event = crate::common::events::generic_adversarial_event("agent", "agent.heartbeat", json!({
            "agent_name": phantom_agent,
            "status": "alive",
            "metrics": {
                "events_processed": 0,
                "uptime_seconds": 10
            }
        }), None);
    
    // This should either:
    // 1. Fail gracefully
    // 2. Auto-register phantom agent (potential security issue)
    // 3. Create inconsistent state
    match queries::insert_event(&pool, &heartbeat_event).await {
        Ok(_) => {
            println!("Phantom heartbeat was accepted - checking for side effects");
            
            // Check if phantom agent was auto-created
            let phantom_agents = sqlx::query!(
                "SELECT agent_name, version FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                phantom_agent
            ).fetch_all(&pool).await.unwrap();
            
            if !phantom_agents.is_empty() {
                println!("SECURITY ISSUE: Phantom agent auto-registered from heartbeat!");
                for agent in phantom_agents {
                    println!("  Phantom agent: {} v{}", agent.agent_name, agent.version);
                }
            }
        }
        Err(e) => {
            println!("Phantom heartbeat rejected (good): {}", e);
        }
    }
}

#[tokio::test]
async fn test_agent_downgrade_during_operation() {
    let pool = create_test_db_pool().await.unwrap();
    
    let agent_name = "version-chaos-agent";
    
    // Register agent v2.0
    let manifest_v2 = AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Version chaos test agent v2".to_string()),
        version: "2.0.0".to_string(),
        status: "running".to_string(),
        agent_type: "filesystem".to_string(),
        config_template_json: Some(json!({
            "type": "object",
            "properties": {
                "paths": {"type": "array"},
                "new_feature": {"type": "boolean"}  // v2.0 feature
            }
        })),
        produces_event_types: Some(json!(["file.created", "file.modified", "file.deleted"])),
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["read", "write", "delete"])),
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    queries::upsert_agent_manifest(
        &pool,
        &manifest_v2.agent_name,
        &manifest_v2.version,
        &manifest_v2.status,
        &manifest_v2.agent_type,
        manifest_v2.description.as_deref(),
        manifest_v2.produces_event_types.clone(),
        manifest_v2.subscribes_to_event_types.clone(),
    ).await.unwrap();
    println!("Registered agent v2.0");
    
    // Send some v2.0 events
    let v2_event = events::filesystem_chaos_event("file.deleted", "/test/path", Some("2.0.0"));
    
    queries::insert_event(&pool, &v2_event).await.unwrap();
    println!("Sent v2.0 event");
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Now try to "downgrade" to v1.0 (different capabilities, schema)
    let manifest_v1 = AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Version chaos test agent v1".to_string()),
        version: "1.0.0".to_string(),
        status: "running".to_string(),
        agent_type: "filesystem".to_string(),
        config_template_json: Some(json!({
            "type": "object",
            "properties": {
                "paths": {"type": "array"}
                // No new_feature property
            }
        })),
        produces_event_types: Some(json!(["file.created", "file.modified"])), // No file.deleted
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["read", "write"])), // No delete
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    // Try to send v1.0 event with old capabilities
    let v1_event = events::filesystem_chaos_event("file.deleted", "/test/path", Some("1.0.0"));
    
    match queries::upsert_agent_manifest(
        &pool,
        &manifest_v1.agent_name,
        &manifest_v1.version,
        &manifest_v1.status,
        &manifest_v1.agent_type,
        manifest_v1.description.as_deref(),
        manifest_v1.produces_event_types.clone(),
        manifest_v1.subscribes_to_event_types.clone(),
    ).await {
        Ok(_) => {
            println!("Agent downgrade succeeded - checking for issues");
            
            // Check what version is actually registered
            let current_agents = sqlx::query!(
                "SELECT agent_name, version FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).fetch_all(&pool).await.unwrap();
            
            println!("Agents after downgrade attempt: {}", current_agents.len());
            for agent in &current_agents {
                println!("  Agent: {} v{}", agent.agent_name, agent.version);
            }
            
            if current_agents.len() > 1 {
                println!("VERSION CHAOS: Multiple versions of same agent registered!");
            }
            
            match queries::insert_event(&pool, &v1_event).await {
                Ok(_) => {
                    println!("COMPATIBILITY ISSUE: v1.0 agent sent event type it doesn't support!");
                }
                Err(e) => {
                    println!("v1.0 event rejected (good): {}", e);
                }
            }
        }
        Err(e) => {
            println!("Agent downgrade rejected: {}", e);
        }
    }
}

#[tokio::test]
async fn test_concurrent_agent_status_updates() {
    let pool = create_test_db_pool().await.unwrap();
    
    let agent_name = "status-chaos-agent";
    
    // Register agent
    let manifest = AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Status chaos test agent".to_string()),
        version: "1.0.0".to_string(),
        status: "running".to_string(),
        agent_type: "filesystem".to_string(),
        config_template_json: None,
        produces_event_types: Some(json!(["file.created"])),
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["read"])),
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    queries::upsert_agent_manifest(
        &pool,
        &manifest.agent_name,
        &manifest.version,
        &manifest.status,
        &manifest.agent_type,
        manifest.description.as_deref(),
        manifest.produces_event_types.clone(),
        manifest.subscribes_to_event_types.clone(),
    ).await.unwrap();
    
    let mut handles = vec![];
    let status_updates = Arc::new(AtomicU64::new(0));
    
    // Multiple workers try to update agent status simultaneously
    let statuses = vec![
        "running",
        "stopped",
        "error_state",
        "running",
        "degraded",
    ];
    
    for (i, status) in statuses.iter().enumerate() {
        let pool_clone = pool.clone();
        let update_count = status_updates.clone();
        let status_str = status.to_string();
        
        let handle = tokio::spawn(async move {
            // Try to update status
            let result = sqlx::query!(
                r#"
                UPDATE sinex_schemas.agent_manifests 
                SET status = $2, last_heartbeat_ts = $3, updated_at = $4
                WHERE agent_name = $1
                "#,
                agent_name,
                status_str,
                Utc::now(),
                Utc::now()
            ).execute(&pool_clone).await;
            
            match result {
                Ok(rows) => {
                    if rows.rows_affected() > 0 {
                        update_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} updated status to {}", i, status_str);
                    }
                }
                Err(e) => {
                    println!("Worker {} failed to update status: {}", i, e);
                }
            }
            
            // Add some processing delay to increase race condition chances
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    // Check final status
    let final_agent = sqlx::query!(
        "SELECT agent_name, status FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    ).fetch_one(&pool).await.unwrap();
    
    let total_updates = status_updates.load(Ordering::SeqCst);
    println!("Status update chaos results:");
    println!("- Total successful updates: {}", total_updates);
    println!("- Final status: {}", final_agent.status);
    
    // The final status is essentially random due to race conditions
    // This test exposes lost update problems in agent status management
}

#[tokio::test]
async fn test_agent_zombie_heartbeat_scenario() {
    let pool = create_test_db_pool().await.unwrap();
    
    let agent_name = "zombie-agent";
    
    // Register agent
    let manifest = AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Zombie test agent".to_string()),
        version: "1.0.0".to_string(),
        status: "running".to_string(),
        agent_type: "filesystem".to_string(),
        config_template_json: None,
        produces_event_types: Some(json!(["file.created"])),
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["read"])),
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    queries::upsert_agent_manifest(
        &pool,
        &manifest.agent_name,
        &manifest.version,
        &manifest.status,
        &manifest.agent_type,
        manifest.description.as_deref(),
        manifest.produces_event_types.clone(),
        manifest.subscribes_to_event_types.clone(),
    ).await.unwrap();
    
    // Simulate agent that stops sending heartbeats but doesn't unregister
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // New agent with same name tries to register (recovery scenario)
    let recovery_manifest = AgentManifest {
        agent_name: agent_name.to_string(),
        description: Some("Zombie test agent (recovered)".to_string()),
        version: "1.0.1".to_string(), // Slightly newer
        status: "running".to_string(),
        agent_type: "filesystem".to_string(),
        config_template_json: None,
        produces_event_types: Some(json!(["file.created"])),
        subscribes_to_event_types: None,
        required_capabilities: Some(json!(["read"])),
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    match queries::upsert_agent_manifest(
        &pool,
        &recovery_manifest.agent_name,
        &recovery_manifest.version,
        &recovery_manifest.status,
        &recovery_manifest.agent_type,
        recovery_manifest.description.as_deref(),
        recovery_manifest.produces_event_types.clone(),
        recovery_manifest.subscribes_to_event_types.clone(),
    ).await {
        Ok(_) => {
            println!("Recovery agent registration succeeded");
            
            // Check how many agents exist now
            let agents = sqlx::query!(
                "SELECT agent_name, version, status, last_heartbeat_ts FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
                agent_name
            ).fetch_all(&pool).await.unwrap();
            
            println!("Agents after recovery: {}", agents.len());
            
            if agents.len() > 1 {
                println!("ZOMBIE AGENT DETECTED: Multiple instances of same agent exist!");
                for (i, agent) in agents.iter().enumerate() {
                    println!("  Agent {}: v{} status={} last_heartbeat={:?}", 
                             i, agent.version, agent.status, agent.last_heartbeat_ts);
                }
            }
        }
        Err(e) => {
            println!("Recovery agent registration failed: {}", e);
        }
    }
    
    // Try to send heartbeat from "recovered" agent
    let heartbeat = events::agent_heartbeat_chaos_event(agent_name, Some("1.0.1"));
    
    match queries::insert_event(&pool, &heartbeat).await {
        Ok(_) => {
            println!("Recovery heartbeat accepted");
        }
        Err(e) => {
            println!("Recovery heartbeat failed: {}", e);
        }
    }
}
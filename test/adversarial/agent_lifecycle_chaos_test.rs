use crate::common::create_test_db_pool;
use sinex_db::{queries, models::{RawEvent, AgentManifest, AgentStatus}};
use sinex_ulid::Ulid;
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use std::sync::atomic::{AtomicU64, Ordering};
use futures::future::join_all;

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
                id: Ulid::new(),
                name: agent_name.to_string(),
                version: format!("1.0.{}", instance_id), // Slightly different versions
                status: AgentStatus::Active,
                capabilities: vec!["file.created".to_string(), "file.modified".to_string()],
                config_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "paths": {"type": "array"}
                    }
                })),
                last_heartbeat: chrono::Utc::now(),
            };
            
            match queries::register_agent(&pool_clone, &manifest).await {
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
    let agents = sqlx::query!(
        "SELECT * FROM sinex_schemas.agent_manifests WHERE name = $1",
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
    let heartbeat_event = RawEvent {
        id: Ulid::new(),
        source: "agent".to_string(),
        event_type: "agent.heartbeat".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: serde_json::json!({
            "agent_name": phantom_agent,
            "status": "alive",
            "metrics": {
                "events_processed": 0,
                "uptime_seconds": 10
            }
        }),
    };
    
    // This should either:
    // 1. Fail gracefully
    // 2. Auto-register phantom agent (potential security issue)
    // 3. Create inconsistent state
    match queries::insert_event(&pool, &heartbeat_event).await {
        Ok(_) => {
            println!("Phantom heartbeat was accepted - checking for side effects");
            
            // Check if phantom agent was auto-created
            let phantom_agents = sqlx::query!(
                "SELECT * FROM sinex_schemas.agent_manifests WHERE name = $1",
                phantom_agent
            ).fetch_all(&pool).await.unwrap();
            
            if !phantom_agents.is_empty() {
                println!("SECURITY ISSUE: Phantom agent auto-registered from heartbeat!");
                for agent in phantom_agents {
                    println!("  Phantom agent: {} v{}", agent.name, agent.version);
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
        id: Ulid::new(),
        name: agent_name.to_string(),
        version: "2.0.0".to_string(),
        status: AgentStatus::Active,
        capabilities: vec!["file.created".to_string(), "file.modified".to_string(), "file.deleted".to_string()],
        config_schema: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "paths": {"type": "array"},
                "new_feature": {"type": "boolean"}  // v2.0 feature
            }
        })),
        last_heartbeat: chrono::Utc::now(),
    };
    
    queries::register_agent(&pool, &manifest_v2).await.unwrap();
    println!("Registered agent v2.0");
    
    // Send some v2.0 events
    let v2_event = RawEvent {
        id: Ulid::new(),
        source: "filesystem".to_string(),
        event_type: "file.deleted".to_string(), // v2.0 capability
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: Some("2.0.0".to_string()),
        payload_schema_id: None,
        payload: serde_json::json!({
            "path": "/tmp/deleted.txt",
            "v2_feature_data": true
        }),
    };
    
    queries::insert_event(&pool, &v2_event).await.unwrap();
    println!("Sent v2.0 event");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Now try to "downgrade" to v1.0 (different capabilities, schema)
    let manifest_v1 = AgentManifest {
        id: Ulid::new(),
        name: agent_name.to_string(),
        version: "1.0.0".to_string(),
        status: AgentStatus::Active,
        capabilities: vec!["file.created".to_string(), "file.modified".to_string()], // No file.deleted
        config_schema: Some(serde_json::json!({
            "type": "object",
            "properties": {
                "paths": {"type": "array"}
                // No new_feature property
            }
        })),
        last_heartbeat: chrono::Utc::now(),
    };
    
    match queries::register_agent(&pool, &manifest_v1).await {
        Ok(_) => {
            println!("Agent downgrade succeeded - checking for issues");
            
            // Check what version is actually registered
            let current_agents = sqlx::query!(
                "SELECT * FROM sinex_schemas.agent_manifests WHERE name = $1",
                agent_name
            ).fetch_all(&pool).await.unwrap();
            
            println!("Agents after downgrade attempt: {}", current_agents.len());
            for agent in &current_agents {
                println!("  Agent: {} v{}", agent.name, agent.version);
            }
            
            if current_agents.len() > 1 {
                println!("VERSION CHAOS: Multiple versions of same agent registered!");
            }
            
            // Try to send v1.0 event with old capabilities
            let v1_event = RawEvent {
                id: Ulid::new(),
                source: "filesystem".to_string(),
                event_type: "file.deleted".to_string(), // This capability no longer exists in v1.0
                ts_ingest: chrono::Utc::now(),
                ts_orig: None,
                host: "test".to_string(),
                ingestor_version: Some("1.0.0".to_string()),
                payload_schema_id: None,
                payload: serde_json::json!({
                    "path": "/tmp/another_deleted.txt"
                }),
            };
            
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
        id: Ulid::new(),
        name: agent_name.to_string(),
        version: "1.0.0".to_string(),
        status: AgentStatus::Active,
        capabilities: vec!["file.created".to_string()],
        config_schema: None,
        last_heartbeat: chrono::Utc::now(),
    };
    
    queries::register_agent(&pool, &manifest).await.unwrap();
    
    let mut handles = vec![];
    let status_updates = Arc::new(AtomicU64::new(0));
    
    // Multiple workers try to update agent status simultaneously
    let statuses = vec![
        AgentStatus::Active,
        AgentStatus::Inactive,
        AgentStatus::Error,
        AgentStatus::Active,
        AgentStatus::Inactive,
    ];
    
    for (i, status) in statuses.iter().enumerate() {
        let pool_clone = pool.clone();
        let update_count = status_updates.clone();
        let status_clone = status.clone();
        
        let handle = tokio::spawn(async move {
            // Try to update status
            let result = sqlx::query!(
                r#"
                UPDATE sinex_schemas.agent_manifests 
                SET status = $2, last_heartbeat = $3
                WHERE name = $1
                "#,
                agent_name,
                status_clone as AgentStatus,
                chrono::Utc::now()
            ).execute(&pool_clone).await;
            
            match result {
                Ok(rows) => {
                    if rows.rows_affected() > 0 {
                        update_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} updated status to {:?}", i, status_clone);
                    }
                }
                Err(e) => {
                    println!("Worker {} failed to update status: {}", i, e);
                }
            }
            
            // Add some processing delay to increase race condition chances
            tokio::time::sleep(Duration::from_millis(10)).await;
        });
        
        handles.push(handle);
    }
    
    join_all(handles).await;
    
    // Check final status
    let final_agent = sqlx::query!(
        "SELECT * FROM sinex_schemas.agent_manifests WHERE name = $1",
        agent_name
    ).fetch_one(&pool).await.unwrap();
    
    let total_updates = status_updates.load(Ordering::SeqCst);
    println!("Status update chaos results:");
    println!("- Total successful updates: {}", total_updates);
    println!("- Final status: {:?}", final_agent.status);
    
    // The final status is essentially random due to race conditions
    // This test exposes lost update problems in agent status management
}

#[tokio::test]
async fn test_agent_zombie_heartbeat_scenario() {
    let pool = create_test_db_pool().await.unwrap();
    
    let agent_name = "zombie-agent";
    
    // Register agent
    let manifest = AgentManifest {
        id: Ulid::new(),
        name: agent_name.to_string(),
        version: "1.0.0".to_string(),
        status: AgentStatus::Active,
        capabilities: vec!["file.created".to_string()],
        config_schema: None,
        last_heartbeat: chrono::Utc::now(),
    };
    
    queries::register_agent(&pool, &manifest).await.unwrap();
    
    // Simulate agent that stops sending heartbeats but doesn't unregister
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // New agent with same name tries to register (recovery scenario)
    let recovery_manifest = AgentManifest {
        id: Ulid::new(),
        name: agent_name.to_string(),
        version: "1.0.1".to_string(), // Slightly newer
        status: AgentStatus::Active,
        capabilities: vec!["file.created".to_string()],
        config_schema: None,
        last_heartbeat: chrono::Utc::now(),
    };
    
    match queries::register_agent(&pool, &recovery_manifest).await {
        Ok(_) => {
            println!("Recovery agent registration succeeded");
            
            // Check how many agents exist now
            let agents = sqlx::query!(
                "SELECT * FROM sinex_schemas.agent_manifests WHERE name = $1",
                agent_name
            ).fetch_all(&pool).await.unwrap();
            
            println!("Agents after recovery: {}", agents.len());
            
            if agents.len() > 1 {
                println!("ZOMBIE AGENT DETECTED: Multiple instances of same agent exist!");
                for (i, agent) in agents.iter().enumerate() {
                    println!("  Agent {}: v{} status={:?} last_heartbeat={:?}", 
                             i, agent.version, agent.status, agent.last_heartbeat);
                }
            }
        }
        Err(e) => {
            println!("Recovery agent registration failed: {}", e);
        }
    }
    
    // Try to send heartbeat from "recovered" agent
    let heartbeat = RawEvent {
        id: Ulid::new(),
        source: "agent".to_string(),
        event_type: "agent.heartbeat".to_string(),
        ts_ingest: chrono::Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: Some("1.0.1".to_string()),
        payload_schema_id: None,
        payload: serde_json::json!({
            "agent_name": agent_name,
            "status": "alive",
            "version": "1.0.1"
        }),
    };
    
    match queries::insert_event(&pool, &heartbeat).await {
        Ok(_) => {
            println!("Recovery heartbeat accepted");
        }
        Err(e) => {
            println!("Recovery heartbeat failed: {}", e);
        }
    }
}
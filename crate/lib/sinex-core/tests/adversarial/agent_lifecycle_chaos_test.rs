// # Agent Lifecycle Chaos Tests
//
// Tests for automaton registration, heartbeat, and lifecycle operations under chaos conditions.
// Simulates concurrent registrations, network failures, and lifecycle state conflicts.

use chrono::Utc;
use futures::future::join_all;
use serde_json::json;
use sinex_core::db::models::AutomatonManifest;
use sinex_test_utils::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Test multiple agent instances registering simultaneously
#[sinex_test]
async fn test_agent_registering_from_multiple_instances(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();

    let processor_name = "chaos-agent";
    let successful_registrations = Arc::new(AtomicU64::new(0));
    let failed_registrations = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // 10 instances try to register the same agent simultaneously
    for instance_id in 0..10 {
        let pool_clone = pool.clone();
        let success_count = successful_registrations.clone();
        let fail_count = failed_registrations.clone();

        let handle = tokio::spawn(async move {
            match sinex_core::db::upsert_automaton_manifest(
                &pool_clone,
                processor_name,
                &format!("1.0.{}", instance_id),
                Some(&format!("Chaos agent instance {}", instance_id)),
                "fs",
                json!({
                    "type": "object",
                    "properties": {
                        "paths": {"type": "array"}
                    }
                }),
                json!(["file.created", "file.modified"]),
                json!([]),
                json!(["read", "write"]),
            )
            .await
            {
                Ok(_) => {
                    println!(
                        "Instance {} successfully registered agent {}",
                        instance_id, processor_name
                    );
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!(
                        "Instance {} failed to register agent {}: {}",
                        instance_id, processor_name, e
                    );
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
        r#"
        SELECT COUNT(*) as "count!"
        FROM core.processor_manifests
        WHERE processor_name = $1 AND node_type = 'automaton'
        "#,
        processor_name
    )
    .fetch_one(ctx.pool())
    .await?;

    println!("Agents in database: {}", agents.count);

    // The system should handle concurrent registration gracefully
    assert!(successes > 0, "At least one registration should succeed");
    assert!(agents.count > 0, "Agent should be registered in database");

    Ok(())
}

/// Test agent heartbeat chaos with network failures
#[sinex_test]
async fn test_agent_heartbeat_chaos_with_network_failures(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let processor_name = "heartbeat-chaos-agent";

    // Register initial agent
    sinex_core::db::upsert_automaton_manifest(
        &pool,
        processor_name,
        "1.0.0",
        Some("Heartbeat chaos test agent"),
        "test",
        json!({}),
        json!(["test.event"]),
        json!([]),
        json!(["test"]),
    )
    .await?;

    let successful_heartbeats = Arc::new(AtomicU64::new(0));
    let failed_heartbeats = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Simulate multiple heartbeat attempts with intermittent failures
    for heartbeat_id in 0..20 {
        let pool_clone = pool.clone();
        let success_count = successful_heartbeats.clone();
        let fail_count = failed_heartbeats.clone();

        let handle = tokio::spawn(async move {
            // Simulate network instability - some heartbeats fail
            if heartbeat_id % 3 == 0 {
                // Simulate network failure
                println!("Heartbeat {} simulated network failure", heartbeat_id);
                fail_count.fetch_add(1, Ordering::SeqCst);
                return;
            }

            // Attempt heartbeat update
            match sqlx::query!(
                "UPDATE core.processor_manifests
                 SET last_heartbeat_ts = $1, updated_at = $2
                 WHERE processor_name = $3 AND node_type = 'automaton'",
                Utc::now(),
                Utc::now(),
                processor_name
            )
            .execute(&pool_clone)
            .await
            {
                Ok(_) => {
                    println!("Heartbeat {} successful", heartbeat_id);
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
                Err(e) => {
                    println!("Heartbeat {} failed: {}", heartbeat_id, e);
                    fail_count.fetch_add(1, Ordering::SeqCst);
                }
            }

            // Small delay between heartbeats
            tokio::time::sleep(Duration::from_millis(50)).await;
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let successes = successful_heartbeats.load(Ordering::SeqCst);
    let failures = failed_heartbeats.load(Ordering::SeqCst);

    println!("Heartbeat chaos results:");
    println!("- Successful heartbeats: {}", successes);
    println!("- Failed heartbeats: {}", failures);

    // System should handle heartbeat failures gracefully
    assert!(successes > 0, "Some heartbeats should succeed");
    assert!(failures > 0, "Some heartbeats should fail (simulated)");

    Ok(())
}

/// Test agent lifecycle during concurrent operations
#[sinex_test]
async fn test_agent_lifecycle_during_concurrent_operations(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let base_processor_name = "lifecycle-chaos";

    let registration_count = Arc::new(AtomicU64::new(0));
    let heartbeat_count = Arc::new(AtomicU64::new(0));
    let deregistration_count = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Simulate chaotic agent lifecycle operations
    for agent_id in 0..10 {
        let pool_clone = pool.clone();
        let reg_count = registration_count.clone();
        let hb_count = heartbeat_count.clone();
        let dereg_count = deregistration_count.clone();
        let processor_name = format!("{}-{}", base_processor_name, agent_id);

        let handle = tokio::spawn(async move {
            // Register agent
            match sinex_core::db::upsert_automaton_manifest(
                &pool_clone,
                &processor_name,
                "1.0.0",
                Some("Chaos lifecycle agent"),
                "test",
                json!({}),
                json!(["test.event"]),
                json!([]),
                json!(["test"]),
            )
            .await
            {
                Ok(_) => {
                    reg_count.fetch_add(1, Ordering::SeqCst);
                    println!("Agent {} registered", processor_name);
                }
                Err(e) => {
                    println!("Agent {} registration failed: {}", processor_name, e);
                    return;
                }
            }

            // Send some heartbeats
            for _ in 0..3 {
                match sqlx::query!(
                    "UPDATE core.processor_manifests
                     SET last_heartbeat_ts = $1, updated_at = $2
                     WHERE processor_name = $3 AND node_type = 'automaton'",
                    Utc::now(),
                    Utc::now(),
                    processor_name
                )
                .execute(&pool_clone)
                .await
                {
                    Ok(_) => {
                        hb_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(e) => {
                        println!("Heartbeat failed for {}: {}", processor_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Deregister agent
            match sqlx::query!(
                "DELETE FROM core.processor_manifests WHERE processor_name = $1 AND node_type = 'automaton'",
                processor_name
            )
            .execute(&pool_clone)
            .await
            {
                Ok(_) => {
                    dereg_count.fetch_add(1, Ordering::SeqCst);
                    println!("Agent {} deregistered", processor_name);
                }
                Err(e) => {
                    println!("Agent {} deregistration failed: {}", processor_name, e);
                }
            }
        });

        handles.push(handle);
    }

    join_all(handles).await;

    let registrations = registration_count.load(Ordering::SeqCst);
    let heartbeats = heartbeat_count.load(Ordering::SeqCst);
    let deregistrations = deregistration_count.load(Ordering::SeqCst);

    println!("Agent lifecycle chaos results:");
    println!("- Registrations: {}", registrations);
    println!("- Heartbeats: {}", heartbeats);
    println!("- Deregistrations: {}", deregistrations);

    // Verify final database state
    let remaining_agents = sqlx::query!(
        r#"SELECT COUNT(*) as "count!" FROM core.processor_manifests WHERE processor_name LIKE $1 AND node_type = 'automaton'"#,
        format!("{}%", base_processor_name)
    )
    .fetch_one(ctx.pool())
    .await?;

    println!("Remaining agents in database: {}", remaining_agents.count);

    // Most operations should succeed despite chaos
    assert!(registrations >= 5, "Most registrations should succeed");
    assert!(heartbeats >= 10, "Most heartbeats should succeed");
    assert!(deregistrations >= 5, "Most deregistrations should succeed");

    Ok(())
}

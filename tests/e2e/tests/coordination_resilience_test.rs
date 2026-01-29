//! Distributed Coordination Resilience Tests
//!
//! These tests verify the distributed coordination mechanisms used for
//! work distribution, including advisory locking and node coordination.
//!
//! ## Coverage Areas
//! - Advisory lock acquisition and recovery
//! - Concurrent coordination under stress
//! - Node instance registration and heartbeat tracking

use sinex_primitives::coordination::kv_client::{CoordinationKvClient, InstanceMetadata};
use sinex_primitives::environment::environment;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use xtask::sandbox::nats::ensure_coordination_buckets;

use sinex_primitives::db::advisory_lock::AdvisoryLock;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::WaitHelpers;

// =============================================================================
// Advisory Lock Tests
// =============================================================================

/// Test that advisory locks can be acquired and released.
#[sinex_test]
async fn test_advisory_lock_acquire_release(ctx: TestContext) -> Result<()> {
    let lock_key = format!("test-lock-{}", uuid::Uuid::new_v4());

    // Acquire lock
    let lock_guard = AdvisoryLock::try_acquire(&ctx.pool, &lock_key)
        .await?
        .expect("Should acquire lock");

    // Verify lock is held
    let is_locked = AdvisoryLock::is_locked(&ctx.pool, &lock_key).await?;
    assert!(is_locked, "Lock should be held after acquisition");

    // Release lock
    drop(lock_guard);

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let key = lock_key.clone();
            async move {
                Ok::<bool, xtask::sandbox::SinexError>(
                    !AdvisoryLock::is_locked(&pool, &key).await?,
                )
            }
        },
        5,
    )
    .await?;

    // Verify lock is released (should be able to acquire again)
    let lock_guard2 = AdvisoryLock::try_acquire(&ctx.pool, &lock_key).await?;
    assert!(
        lock_guard2.is_some(),
        "Should be able to acquire lock after release"
    );

    Ok(())
}

/// Test that concurrent lock attempts are properly serialized.
#[sinex_test]
async fn test_advisory_lock_concurrent_acquisition(ctx: TestContext) -> Result<()> {
    let lock_key = format!("concurrent-lock-{}", uuid::Uuid::new_v4());
    let acquired_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Spawn multiple tasks trying to acquire the same lock
    for task_id in 0..10 {
        let pool = ctx.pool.clone();
        let key = lock_key.clone();
        let count = acquired_count.clone();

        let handle = tokio::spawn(async move {
            match AdvisoryLock::try_acquire(&pool, &key).await {
                Ok(Some(_guard)) => {
                    count.fetch_add(1, Ordering::SeqCst);
                    tracing::info!("Task {} acquired lock", task_id);
                    // Yield to let other tasks contend deterministically.
                    tokio::task::yield_now().await;
                    // Lock released when guard drops
                }
                Ok(None) => {
                    tracing::info!("Task {} could not acquire lock (already held)", task_id);
                }
                Err(e) => {
                    tracing::warn!("Task {} error: {}", task_id, e);
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all attempts
    for handle in handles {
        let _ = handle.await;
    }

    let acquisitions = acquired_count.load(Ordering::SeqCst);

    // At most one task can hold the lock at a time, but over time
    // multiple tasks may acquire it sequentially
    assert!(
        acquisitions >= 1,
        "At least one task should have acquired the lock"
    );

    tracing::info!("Total lock acquisitions: {}", acquisitions);

    Ok(())
}

/// Test advisory lock timeout behavior.
#[sinex_test]
async fn test_advisory_lock_timeout(ctx: TestContext) -> Result<()> {
    let lock_key = format!("timeout-lock-{}", uuid::Uuid::new_v4());

    // Acquire lock first
    let _lock_guard = AdvisoryLock::try_acquire(&ctx.pool, &lock_key)
        .await?
        .expect("Should acquire lock");

    // Try to acquire with timeout from another task
    let pool = ctx.pool.clone();
    let key = lock_key.clone();

    let timeout_result = tokio::spawn(async move {
        AdvisoryLock::acquire_or_wait(&pool, &key, Duration::from_millis(100)).await
    })
    .await?;

    // Should timeout since lock is held
    assert!(
        timeout_result.is_err(),
        "Should timeout when lock is held by another"
    );

    Ok(())
}

// =============================================================================
// Node Instance Coordination Tests
// =============================================================================

/// Test that node instances can be registered and tracked using NATS KV.
#[sinex_test]
async fn test_node_instance_registration(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("test-node-{}", uuid::Uuid::new_v4());
    let instance_id = uuid::Uuid::new_v4().to_string();

    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());

    let metadata = InstanceMetadata {
        instance_id: instance_id.clone(),
        hostname: "test-host".to_string(),
        version: "1.0.0".to_string(),
        started_at: crate::temporal::now().timestamp(),
        last_heartbeat: crate::temporal::now().timestamp(),
    };

    // Register instance
    kv_client.register_instance(&metadata).await?;

    // Verify registration by reading bucket directly
    let env = environment();
    let bucket = js
        .get_key_value(&format!(
            "KV_{}",
            env.nats_kv_bucket_name("sinex_instances")
        ))
        .await?;
    let key = format!("{}.{}", service_name, instance_id);
    let entry = bucket.entry(&key).await?;

    assert!(entry.is_some(), "Instance should be registered in KV");
    let entry = entry.unwrap();
    let stored_meta: InstanceMetadata = serde_json::from_slice(&entry.value)?;

    assert_eq!(stored_meta.instance_id, instance_id);
    assert_eq!(stored_meta.version, "1.0.0");

    // Update heartbeat (re-register)
    let mut updated_meta = metadata.clone();
    updated_meta.hostname = "updated-host".to_string();
    kv_client.heartbeat(&instance_id, &updated_meta).await?;

    let entry = bucket.entry(&key).await?.unwrap();
    let stored_meta: InstanceMetadata = serde_json::from_slice(&entry.value)?;
    assert_eq!(stored_meta.hostname, "updated-host");

    Ok(())
}

/// Test that multiple node instances can coexist in KV.
#[sinex_test]
async fn test_multiple_node_instances(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("multi-instance-{}", uuid::Uuid::new_v4());
    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());
    let mut instance_ids = vec![];

    // Register multiple instances
    for i in 0..5 {
        let instance_id = uuid::Uuid::new_v4().to_string();
        instance_ids.push(instance_id.clone());

        let metadata = InstanceMetadata {
            instance_id: instance_id.clone(),
            hostname: format!("host-{}", i),
            version: format!("1.0.{}", i),
            started_at: crate::temporal::now().timestamp(),
            last_heartbeat: crate::temporal::now().timestamp(),
        };

        kv_client.register_instance(&metadata).await?;
    }

    // Verify all instances exist in KV
    let env = environment();
    let bucket = js
        .get_key_value(&format!(
            "KV_{}",
            env.nats_kv_bucket_name("sinex_instances")
        ))
        .await?;

    // KV bucket keys method usually requires listing or watching.
    // For test simplicity, we just check each key directly.
    for (i, instance_id) in instance_ids.iter().enumerate() {
        let key = format!("{}.{}", service_name, instance_id);
        let entry = bucket.entry(&key).await?;
        assert!(entry.is_some(), "Instance {} should exist", i);

        let meta: InstanceMetadata = serde_json::from_slice(&entry.unwrap().value)?;
        assert_eq!(meta.version, format!("1.0.{}", i));
    }

    Ok(())
}

/// Test that heartbeats update KV entry revisions.
#[sinex_test]
async fn test_heartbeat_revision_update(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("stale-heartbeat-{}", uuid::Uuid::new_v4());
    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());
    let env = environment();
    let bucket = js
        .get_key_value(&format!(
            "KV_{}",
            env.nats_kv_bucket_name("sinex_instances")
        ))
        .await?;

    let fresh_id = uuid::Uuid::new_v4().to_string();
    let stale_id = uuid::Uuid::new_v4().to_string();

    let meta_fresh = InstanceMetadata {
        instance_id: fresh_id.clone(),
        hostname: "fresh".to_string(),
        version: "1.0.0".to_string(),
        started_at: crate::temporal::now().timestamp(),
        last_heartbeat: crate::temporal::now().timestamp(),
    };
    let meta_stale = InstanceMetadata {
        instance_id: stale_id.clone(),
        hostname: "stale".to_string(),
        version: "1.0.0".to_string(),
        started_at: crate::temporal::now().timestamp(),
        last_heartbeat: crate::temporal::now().timestamp(),
    };

    kv_client.register_instance(&meta_fresh).await?;
    kv_client.register_instance(&meta_stale).await?;

    // Capture initial state
    let entry_fresh_1 = bucket
        .entry(&format!("{}.{}", service_name, fresh_id))
        .await?
        .unwrap();
    let entry_stale_1 = bucket
        .entry(&format!("{}.{}", service_name, stale_id))
        .await?
        .unwrap();

    sleep(Duration::from_millis(100)).await;

    // Heartbeat fresh only (note: 100ms is intentional timing for test, not from Timeouts)
    kv_client.heartbeat(&fresh_id, &meta_fresh).await?;

    // Verify fresh has new revision, stale is same
    let entry_fresh_2 = bucket
        .entry(&format!("{}.{}", service_name, fresh_id))
        .await?
        .unwrap();
    let entry_stale_2 = bucket
        .entry(&format!("{}.{}", service_name, stale_id))
        .await?
        .unwrap();

    assert!(
        entry_fresh_2.revision > entry_fresh_1.revision,
        "Fresh instance should have updated revision"
    );
    assert_eq!(
        entry_stale_2.revision, entry_stale_1.revision,
        "Stale instance should unchanged revision"
    );

    Ok(())
}

// =============================================================================
// Service Leadership Tests
// =============================================================================

/// Test that leadership can be acquired and maintained via CAS.
#[sinex_test]
async fn test_service_leadership_acquisition(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("leadership-test-{}", uuid::Uuid::new_v4());
    let instance_id = uuid::Uuid::new_v4().to_string();

    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());

    // Acquire leadership
    let acquired = kv_client.acquire_leadership(&instance_id).await?;
    assert!(acquired, "Should acquire leadership initially");

    // Verify leadership
    let attempt_2 = kv_client.acquire_leadership(&instance_id).await?;
    assert!(attempt_2, "Should still have leadership (idempotent)");

    let other_id = uuid::Uuid::new_v4().to_string();
    let acquired_other = kv_client.acquire_leadership(&other_id).await?;
    assert!(
        !acquired_other,
        "Other instance should NOT acquire leadership"
    );

    Ok(())
}

/// Test leadership transfer between instances via release.
#[sinex_test]
async fn test_leadership_transfer(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("transfer-test-{}", uuid::Uuid::new_v4());
    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());

    let old_leader_id = uuid::Uuid::new_v4().to_string();
    let new_leader_id = uuid::Uuid::new_v4().to_string();

    // 1. Old leader acquires
    let acquired = kv_client.acquire_leadership(&old_leader_id).await?;
    assert!(acquired);

    // 2. New leader tries -> fails
    let acquired_new = kv_client.acquire_leadership(&new_leader_id).await?;
    assert!(!acquired_new);

    // 3. Old leader releases
    kv_client.release_leadership(&old_leader_id).await?;

    // 4. New leader tries -> succeeds
    let acquired_new_2 = kv_client.acquire_leadership(&new_leader_id).await?;
    assert!(
        acquired_new_2,
        "Leadership should be available after release"
    );

    Ok(())
}

// =============================================================================
// Concurrent Coordination Stress Tests
// =============================================================================

/// Test coordination mechanisms under concurrent load (KV).
#[sinex_test]
async fn test_concurrent_coordination_stress(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let nats = ctx.nats_handle()?;
    // Share client? Or new client per task? Real nodes have own clients.
    // For test perf, we can share NATS connection but new KV clients.

    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    let service_name = format!("stress-coord-{}", uuid::Uuid::new_v4());
    let registration_count = Arc::new(AtomicU32::new(0));
    let leadership_count = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];

    // Spawn multiple tasks
    for task_id in 0..10 {
        let nats = nats.clone();
        let service = service_name.clone();
        let registrations = registration_count.clone();
        let leaderships = leadership_count.clone();

        let handle = tokio::spawn(async move {
            let client = nats.connect().await.unwrap();
            let js = async_nats::jetstream::new(client);
            let kv_client = CoordinationKvClient::new(js, service);

            let instance_id = uuid::Uuid::new_v4().to_string();

            let meta = InstanceMetadata {
                instance_id: instance_id.clone(),
                hostname: format!("host-{}", task_id),
                version: "1.0.0".to_string(),
                started_at: crate::temporal::now().timestamp(),
                last_heartbeat: crate::temporal::now().timestamp(),
            };

            // Register
            if kv_client.register_instance(&meta).await.is_ok() {
                registrations.fetch_add(1, Ordering::SeqCst);

                // Try to acquire leadership
                if let Ok(true) = kv_client.acquire_leadership(&instance_id).await {
                    leaderships.fetch_add(1, Ordering::SeqCst);
                    // Hold for a bit then release?
                    // To maximize contention, let's just hold it.
                    // Only one task should succeed total.
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        let _ = handle.await;
    }

    let registrations = registration_count.load(Ordering::SeqCst);
    let leaderships = leadership_count.load(Ordering::SeqCst);

    tracing::info!(
        "Stress test: {} registrations, {} leaders",
        registrations,
        leaderships
    );

    assert_eq!(
        registrations, 10,
        "All registrations should succeed (KV writes are independent)"
    );
    assert_eq!(leaderships, 1, "Exactly one instance should become leader");

    Ok(())
}

/// Verify that KV coordination works over mTLS.
#[sinex_test]
async fn test_kv_functionality_with_mtls(_ctx: TestContext) -> Result<()> {
    use std::path::PathBuf;
    use xtask::sandbox::EphemeralNats;

    // Use absolute path to the generated fixtures in workspace root
    let fixture_path = PathBuf::from("/realm/project/sinex/tests/fixtures/tls");

    // 1. Start mTLS-enforced NATS
    let nats = EphemeralNats::builder()
        .with_tls_fixtures_path(&fixture_path)
        .start()
        .await?;

    // 2. Connect (harness should load client certs automatically)
    let client = nats.connect().await?;
    let js = async_nats::jetstream::new(client);
    ensure_coordination_buckets(&js).await?;

    // 3. Perform basic coordination ops
    let service_name = "tls-secure-service".to_string();
    let kv_client = CoordinationKvClient::new(js.clone(), service_name.clone());
    let instance_id = "secure-instance-1";

    let meta = InstanceMetadata {
        instance_id: instance_id.to_string(),
        hostname: "secure-host".to_string(),
        version: "0.0.0".to_string(),
        started_at: crate::temporal::now().timestamp(),
        last_heartbeat: crate::temporal::now().timestamp(),
    };

    kv_client.register_instance(&meta).await?;
    let acquired = kv_client.acquire_leadership(instance_id).await?;

    assert!(acquired, "Should acquire leadership over mTLS");
    tracing::info!("mTLS KV coordination verified!");

    Ok(())
}

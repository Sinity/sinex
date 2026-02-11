// # System Stress Testing
//
// Focused stress tests that exercise production checkpoint persistence and
// event ingestion under concurrent load.

use sinex_db::DbPoolExt;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::ulid::Ulid;
use sinex_primitives::{
    Event, EventSource, HostName, Id, OffsetKind, Provenance, SourceMaterial, Timestamp,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use xtask::sandbox::prelude::*;

const STRESS_GROUP: &str = "stress";

#[sinex_test(timeout = 120)]
async fn test_checkpoint_kv_stress_load(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let kv = ctx.checkpoint_kv().await?;
    let processor = format!(
        "stress_processor_{}",
        Ulid::new().to_string().to_lowercase()
    );

    let consumer_count = 16usize;
    let updates_per_consumer = 40u64;
    let total_updates = consumer_count as u64 * updates_per_consumer;

    let successes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    let mut handles = Vec::new();

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor.clone(),
            STRESS_GROUP.to_string(),
            format!("worker-{consumer_id}"),
        );
        let successes = successes.clone();
        handles.push(tokio::spawn(async move {
            for update in 0..updates_per_consumer {
                let mut state = CheckpointState::default();
                state.checkpoint = Checkpoint::internal(Ulid::new(), update + 1);
                state.processed_count = update + 1;
                state.last_activity = Timestamp::now();
                if manager.save_checkpoint(&state).await.is_ok() {
                    successes.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    futures::future::join_all(handles).await;

    let duration = start.elapsed();
    let successful = successes.load(Ordering::Relaxed);
    println!("Checkpoint KV stress: {successful} updates in {duration:?}");

    assert_eq!(
        successful, total_updates,
        "all checkpoint updates should succeed"
    );

    for consumer_id in 0..consumer_count {
        let manager = CheckpointManager::new(
            kv.clone(),
            processor.clone(),
            STRESS_GROUP.to_string(),
            format!("worker-{consumer_id}"),
        );
        let state = manager.load_checkpoint().await?;
        assert_eq!(
            state.processed_count, updates_per_consumer,
            "consumer {consumer_id} should report full progress"
        );
    }

    Ok(())
}

#[sinex_test(timeout = 120)]
async fn test_event_ingestion_stress(ctx: TestContext) -> TestResult<()> {
    let pool = ctx.pool().clone();
    let total_events = 200usize;

    // First, create source materials for all events (required for FK constraints)
    let material_ids: Vec<Id<SourceMaterial>> = (0..total_events).map(|_| Id::new()).collect();
    for material_id in &material_ids {
        ctx.ensure_source_material(*material_id, Some("stress.ingestion"))
            .await?;
    }

    let mut handles = Vec::new();

    for (i, material_id) in material_ids.into_iter().enumerate() {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let event = Event {
                id: None,
                source: EventSource::new("stress.ingestion"),
                event_type: sinex_primitives::EventType::new("bulk_load"),
                payload: serde_json::json!({"sequence": i}),
                ts_orig: Some(Timestamp::now()),
                host: HostName::new("localhost"),
                ingestor_version: Some("1.0.0".to_string()),
                payload_schema_id: None,
                provenance: Provenance::Material {
                    id: material_id,
                    anchor_byte: 0,
                    offset_start: None,
                    offset_end: None,
                    offset_kind: OffsetKind::Byte,
                },
                associated_blob_ids: None,
            };
            pool.events().insert(event).await
        }));
    }

    for handle in handles {
        handle.await??;
    }

    let inserted: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE source = $1",
        "stress.ingestion"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    assert!(
        inserted >= total_events as i64,
        "expected at least {total_events} events, got {inserted}"
    );

    sqlx::query!(
        "DELETE FROM core.events WHERE source = $1",
        "stress.ingestion"
    )
    .execute(&pool)
    .await
    .ok();

    Ok(())
}

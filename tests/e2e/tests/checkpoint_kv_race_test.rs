use sinex_primitives::Uuid;
use sinexd::runtime::{Checkpoint, CheckpointManager, CheckpointState};
use std::sync::Arc;
use tokio::sync::Barrier;
use xtask::sandbox::prelude::*;

const RACE_GROUP: &str = "race-group";
const RACE_CONSUMER: &str = "consumer-a";

#[sinex_test]
async fn checkpoint_kv_rejects_stale_revision_regression(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let module = format!("checkpoint-race-{}", Uuid::now_v7());
    let manager = CheckpointManager::new(
        kv,
        module,
        RACE_GROUP.to_string(),
        RACE_CONSUMER.to_string(),
    );

    let mut initial = CheckpointState {
        checkpoint: Checkpoint::internal(Uuid::now_v7(), 1),
        processed_count: 1,
        ..CheckpointState::default()
    };
    let first_revision = manager.save_checkpoint(&initial).await?;

    let advanced = CheckpointState {
        checkpoint: Checkpoint::internal(Uuid::now_v7(), 2),
        processed_count: 2,
        revision: first_revision,
        ..CheckpointState::default()
    };
    let second_revision = manager.save_checkpoint(&advanced).await?;

    initial.revision = first_revision;
    let error = manager
        .save_checkpoint(&initial)
        .await
        .expect_err("stale checkpoint writer must not regress the KV state");

    assert!(
        error
            .to_string()
            .contains("Refusing to overwrite newer checkpoint after CAS conflict"),
        "unexpected stale checkpoint error: {error}"
    );

    let loaded = manager.load_checkpoint().await?;
    assert_eq!(loaded.revision, second_revision);
    assert_eq!(loaded.processed_count, advanced.processed_count);
    assert_eq!(loaded.checkpoint, advanced.checkpoint);

    Ok(())
}

#[sinex_test]
async fn checkpoint_kv_treats_concurrent_identical_create_as_idempotent(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let module = format!("checkpoint-idempotent-{}", Uuid::now_v7());
    let manager_a = CheckpointManager::new(
        kv.clone(),
        module.clone(),
        RACE_GROUP.to_string(),
        RACE_CONSUMER.to_string(),
    );
    let manager_b = CheckpointManager::new(
        kv.clone(),
        module.clone(),
        RACE_GROUP.to_string(),
        RACE_CONSUMER.to_string(),
    );
    let verifier = CheckpointManager::new(
        kv,
        module,
        RACE_GROUP.to_string(),
        RACE_CONSUMER.to_string(),
    );
    let state = CheckpointState {
        checkpoint: Checkpoint::internal(Uuid::now_v7(), 1),
        processed_count: 1,
        ..CheckpointState::default()
    };

    let barrier = Arc::new(Barrier::new(2));
    let state_a = state.clone();
    let barrier_a = barrier.clone();
    let write_a = tokio::spawn(async move {
        barrier_a.wait().await;
        manager_a.save_checkpoint(&state_a).await
    });
    let state_b = state.clone();
    let barrier_b = barrier.clone();
    let write_b = tokio::spawn(async move {
        barrier_b.wait().await;
        manager_b.save_checkpoint(&state_b).await
    });

    let revision_a = write_a.await??;
    let revision_b = write_b.await??;

    assert_eq!(
        revision_a, revision_b,
        "identical create race should resolve to one KV revision"
    );

    let loaded = verifier.load_checkpoint().await?;
    assert_eq!(loaded.processed_count, state.processed_count);
    assert_eq!(loaded.checkpoint, state.checkpoint);
    assert_eq!(loaded.revision, revision_a);

    Ok(())
}

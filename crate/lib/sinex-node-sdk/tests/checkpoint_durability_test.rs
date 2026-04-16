use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::Uuid;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn checkpoint_survives_simulated_crash(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;

    let node_name = format!("durability-test-{}", Uuid::now_v7().simple());
    let mgr = CheckpointManager::new(
        kv.clone(),
        node_name.clone(),
        "default".to_string(),
        "instance-1".to_string(),
    );

    let mut state = CheckpointState::default();
    state.checkpoint = Checkpoint::Timestamp {
        timestamp: sinex_primitives::temporal::Timestamp::now(),
        metadata: None,
    };
    state.processed_count = 500;
    mgr.save_checkpoint(&state).await?;

    state.processed_count = 1000;
    mgr.save_checkpoint(&state).await?;

    drop(mgr);

    let recovered_mgr = CheckpointManager::new(
        kv,
        node_name,
        "default".to_string(),
        "instance-2".to_string(),
    );
    let recovered = recovered_mgr.load_checkpoint().await?;

    ctx.assert("checkpoint survives crash")
        .eq(&recovered.processed_count, &1000u64)?;

    match &recovered.checkpoint {
        Checkpoint::Timestamp { .. } => {}
        other => {
            return Err(
                color_eyre::eyre::eyre!("Expected Timestamp checkpoint, got: {other:?}"),
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn checkpoint_last_save_wins_after_rapid_updates(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;

    let node_name = format!("rapid-test-{}", Uuid::now_v7().simple());
    let mgr = CheckpointManager::new(
        kv.clone(),
        node_name.clone(),
        "default".to_string(),
        "instance-1".to_string(),
    );

    for i in 0..50 {
        let mut state = CheckpointState::default();
        state.processed_count = i;
        state.checkpoint = Checkpoint::Timestamp {
            timestamp: sinex_primitives::temporal::Timestamp::now(),
            metadata: Some(serde_json::json!({"iteration": i})),
        };
        mgr.save_checkpoint(&state).await?;
    }

    drop(mgr);

    let recovered_mgr = CheckpointManager::new(
        kv,
        node_name,
        "default".to_string(),
        "instance-new".to_string(),
    );
    let recovered = recovered_mgr.load_checkpoint().await?;

    ctx.assert("last save wins")
        .eq(&recovered.processed_count, &49u64)?;

    Ok(())
}

#[sinex_test]
async fn checkpoint_fresh_start_when_no_prior_state(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;

    let node_name = format!("fresh-test-{}", Uuid::now_v7().simple());
    let mgr = CheckpointManager::new(
        kv,
        node_name,
        "default".to_string(),
        "instance-1".to_string(),
    );

    let state = mgr.load_checkpoint().await?;

    ctx.assert("fresh start has zero processed")
        .eq(&state.processed_count, &0u64)?;

    match &state.checkpoint {
        Checkpoint::None => {}
        other => {
            return Err(color_eyre::eyre::eyre!(
                "Expected None checkpoint for fresh start, got: {other:?}"
            ));
        }
    }

    Ok(())
}

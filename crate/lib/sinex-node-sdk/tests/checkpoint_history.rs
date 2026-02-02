use sinex_node_sdk::Ulid;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn checkpoint_history_stats_and_reset(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;
    let processor_name = format!("history-test-{}", Ulid::new());
    let consumer_group = "history-group";
    let consumer_name = "history-consumer";

    let kv = ctx.checkpoint_kv().await?;
    let manager = CheckpointManager::new(
        kv,
        processor_name.clone(),
        consumer_group.to_string(),
        consumer_name.to_string(),
    );

    let mut state = CheckpointState::default();
    state.processed_count = 5;
    state.checkpoint = Checkpoint::Stream {
        message_id: "jetstream.1".into(),
        event_id: None,
    };
    manager.save_checkpoint(&state).await?;

    let history = manager.get_checkpoint_history(10).await?;
    ctx.assert("history returns entries")
        .that(!history.is_empty(), "history should not be empty")?;

    let stats = manager.get_checkpoint_stats().await?;
    ctx.assert("stats reflect stored checkpoint")
        .eq(&stats.total_checkpoints, &1)?
        .eq(&stats.max_processed, &5)?;

    manager.reset_checkpoint().await?;
    let empty_history = manager.get_checkpoint_history(5).await?;
    ctx.assert("history cleared after reset")
        .eq(&empty_history.len(), &0usize)?;

    Ok(())
}

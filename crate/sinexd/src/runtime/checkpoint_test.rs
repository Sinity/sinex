// Inline because this covers local checkpoint env/default semantics.
use super::{
    CheckpointCleanupConfig, CheckpointManager, checkpoint_cleanup_cutoff,
    ensure_checkpoint_kv_payload_fits,
};
use crate::runtime::nats_payload::NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES;
use sinex_primitives::prelude::Timestamp;
use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

#[sinex_serial_test]
async fn checkpoint_cleanup_default_is_disabled() -> xtask::sandbox::TestResult<()> {
    assert!(!CheckpointCleanupConfig::default().enabled);
    Ok(())
}

#[sinex_serial_test]
async fn checkpoint_cleanup_from_env_defaults_invalid_overrides()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_CHECKPOINT_CLEANUP_ENABLED", "maybe");
    env.set("SINEX_CHECKPOINT_CLEANUP_MAX_AGE_DAYS", "bogus");
    env.set("SINEX_CHECKPOINT_CLEANUP_INTERVAL_HOURS", "bogus");

    let config = CheckpointCleanupConfig::from_env();
    assert!(!config.enabled);
    assert_eq!(config.max_age.as_secs(), 30 * 24 * 60 * 60);
    assert_eq!(config.interval.as_secs(), 24 * 60 * 60);
    Ok(())
}

#[sinex_serial_test]
async fn checkpoint_cleanup_cutoff_rejects_out_of_range_max_age()
-> xtask::sandbox::TestResult<()> {
    let error = checkpoint_cleanup_cutoff(Timestamp::now(), std::time::Duration::MAX)
        .expect_err("out-of-range cleanup ages must fail honestly");
    assert!(
        error
            .to_string()
            .contains("Checkpoint cleanup max age is out of range")
    );
    Ok(())
}

#[sinex_test]
async fn checkpoint_manager_can_enable_warning_for_missing_checkpoint(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let kv = ctx.with_nats().shared().await?.checkpoint_kv().await?;
    let manager = CheckpointManager::with_missing_checkpoint_warning(
        kv,
        "checkpoint-test-module".to_string(),
        "test-group".to_string(),
        "test-consumer".to_string(),
        true,
    );

    assert!(manager.missing_checkpoint_logs_as_warning());
    Ok(())
}

#[sinex_test]
async fn checkpoint_kv_payload_guard_rejects_oversized_entries()
-> xtask::sandbox::TestResult<()> {
    let error = ensure_checkpoint_kv_payload_fits(
        "oversized.module.consumer",
        NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES + 1,
    )
    .expect_err("oversized checkpoint KV entries must be rejected before NATS publish");

    assert!(
        error
            .to_string()
            .contains("Checkpoint KV payload exceeds NATS max payload")
    );
    Ok(())
}

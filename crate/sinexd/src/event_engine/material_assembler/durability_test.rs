use super::*;
use xtask::sandbox::prelude::*;

fn tiny_policy() -> TestResult<DefaultDurabilityPolicy> {
    Ok(DefaultDurabilityPolicy::new(DurabilityThresholds::try_new(
        8,
        Duration::from_millis(10),
        16,
        3,
        Duration::from_millis(20),
    )?))
}

#[sinex_test]
async fn default_thresholds_match_existing_durability_contract() -> TestResult<()> {
    let thresholds = DurabilityThresholds::default();

    assert_eq!(
        thresholds.staged_file_sync_bytes(),
        DEFAULT_STAGED_FILE_SYNC_BYTES
    );
    assert_eq!(
        thresholds.staged_file_sync_interval(),
        DEFAULT_STAGED_FILE_SYNC_INTERVAL
    );
    assert_eq!(thresholds.wal_sync_bytes(), DEFAULT_WAL_SYNC_BYTES);
    assert_eq!(thresholds.wal_sync_entries(), DEFAULT_WAL_SYNC_ENTRIES);
    assert_eq!(thresholds.wal_sync_interval(), DEFAULT_WAL_SYNC_INTERVAL);
    Ok(())
}

#[sinex_test]
async fn thresholds_reject_zero_or_negative_values() -> TestResult<()> {
    assert!(
        DurabilityThresholds::try_new(0, Duration::from_secs(1), 1, 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        DurabilityThresholds::try_new(-1, Duration::from_secs(1), 1, 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        DurabilityThresholds::try_new(1, Duration::ZERO, 1, 1, Duration::from_secs(1)).is_err()
    );
    assert!(
        DurabilityThresholds::try_new(1, Duration::from_secs(1), 0, 1, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        DurabilityThresholds::try_new(1, Duration::from_secs(1), 1, 0, Duration::from_secs(1))
            .is_err()
    );
    assert!(
        DurabilityThresholds::try_new(1, Duration::from_secs(1), 1, 1, Duration::ZERO).is_err()
    );
    Ok(())
}

#[sinex_test]
async fn wal_decision_skips_below_thresholds() -> TestResult<()> {
    let policy = tiny_policy()?;
    let decision = policy.wal_sync_decision(
        WalDurabilityCounters {
            entries_since_sync: 2,
            bytes_since_sync: 15,
            elapsed_since_sync: Duration::from_millis(19),
        },
        false,
    );

    assert_eq!(decision, DurabilityDecision::Skip);
    Ok(())
}

#[sinex_test]
async fn wal_decision_syncs_on_force_entries_bytes_or_elapsed() -> TestResult<()> {
    let policy = tiny_policy()?;

    assert_eq!(
        policy.wal_sync_decision(
            WalDurabilityCounters {
                entries_since_sync: 0,
                bytes_since_sync: 0,
                elapsed_since_sync: Duration::ZERO,
            },
            true,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::Forced)
    );
    assert_eq!(
        policy.wal_sync_decision(
            WalDurabilityCounters {
                entries_since_sync: 3,
                bytes_since_sync: 0,
                elapsed_since_sync: Duration::ZERO,
            },
            false,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::WalEntryCount)
    );
    assert_eq!(
        policy.wal_sync_decision(
            WalDurabilityCounters {
                entries_since_sync: 0,
                bytes_since_sync: 16,
                elapsed_since_sync: Duration::ZERO,
            },
            false,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::WalBytes)
    );
    assert_eq!(
        policy.wal_sync_decision(
            WalDurabilityCounters {
                entries_since_sync: 0,
                bytes_since_sync: 0,
                elapsed_since_sync: Duration::from_millis(20),
            },
            false,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::WalElapsed)
    );
    Ok(())
}

#[sinex_test]
async fn staged_decision_skips_below_thresholds() -> TestResult<()> {
    let policy = tiny_policy()?;
    let decision = policy.staged_sync_decision(
        StagedDurabilityCounters {
            bytes_since_sync: 7,
            elapsed_since_sync: Duration::from_millis(9),
        },
        false,
    );

    assert_eq!(decision, DurabilityDecision::Skip);
    Ok(())
}

#[sinex_test]
async fn staged_decision_syncs_on_force_bytes_or_elapsed() -> TestResult<()> {
    let policy = tiny_policy()?;

    assert_eq!(
        policy.staged_sync_decision(
            StagedDurabilityCounters {
                bytes_since_sync: 0,
                elapsed_since_sync: Duration::ZERO,
            },
            true,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::Forced)
    );
    assert_eq!(
        policy.staged_sync_decision(
            StagedDurabilityCounters {
                bytes_since_sync: 8,
                elapsed_since_sync: Duration::ZERO,
            },
            false,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::StagedBytes)
    );
    assert_eq!(
        policy.staged_sync_decision(
            StagedDurabilityCounters {
                bytes_since_sync: 0,
                elapsed_since_sync: Duration::from_millis(10),
            },
            false,
        ),
        DurabilityDecision::Sync(DurabilitySyncReason::StagedElapsed)
    );
    Ok(())
}

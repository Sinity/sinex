//! Durability policy for material assembler WAL and staged-material writes.
//!
//! The assembler still owns the actual write order. This module owns the policy:
//! thresholds, sync decisions, and the flush/fsync effects that implement those
//! decisions.

use super::state::AssemblerState;
use crate::{IngestdResult, SinexError};
use sinex_primitives::Uuid;
use std::time::{Duration, Instant};
use tokio::{fs::File, io::AsyncWriteExt};

const DEFAULT_STAGED_FILE_SYNC_BYTES: i64 = 1024 * 1024;
const DEFAULT_STAGED_FILE_SYNC_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_WAL_SYNC_BYTES: usize = 256 * 1024;
const DEFAULT_WAL_SYNC_ENTRIES: u32 = 128;
const DEFAULT_WAL_SYNC_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurabilityThresholds {
    staged_file_sync_bytes: i64,
    staged_file_sync_interval: Duration,
    wal_sync_bytes: usize,
    wal_sync_entries: u32,
    wal_sync_interval: Duration,
}

impl DurabilityThresholds {
    pub(crate) fn default_checked() -> IngestdResult<Self> {
        Self::try_new(
            DEFAULT_STAGED_FILE_SYNC_BYTES,
            DEFAULT_STAGED_FILE_SYNC_INTERVAL,
            DEFAULT_WAL_SYNC_BYTES,
            DEFAULT_WAL_SYNC_ENTRIES,
            DEFAULT_WAL_SYNC_INTERVAL,
        )
    }

    pub(crate) fn try_new(
        staged_file_sync_bytes: i64,
        staged_file_sync_interval: Duration,
        wal_sync_bytes: usize,
        wal_sync_entries: u32,
        wal_sync_interval: Duration,
    ) -> IngestdResult<Self> {
        if staged_file_sync_bytes <= 0 {
            return Err(
                SinexError::configuration("staged file sync threshold must be positive")
                    .with_context("staged_file_sync_bytes", staged_file_sync_bytes.to_string()),
            );
        }
        if staged_file_sync_interval.is_zero() {
            return Err(SinexError::configuration(
                "staged file sync interval must be positive",
            ));
        }
        if wal_sync_bytes == 0 {
            return Err(SinexError::configuration(
                "WAL byte sync threshold must be positive",
            ));
        }
        if wal_sync_entries == 0 {
            return Err(SinexError::configuration(
                "WAL entry sync threshold must be positive",
            ));
        }
        if wal_sync_interval.is_zero() {
            return Err(SinexError::configuration(
                "WAL sync interval must be positive",
            ));
        }

        Ok(Self {
            staged_file_sync_bytes,
            staged_file_sync_interval,
            wal_sync_bytes,
            wal_sync_entries,
            wal_sync_interval,
        })
    }

    pub(super) const fn staged_file_sync_bytes(self) -> i64 {
        self.staged_file_sync_bytes
    }

    pub(super) const fn staged_file_sync_interval(self) -> Duration {
        self.staged_file_sync_interval
    }

    pub(super) const fn wal_sync_bytes(self) -> usize {
        self.wal_sync_bytes
    }

    pub(super) const fn wal_sync_entries(self) -> u32 {
        self.wal_sync_entries
    }

    pub(super) const fn wal_sync_interval(self) -> Duration {
        self.wal_sync_interval
    }
}

impl Default for DurabilityThresholds {
    fn default() -> Self {
        Self {
            staged_file_sync_bytes: DEFAULT_STAGED_FILE_SYNC_BYTES,
            staged_file_sync_interval: DEFAULT_STAGED_FILE_SYNC_INTERVAL,
            wal_sync_bytes: DEFAULT_WAL_SYNC_BYTES,
            wal_sync_entries: DEFAULT_WAL_SYNC_ENTRIES,
            wal_sync_interval: DEFAULT_WAL_SYNC_INTERVAL,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalDurabilityCounters {
    pub entries_since_sync: u32,
    pub bytes_since_sync: usize,
    pub elapsed_since_sync: Duration,
}

impl WalDurabilityCounters {
    pub(super) fn from_state(state: &AssemblerState) -> Self {
        Self {
            entries_since_sync: state.wal_entries_since_sync,
            bytes_since_sync: state.wal_bytes_since_sync,
            elapsed_since_sync: state.last_wal_sync.elapsed(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StagedDurabilityCounters {
    pub bytes_since_sync: i64,
    pub elapsed_since_sync: Duration,
}

impl StagedDurabilityCounters {
    pub(super) fn from_state(state: &AssemblerState) -> Self {
        Self {
            bytes_since_sync: state.staged_bytes_since_sync,
            elapsed_since_sync: state.last_staged_sync.elapsed(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurabilityDecision {
    Skip,
    Sync(DurabilitySyncReason),
}

impl DurabilityDecision {
    pub(super) const fn should_sync(self) -> bool {
        matches!(self, Self::Sync(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DurabilitySyncReason {
    Forced,
    WalEntryCount,
    WalBytes,
    WalElapsed,
    StagedBytes,
    StagedElapsed,
}

pub(super) trait DurabilityPolicy {
    fn wal_sync_decision(&self, counters: WalDurabilityCounters, force: bool)
    -> DurabilityDecision;

    fn staged_sync_decision(
        &self,
        counters: StagedDurabilityCounters,
        force: bool,
    ) -> DurabilityDecision;

    async fn flush_wal_after_append(&self, file: &mut File) -> IngestdResult<()>;

    async fn sync_wal_if_needed(
        &self,
        state: &mut AssemblerState,
        force: bool,
    ) -> IngestdResult<()>;

    async fn flush_staged_after_write(
        &self,
        file: &mut File,
        material_id: Uuid,
    ) -> IngestdResult<()>;

    async fn sync_staged_file_if_needed(
        &self,
        state: &mut AssemblerState,
        material_id: Uuid,
        force: bool,
    ) -> IngestdResult<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DefaultDurabilityPolicy {
    thresholds: DurabilityThresholds,
}

impl DefaultDurabilityPolicy {
    pub(super) const fn new(thresholds: DurabilityThresholds) -> Self {
        Self { thresholds }
    }
}

impl DurabilityPolicy for DefaultDurabilityPolicy {
    fn wal_sync_decision(
        &self,
        counters: WalDurabilityCounters,
        force: bool,
    ) -> DurabilityDecision {
        if force {
            return DurabilityDecision::Sync(DurabilitySyncReason::Forced);
        }
        if counters.entries_since_sync >= self.thresholds.wal_sync_entries() {
            return DurabilityDecision::Sync(DurabilitySyncReason::WalEntryCount);
        }
        if counters.bytes_since_sync >= self.thresholds.wal_sync_bytes() {
            return DurabilityDecision::Sync(DurabilitySyncReason::WalBytes);
        }
        if counters.elapsed_since_sync >= self.thresholds.wal_sync_interval() {
            return DurabilityDecision::Sync(DurabilitySyncReason::WalElapsed);
        }

        DurabilityDecision::Skip
    }

    fn staged_sync_decision(
        &self,
        counters: StagedDurabilityCounters,
        force: bool,
    ) -> DurabilityDecision {
        if force {
            return DurabilityDecision::Sync(DurabilitySyncReason::Forced);
        }
        if counters.bytes_since_sync >= self.thresholds.staged_file_sync_bytes() {
            return DurabilityDecision::Sync(DurabilitySyncReason::StagedBytes);
        }
        if counters.elapsed_since_sync >= self.thresholds.staged_file_sync_interval() {
            return DurabilityDecision::Sync(DurabilitySyncReason::StagedElapsed);
        }

        DurabilityDecision::Skip
    }

    async fn flush_wal_after_append(&self, file: &mut File) -> IngestdResult<()> {
        file.flush()
            .await
            .map_err(|e| SinexError::io("WAL flush failed").with_source(e))
    }

    async fn sync_wal_if_needed(
        &self,
        state: &mut AssemblerState,
        force: bool,
    ) -> IngestdResult<()> {
        if !self
            .wal_sync_decision(WalDurabilityCounters::from_state(state), force)
            .should_sync()
        {
            return Ok(());
        }

        if let Some(file) = state.wal_file.as_mut() {
            file.sync_data()
                .await
                .map_err(|e| SinexError::io("WAL sync failed").with_source(e))?;
        }
        state.wal_entries_since_sync = 0;
        state.wal_bytes_since_sync = 0;
        state.last_wal_sync = Instant::now();
        Ok(())
    }

    async fn flush_staged_after_write(
        &self,
        file: &mut File,
        material_id: Uuid,
    ) -> IngestdResult<()> {
        file.flush().await.map_err(|e| {
            SinexError::io(format!("Failed to flush staged material for {material_id}"))
                .with_source(e)
        })
    }

    async fn sync_staged_file_if_needed(
        &self,
        state: &mut AssemblerState,
        material_id: Uuid,
        force: bool,
    ) -> IngestdResult<()> {
        if !self
            .staged_sync_decision(StagedDurabilityCounters::from_state(state), force)
            .should_sync()
        {
            return Ok(());
        }

        if let Some(file) = state.temp_file.as_mut() {
            self.flush_staged_after_write(file, material_id).await?;
            file.sync_data().await.map_err(|e| {
                SinexError::io(format!("Failed to sync staged material for {material_id}"))
                    .with_source(e)
            })?;
        }
        state.staged_bytes_since_sync = 0;
        state.last_staged_sync = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
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
}

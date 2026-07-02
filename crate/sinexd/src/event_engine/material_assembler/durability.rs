//! Durability policy for material assembler WAL and staged-material writes.
//!
//! The assembler still owns the actual write order. This module owns the policy:
//! thresholds, sync decisions, and the flush/fsync effects that implement those
//! decisions.

use super::state::AssemblerState;
use crate::event_engine::{EventEngineResult, SinexError};
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
    pub(crate) fn default_checked() -> EventEngineResult<Self> {
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
    ) -> EventEngineResult<Self> {
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

    async fn flush_wal_after_append(&self, file: &mut File) -> EventEngineResult<()>;

    async fn sync_wal_if_needed(
        &self,
        state: &mut AssemblerState,
        force: bool,
    ) -> EventEngineResult<()>;

    async fn flush_staged_after_write(
        &self,
        file: &mut File,
        material_id: Uuid,
    ) -> EventEngineResult<()>;

    async fn sync_staged_file_if_needed(
        &self,
        state: &mut AssemblerState,
        material_id: Uuid,
        force: bool,
    ) -> EventEngineResult<()>;
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

    async fn flush_wal_after_append(&self, file: &mut File) -> EventEngineResult<()> {
        file.flush()
            .await
            .map_err(|e| SinexError::io("WAL flush failed").with_source(e))
    }

    async fn sync_wal_if_needed(
        &self,
        state: &mut AssemblerState,
        force: bool,
    ) -> EventEngineResult<()> {
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
    ) -> EventEngineResult<()> {
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
    ) -> EventEngineResult<()> {
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
#[path = "durability_test.rs"]
mod tests;

//! `sinexctl state` — local runtime-state snapshot and restore surfaces.

use clap::Subcommand;
use color_eyre::eyre::Result;

use crate::admin::snapshot::{
    AdminSnapshotCommand, AdminSnapshotInspectCommand, AdminSnapshotRestoreCommand,
    format_snapshot_inspect_result, format_snapshot_restore_plan_result, format_snapshot_result,
};
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Runtime-state snapshot and restore commands.
#[derive(Debug, Subcommand)]
pub enum StateCommands {
    /// Create a quiesce-mode snapshot of the complete sinex runtime state.
    Snapshot(AdminSnapshotCommand),
    /// Inspect a snapshot archive manifest and member list.
    Inspect(AdminSnapshotInspectCommand),
    /// Validate or execute an isolated snapshot restore drill.
    Restore(AdminSnapshotRestoreCommand),
}

impl StateCommands {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::Snapshot(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_result).display(&format)
            }
            Self::Inspect(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_inspect_result).display(&format)
            }
            Self::Restore(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_restore_plan_result).display(&format)
            }
        }
    }
}

//! Internal admin implementations surfaced through canonical `sinexctl ops`
//! commands.

pub mod exec;
pub mod manifest;
pub mod snapshot;
pub mod staging;

use clap::Subcommand;
use color_eyre::eyre::Result;

use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use snapshot::{
    AdminSnapshotCommand, AdminSnapshotInspectCommand, AdminSnapshotRestoreCommand,
    format_snapshot_inspect_result, format_snapshot_restore_plan_result, format_snapshot_result,
};

/// Admin subcommands.
#[derive(Debug, Subcommand)]
pub enum AdminCommands {
    /// Create a snapshot of the complete sinex runtime state.
    Snapshot(AdminSnapshotCommand),
    /// Inspect a snapshot archive manifest and member list.
    SnapshotInspect(AdminSnapshotInspectCommand),
    /// Validate a snapshot restore drill plan without writing target state.
    SnapshotRestore(AdminSnapshotRestoreCommand),
}

impl AdminCommands {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::Snapshot(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_result).display(&format)
            }
            Self::SnapshotInspect(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_inspect_result).display(&format)
            }
            Self::SnapshotRestore(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_restore_plan_result).display(&format)
            }
        }
    }
}

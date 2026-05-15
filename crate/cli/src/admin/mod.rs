//! `sinexctl admin` — operator-level commands for backup, maintenance, and
//! horizon-3 reshaping.

pub mod exec;
pub mod manifest;
pub mod snapshot;
pub mod staging;

use clap::Subcommand;
use color_eyre::eyre::Result;

use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use snapshot::{AdminSnapshotCommand, format_snapshot_result};

/// Admin subcommands.
#[derive(Debug, Subcommand)]
pub enum AdminCommands {
    /// Create a quiesce-mode snapshot of the complete sinex runtime state.
    Snapshot(AdminSnapshotCommand),
}

impl AdminCommands {
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match self {
            Self::Snapshot(cmd) => {
                let result = cmd.execute()?;
                CommandOutput::single(result, format_snapshot_result).display(&format)
            }
        }
    }
}

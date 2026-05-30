//! `record-drift-bypass` command — records SINEX_SKIP_DRIFT_GUARD bypass events (#1565).
//!
//! Called by the pre-push hook when `SINEX_SKIP_DRIFT_GUARD=1` is set, before the
//! push proceeds. The hook records the bypass intent; the push outcome is unknown
//! at record time but the event is still persisted.

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use color_eyre::eyre::{Result, bail};

/// Record a drift guard bypass event in the xtask history database.
#[derive(Debug, Clone, clap::Args)]
pub struct RecordDriftBypassCommand {
    /// The git branch being pushed
    #[arg(long)]
    pub branch: Option<String>,

    /// The HEAD commit SHA at the time of bypass
    #[arg(long)]
    pub sha: Option<String>,
}

impl XtaskCommand for RecordDriftBypassCommand {
    fn name(&self) -> &'static str {
        "record-drift-bypass"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let result = ctx.try_with_history_db(|db| {
            db.record_drift_guard_bypass(
                self.branch.as_deref(),
                self.sha.as_deref(),
                None, // push outcome unknown at record time
            )
        });

        let id = match result {
            Some(Ok(id)) => id,
            Some(Err(error)) => return Err(error),
            None => {
                bail!(
                    "history DB unavailable at {}",
                    ctx.history_db_path().display()
                );
            }
        };

        if ctx.is_human() {
            eprintln!(
                "[record-drift-bypass] Recorded bypass (id={id}) — branch={}, sha={}",
                self.branch.as_deref().unwrap_or("?"),
                self.sha.as_deref().unwrap_or("?"),
            );
        }

        Ok(CommandResult::success()
            .with_message(format!("recorded drift guard bypass (id={id})"))
            .with_data(serde_json::json!({
                "id": id,
                "branch": self.branch,
                "sha": self.sha,
            })))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::diagnostics()
            .with_history_tracking(false)
            .with_history_access(crate::command::HistoryAccessMode::ReadWrite)
    }
}

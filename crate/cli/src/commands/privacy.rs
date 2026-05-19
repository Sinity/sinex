use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use clap::{Args, Subcommand};
use color_eyre::Result;
use sinex_primitives::privacy::{PrivateModeReasonClass, RuntimePrivateModeState};

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl privacy private-mode status -f json
    sinexctl privacy private-mode enable --actor sinity --source-class desktop
    sinexctl privacy private-mode disable
")]
pub struct PrivacyCommand {
    #[command(subcommand)]
    cmd: PrivacySubcommand,
}

#[derive(Debug, Subcommand)]
enum PrivacySubcommand {
    /// Query or toggle runtime private mode.
    PrivateMode {
        #[command(subcommand)]
        cmd: PrivateModeCommand,
    },
}

#[derive(Debug, Subcommand)]
enum PrivateModeCommand {
    /// Show the gateway-observed private-mode state.
    Status,

    /// Enable runtime private mode.
    Enable(PrivateModeEnableArgs),

    /// Disable runtime private mode.
    Disable,
}

#[derive(Debug, Args)]
struct PrivateModeEnableArgs {
    /// Coarse actor label to persist.
    #[arg(long, default_value = "operator")]
    actor: String,

    /// Coarse reason class. Avoid detailed reasons that weaken deniability.
    #[arg(long, default_value = "operator_private")]
    reason_class: PrivateModeReasonClass,

    /// Source class covered by private mode. Repeatable; omit for all classes.
    #[arg(long = "source-class")]
    source_classes: Vec<String>,
}

impl PrivacyCommand {
    #[must_use]
    pub fn command_path(&self) -> &'static str {
        match &self.cmd {
            PrivacySubcommand::PrivateMode { cmd } => match cmd {
                PrivateModeCommand::Status => "privacy private-mode status",
                PrivateModeCommand::Enable(_) => "privacy private-mode enable",
                PrivateModeCommand::Disable => "privacy private-mode disable",
            },
        }
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            PrivacySubcommand::PrivateMode { cmd } => cmd.execute(client, format).await,
        }
    }
}

impl PrivateModeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let state = match self {
            Self::Status => client.private_mode_status().await?.state,
            Self::Enable(args) => {
                client
                    .private_mode_enable(
                        args.actor.clone(),
                        args.reason_class.clone(),
                        args.source_classes.clone(),
                    )
                    .await?
                    .state
            }
            Self::Disable => client.private_mode_disable().await?.state,
        };

        CommandOutput::single(state, format_private_mode_state).display(&format)?;
        Ok(())
    }
}

fn format_private_mode_state(state: &RuntimePrivateModeState) -> String {
    let scope = if state.affected_source_classes.is_empty() {
        "all".to_string()
    } else {
        state.affected_source_classes.join(",")
    };
    let started = state
        .started_at
        .as_ref()
        .map_or_else(|| "-".to_string(), ToString::to_string);
    format!(
        "Private mode: {}\nReason: {}\nActor: {}\nStarted: {}\nSource classes: {}",
        if state.enabled { "enabled" } else { "disabled" },
        state.reason_class,
        state.actor,
        started,
        scope
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn private_mode_table_summary_keeps_coarse_scope() -> xtask::sandbox::TestResult<()> {
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["clipboard".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        let summary = format_private_mode_state(&state);

        assert!(summary.contains("Private mode: enabled"));
        assert!(summary.contains("Actor: sinity"));
        assert!(summary.contains("Source classes: clipboard"));
        Ok(())
    }
}

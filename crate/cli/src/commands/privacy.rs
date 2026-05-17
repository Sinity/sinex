use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use clap::{Args, Subcommand};
use color_eyre::Result;
use sinex_primitives::privacy::{
    PrivateModeReasonClass, RuntimePrivateModeState, load_private_mode_state,
    save_private_mode_state,
};
use sinex_primitives::temporal::Timestamp;
use std::path::PathBuf;

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
    /// Show the persisted private-mode state.
    Status(StateDirArg),

    /// Enable runtime private mode.
    Enable(PrivateModeEnableArgs),

    /// Disable runtime private mode.
    Disable(StateDirArg),
}

#[derive(Debug, Args)]
struct StateDirArg {
    /// Sinex state directory root.
    #[arg(long, env = "SINEX_STATE_DIR")]
    state_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PrivateModeEnableArgs {
    /// Sinex state directory root.
    #[arg(long, env = "SINEX_STATE_DIR")]
    state_dir: Option<PathBuf>,

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
    pub fn execute(&self, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            PrivacySubcommand::PrivateMode { cmd } => cmd.execute(format),
        }
    }
}

impl PrivateModeCommand {
    fn execute(&self, format: OutputFormat) -> Result<()> {
        let state = match self {
            Self::Status(args) => load_private_mode_state(&resolve_state_dir(&args.state_dir)?)?,
            Self::Enable(args) => {
                let state_dir = resolve_state_dir(&args.state_dir)?;
                let mut state = RuntimePrivateModeState::enabled_by(
                    args.actor.clone(),
                    args.source_classes.clone(),
                    Timestamp::now(),
                );
                state.reason_class = args.reason_class.clone();
                save_private_mode_state(&state_dir, &state)?;
                state
            }
            Self::Disable(args) => {
                let state_dir = resolve_state_dir(&args.state_dir)?;
                let state = load_private_mode_state(&state_dir)?.disable();
                save_private_mode_state(&state_dir, &state)?;
                state
            }
        };

        CommandOutput::single(state, format_private_mode_state).display(&format)?;
        Ok(())
    }
}

fn resolve_state_dir(explicit: &Option<PathBuf>) -> Result<PathBuf> {
    Ok(explicit
        .clone()
        .or_else(|| std::env::var_os("SINEX_STATE_DIR").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("/var/lib/sinex")))
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
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn enable_private_mode_persists_state() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let cmd = PrivateModeCommand::Enable(PrivateModeEnableArgs {
            state_dir: Some(dir.path().to_path_buf()),
            actor: "sinity".to_string(),
            reason_class: PrivateModeReasonClass::OperatorPrivate,
            source_classes: vec!["desktop".to_string()],
        });

        cmd.execute(OutputFormat::Json)?;
        let state = load_private_mode_state(dir.path())?;

        assert!(state.enabled);
        assert_eq!(state.actor, "sinity");
        assert_eq!(state.affected_source_classes, vec!["desktop"]);
        Ok(())
    }

    #[sinex_test]
    async fn disable_private_mode_keeps_coarse_actor_history() -> xtask::sandbox::TestResult<()> {
        let dir = tempfile::tempdir()?;
        let state = RuntimePrivateModeState::enabled_by(
            "sinity",
            vec!["clipboard".to_string()],
            Timestamp::UNIX_EPOCH,
        );
        save_private_mode_state(dir.path(), &state)?;
        let cmd = PrivateModeCommand::Disable(StateDirArg {
            state_dir: Some(dir.path().to_path_buf()),
        });

        cmd.execute(OutputFormat::Json)?;
        let state = load_private_mode_state(dir.path())?;

        assert!(!state.enabled);
        assert_eq!(state.actor, "sinity");
        assert_eq!(state.affected_source_classes, vec!["clipboard"]);
        Ok(())
    }
}

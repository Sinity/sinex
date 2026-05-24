use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::coordinator::{self, FreshnessScopeExplanation};
use crate::history::ProofEvidence;
use color_eyre::eyre::Result;
use serde::Serialize;

/// Inspect coordinator freshness keys and reuse decisions.
#[derive(Debug, Clone, clap::Args)]
pub struct FreshnessCommand {
    #[command(subcommand)]
    pub subcommand: FreshnessSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum FreshnessSubcommand {
    /// Explain the freshness key for a command scope.
    Explain(FreshnessExplainCommand),
}

/// Explain the coordinator freshness key for a command and its args.
#[derive(Debug, Clone, clap::Args)]
pub struct FreshnessExplainCommand {
    /// Coordinated command to explain, such as check, build, fix, test, or vm.
    pub command: String,

    /// Arguments for the explained command. Use `--` before hyphen-prefixed args.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum FreshnessDecision {
    Hit,
    Miss,
    Disabled,
    HistoryUnavailable,
}

#[derive(Debug, Clone, Serialize)]
struct FreshnessReuseExplanation {
    enabled: bool,
    decision: FreshnessDecision,
    reason: String,
    last_completed: Option<ProofEvidence>,
}

#[derive(Debug, Clone, Serialize)]
struct FreshnessExplainOutput {
    #[serde(flatten)]
    key: coordinator::FreshnessExplanation,
    reuse: FreshnessReuseExplanation,
}

impl XtaskCommand for FreshnessCommand {
    fn name(&self) -> &'static str {
        "freshness"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            FreshnessSubcommand::Explain(command) => command.execute(ctx).await,
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
            .with_history_tracking(false)
            .with_history_access(HistoryAccessMode::Query)
    }
}

impl FreshnessExplainCommand {
    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let key = coordinator::explain_freshness(&self.command, &self.args)?;
        let reuse = explain_reuse(ctx, &key);

        if ctx.is_human() {
            print_explanation(&key, &reuse);
        }

        Ok(CommandResult::success()
            .with_message(format!(
                "freshness explanation for {}",
                render_command(&self.command, &self.args)
            ))
            .with_duration(ctx.elapsed())
            .with_data(serde_json::to_value(FreshnessExplainOutput { key, reuse })?))
    }
}

fn explain_reuse(
    ctx: &CommandContext,
    key: &coordinator::FreshnessExplanation,
) -> FreshnessReuseExplanation {
    if !key.fresh_reuse_enabled {
        return FreshnessReuseExplanation {
            enabled: false,
            decision: FreshnessDecision::Disabled,
            reason: format!("fresh reuse is disabled for `{}`", key.command),
            last_completed: None,
        };
    }

    let Some(last_result) = ctx.try_with_history_db_query(|db| {
        db.get_successful_proof_evidence(
            &key.command,
            &key.proof_kind,
            &key.tree_fingerprint,
            &key.scope_key,
        )
    }) else {
        return FreshnessReuseExplanation {
            enabled: true,
            decision: FreshnessDecision::HistoryUnavailable,
            reason: format!(
                "history DB unavailable at {}",
                ctx.history_db_path().display()
            ),
            last_completed: None,
        };
    };

    let matching_invocation = match last_result {
        Ok(value) => value,
        Err(error) => {
            return FreshnessReuseExplanation {
                enabled: true,
                decision: FreshnessDecision::HistoryUnavailable,
                reason: format!("history query failed: {error}"),
                last_completed: None,
            };
        }
    };

    let Some(last) = matching_invocation else {
        return FreshnessReuseExplanation {
            enabled: true,
            decision: FreshnessDecision::Miss,
            reason: "no prior successful invocation matches this exact freshness key".to_string(),
            last_completed: None,
        };
    };

    FreshnessReuseExplanation {
        enabled: true,
        decision: FreshnessDecision::Hit,
        reason: format!(
            "successful invocation #{} matches this exact proof key",
            last.invocation_id
        ),
        last_completed: Some(last),
    }
}

fn print_explanation(key: &coordinator::FreshnessExplanation, reuse: &FreshnessReuseExplanation) {
    println!("Freshness: {}", render_command(&key.command, &key.args));
    println!("  Coordinated:        {}", yes_no(key.should_coordinate));
    println!("  Fresh reuse:        {}", yes_no(key.fresh_reuse_enabled));
    println!("  Proof kind:         {}", key.proof_kind);
    println!("  Scope key:          {}", short_hash(&key.scope_key));
    println!(
        "  Tree fingerprint:   {}",
        short_hash(&key.tree_fingerprint)
    );
    match &key.scope {
        FreshnessScopeExplanation::Workspace => {
            println!("  Scope:              workspace");
        }
        FreshnessScopeExplanation::Packages { packages } => {
            println!("  Scope:              packages");
            for package in packages {
                println!("    {} -> {}", package.package, package.path);
            }
        }
    }
    if !key.shared_inputs.is_empty() {
        println!("  Shared inputs:      {}", key.shared_inputs.join(", "));
    }
    println!("  Reuse decision:     {:?}", reuse.decision);
    println!("  Reuse reason:       {}", reuse.reason);
}

fn render_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    }
}

fn short_hash(value: &str) -> &str {
    &value[..value.len().min(16)]
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::prelude::*;

    #[sinex_test]
    async fn freshness_explain_marks_test_reuse_disabled() -> TestResult<()> {
        let ctx = CommandContext::new(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "freshness",
        );
        let command = FreshnessExplainCommand {
            command: "test".to_string(),
            args: vec!["-p".to_string(), "xtask".to_string()],
        };

        let result = command.execute(&ctx).await?;
        let data = result.data.expect("freshness explain should emit data");

        assert_eq!(data["command"], "test");
        assert_eq!(data["fresh_reuse_enabled"], false);
        assert_eq!(data["reuse"]["decision"], "disabled");
        assert_eq!(data["scope"]["kind"], "packages");
        Ok(())
    }

    #[sinex_test]
    async fn freshness_explain_reports_shared_inputs_for_scoped_check() -> TestResult<()> {
        let explanation =
            coordinator::explain_freshness("check", &["-p".to_string(), "xtask".to_string()])?;

        assert!(explanation.fresh_reuse_enabled);
        assert!(
            explanation
                .shared_inputs
                .contains(&"Cargo.lock".to_string())
        );
        assert!(matches!(
            explanation.scope,
            FreshnessScopeExplanation::Packages { .. }
        ));
        Ok(())
    }
}

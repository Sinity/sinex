use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::commands::test::TestCommand;
use crate::coordinator::{self, FreshnessScopeExplanation};
use crate::history::{ProofEvidence, TestProofUnit};
use clap::Parser;
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
    last_completed: Option<FreshnessCompletedProof>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "source", rename_all = "snake_case")]
enum FreshnessCompletedProof {
    ProofEvidence(ProofEvidence),
    TestProofUnit(TestProofUnit),
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
            FreshnessSubcommand::Explain(command) => command.execute(ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
            .with_history_tracking(false)
            .with_history_access(HistoryAccessMode::Query)
    }
}

impl FreshnessExplainCommand {
    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let explained_args = explain_args_for_command(&self.command, &self.args, ctx)?;
        let key = coordinator::explain_freshness(&self.command, &explained_args)?;
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

#[derive(Debug, Parser)]
struct TestExplainArgs {
    #[command(flatten)]
    command: TestCommand,
}

fn explain_args_for_command(
    command: &str,
    args: &[String],
    ctx: &CommandContext,
) -> Result<Vec<String>> {
    if command != "test" {
        return Ok(args.to_vec());
    }

    let mut parse_args = Vec::with_capacity(args.len() + 1);
    parse_args.push("test".to_string());
    parse_args.extend(args.iter().cloned());
    let parsed = TestExplainArgs::try_parse_from(parse_args)?;
    parsed.command.freshness_explain_args(Some(ctx))
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

    if key.command == "test" {
        let Some(last_result) = ctx.try_with_history_db_query(|db| {
            db.get_successful_reusable_test_proof_unit(
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

        return match last_result {
            Ok(Some(last)) => FreshnessReuseExplanation {
                enabled: true,
                decision: FreshnessDecision::Hit,
                reason: format!(
                    "successful test proof unit #{} matches this exact proof key",
                    last.invocation_id
                ),
                last_completed: Some(FreshnessCompletedProof::TestProofUnit(last)),
            },
            Ok(None) => FreshnessReuseExplanation {
                enabled: true,
                decision: FreshnessDecision::Miss,
                reason: "no prior successful test proof unit matches this exact freshness key"
                    .to_string(),
                last_completed: None,
            },
            Err(error) => FreshnessReuseExplanation {
                enabled: true,
                decision: FreshnessDecision::HistoryUnavailable,
                reason: format!("history query failed: {error}"),
                last_completed: None,
            },
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
        last_completed: Some(FreshnessCompletedProof::ProofEvidence(last)),
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
#[path = "freshness_test.rs"]
mod tests;

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
mod tests {
    use super::*;
    use crate::history::HistoryDb;
    use crate::output::{OutputFormat, OutputWriter};
    use crate::sandbox::prelude::*;

    #[sinex_test]
    async fn freshness_explain_marks_exact_test_reuse_enabled() -> TestResult<()> {
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
        assert_eq!(data["fresh_reuse_enabled"], true);
        assert_ne!(data["reuse"]["decision"], "disabled");
        assert_eq!(data["proof_kind"], "test.nextest.exact");
        assert_eq!(data["scope"]["kind"], "packages");
        Ok(())
    }

    #[sinex_test]
    async fn freshness_explain_test_hits_test_proof_units() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("history.db");
        let db = HistoryDb::open(&db_path)?;
        let args = vec!["-p".to_string(), "xtask".to_string()];
        let planning_ctx = CommandContext::new(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "freshness",
        );
        let semantic_args = explain_args_for_command("test", &args, &planning_ctx)?;
        let key = coordinator::explain_freshness("test", &semantic_args)?;
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            &key.proof_kind,
            &key.scope_key,
            &key.tree_fingerprint,
            r#"{"scope":"packages:xtask"}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);
        let ctx = CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "freshness",
            db_path,
        );
        let command = FreshnessExplainCommand {
            command: "test".to_string(),
            args,
        };

        let result = command.execute(&ctx).await?;
        let data = result.data.expect("freshness explain should emit data");

        assert_eq!(data["reuse"]["decision"], "hit");
        assert_eq!(data["reuse"]["last_completed"]["source"], "test_proof_unit");
        assert_eq!(
            data["reuse"]["last_completed"]["invocation_id"],
            invocation_id
        );
        Ok(())
    }

    #[sinex_test]
    async fn freshness_explain_test_uses_resolved_test_semantics() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let db_path = dir.path().join("history.db");
        let raw_args = vec![
            "-p".to_string(),
            "xtask".to_string(),
            "-E".to_string(),
            "test(command_catalog_exposes_core_public_surface)".to_string(),
        ];
        let planning_ctx = CommandContext::new(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "freshness",
        );
        let semantic_args = TestCommand {
            packages: vec!["xtask".to_string()],
            filter: Some("test(command_catalog_exposes_core_public_surface)".to_string()),
            ..Default::default()
        }
        .freshness_explain_args(Some(&planning_ctx))?;
        assert!(
            semantic_args.contains(&"--lib".to_string()),
            "simple package-scoped unit-test filters should explain the same inferred --lib key as execution: {semantic_args:?}"
        );
        assert_ne!(
            coordinator::compute_scope_key("test", &raw_args),
            coordinator::compute_scope_key("test", &semantic_args),
            "fixture must cover the raw-vs-semantic key mismatch"
        );

        let key = coordinator::explain_freshness("test", &semantic_args)?;
        let db = HistoryDb::open(&db_path)?;
        let invocation_id = db.start_invocation("test", None, None, None)?;
        db.record_test_proof_unit(
            invocation_id,
            &key.proof_kind,
            &key.scope_key,
            &key.tree_fingerprint,
            r#"{"scope":"packages:xtask","lib":true}"#,
            true,
        )?;
        db.finish_invocation(
            invocation_id,
            crate::history::InvocationStatus::Success,
            Some(0),
            0.1,
        )?;
        drop(db);

        let ctx = CommandContext::new_with_db_override(
            OutputWriter::new(OutputFormat::Json),
            false,
            None,
            "freshness",
            db_path,
        );
        let command = FreshnessExplainCommand {
            command: "test".to_string(),
            args: raw_args,
        };

        let result = command.execute(&ctx).await?;
        let data = result.data.expect("freshness explain should emit data");

        assert_eq!(data["scope_key"], key.scope_key);
        assert_eq!(data["reuse"]["decision"], "hit");
        assert_eq!(
            data["reuse"]["last_completed"]["invocation_id"],
            invocation_id
        );
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

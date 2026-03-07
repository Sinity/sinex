//! `xtask work <target>` — run the minimum sequence of operations to reach a desired state.
//!
//! Uses the workflow dependency graph in `coordinator::WorkflowGraph` to determine
//! which steps must run before the target, then executes them in order. Steps that are
//! already "fresh" (coordinator reports no changes since last successful run) are skipped.
//!
//! # Examples
//!
//! ```bash
//! xtask work test   # runs: check → test  (skips check if already fresh)
//! xtask work check  # runs: check
//! ```

use clap::Args;
use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::commands::{BuildCommand, CheckCommand, TestCommand};
use crate::coordinator::WorkflowGraph;
use crate::output::StructuredError;

/// Execute the minimum workflow sequence to reach a target state.
#[derive(Debug, Clone, Args)]
pub struct WorkCommand {
    /// Target operation to reach (check, test, build).
    ///
    /// Prerequisites are run first based on the workflow dependency graph.
    /// Example: `xtask work test` runs `check` then `test`.
    target: String,
}

impl XtaskCommand for WorkCommand {
    fn name(&self) -> &str {
        "work"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let sequence = WorkflowGraph::sequence_to(&self.target);

        // sequence always contains at least the target itself
        if sequence.is_empty() {
            return Ok(CommandResult::failure(StructuredError::new(
                "UNKNOWN_TARGET",
                format!(
                    "no workflow known for target '{}'; known targets: check, test, build",
                    self.target
                ),
            )));
        }

        if ctx.is_human() {
            println!("Work: {}", sequence.join(" → "));
        }

        let mut last_result = CommandResult::success();

        for step in &sequence {
            if ctx.is_human() {
                println!("\n[{step}]");
            }

            let step_result = match step.as_str() {
                "check" => CheckCommand::default().execute(ctx).await?,
                "test" => TestCommand::default().execute(ctx).await?,
                "build" => BuildCommand::default().execute(ctx).await?,
                other => {
                    if ctx.is_human() {
                        eprintln!("  ⚠ unknown workflow step: {other}");
                    }
                    continue;
                }
            };

            if step_result.is_failure() {
                if ctx.is_human() {
                    eprintln!("  ✗ {step} failed — stopping workflow");
                }
                return Ok(step_result.with_duration(ctx.elapsed()));
            }

            last_result = step_result;
        }

        if ctx.is_human() {
            println!("\n✅ Workflow complete: {}", sequence.join(" → "));
        }

        Ok(last_result
            .with_duration(ctx.elapsed())
            .with_data(serde_json::json!({
                "target": self.target,
                "steps_executed": sequence,
            })))
    }

    fn metadata(&self) -> CommandMetadata {
        // Workflow commands have no hard timeout — they compose individual commands
        // which each have their own timeouts.
        CommandMetadata::default()
    }
}

#[cfg(test)]
mod tests {
    use crate::coordinator::WorkflowGraph;

    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_work_sequence_check_no_prereqs() -> TestResult<()> {
        let seq = WorkflowGraph::sequence_to("check");
        assert_eq!(seq, vec!["check"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_work_sequence_test_includes_check() -> TestResult<()> {
        let seq = WorkflowGraph::sequence_to("test");
        assert_eq!(seq, vec!["check", "test"]);
        Ok(())
    }

    #[sinex_test]
    async fn test_work_sequence_unknown_target_contains_target() -> TestResult<()> {
        // Unknown targets have no prerequisites but still appear in the sequence
        let seq = WorkflowGraph::sequence_to("nonexistent-xyz");
        assert!(seq.contains(&"nonexistent-xyz".to_string()));
        Ok(())
    }
}

use color_eyre::eyre::Result;

use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};

#[derive(Debug, Clone, clap::Args)]
pub struct ImpactCommand {
    #[command(subcommand)]
    pub subcommand: ImpactSubcommand,
}

#[derive(Debug, Clone, clap::Subcommand)]
pub enum ImpactSubcommand {
    /// Explain the default `xtask test` impact plan for the current diff.
    Explain,

    /// Sample skipped tests by forcing a broader local run.
    Audit {
        /// Number of skipped proof decisions to sample.
        #[arg(long = "sample-skips", default_value_t = 10)]
        sample_skips: usize,
    },
}

impl XtaskCommand for ImpactCommand {
    fn name(&self) -> &'static str {
        "impact"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match self.subcommand {
            ImpactSubcommand::Explain => explain(ctx),
            ImpactSubcommand::Audit { sample_skips } => audit(ctx, sample_skips),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis()
            .with_history_tracking(false)
            .with_history_access(HistoryAccessMode::Query)
    }
}

fn explain(ctx: &CommandContext) -> Result<CommandResult> {
    let plan = match ctx.try_with_history_db_query(|db| {
        crate::impact::plan_default_test_impact_with_history(Some(db))
    }) {
        Some(result) => result?,
        None => crate::impact::plan_default_test_impact()?,
    };
    if ctx.is_human() {
        print_plan(&plan);
    }
    Ok(CommandResult::success()
        .with_message("impact plan resolved")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::to_value(&plan)?))
}

fn audit(ctx: &CommandContext, sample_skips: usize) -> Result<CommandResult> {
    let plan = match ctx.try_with_history_db_query(|db| {
        crate::impact::plan_default_test_impact_with_history(Some(db))
    }) {
        Some(result) => result?,
        None => crate::impact::plan_default_test_impact()?,
    };
    let sampled_skips = plan
        .decisions
        .iter()
        .filter(|decision| decision.action == crate::impact::ImpactAction::ReuseExactProof)
        .take(sample_skips)
        .cloned()
        .collect::<Vec<_>>();
    if ctx.is_human() {
        println!("Impact audit");
        println!("  sampled skipped decisions: {}", sampled_skips.len());
        if !sampled_skips.is_empty() {
            println!("  recommended verification: xtask test --all");
        }
    }
    Ok(CommandResult::success()
        .with_message("impact audit plan resolved")
        .with_duration(ctx.elapsed())
        .with_data(serde_json::json!({
            "sample_skips": sample_skips,
            "sampled_skips": sampled_skips,
            "recommended_command": if sampled_skips.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::String("xtask test --all".to_string())
            },
            "plan": plan,
        })))
}

fn print_plan(plan: &crate::impact::ImpactPlan) {
    println!("Impact plan");
    println!("  changed files: {}", plan.changed.len());
    if !plan.affected_packages.is_empty() {
        println!("  packages: {}", plan.affected_packages.join(", "));
    } else if !plan.impacted_tests.is_empty() {
        println!("  impacted tests: {}", plan.impacted_tests.len());
        if let Some(filter) = &plan.impact_filter {
            println!("  filter: {filter}");
        }
    } else if plan.is_workspace() {
        println!("  scope: workspace");
    } else if plan.can_reuse_exact_proof() {
        println!("  scope: exact proof reuse candidate");
    }
    for decision in &plan.decisions {
        let subject = decision.subject.as_deref().unwrap_or("workspace");
        println!("  {:?}: {subject} ({})", decision.action, decision.reason);
    }
    for risk in &plan.accepted_risks {
        println!("  accepted risk: {risk}");
    }
    for gap in &plan.evidence_gaps {
        println!("  evidence gap: {gap}");
    }
}

//! Git stack planning and materialization commands.

use std::path::Path;

use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::git_stack::{
    MaterializeOptions, PlanOptions, PublishOptions, SplitOptions, execute_materialize,
    execute_plan, execute_publish, execute_split,
};

/// Plan and materialize PR-sized git branch stacks from the current commit graph.
#[derive(Debug, Clone, clap::Args)]
pub struct GitStackCommand {
    #[command(subcommand)]
    pub subcommand: GitStackSubcommand,
}

/// Git stack subcommands.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum GitStackSubcommand {
    /// Walk the current commit graph and generate a stack plan plus PR/squash bodies.
    Plan {
        /// Base ref to split against. Defaults to origin/master when available, else master.
        #[arg(long)]
        base: Option<String>,

        /// Head ref to analyze.
        #[arg(long, default_value = "HEAD")]
        head: String,

        /// Branch prefix for generated slice refs.
        #[arg(long, default_value = "stack")]
        branch_prefix: String,

        /// Maximum number of commits per generated slice before forcing a split.
        #[arg(long, default_value_t = 12)]
        max_commits_per_slice: usize,

        /// Output directory for generated artifacts.
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Overwrite an existing output directory.
        #[arg(long)]
        force: bool,
    },

    /// Materialize branch refs from an existing stack plan.
    Materialize {
        /// Path to a generated `plan.yaml`.
        #[arg(long)]
        plan: std::path::PathBuf,

        /// Overwrite existing branch refs.
        #[arg(long)]
        force: bool,

        /// Continue even when the plan recorded blockers.
        #[arg(long)]
        allow_blockers: bool,
    },

    /// Generate a stack plan and immediately materialize the resulting branches.
    Split {
        /// Base ref to split against. Defaults to origin/master when available, else master.
        #[arg(long)]
        base: Option<String>,

        /// Head ref to analyze.
        #[arg(long, default_value = "HEAD")]
        head: String,

        /// Branch prefix for generated slice refs.
        #[arg(long, default_value = "stack")]
        branch_prefix: String,

        /// Maximum number of commits per generated slice before forcing a split.
        #[arg(long, default_value_t = 12)]
        max_commits_per_slice: usize,

        /// Output directory for generated artifacts.
        #[arg(long)]
        output: Option<std::path::PathBuf>,

        /// Overwrite an existing output directory and existing branch refs.
        #[arg(long)]
        force: bool,

        /// Continue even when the generated plan records blockers.
        #[arg(long)]
        allow_blockers: bool,
    },

    /// Push materialized branches and open/reuse PRs from a generated stack plan.
    Publish {
        /// Path to a generated `plan.yaml`.
        #[arg(long)]
        plan: std::path::PathBuf,

        /// Git remote to push branch refs to.
        #[arg(long, default_value = "origin")]
        remote: String,

        /// Create PRs as ready-for-review instead of drafts.
        #[arg(long)]
        ready: bool,

        /// Push branches only; skip GitHub PR creation.
        #[arg(long)]
        push_only: bool,

        /// Force-update the remote branches with `--force-with-lease`.
        #[arg(long)]
        force_with_lease: bool,

        /// Optional GitHub repo override for `gh`, e.g. `owner/name`.
        #[arg(long)]
        repo: Option<String>,

        /// Continue even when the plan recorded blockers.
        #[arg(long)]
        allow_blockers: bool,
    },
}

impl XtaskCommand for GitStackCommand {
    fn name(&self) -> &'static str {
        "git-stack"
    }

    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            GitStackSubcommand::Plan {
                base,
                head,
                branch_prefix,
                max_commits_per_slice,
                output,
                force,
            } => execute_plan(PlanOptions {
                repo_root: None,
                base_ref: base.clone(),
                head_ref: head.clone(),
                branch_prefix: branch_prefix.clone(),
                max_commits_per_slice: *max_commits_per_slice,
                output_dir: output.as_deref().map(Path::to_path_buf),
                force: *force,
            }),
            GitStackSubcommand::Materialize {
                plan,
                force,
                allow_blockers,
            } => execute_materialize(MaterializeOptions {
                plan_path: plan.clone(),
                force: *force,
                allow_blockers: *allow_blockers,
            }),
            GitStackSubcommand::Split {
                base,
                head,
                branch_prefix,
                max_commits_per_slice,
                output,
                force,
                allow_blockers,
            } => execute_split(SplitOptions {
                plan: PlanOptions {
                    repo_root: None,
                    base_ref: base.clone(),
                    head_ref: head.clone(),
                    branch_prefix: branch_prefix.clone(),
                    max_commits_per_slice: *max_commits_per_slice,
                    output_dir: output.as_deref().map(Path::to_path_buf),
                    force: *force,
                },
                materialize_force: *force,
                allow_blockers: *allow_blockers,
            }),
            GitStackSubcommand::Publish {
                plan,
                remote,
                ready,
                push_only,
                force_with_lease,
                repo,
                allow_blockers,
            } => execute_publish(PublishOptions {
                plan_path: plan.clone(),
                remote: remote.clone(),
                draft: !ready,
                create_prs: !push_only,
                force_with_lease: *force_with_lease,
                repo: repo.clone(),
                allow_blockers: *allow_blockers,
            }),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis().with_state_mutation(true)
    }
}

//! Git stack planning and materialization support.

use crate::affected::package_for_path;
use crate::command::CommandResult;
use crate::process::ProcessBuilder;
use color_eyre::eyre::{Context, ContextCompat, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Instant;
use time::OffsetDateTime;

const DEFAULT_BRANCH_PREFIX: &str = "stack";
const MAX_PACKAGES_PER_SLICE: usize = 4;
const LARGE_COMMIT_FILE_COUNT: usize = 40;
const LARGE_SLICE_FILE_COUNT: usize = 40;

#[derive(Debug, Clone)]
pub struct PlanOptions {
    pub repo_root: Option<PathBuf>,
    pub base_ref: Option<String>,
    pub head_ref: String,
    pub branch_prefix: String,
    pub max_commits_per_slice: usize,
    pub output_dir: Option<PathBuf>,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct MaterializeOptions {
    pub plan_path: PathBuf,
    pub force: bool,
    pub allow_blockers: bool,
}

#[derive(Debug, Clone)]
pub struct SplitOptions {
    pub plan: PlanOptions,
    pub materialize_force: bool,
    pub allow_blockers: bool,
}

#[derive(Debug, Clone)]
pub struct PublishOptions {
    pub plan_path: PathBuf,
    pub remote: String,
    pub draft: bool,
    pub create_prs: bool,
    pub force_with_lease: bool,
    pub repo: Option<String>,
    pub allow_blockers: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStackPlan {
    pub version: u32,
    pub generated_at: String,
    pub repo_root: String,
    pub base_ref: String,
    pub head_ref: String,
    pub head_branch: Option<String>,
    pub merge_base: String,
    pub graph: GitGraphSummary,
    pub loose_ends: GitLooseEnds,
    pub slices: Vec<GitStackSlice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitGraphSummary {
    pub full_graph_commits: usize,
    pub first_parent_commits: usize,
    pub merge_commits: Vec<GitStackCommitSummary>,
    pub non_first_parent_commits: Vec<GitStackCommitSummary>,
    pub first_parent_linear: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GitLooseEnds {
    pub dirty_paths: Vec<String>,
    pub untracked_paths: Vec<String>,
    pub blockers: Vec<String>,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStackSlice {
    pub index: usize,
    pub id: String,
    pub slug: String,
    pub branch: String,
    pub pr_base: String,
    pub depends_on: Option<String>,
    pub original_base_commit: String,
    pub original_tip_commit: String,
    pub title: String,
    pub rationale: String,
    pub diffstat: Option<String>,
    pub packages: Vec<String>,
    pub topics: Vec<String>,
    pub files: Vec<String>,
    pub commits: Vec<GitStackCommitSummary>,
    pub pr_body: String,
    pub squash_title: String,
    pub squash_body: String,
    pub verification: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitStackCommitSummary {
    pub sha: String,
    pub short_sha: String,
    pub subject: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedBranch {
    pub branch: String,
    pub pr_base: String,
    pub commit: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishedBranch {
    pub branch: String,
    pub remote: String,
    pub pr_base: String,
    pub remote_branch: String,
    pub pushed: bool,
    pub title: String,
    pub pr_url: Option<String>,
    pub pr_number: Option<u64>,
    pub reused_existing_pr: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExistingPullRequest {
    number: u64,
    url: String,
}

#[derive(Debug, Clone)]
struct CommitSubjectParts {
    kind: String,
    scope: Option<String>,
}

#[derive(Debug, Clone)]
struct CommitInfo {
    sha: String,
    short_sha: String,
    subject: String,
    body: String,
    files: Vec<String>,
    packages: BTreeSet<String>,
    topics: BTreeSet<String>,
    subject_parts: CommitSubjectParts,
}

#[derive(Debug, Clone)]
struct SliceAccumulator {
    commits: Vec<CommitInfo>,
    packages: BTreeSet<String>,
    topics: BTreeSet<String>,
    scopes: BTreeSet<String>,
    file_prefixes: BTreeSet<String>,
    file_count: usize,
}

impl SliceAccumulator {
    fn new(first: CommitInfo) -> Self {
        let mut slice = Self {
            commits: Vec::new(),
            packages: BTreeSet::new(),
            topics: BTreeSet::new(),
            scopes: BTreeSet::new(),
            file_prefixes: BTreeSet::new(),
            file_count: 0,
        };
        slice.push(first);
        slice
    }

    fn push(&mut self, commit: CommitInfo) {
        self.packages.extend(commit.packages.iter().cloned());
        self.topics.extend(commit.topics.iter().cloned());
        if let Some(scope) = &commit.subject_parts.scope {
            self.scopes.insert(scope.clone());
        }
        self.file_prefixes.extend(commit.files.iter().map(|file| major_file_prefix(file)));
        self.file_count += commit.files.len();
        self.commits.push(commit);
    }

    fn merge_from(&mut self, other: Self) {
        for commit in other.commits {
            self.push(commit);
        }
    }
}

struct WorktreeHandle {
    repo_root: PathBuf,
    path: PathBuf,
}

impl WorktreeHandle {
    fn create(repo_root: &Path, path: &Path, base_ref: &str) -> Result<Self> {
        let path_arg = path.to_string_lossy().into_owned();
        run_git(
            repo_root,
            ["worktree", "add", "--detach", &path_arg, base_ref],
            None,
        )
        .with_context(|| {
            format!(
                "failed to create temporary worktree at {} from {base_ref}",
                path.display()
            )
        })?;
        Ok(Self {
            repo_root: repo_root.to_path_buf(),
            path: path.to_path_buf(),
        })
    }

    fn close(&self) -> Result<()> {
        let path_arg = self.path.to_string_lossy().into_owned();
        run_git(
            &self.repo_root,
            ["worktree", "remove", "--force", &path_arg],
            None,
        )
        .with_context(|| format!("failed to remove worktree {}", self.path.display()))
    }
}

impl Drop for WorktreeHandle {
    fn drop(&mut self) {
        let path_arg = self.path.to_string_lossy().into_owned();
        let _ = run_git(
            &self.repo_root,
            ["worktree", "remove", "--force", &path_arg],
            None,
        );
    }
}

pub fn execute_plan(opts: PlanOptions) -> Result<CommandResult> {
    let started_at = Instant::now();
    let repo_root = resolve_repo_root(opts.repo_root.as_deref())?;
    let base_ref = resolve_base_ref(&repo_root, opts.base_ref.as_deref())?;
    let plan = build_plan(
        &repo_root,
        &base_ref,
        &opts.head_ref,
        normalize_branch_prefix(&opts.branch_prefix),
        opts.max_commits_per_slice.max(1),
    )?;
    let output_dir = resolve_output_dir(
        &repo_root,
        plan.head_branch.as_deref(),
        opts.output_dir.as_deref(),
        opts.force,
    )?;
    let bundle = write_plan_bundle(&output_dir, &plan)?;

    let mut result = if plan.loose_ends.blockers.is_empty() {
        CommandResult::success()
    } else {
        CommandResult::partial()
    };

    result = result
        .with_message(format!(
            "Generated git stack plan with {} slice(s)",
            plan.slices.len()
        ))
        .with_details(render_plan_details(&plan, &bundle))
        .with_warnings(plan.loose_ends.blockers.clone())
        .with_data(json!({
            "repo_root": repo_root,
            "base_ref": plan.base_ref,
            "head_ref": plan.head_ref,
            "merge_base": plan.merge_base,
            "plan_path": bundle.plan_path,
            "summary_path": bundle.summary_path,
            "slice_count": plan.slices.len(),
            "blockers": plan.loose_ends.blockers,
            "slices": plan.slices.iter().map(|slice| json!({
                "index": slice.index,
                "branch": slice.branch,
                "pr_base": slice.pr_base,
                "title": slice.title,
                "commit_count": slice.commits.len(),
                "file_count": slice.files.len(),
            })).collect::<Vec<_>>(),
        }))
        .with_duration(started_at.elapsed());

    Ok(result)
}

pub fn execute_materialize(opts: MaterializeOptions) -> Result<CommandResult> {
    let started_at = Instant::now();
    let plan_path = opts.plan_path.canonicalize().with_context(|| {
        format!(
            "failed to resolve stack plan path {}",
            opts.plan_path.display()
        )
    })?;
    let plan_dir = plan_path
        .parent()
        .context("stack plan path has no parent directory")?;
    let plan: GitStackPlan =
        serde_yaml::from_str(&fs::read_to_string(&plan_path).with_context(|| {
            format!("failed to read {}", plan_path.display())
        })?)
        .with_context(|| format!("failed to parse {}", plan_path.display()))?;

    if !opts.allow_blockers && !plan.loose_ends.blockers.is_empty() {
        bail!(
            "stack plan recorded blockers; resolve them or rerun with --allow-blockers:\n{}",
            plan.loose_ends.blockers.join("\n")
        );
    }

    let repo_root = PathBuf::from(&plan.repo_root);
    let materialized = materialize_plan(&repo_root, &plan, opts.force)?;

    Ok(CommandResult::success()
        .with_message(format!(
            "Materialized {} branch(es) from {}",
            materialized.len(),
            plan_path.display()
        ))
        .with_details(
            materialized
                .iter()
                .map(|branch| {
                    format!(
                        "{} -> {} ({})",
                        branch.branch, branch.pr_base, branch.commit
                    )
                })
                .collect::<Vec<_>>(),
        )
        .with_data(json!({
            "plan_path": plan_path,
            "artifacts_dir": plan_dir,
            "branches": materialized,
        }))
        .with_duration(started_at.elapsed()))
}

pub fn execute_split(opts: SplitOptions) -> Result<CommandResult> {
    let started_at = Instant::now();
    let repo_root = resolve_repo_root(opts.plan.repo_root.as_deref())?;
    let base_ref = resolve_base_ref(&repo_root, opts.plan.base_ref.as_deref())?;
    let plan = build_plan(
        &repo_root,
        &base_ref,
        &opts.plan.head_ref,
        normalize_branch_prefix(&opts.plan.branch_prefix),
        opts.plan.max_commits_per_slice.max(1),
    )?;
    let output_dir = resolve_output_dir(
        &repo_root,
        plan.head_branch.as_deref(),
        opts.plan.output_dir.as_deref(),
        opts.plan.force,
    )?;
    let bundle = write_plan_bundle(&output_dir, &plan)?;

    if !opts.allow_blockers && !plan.loose_ends.blockers.is_empty() {
        return Ok(CommandResult::partial()
            .with_message(format!(
                "Generated git stack plan with blockers; branches were not materialized"
            ))
            .with_details(render_plan_details(&plan, &bundle))
            .with_warnings(plan.loose_ends.blockers.clone())
            .with_data(json!({
                "plan_path": bundle.plan_path,
                "summary_path": bundle.summary_path,
                "blockers": plan.loose_ends.blockers,
            }))
            .with_duration(started_at.elapsed()));
    }

    let materialized = materialize_plan(&repo_root, &plan, opts.materialize_force)?;
    Ok(CommandResult::success()
        .with_message(format!(
            "Generated and materialized {} git stack slice(s)",
            materialized.len()
        ))
        .with_details(render_plan_details(&plan, &bundle))
        .with_data(json!({
            "plan_path": bundle.plan_path,
            "summary_path": bundle.summary_path,
            "branches": materialized,
        }))
        .with_duration(started_at.elapsed()))
}

pub fn execute_publish(opts: PublishOptions) -> Result<CommandResult> {
    let started_at = Instant::now();
    let plan_path = opts.plan_path.canonicalize().with_context(|| {
        format!(
            "failed to resolve stack plan path {}",
            opts.plan_path.display()
        )
    })?;
    let (plan, plan_dir) = load_plan_bundle(&plan_path)?;
    validate_publish_preconditions(&plan, opts.allow_blockers)?;
    if opts.create_prs {
        ensure_tool_available(
            "gh",
            "GitHub CLI (`gh`) is required for `xtask git-stack publish` when PR creation is enabled",
        )?;
    }

    let repo_root = PathBuf::from(&plan.repo_root);
    let published = publish_plan(&repo_root, &plan, &plan_dir, &opts)?;
    let pushed = published.iter().filter(|branch| branch.pushed).count();
    let created_prs = published.iter().filter(|branch| branch.pr_url.is_some()).count();
    let reused_prs = published
        .iter()
        .filter(|branch| branch.reused_existing_pr)
        .count();

    Ok(CommandResult::success()
        .with_message(format!(
            "Published {} branch(es) from {}",
            published.len(),
            plan_path.display()
        ))
        .with_detail(format!("pushed {pushed} branch(es) to `{}`", opts.remote))
        .with_detail(format!(
            "created or reused {created_prs} PR(s) ({reused_prs} reused existing)"
        ))
        .with_data(json!({
            "plan_path": plan_path,
            "remote": opts.remote,
            "draft": opts.draft,
            "create_prs": opts.create_prs,
            "branches": published,
        }))
        .with_duration(started_at.elapsed()))
}

fn build_plan(
    repo_root: &Path,
    base_ref: &str,
    head_ref: &str,
    branch_prefix: String,
    max_commits_per_slice: usize,
) -> Result<GitStackPlan> {
    let merge_base = git_stdout(repo_root, ["merge-base", base_ref, head_ref])?;
    let range = format!("{merge_base}..{head_ref}");
    let full_graph = read_full_graph(repo_root, &range)?;
    let first_parent_commits = read_rev_list(repo_root, &["rev-list", "--reverse", "--first-parent", &range])?;
    if first_parent_commits.is_empty() {
        bail!("no commits found between {base_ref} and {head_ref}");
    }

    let full_graph_shas: Vec<String> = full_graph.iter().map(|entry| entry.sha.clone()).collect();
    let first_parent_set: HashSet<String> = first_parent_commits.iter().cloned().collect();
    let non_first_parent_commit_shas = full_graph_shas
        .iter()
        .filter(|sha| !first_parent_set.contains(*sha))
        .cloned()
        .collect::<Vec<_>>();
    let merge_commit_shas = full_graph
        .iter()
        .filter(|entry| entry.parents.len() > 1)
        .map(|entry| entry.sha.clone())
        .collect::<Vec<_>>();
    let first_parent_linear =
        merge_commit_shas.is_empty() && non_first_parent_commit_shas.is_empty();

    let commit_infos = first_parent_commits
        .iter()
        .map(|sha| load_commit_info(repo_root, sha))
        .collect::<Result<Vec<_>>>()?;
    let merge_commits = load_commit_summaries(repo_root, &merge_commit_shas)?;
    let non_first_parent_commits = load_commit_summaries(repo_root, &non_first_parent_commit_shas)?;
    let mut loose_ends = read_loose_ends(repo_root)?;
    if !first_parent_linear {
        let merge_labels = if merge_commits.is_empty() {
            "none".to_string()
        } else {
            merge_commits
                .iter()
                .map(commit_summary_label)
                .collect::<Vec<_>>()
                .join(", ")
        };
        let side_labels = if non_first_parent_commits.is_empty() {
            "none".to_string()
        } else {
            non_first_parent_commits
                .iter()
                .map(commit_summary_label)
                .collect::<Vec<_>>()
                .join(", ")
        };
        loose_ends.blockers.push(format!(
            "non-linear history detected in {range}; planner only auto-materializes first-parent linear stacks. merge commits: {merge_labels}. non-first-parent commits: {side_labels}."
        ));
    }
    if !loose_ends.dirty_paths.is_empty() || !loose_ends.untracked_paths.is_empty() {
        loose_ends.notes.push(
            "dirty worktree state was captured in the plan; materialization uses a temporary worktree and does not rewrite the active checkout".to_string(),
        );
    }
    let grouped = group_commits(commit_infos, max_commits_per_slice);
    let head_branch = current_branch(repo_root)?;

    let mut slices = Vec::new();
    let mut boundary_base = merge_base.clone();
    let mut previous_branch = None::<String>;
    for (index, group) in grouped.into_iter().enumerate() {
        let slice = finalize_slice(
            repo_root,
            index,
            &branch_prefix,
            base_ref,
            previous_branch.as_deref(),
            &boundary_base,
            group,
        )?;
        boundary_base = slice.original_tip_commit.clone();
        previous_branch = Some(slice.branch.clone());
        slices.push(slice);
    }

    if slices.is_empty() {
        bail!("planner produced no slices for {range}");
    }

    Ok(GitStackPlan {
        version: 1,
        generated_at: OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("static RFC3339 format should be valid"),
        repo_root: repo_root.display().to_string(),
        base_ref: base_ref.to_string(),
        head_ref: head_ref.to_string(),
        head_branch,
        merge_base,
        graph: GitGraphSummary {
            full_graph_commits: full_graph.len(),
            first_parent_commits: first_parent_commits.len(),
            merge_commits,
            non_first_parent_commits,
            first_parent_linear,
        },
        loose_ends,
        slices,
    })
}

fn materialize_plan(
    repo_root: &Path,
    plan: &GitStackPlan,
    force: bool,
) -> Result<Vec<MaterializedBranch>> {
    let mut materialized = Vec::new();

    for slice in &plan.slices {
        if branch_exists(repo_root, &slice.branch)? && !force {
            bail!(
                "branch {} already exists; rerun with --force to overwrite",
                slice.branch
            );
        }

        let temp_root = tempfile::tempdir().context("failed to create temporary worktree root")?;
        let worktree_path = temp_root.path().join(format!("slice-{:02}-{}", slice.index, slice.slug));
        let worktree = WorktreeHandle::create(repo_root, &worktree_path, &slice.pr_base)?;
        let patch = git_stdout_bytes(
            repo_root,
            [
                "diff",
                "--binary",
                &slice.original_base_commit,
                &slice.original_tip_commit,
            ],
        )?;
        if patch.is_empty() {
            bail!(
                "slice {} has an empty net diff between {} and {}",
                slice.branch,
                slice.original_base_commit,
                slice.original_tip_commit
            );
        }

        run_git(
            &worktree.path,
            ["apply", "--index", "--3way", "-"],
            Some(&patch),
        )
        .with_context(|| format!("failed to apply slice patch for {}", slice.branch))?;

        let message_path = worktree.path.join(".git-stack-squash-message.txt");
        fs::write(&message_path, &slice.squash_body)
            .with_context(|| format!("failed to write {}", message_path.display()))?;
        let message_arg = message_path.to_string_lossy().into_owned();
        run_git(&worktree.path, ["commit", "-F", &message_arg], None)
            .with_context(|| format!("failed to create squashed commit for {}", slice.branch))?;
        let commit = git_stdout(&worktree.path, ["rev-parse", "HEAD"])?;
        run_git(&worktree.path, ["branch", "-f", &slice.branch, &commit], None).with_context(
            || format!("failed to update branch {} to {}", slice.branch, commit),
        )?;
        worktree.close()?;

        materialized.push(MaterializedBranch {
            branch: slice.branch.clone(),
            pr_base: slice.pr_base.clone(),
            commit,
            title: slice.title.clone(),
        });
    }

    Ok(materialized)
}

fn publish_plan(
    repo_root: &Path,
    plan: &GitStackPlan,
    plan_dir: &Path,
    opts: &PublishOptions,
) -> Result<Vec<PublishedBranch>> {
    let mut published = Vec::with_capacity(plan.slices.len());

    for slice in &plan.slices {
        if !branch_exists(repo_root, &slice.branch)? {
            bail!(
                "branch {} does not exist locally; materialize the plan before publishing",
                slice.branch
            );
        }

        let remote_branch = slice.branch.clone();
        push_branch(repo_root, &opts.remote, &slice.branch, &remote_branch, opts.force_with_lease)?;

        let mut published_branch = PublishedBranch {
            branch: slice.branch.clone(),
            remote: opts.remote.clone(),
            pr_base: normalize_pr_base(&slice.pr_base, &opts.remote),
            remote_branch,
            pushed: true,
            title: slice.title.clone(),
            pr_url: None,
            pr_number: None,
            reused_existing_pr: false,
        };

        if opts.create_prs {
            if let Some(existing) = find_existing_pr(
                repo_root,
                opts.repo.as_deref(),
                &slice.branch,
            )? {
                published_branch.pr_url = Some(existing.url);
                published_branch.pr_number = Some(existing.number);
                published_branch.reused_existing_pr = true;
            } else {
                let pr_body_path = pr_body_path(plan_dir, slice);
                let created = create_pull_request(
                    repo_root,
                    opts.repo.as_deref(),
                    &slice.title,
                    &pr_body_path,
                    &published_branch.pr_base,
                    &slice.branch,
                    opts.draft,
                )?;
                published_branch.pr_url = Some(created.url);
                published_branch.pr_number = Some(created.number);
            }
        }

        published.push(published_branch);
    }

    Ok(published)
}

fn load_plan_bundle(plan_path: &Path) -> Result<(GitStackPlan, PathBuf)> {
    let plan_dir = plan_path
        .parent()
        .context("stack plan path has no parent directory")?
        .to_path_buf();
    let plan: GitStackPlan =
        serde_yaml::from_str(&fs::read_to_string(plan_path).with_context(|| {
            format!("failed to read {}", plan_path.display())
        })?)
        .with_context(|| format!("failed to parse {}", plan_path.display()))?;
    Ok((plan, plan_dir))
}

fn validate_publish_preconditions(plan: &GitStackPlan, allow_blockers: bool) -> Result<()> {
    if !allow_blockers && !plan.loose_ends.blockers.is_empty() {
        bail!(
            "stack plan recorded blockers; resolve them or rerun with --allow-blockers:\n{}",
            plan.loose_ends.blockers.join("\n")
        );
    }
    if plan.slices.is_empty() {
        bail!("stack plan contains no slices");
    }
    Ok(())
}

fn ensure_tool_available(tool: &str, message: &str) -> Result<()> {
    which::which(tool).with_context(|| message.to_string())?;
    Ok(())
}

fn push_branch(
    repo_root: &Path,
    remote: &str,
    local_branch: &str,
    remote_branch: &str,
    force_with_lease: bool,
) -> Result<()> {
    let refspec = format!("refs/heads/{local_branch}:refs/heads/{remote_branch}");
    let mut args = vec!["push"];
    if force_with_lease {
        args.push("--force-with-lease");
    }
    args.push(remote);
    args.push(&refspec);
    run_git_dynamic(repo_root, "git", &args)
        .with_context(|| format!("failed to push {local_branch} to {remote}/{remote_branch}"))
}

fn normalize_pr_base(pr_base: &str, remote: &str) -> String {
    if let Some(stripped) = pr_base.strip_prefix(&format!("{remote}/")) {
        return stripped.to_string();
    }
    if let Some(stripped) = pr_base.strip_prefix(&format!("refs/remotes/{remote}/")) {
        return stripped.to_string();
    }
    if let Some(stripped) = pr_base.strip_prefix("refs/heads/") {
        return stripped.to_string();
    }
    pr_base.to_string()
}

fn pr_body_path(plan_dir: &Path, slice: &GitStackSlice) -> PathBuf {
    plan_dir
        .join(format!("slice-{:02}-{}", slice.index + 1, slice.slug))
        .join("pr-body.md")
}

fn find_existing_pr(
    repo_root: &Path,
    repo: Option<&str>,
    branch: &str,
) -> Result<Option<ExistingPullRequest>> {
    let mut args = vec![
        "pr",
        "view",
        branch,
        "--json",
        "number,url",
    ];
    if let Some(repo) = repo {
        args.push("--repo");
        args.push(repo);
    }

    match command_stdout(repo_root, "gh", &args) {
        Ok(stdout) => Ok(Some(
            serde_json::from_str(&stdout)
                .with_context(|| format!("failed to parse gh pr view output for {branch}"))?,
        )),
        Err(error) => {
            let rendered = format!("{error:#}");
            if rendered.contains("no pull requests found")
                || rendered.contains("could not find pull request")
                || rendered.contains("not found")
            {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

fn create_pull_request(
    repo_root: &Path,
    repo: Option<&str>,
    title: &str,
    body_file: &Path,
    base: &str,
    head: &str,
    draft: bool,
) -> Result<ExistingPullRequest> {
    let body_arg = body_file.to_string_lossy().into_owned();
    let mut args = vec![
        "pr",
        "create",
        "--title",
        title,
        "--body-file",
        &body_arg,
        "--base",
        base,
        "--head",
        head,
    ];
    if draft {
        args.push("--draft");
    }
    if let Some(repo) = repo {
        args.push("--repo");
        args.push(repo);
    }

    let url = command_stdout(repo_root, "gh", &args)
        .with_context(|| format!("failed to create PR for branch {head}"))?;
    let existing = find_existing_pr(repo_root, repo, head)?
        .with_context(|| format!("created PR for {head} but could not resolve it via gh pr view"))?;
    if existing.url != url && !url.is_empty() {
        return Ok(ExistingPullRequest { url, ..existing });
    }
    Ok(existing)
}

fn read_loose_ends(repo_root: &Path) -> Result<GitLooseEnds> {
    let output = git_stdout(repo_root, ["status", "--porcelain=v1", "--untracked-files=all"])?;
    let mut loose_ends = GitLooseEnds::default();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        if let Some(path) = line.strip_prefix("?? ") {
            loose_ends.untracked_paths.push(path.to_string());
            continue;
        }
        if line.len() < 4 {
            continue;
        }
        let path = extract_status_path(&line[3..]);
        loose_ends.dirty_paths.push(path);
    }
    loose_ends.dirty_paths.sort();
    loose_ends.dirty_paths.dedup();
    loose_ends.untracked_paths.sort();
    loose_ends.untracked_paths.dedup();
    Ok(loose_ends)
}

fn extract_status_path(rest: &str) -> String {
    rest.rsplit(" -> ")
        .next()
        .map(str::trim)
        .unwrap_or(rest)
        .to_string()
}

#[derive(Debug, Clone)]
struct GraphEntry {
    sha: String,
    parents: Vec<String>,
}

fn read_full_graph(repo_root: &Path, range: &str) -> Result<Vec<GraphEntry>> {
    let output = git_stdout(
        repo_root,
        ["rev-list", "--reverse", "--topo-order", "--parents", range],
    )?;
    output
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split_whitespace();
            let sha = parts
                .next()
                .context("rev-list --parents returned a malformed line")?;
            Ok(GraphEntry {
                sha: sha.to_string(),
                parents: parts.map(str::to_string).collect(),
            })
        })
        .collect()
}

fn read_rev_list<const N: usize>(repo_root: &Path, args: &[&str; N]) -> Result<Vec<String>> {
    Ok(git_stdout(repo_root, *args)?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .collect())
}

fn load_commit_info(repo_root: &Path, sha: &str) -> Result<CommitInfo> {
    let header = git_stdout(
        repo_root,
        [
            "show",
            "-s",
            "--format=%H%x1f%s%x1f%b",
            sha,
        ],
    )?;
    let mut parts = header.splitn(3, '\u{1f}');
    let full_sha = parts
        .next()
        .context("missing commit sha in git show output")?
        .trim()
        .to_string();
    let subject = parts
        .next()
        .context("missing commit subject in git show output")?
        .trim()
        .to_string();
    let body = parts.next().unwrap_or("").trim().to_string();
    let files = git_stdout(repo_root, ["diff-tree", "--no-commit-id", "--name-only", "-r", sha])?
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let packages = files
        .iter()
        .filter_map(|file| package_for_path(file))
        .collect::<BTreeSet<_>>();
    let topics = files
        .iter()
        .filter_map(|file| topic_key_for_path(file))
        .collect::<BTreeSet<_>>();
    let subject_parts = parse_subject(&subject);

    Ok(CommitInfo {
        short_sha: shorten_sha(&full_sha),
        sha: full_sha,
        subject,
        body,
        files,
        packages,
        topics,
        subject_parts,
    })
}

fn load_commit_summaries(repo_root: &Path, shas: &[String]) -> Result<Vec<GitStackCommitSummary>> {
    shas.iter()
        .map(|sha| {
            let subject = git_stdout(repo_root, ["show", "-s", "--format=%s", sha])?;
            Ok(GitStackCommitSummary {
                sha: sha.clone(),
                short_sha: shorten_sha(sha),
                subject,
            })
        })
        .collect()
}

fn group_commits(commits: Vec<CommitInfo>, max_commits_per_slice: usize) -> Vec<SliceAccumulator> {
    let mut slices = Vec::<SliceAccumulator>::new();

    for commit in commits {
        if let Some(current) = slices.last_mut()
            && should_extend_slice(current, &commit, max_commits_per_slice)
        {
            current.push(commit);
            continue;
        }
        slices.push(SliceAccumulator::new(commit));
    }

    let target_slice_count = target_slice_count(slices.iter().map(|slice| slice.commits.len()).sum());
    coalesce_slices(slices, max_commits_per_slice, target_slice_count)
}

fn should_extend_slice(
    current: &SliceAccumulator,
    commit: &CommitInfo,
    max_commits_per_slice: usize,
) -> bool {
    let topic_overlap = intersection_count(&current.topics, &commit.topics);
    let package_overlap = intersection_count(&current.packages, &commit.packages);
    let scope_match = commit
        .subject_parts
        .scope
        .as_ref()
        .is_some_and(|scope| current.scopes.contains(scope));
    let prefix_overlap = commit
        .files
        .iter()
        .map(|file| major_file_prefix(file))
        .any(|prefix| current.file_prefixes.contains(&prefix));
    let package_span = current.packages.union(&commit.packages).count();
    let follow_up_kind = matches!(
        commit.subject_parts.kind.as_str(),
        "test" | "fix" | "lint" | "docs"
    );
    let weak_overlap = topic_overlap == 0 && package_overlap == 0 && !scope_match;
    let large_footprint =
        current.file_count >= LARGE_SLICE_FILE_COUNT || commit.files.len() >= LARGE_COMMIT_FILE_COUNT;

    if current.commits.len() >= max_commits_per_slice && topic_overlap == 0 {
        return false;
    }
    if package_span > MAX_PACKAGES_PER_SLICE && topic_overlap == 0 {
        return false;
    }
    if weak_overlap && large_footprint {
        return false;
    }

    if topic_overlap > 0 || prefix_overlap {
        return true;
    }

    if follow_up_kind && (package_overlap > 0 || scope_match) {
        return true;
    }

    false
}

fn coalesce_slices(
    mut slices: Vec<SliceAccumulator>,
    max_commits_per_slice: usize,
    target_slice_count: usize,
) -> Vec<SliceAccumulator> {
    const MIN_MERGE_SCORE: usize = 2;

    if slices.len() <= 1 {
        return slices;
    }

    loop {
        let should_force = slices.len() > target_slice_count;
        let mut best: Option<(usize, usize)> = None;

        for index in 0..slices.len().saturating_sub(1) {
            let combined_commits = slices[index].commits.len() + slices[index + 1].commits.len();
            if combined_commits > max_commits_per_slice {
                continue;
            }
            let score = adjacent_merge_score(&slices[index], &slices[index + 1]);
            if score == 0 {
                continue;
            }
            if best.is_none_or(|(_, current_score)| score > current_score) {
                best = Some((index, score));
            }
        }

        let Some((index, score)) = best else {
            break;
        };
        if score < MIN_MERGE_SCORE && !should_force {
            break;
        }

        let right = slices.remove(index + 1);
        slices[index].merge_from(right);
    }

    slices
}

fn adjacent_merge_score(left: &SliceAccumulator, right: &SliceAccumulator) -> usize {
    let topic_overlap = intersection_count(&left.topics, &right.topics);
    let package_overlap = intersection_count(&left.packages, &right.packages);
    if left.packages.union(&right.packages).count() > MAX_PACKAGES_PER_SLICE && topic_overlap == 0 {
        return 0;
    }
    let scope_overlap = intersection_count(&left.scopes, &right.scopes);
    let weak_overlap = topic_overlap == 0 && package_overlap == 0 && scope_overlap == 0;
    if weak_overlap
        && (left.file_count >= LARGE_SLICE_FILE_COUNT || right.file_count >= LARGE_COMMIT_FILE_COUNT)
    {
        return 0;
    }
    let prefix_overlap = intersection_count(&left.file_prefixes, &right.file_prefixes);
    let left_tail = left.commits.last().expect("slice should not be empty");
    let right_head = right.commits.first().expect("slice should not be empty");
    let kind_match = left_tail.subject_parts.kind == right_head.subject_parts.kind
        && left_tail.subject_parts.kind != "misc";
    let follow_up_kind = matches!(
        right_head.subject_parts.kind.as_str(),
        "fix" | "test" | "perf" | "docs" | "lint"
    );
    let singleton_bonus =
        usize::from(left.commits.len() == 1 || right.commits.len() == 1) * usize::from(package_overlap > 0);

    topic_overlap * 4
        + package_overlap * 2
        + scope_overlap * 2
        + prefix_overlap
        + usize::from(kind_match)
        + usize::from(follow_up_kind && package_overlap > 0)
        + singleton_bonus
}

fn target_slice_count(commit_count: usize) -> usize {
    commit_count.div_ceil(4).max(1)
}

fn finalize_slice(
    repo_root: &Path,
    index: usize,
    branch_prefix: &str,
    base_ref: &str,
    previous_branch: Option<&str>,
    original_base_commit: &str,
    group: SliceAccumulator,
) -> Result<GitStackSlice> {
    let anchor = select_anchor_commit(&group.commits);
    let title = anchor.subject.clone();
    let slug = slugify(title.split_once(':').map(|(_, summary)| summary).unwrap_or(&title));
    let branch = if branch_prefix.is_empty() {
        format!("{:02}-{slug}", index + 1)
    } else {
        format!("{branch_prefix}/{:02}-{slug}", index + 1)
    };
    let pr_base = previous_branch.unwrap_or(base_ref).to_string();
    let depends_on = previous_branch.map(str::to_string);
    let original_tip_commit = group
        .commits
        .last()
        .context("slice unexpectedly has no commits")?
        .sha
        .clone();
    let files = git_stdout(
        repo_root,
        ["diff", "--name-only", original_base_commit, &original_tip_commit],
    )?
    .lines()
    .filter(|line| !line.trim().is_empty())
    .map(str::to_string)
    .collect::<Vec<_>>();
    let diffstat = {
        let raw = git_stdout(
            repo_root,
            ["diff", "--shortstat", original_base_commit, &original_tip_commit],
        )?;
        let trimmed = raw.trim().to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    };

    let topics = top_topics(&group.commits, 3);
    let packages = group.packages.iter().cloned().collect::<Vec<_>>();
    let commits = group
        .commits
        .iter()
        .map(|commit| GitStackCommitSummary {
            sha: commit.sha.clone(),
            short_sha: commit.short_sha.clone(),
            subject: commit.subject.clone(),
        })
        .collect::<Vec<_>>();
    let verification = suggested_verification(&packages);
    let body_highlights = collect_commit_highlights(&group.commits, 4);
    let rationale = render_rationale(&title, &topics, &packages, group.commits.len(), depends_on.as_deref());
    let pr_body = render_pr_body(
        &title,
        &rationale,
        &branch,
        &pr_base,
        depends_on.as_deref(),
        original_base_commit,
        &original_tip_commit,
        diffstat.as_deref(),
        &packages,
        &topics,
        &files,
        &commits,
        &body_highlights,
        &verification,
    );
    let squash_title = title.clone();
    let squash_body = render_squash_body(
        &squash_title,
        &rationale,
        &packages,
        &topics,
        diffstat.as_deref(),
        &commits,
        &body_highlights,
        &verification,
    );

    Ok(GitStackSlice {
        index,
        id: format!("slice-{:02}", index + 1),
        slug,
        branch,
        pr_base,
        depends_on,
        original_base_commit: original_base_commit.to_string(),
        original_tip_commit,
        title,
        rationale,
        diffstat,
        packages,
        topics,
        files,
        commits,
        pr_body,
        squash_title,
        squash_body,
        verification,
    })
}

fn select_anchor_commit(commits: &[CommitInfo]) -> &CommitInfo {
    commits
        .iter()
        .rev()
        .find(|commit| !matches!(commit.subject_parts.kind.as_str(), "test" | "lint" | "docs"))
        .unwrap_or_else(|| commits.last().expect("slice should have at least one commit"))
}

fn top_topics(commits: &[CommitInfo], limit: usize) -> Vec<String> {
    let mut counts = BTreeMap::<String, usize>::new();
    for commit in commits {
        for topic in &commit.topics {
            *counts.entry(topic.clone()).or_default() += 1;
        }
    }

    let mut ranked = counts.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    ranked
        .into_iter()
        .take(limit)
        .map(|(topic, _)| topic)
        .collect()
}

fn render_rationale(
    title: &str,
    topics: &[String],
    packages: &[String],
    commit_count: usize,
    depends_on: Option<&str>,
) -> String {
    let topic_text = if topics.is_empty() {
        "a single coherent area".to_string()
    } else {
        topics
            .iter()
            .map(|topic| format!("`{topic}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let package_text = if packages.is_empty() {
        "workspace-level changes".to_string()
    } else {
        packages
            .iter()
            .map(|pkg| format!("`{pkg}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };

    match depends_on {
        Some(previous) => format!(
            "This slice keeps the {topic_text} work together across {commit_count} original commit(s), builds on `{previous}`, and isolates the {package_text} changes behind the single review line `{title}`.",
        ),
        None => format!(
            "This bottom-of-stack slice keeps the {topic_text} work together across {commit_count} original commit(s) so later slices can build on a stable {package_text} base.",
        ),
    }
}

fn render_pr_body(
    title: &str,
    rationale: &str,
    branch: &str,
    pr_base: &str,
    depends_on: Option<&str>,
    original_base_commit: &str,
    original_tip_commit: &str,
    diffstat: Option<&str>,
    packages: &[String],
    topics: &[String],
    files: &[String],
    commits: &[GitStackCommitSummary],
    body_highlights: &[String],
    verification: &[String],
) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {title}\n\n"));
    out.push_str("## Why\n\n");
    out.push_str(rationale);
    out.push_str("\n\n## Stack Context\n\n");
    out.push_str(&format!("- Materialized branch: `{branch}`\n"));
    out.push_str(&format!("- Recommended PR base: `{pr_base}`\n"));
    if let Some(depends_on) = depends_on {
        out.push_str(&format!("- Depends on: `{depends_on}`\n"));
    }
    out.push_str(&format!(
        "- Original commit window: `{}` -> `{}`\n",
        shorten_sha(original_base_commit),
        shorten_sha(original_tip_commit)
    ));
    if let Some(diffstat) = diffstat {
        out.push_str(&format!("- Net diff: {diffstat}\n"));
    }
    out.push_str("\n## Scope\n\n");
    if !packages.is_empty() {
        out.push_str(&format!(
            "- Packages: {}\n",
            packages
                .iter()
                .map(|pkg| format!("`{pkg}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !topics.is_empty() {
        out.push_str(&format!(
            "- Dominant topics: {}\n",
            topics
                .iter()
                .map(|topic| format!("`{topic}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    out.push_str(&format!("- Files in net diff: {}\n", files.len()));
    out.push_str("\n## Included commits\n\n");
    for commit in commits {
        out.push_str(&format!("- `{}` {}\n", commit.short_sha, commit.subject));
    }
    if !body_highlights.is_empty() {
        out.push_str("\n## Original commit intent\n\n");
        for highlight in body_highlights {
            out.push_str(&format!("- {highlight}\n"));
        }
    }
    out.push_str("\n## Review focus\n\n");
    for file in files.iter().take(8) {
        out.push_str(&format!("- `{file}`\n"));
    }
    out.push_str("\n## Suggested verification\n\n");
    for command in verification {
        out.push_str(&format!("- `{command}`\n"));
    }
    out
}

fn render_squash_body(
    title: &str,
    rationale: &str,
    packages: &[String],
    topics: &[String],
    diffstat: Option<&str>,
    commits: &[GitStackCommitSummary],
    body_highlights: &[String],
    verification: &[String],
) -> String {
    let mut out = String::new();
    out.push_str(title);
    out.push_str("\n\n");
    out.push_str(&format!("- {rationale}\n"));
    if !packages.is_empty() {
        out.push_str(&format!(
            "- Packages: {}\n",
            packages
                .iter()
                .map(|pkg| format!("`{pkg}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !topics.is_empty() {
        out.push_str(&format!(
            "- Topics: {}\n",
            topics
                .iter()
                .map(|topic| format!("`{topic}`"))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if let Some(diffstat) = diffstat {
        out.push_str(&format!("- Net diff: {diffstat}\n"));
    }
    out.push_str(&format!(
        "- Original commits: {}\n",
        commits
            .iter()
            .map(|commit| format!("`{}` {}", commit.short_sha, commit.subject))
            .collect::<Vec<_>>()
            .join("; ")
    ));
    if !body_highlights.is_empty() {
        out.push_str(&format!(
            "- Original intent: {}\n",
            body_highlights.join("; ")
        ));
    }
    out.push_str(&format!(
        "- Verification: {}\n",
        verification
            .iter()
            .map(|command| format!("`{command}`"))
            .collect::<Vec<_>>()
            .join("; ")
    ));
    if !commits.is_empty() && commits.len() > 5 {
        out.push_str(&format!(
            "- Folded {} original commit(s) into one reviewable stack slice.\n",
            commits.len()
        ));
    }
    out
}

fn collect_commit_highlights(commits: &[CommitInfo], limit: usize) -> Vec<String> {
    commits
        .iter()
        .filter_map(|commit| {
            let line = first_meaningful_body_line(&commit.body)?;
            Some(format!("`{}` {line}", commit.short_sha))
        })
        .take(limit)
        .collect()
}

fn first_meaningful_body_line(body: &str) -> Option<String> {
    const META_PREFIXES: &[&str] = &[
        "signed-off-by:",
        "co-authored-by:",
        "refs:",
        "ref:",
        "fixes:",
        "closes:",
    ];

    body.lines().find_map(|raw| {
        let line = raw
            .trim()
            .trim_start_matches("- ")
            .trim_start_matches("* ")
            .trim();
        if line.is_empty() {
            return None;
        }
        let lowercase = line.to_ascii_lowercase();
        if META_PREFIXES.iter().any(|prefix| lowercase.starts_with(prefix)) {
            return None;
        }
        Some(line.to_string())
    })
}

fn suggested_verification(packages: &[String]) -> Vec<String> {
    if packages.is_empty() {
        return vec!["xtask check --full".to_string()];
    }

    let mut commands = vec![
        format!(
            "xtask check {}",
            packages
                .iter()
                .map(|pkg| format!("-p {pkg}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
        .trim()
        .to_string(),
    ];
    commands.extend(packages.iter().map(|pkg| format!("xtask test -p {pkg}")));
    commands
}

struct WrittenPlanBundle {
    plan_path: PathBuf,
    summary_path: PathBuf,
}

fn write_plan_bundle(output_dir: &Path, plan: &GitStackPlan) -> Result<WrittenPlanBundle> {
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;

    let plan_path = output_dir.join("plan.yaml");
    let summary_path = output_dir.join("summary.md");
    fs::write(
        &plan_path,
        serde_yaml::to_string(plan).context("failed to serialize stack plan")?,
    )
    .with_context(|| format!("failed to write {}", plan_path.display()))?;
    fs::write(&summary_path, render_summary_markdown(plan))
        .with_context(|| format!("failed to write {}", summary_path.display()))?;

    for slice in &plan.slices {
        let slice_dir = output_dir.join(format!("slice-{:02}-{}", slice.index + 1, slice.slug));
        fs::create_dir_all(&slice_dir)
            .with_context(|| format!("failed to create {}", slice_dir.display()))?;
        fs::write(slice_dir.join("pr-body.md"), &slice.pr_body).with_context(|| {
            format!("failed to write PR body for {}", slice.branch)
        })?;
        fs::write(slice_dir.join("squash-body.txt"), &slice.squash_body).with_context(|| {
            format!("failed to write squash body for {}", slice.branch)
        })?;
    }

    Ok(WrittenPlanBundle {
        plan_path,
        summary_path,
    })
}

fn render_summary_markdown(plan: &GitStackPlan) -> String {
    let mut out = String::new();
    out.push_str("# Git Stack Plan\n\n");
    out.push_str(&format!(
        "- Base ref: `{}`\n- Head ref: `{}`\n- Merge base: `{}`\n- First-parent linear: `{}`\n- Full graph commits: `{}`\n- First-parent commits: `{}`\n",
        plan.base_ref,
        plan.head_ref,
        shorten_sha(&plan.merge_base),
        plan.graph.first_parent_linear,
        plan.graph.full_graph_commits,
        plan.graph.first_parent_commits,
    ));
    if !plan.loose_ends.blockers.is_empty() {
        out.push_str("\n## Blockers\n\n");
        for blocker in &plan.loose_ends.blockers {
            out.push_str(&format!("- {blocker}\n"));
        }
    }
    if !plan.graph.merge_commits.is_empty() {
        out.push_str("\n## Merge commits in range\n\n");
        for commit in &plan.graph.merge_commits {
            out.push_str(&format!(
                "- `{}` {}\n",
                commit.short_sha, commit.subject
            ));
        }
    }
    if !plan.graph.non_first_parent_commits.is_empty() {
        out.push_str("\n## Non-first-parent commits in range\n\n");
        for commit in &plan.graph.non_first_parent_commits {
            out.push_str(&format!(
                "- `{}` {}\n",
                commit.short_sha, commit.subject
            ));
        }
    }
    if !plan.loose_ends.dirty_paths.is_empty() || !plan.loose_ends.untracked_paths.is_empty() {
        out.push_str("\n## Loose ends\n\n");
        for line in render_loose_end_lines(&plan.loose_ends) {
            out.push_str(&line);
            out.push('\n');
        }
    }
    out.push_str("\n## Slices\n\n");
    for slice in &plan.slices {
        out.push_str(&format!(
            "### {:02}. `{}`\n\n- PR base: `{}`\n- Title: {}\n- Commits: {}\n- Files: {}\n",
            slice.index + 1,
            slice.branch,
            slice.pr_base,
            slice.title,
            slice.commits.len(),
            slice.files.len(),
        ));
        if let Some(diffstat) = &slice.diffstat {
            out.push_str(&format!("- Net diff: {diffstat}\n"));
        }
        if let Some(depends_on) = &slice.depends_on {
            out.push_str(&format!("- Depends on: `{depends_on}`\n"));
        }
        out.push('\n');
    }
    out
}

fn render_plan_details(plan: &GitStackPlan, bundle: &WrittenPlanBundle) -> Vec<String> {
    let mut details = vec![
        format!("plan: {}", bundle.plan_path.display()),
        format!("summary: {}", bundle.summary_path.display()),
        format!(
            "graph: {} full-graph commit(s), {} first-parent commit(s)",
            plan.graph.full_graph_commits, plan.graph.first_parent_commits
        ),
        format!(
            "loose ends: {} dirty path(s), {} untracked path(s)",
            plan.loose_ends.dirty_paths.len(),
            plan.loose_ends.untracked_paths.len()
        ),
    ];
    details.extend(plan.slices.iter().map(|slice| {
        format!(
            "{:02}. {} -> {} ({} commit(s), {} file(s))",
            slice.index + 1,
            slice.branch,
            slice.pr_base,
            slice.commits.len(),
            slice.files.len()
        )
    }));
    details
}

fn render_loose_end_lines(loose_ends: &GitLooseEnds) -> Vec<String> {
    let mut lines = Vec::new();
    lines.extend(summarize_loose_end_paths("dirty", &loose_ends.dirty_paths));
    lines.extend(summarize_loose_end_paths("untracked", &loose_ends.untracked_paths));
    for note in &loose_ends.notes {
        lines.push(format!("- note: {note}"));
    }
    lines
}

fn summarize_loose_end_paths(label: &str, paths: &[String]) -> Vec<String> {
    let mut collapsed = BTreeMap::<String, usize>::new();
    let mut literal = Vec::<String>::new();

    for path in paths {
        if let Some(key) = collapse_loose_end_key(path) {
            *collapsed.entry(key).or_default() += 1;
        } else {
            literal.push(path.clone());
        }
    }

    literal.sort();
    let mut lines = literal
        .into_iter()
        .map(|path| format!("- {label}: `{path}`"))
        .collect::<Vec<_>>();
    lines.extend(collapsed.into_iter().map(|(key, count)| {
        let suffix = if count == 1 { "file" } else { "files" };
        format!("- {label}: `{key}` ({count} {suffix})")
    }));
    lines
}

fn collapse_loose_end_key(path: &str) -> Option<String> {
    let parts = path.split('/').collect::<Vec<_>>();
    let sinex_index = parts.iter().position(|part| *part == ".sinex")?;
    let mut keep = parts[..=sinex_index].to_vec();
    if let Some(next) = parts.get(sinex_index + 1) {
        keep.push(next);
    }
    Some(keep.join("/"))
}

fn resolve_repo_root(explicit: Option<&Path>) -> Result<PathBuf> {
    if let Some(explicit) = explicit {
        return Ok(explicit.to_path_buf());
    }

    let cwd = std::env::current_dir().context("failed to determine current directory")?;
    let repo_root = git_stdout(&cwd, ["rev-parse", "--show-toplevel"])
        .context("failed to determine git repository root")?;
    Ok(PathBuf::from(repo_root))
}

fn resolve_base_ref(repo_root: &Path, explicit: Option<&str>) -> Result<String> {
    if let Some(explicit) = explicit {
        ensure_ref_exists(repo_root, explicit)?;
        return Ok(explicit.to_string());
    }
    for candidate in ["origin/master", "master"] {
        if branch_exists(repo_root, candidate)? || ref_exists(repo_root, candidate)? {
            return Ok(candidate.to_string());
        }
    }
    bail!(
        "could not resolve default base ref; pass --base explicitly (tried origin/master, master)"
    )
}

fn resolve_output_dir(
    repo_root: &Path,
    head_branch: Option<&str>,
    explicit: Option<&Path>,
    force: bool,
) -> Result<PathBuf> {
    let output_dir = if let Some(explicit) = explicit {
        explicit.to_path_buf()
    } else {
        let stamp = OffsetDateTime::now_utc()
            .format(
                &time::format_description::parse(
                    "[year][month][day]T[hour][minute][second]Z",
                )
                .expect("static timestamp format is valid"),
            )
            .expect("timestamp formatting should succeed");
        let branch = slugify(head_branch.unwrap_or("detached"));
        repo_root.join(".sinex/git-stack").join(format!("{stamp}-{branch}"))
    };

    if output_dir.exists() {
        if !force {
            bail!(
                "output directory {} already exists; rerun with --force to overwrite",
                output_dir.display()
            );
        }
        fs::remove_dir_all(&output_dir)
            .with_context(|| format!("failed to remove {}", output_dir.display()))?;
    }

    Ok(output_dir)
}

fn current_branch(repo_root: &Path) -> Result<Option<String>> {
    let branch = git_stdout(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"])?;
    if branch == "HEAD" {
        Ok(None)
    } else {
        Ok(Some(branch))
    }
}

fn normalize_branch_prefix(prefix: &str) -> String {
    prefix
        .trim_matches('/')
        .trim()
        .replace("//", "/")
        .if_empty_then(DEFAULT_BRANCH_PREFIX)
}

trait EmptyDefault {
    fn if_empty_then(self, fallback: &str) -> String;
}

impl EmptyDefault for String {
    fn if_empty_then(self, fallback: &str) -> String {
        if self.is_empty() {
            fallback.to_string()
        } else {
            self
        }
    }
}

fn parse_subject(subject: &str) -> CommitSubjectParts {
    if let Some((prefix, _summary)) = subject.split_once(':') {
        if let Some((kind, scope)) = prefix.split_once('(') {
            if let Some(scope) = scope.strip_suffix(')') {
                return CommitSubjectParts {
                    kind: kind.trim().to_string(),
                    scope: Some(scope.trim().to_string()),
                };
            }
        }
        return CommitSubjectParts {
            kind: prefix.trim().to_string(),
            scope: None,
        };
    }

    CommitSubjectParts {
        kind: "misc".to_string(),
        scope: None,
    }
}

fn topic_key_for_path(path: &str) -> Option<String> {
    let package = package_for_path(path)?;
    let parts = path.split('/').collect::<Vec<_>>();
    let local = if parts.first() == Some(&"xtask") {
        &parts[1..]
    } else if parts.first() == Some(&"tests") {
        &parts[2..]
    } else if parts.len() >= 3 && parts.first() == Some(&"crate") {
        &parts[3..]
    } else {
        return Some(format!("{package}:root"));
    };
    let dirs = if local.len() > 1 {
        &local[..local.len() - 1]
    } else {
        &[][..]
    };
    let topic = if dirs.is_empty() {
        "root".to_string()
    } else {
        dirs.iter().take(4).copied().collect::<Vec<_>>().join("/")
    };
    Some(format!("{package}:{topic}"))
}

fn major_file_prefix(path: &str) -> String {
    path.split('/')
        .take(4)
        .collect::<Vec<_>>()
        .join("/")
}

fn intersection_count(left: &BTreeSet<String>, right: &BTreeSet<String>) -> usize {
    left.intersection(right).count()
}

fn slugify(input: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;
    for ch in input.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash {
            slug.push('-');
            last_dash = true;
        }
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "slice".to_string()
    } else {
        slug
    }
}

fn shorten_sha(sha: &str) -> String {
    sha.chars().take(8).collect()
}

fn commit_summary_label(summary: &GitStackCommitSummary) -> String {
    format!("`{}` {}", summary.short_sha, summary.subject)
}

fn branch_exists(repo_root: &Path, branch: &str) -> Result<bool> {
    ref_exists(repo_root, &format!("refs/heads/{branch}"))
}

fn ref_exists(repo_root: &Path, reference: &str) -> Result<bool> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "--quiet", reference])
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("failed to probe git ref {reference}"))?;
    Ok(output.status.success())
}

fn ensure_ref_exists(repo_root: &Path, reference: &str) -> Result<()> {
    if ref_exists(repo_root, reference)? {
        return Ok(());
    }
    bail!("git ref {reference} does not exist");
}

fn git_stdout<const N: usize>(repo_root: &Path, args: [&str; N]) -> Result<String> {
    ProcessBuilder::git()
        .args(args)
        .current_dir(repo_root)
        .run()
        .map(|output| output.stdout.trim_end().to_string())
}

fn command_stdout(repo_root: &Path, program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(repo_root)
        .output()
        .with_context(|| format!("failed to spawn {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} command failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim_end().to_string())
}

fn git_stdout_bytes<const N: usize>(repo_root: &Path, args: [&str; N]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .context("failed to spawn git")?;
    if !output.status.success() {
        bail!(
            "git command failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output.stdout)
}

fn run_git<const N: usize>(repo_root: &Path, args: [&str; N], stdin: Option<&[u8]>) -> Result<()> {
    run_command(repo_root, "git", &args, stdin)
}

fn run_git_dynamic(repo_root: &Path, program: &str, args: &[&str]) -> Result<()> {
    run_command(repo_root, program, args, None)
}

fn run_command(repo_root: &Path, program: &str, args: &[&str], stdin: Option<&[u8]>) -> Result<()> {
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(repo_root)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {program}"))?;
    if let Some(stdin_bytes) = stdin
        && let Some(mut child_stdin) = child.stdin.take()
    {
        use std::io::Write;
        child_stdin
            .write_all(stdin_bytes)
            .context("failed to write git stdin")?;
    }
    let output = child
        .wait_with_output()
        .with_context(|| format!("failed to wait for {program}"))?;
    if !output.status.success() {
        bail!(
            "{program} command failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::{EnvGuard, sinex_test};
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    struct TestGitRepo {
        dir: TempDir,
    }

    impl TestGitRepo {
        fn new() -> Result<Self> {
            let dir = tempfile::tempdir()?;
            let repo = Self { dir };
            repo.git(["init", "-b", "master"])?;
            repo.git(["config", "user.name", "Sinex Tests"])?;
            repo.git(["config", "user.email", "sinex-tests@example.invalid"])?;
            fs::write(repo.path().join("README.md"), "base\n")?;
            repo.git(["add", "README.md"])?;
            repo.git(["commit", "-m", "chore: initial commit"])?;
            Ok(repo)
        }

        fn path(&self) -> &Path {
            self.dir.path()
        }

        fn git<const N: usize>(&self, args: [&str; N]) -> Result<String> {
            git_stdout(self.path(), args)
        }

        fn git_raw<const N: usize>(&self, args: [&str; N]) -> Result<()> {
            run_git(self.path(), args, None)
        }

        fn checkout_new_branch(&self, branch: &str) -> Result<()> {
            self.git_raw(["checkout", "-b", branch])
        }

        fn commit_file(&self, path: &str, contents: &str, subject: &str) -> Result<String> {
            let full_path = self.path().join(path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(&full_path, contents)?;
            self.git_raw(["add", path])?;
            self.git_raw(["commit", "-m", subject])?;
            self.git(["rev-parse", "HEAD"])
        }

        fn commit_files(&self, files: &[(&str, &str)], subject: &str) -> Result<String> {
            for (path, contents) in files {
                let full_path = self.path().join(path);
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&full_path, contents)?;
                self.git_raw(["add", path])?;
            }
            self.git_raw(["commit", "-m", subject])?;
            self.git(["rev-parse", "HEAD"])
        }

        fn merge_no_ff(&self, branch: &str, subject: &str) -> Result<()> {
            self.git_raw(["merge", "--no-ff", branch, "-m", subject])
        }

        fn add_remote(&self, name: &str, url: &Path) -> Result<()> {
            let url = url.to_string_lossy().into_owned();
            self.git_raw(["remote", "add", name, &url])
        }
    }

    #[sinex_test]
    async fn planner_groups_commits_by_topic() -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/stack")?;
        repo.commit_file(
            "xtask/src/history/db.rs",
            "history\n",
            "feat(xtask): add history selector surface",
        )?;
        repo.commit_file(
            "xtask/tests/history_analysis_tests.rs",
            "history tests\n",
            "test(xtask): cover history selector surface",
        )?;
        repo.commit_file(
            "xtask/src/sandbox/db/pool/template.rs",
            "template one\n",
            "perf(xtask): split shared template families",
        )?;
        repo.commit_file(
            "xtask/src/sandbox/db/pool/provisioning.rs",
            "template two\n",
            "perf(xtask): trust fresh template clones",
        )?;
        repo.commit_file(
            "crate/core/sinex-gateway/tests/sse_stream_test.rs",
            "gateway tests\n",
            "test(gateway): isolate sse stream bus tests",
        )?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            12,
        )?;

        assert_eq!(plan.slices.len(), 2);
        assert_eq!(plan.slices[0].packages, vec!["xtask".to_string()]);
        assert!(plan.slices[0]
            .files
            .iter()
            .any(|file| file.contains("xtask/src/sandbox/db/pool")));
        assert_eq!(
            plan.slices[1].packages,
            vec!["sinex-gateway".to_string()]
        );
        assert_eq!(plan.slices[0].pr_base, "master");
        assert_eq!(plan.slices[1].depends_on.as_deref(), Some(plan.slices[0].branch.as_str()));
        Ok(())
    }

    #[sinex_test]
    async fn planner_records_dirty_worktree_paths() -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/dirty")?;
        repo.commit_file(
            "xtask/src/history/db.rs",
            "history\n",
            "feat(xtask): add history selector surface",
        )?;
        fs::write(repo.path().join("xtask/src/history/db.rs"), "dirty\n")?;
        fs::write(repo.path().join("UNTRACKED.md"), "extra\n")?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            12,
        )?;

        assert!(plan
            .loose_ends
            .dirty_paths
            .iter()
            .any(|path| path == "xtask/src/history/db.rs"));
        assert!(plan
            .loose_ends
            .untracked_paths
            .iter()
            .any(|path| path == "UNTRACKED.md"));
        Ok(())
    }

    #[sinex_test]
    async fn planner_flags_non_linear_history() -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/nonlinear")?;
        repo.commit_file(
            "xtask/src/history/db.rs",
            "base history\n",
            "feat(xtask): add history selector surface",
        )?;
        repo.checkout_new_branch("feature/side")?;
        repo.commit_file(
            "xtask/src/sandbox/db/pool/template.rs",
            "side\n",
            "perf(xtask): split shared template families",
        )?;
        repo.git_raw(["checkout", "feature/nonlinear"])?;
        repo.merge_no_ff("feature/side", "merge: side topic")?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            12,
        )?;

        assert!(!plan.graph.first_parent_linear);
        assert!(!plan.loose_ends.blockers.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn planner_keeps_large_low_overlap_commits_separate(
    ) -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/large-commits")?;

        let xtask_files = (0..45)
            .map(|index| {
                (
                    format!("xtask/src/generated/file_{index}.rs"),
                    format!("xtask {index}\n"),
                )
            })
            .collect::<Vec<_>>();
        let xtask_refs = xtask_files
            .iter()
            .map(|(path, contents)| (path.as_str(), contents.as_str()))
            .collect::<Vec<_>>();
        repo.commit_files(&xtask_refs, "feat(xtask): refresh generated command surfaces")?;

        let schema_files = (0..45)
            .map(|index| {
                (
                    format!("crate/lib/sinex-schema/src/generated/schema_{index}.rs"),
                    format!("schema {index}\n"),
                )
            })
            .collect::<Vec<_>>();
        let schema_refs = schema_files
            .iter()
            .map(|(path, contents)| (path.as_str(), contents.as_str()))
            .collect::<Vec<_>>();
        repo.commit_files(
            &schema_refs,
            "feat(schema): normalize generated schema bundle",
        )?;

        repo.commit_file(
            "nixos/examples/workstation.nix",
            "workstation\n",
            "feat(nixos): refresh workstation example",
        )?;
        repo.commit_file(
            "nixos/secret/README.md",
            "secret fixture docs\n",
            "chore(nixos): relocate sample secret fixtures under nixos/secret",
        )?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            12,
        )?;

        assert_eq!(plan.slices.len(), 3);
        assert_eq!(plan.slices[0].title, "feat(xtask): refresh generated command surfaces");
        assert_eq!(plan.slices[1].title, "feat(schema): normalize generated schema bundle");
        assert_eq!(
            plan.slices[2].title,
            "chore(nixos): relocate sample secret fixtures under nixos/secret"
        );
        assert_eq!(plan.slices[2].commits.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn materialize_creates_stacked_branches_with_squashed_commits(
    ) -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/materialize")?;
        repo.commit_file(
            "xtask/src/history/db.rs",
            "history\n",
            "feat(xtask): add history selector surface",
        )?;
        repo.commit_file(
            "xtask/src/sandbox/db/pool/template.rs",
            "template one\n",
            "perf(xtask): split shared template families",
        )?;
        repo.commit_file(
            "crate/core/sinex-gateway/tests/sse_stream_test.rs",
            "gateway tests\n",
            "test(gateway): isolate sse stream bus tests",
        )?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            12,
        )?;
        let materialized = materialize_plan(repo.path(), &plan, true)?;

        assert_eq!(materialized.len(), plan.slices.len());
        let first_branch_head = repo.git(["rev-parse", &plan.slices[0].branch])?;
        let second_merge_base = repo.git([
            "merge-base",
            &plan.slices[1].branch,
            &plan.slices[0].branch,
        ])?;
        assert_eq!(first_branch_head, second_merge_base);
        let subject = repo.git(["log", "-1", "--format=%s", &plan.slices[0].branch])?;
        assert_eq!(subject, plan.slices[0].squash_title);
        Ok(())
    }

    #[sinex_test]
    async fn publish_pushes_branches_and_creates_prs() -> crate::sandbox::TestResult<()> {
        let repo = TestGitRepo::new()?;
        repo.checkout_new_branch("feature/publish")?;
        repo.commit_file(
            "xtask/src/history/db.rs",
            "history\n",
            "feat(xtask): add history selector surface",
        )?;
        repo.commit_file(
            "xtask/src/sandbox/db/pool/template.rs",
            "template one\n",
            "perf(xtask): split shared template families",
        )?;

        let plan = build_plan(
            repo.path(),
            "master",
            "HEAD",
            "stack".to_string(),
            1,
        )?;
        let plan_dir = repo.path().join(".sinex/git-stack/publish");
        let bundle = write_plan_bundle(&plan_dir, &plan)?;
        materialize_plan(repo.path(), &plan, true)?;

        let remote_dir = tempfile::tempdir()?;
        let remote_path = remote_dir.path().join("origin.git");
        Command::new("git")
            .args(["init", "--bare", remote_path.to_string_lossy().as_ref()])
            .output()?;
        repo.add_remote("origin", &remote_path)?;

        let fake_bin = tempfile::tempdir()?;
        let gh_path = fake_bin.path().join("gh");
        fs::write(
            &gh_path,
            r#"#!/usr/bin/env bash
set -euo pipefail
state_dir="${GH_FAKE_STATE_DIR:?}"
cmd="${1:-}"
sub="${2:-}"
sanitize() {
  printf '%s' "$1" | tr '/:' '__'
}
if [[ "$cmd" == "pr" && "$sub" == "view" ]]; then
  branch="${3:?}"
  file="$state_dir/pr-$(sanitize "$branch").json"
  if [[ -f "$file" ]]; then
    cat "$file"
    exit 0
  fi
  echo "no pull requests found for branch \"$branch\"" >&2
  exit 1
fi
if [[ "$cmd" == "pr" && "$sub" == "create" ]]; then
  shift 2
  draft=false
  title=""
  body_file=""
  base=""
  head=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --draft) draft=true; shift ;;
      --title) title="$2"; shift 2 ;;
      --body-file) body_file="$2"; shift 2 ;;
      --base) base="$2"; shift 2 ;;
      --head) head="$2"; shift 2 ;;
      --repo) shift 2 ;;
      *) shift ;;
    esac
  done
  next_file="$state_dir/next-number"
  if [[ ! -f "$next_file" ]]; then
    echo 100 > "$next_file"
  fi
  number="$(cat "$next_file")"
  echo $((number + 1)) > "$next_file"
  url="https://example.test/pr/$number"
  file="$state_dir/pr-$(sanitize "$head").json"
  printf '{"number":%s,"url":"%s"}\n' "$number" "$url" > "$file"
  first_line="$(head -n 1 "$body_file")"
  printf 'create|%s|%s|%s|%s|%s\n' "$head" "$base" "$draft" "$title" "$first_line" >> "$state_dir/actions.log"
  printf '%s\n' "$url"
  exit 0
fi
echo "unsupported gh invocation: $*" >&2
exit 2
"#,
        )?;
        fs::set_permissions(&gh_path, fs::Permissions::from_mode(0o755))?;

        let original_path = std::env::var("PATH").unwrap_or_default();
        let mut env = EnvGuard::with_keys(&["PATH", "GH_FAKE_STATE_DIR"]);
        env.set(
            "PATH",
            format!("{}:{}", fake_bin.path().display(), original_path),
        );
        env.set("GH_FAKE_STATE_DIR", fake_bin.path().display().to_string());

        let published = publish_plan(
            repo.path(),
            &plan,
            bundle
                .plan_path
                .parent()
                .context("plan bundle missing parent")?,
            &PublishOptions {
                plan_path: bundle.plan_path.clone(),
                remote: "origin".to_string(),
                draft: true,
                create_prs: true,
                force_with_lease: false,
                repo: None,
                allow_blockers: false,
            },
        )?;

        assert_eq!(published.len(), 2);
        let first_remote = repo.git(["ls-remote", "--heads", "origin", "stack/01-add-history-selector-surface"])?;
        let second_remote =
            repo.git(["ls-remote", "--heads", "origin", "stack/02-split-shared-template-families"])?;
        assert!(!first_remote.is_empty());
        assert!(!second_remote.is_empty());

        let actions = fs::read_to_string(fake_bin.path().join("actions.log"))?;
        assert!(actions.contains("create|stack/01-add-history-selector-surface|master|true|feat(xtask): add history selector surface|# feat(xtask): add history selector surface"));
        assert!(actions.contains("create|stack/02-split-shared-template-families|stack/01-add-history-selector-surface|true|perf(xtask): split shared template families|# perf(xtask): split shared template families"));
        Ok(())
    }

    #[sinex_test]
    async fn normalize_pr_base_strips_remote_prefixes() -> crate::sandbox::TestResult<()> {
        assert_eq!(normalize_pr_base("origin/master", "origin"), "master");
        assert_eq!(
            normalize_pr_base("refs/remotes/origin/master", "origin"),
            "master"
        );
        assert_eq!(normalize_pr_base("refs/heads/main", "origin"), "main");
        assert_eq!(
            normalize_pr_base("stack/03-something", "origin"),
            "stack/03-something"
        );
        Ok(())
    }
}

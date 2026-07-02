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
        "crate/sinexd/tests/sse_stream_test.rs",
        "gateway tests\n",
        "test(gateway): isolate sse stream bus tests",
    )?;

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 12)?;

    assert_eq!(plan.slices.len(), 2);
    assert_eq!(plan.slices[0].packages, vec!["xtask".to_string()]);
    assert!(
        plan.slices[0]
            .files
            .iter()
            .any(|file| file.contains("xtask/src/sandbox/db/pool"))
    );
    assert_eq!(plan.slices[1].packages, vec!["sinexd".to_string()]);
    assert_eq!(plan.slices[0].pr_base, "master");
    assert_eq!(
        plan.slices[1].depends_on.as_deref(),
        Some(plan.slices[0].branch.as_str())
    );
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

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 12)?;

    assert!(
        plan.loose_ends
            .dirty_paths
            .iter()
            .any(|path| path == "xtask/src/history/db.rs")
    );
    assert!(
        plan.loose_ends
            .untracked_paths
            .iter()
            .any(|path| path == "UNTRACKED.md")
    );
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

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 12)?;

    assert!(!plan.graph.first_parent_linear);
    assert!(!plan.loose_ends.blockers.is_empty());
    Ok(())
}

#[sinex_test]
async fn planner_keeps_large_low_overlap_commits_separate() -> crate::sandbox::TestResult<()> {
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
    repo.commit_files(
        &xtask_refs,
        "feat(xtask): refresh generated command surfaces",
    )?;

    let schema_files = (0..45)
        .map(|index| {
            (
                format!("crate/sinex-schema/src/generated/schema_{index}.rs"),
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

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 12)?;

    assert_eq!(plan.slices.len(), 3);
    assert_eq!(
        plan.slices[0].title,
        "feat(xtask): refresh generated command surfaces"
    );
    assert_eq!(
        plan.slices[1].title,
        "feat(schema): normalize generated schema bundle"
    );
    assert_eq!(
        plan.slices[2].title,
        "chore(nixos): relocate sample secret fixtures under nixos/secret"
    );
    assert_eq!(plan.slices[2].commits.len(), 2);
    Ok(())
}

#[sinex_test]
async fn materialize_creates_stacked_branches_with_squashed_commits()
-> crate::sandbox::TestResult<()> {
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
        "crate/sinexd/tests/sse_stream_test.rs",
        "gateway tests\n",
        "test(gateway): isolate sse stream bus tests",
    )?;

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 12)?;
    let materialized = materialize_plan(repo.path(), &plan, true)?;

    assert_eq!(materialized.len(), plan.slices.len());
    let first_branch_head = repo.git(["rev-parse", &plan.slices[0].branch])?;
    let second_merge_base =
        repo.git(["merge-base", &plan.slices[1].branch, &plan.slices[0].branch])?;
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

    let plan = build_plan(repo.path(), "master", "HEAD", "stack".to_string(), 1)?;
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
    let first_remote = repo.git([
        "ls-remote",
        "--heads",
        "origin",
        "stack/01-add-history-selector-surface",
    ])?;
    let second_remote = repo.git([
        "ls-remote",
        "--heads",
        "origin",
        "stack/02-split-shared-template-families",
    ])?;
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

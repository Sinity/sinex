//! Code: commits and ref updates in local git repositories, and the pull
//! requests, reviews, checks, workflow runs and issues of their GitHub
//! repositories.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One commit. A commit never changes once made; its ID is its identity, so
/// seeing it again (another ref, another clone at the same path) is a repeat.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "git.commit", key = ["repository", "commit"], replace = repeat, time = committed_at, quality = intrinsic)]
pub struct GitCommit {
    /// The repository's working directory (or git directory, when bare).
    pub repository: String,
    pub commit: String,
    pub parents: Vec<String>,
    pub tree: String,
    pub author_name: String,
    pub author_email: String,
    pub authored_at: Timestamp,
    pub committer_name: String,
    pub committer_email: String,
    pub committed_at: Timestamp,
    /// The full message, decoded lossily when it is not UTF-8.
    pub message: String,
    /// The refs that pointed at this commit when it was first read.
    pub refs: Vec<String>,
    /// The checkouts (the main one and linked worktrees) whose HEAD was at
    /// this commit when it was first read.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub worktrees: Vec<String>,
    /// What the commit changed against its first parent (against nothing for
    /// a root commit), one entry per file in path order. Absent where the
    /// commit was read without its changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files: Option<Vec<GitFileChange>>,
    /// Fields whose bytes were not UTF-8, by JSON pointer (`/author_name`,
    /// `/files/3/path`): the original bytes in base64. The field itself holds
    /// a lossy reading.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub original_bytes: BTreeMap<String, String>,
}

/// One file a commit added, deleted or modified, with the exact objects and
/// modes on both sides. A rename is a deletion and an addition.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GitFileChange {
    pub path: String,
    pub change: FileChange,
    /// The file's mode after the commit (`100644`, `100755`, `120000` for a
    /// link, `160000` for a submodule); absent when deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// The object after the commit (a blob, or a submodule's commit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_object: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileChange {
    Added,
    Deleted,
    Modified,
}

/// A ref seen at a new target. A force-push is a `rewritten` update: the ref
/// moved to a commit that does not descend from its previous target. The
/// commits it no longer reaches, and no other ref or checkout reaches either,
/// are listed in `unreachable`; their `git.commit` events stay, since those
/// commits were made.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "git.ref_updated", key = ["repository", "ref_name", "before", "after"], replace = repeat, time = observed_at, quality = captured)]
pub struct GitRefUpdated {
    pub repository: String,
    /// The full name (`refs/heads/main`, `refs/remotes/origin/main`).
    pub ref_name: String,
    /// The commit the ref pointed at when last seen; absent when first seen.
    pub before: Option<String>,
    /// The commit it points at now; absent when the ref was deleted.
    pub after: Option<String>,
    pub change: RefChange,
    /// Commits reachable from `before` that no ref or checkout reaches now,
    /// newest first. Empty when `before` is no longer in the repository.
    pub unreachable: Vec<String>,
    pub observed_at: Timestamp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RefChange {
    /// First seen: a new ref, or the first read of the repository.
    Created,
    /// The new target descends from the old one.
    FastForward,
    /// The new target does not descend from the old one (a force-push, a
    /// reset, a rebase), or the old one is no longer in the repository.
    Rewritten,
    Deleted,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum GitRecord {
        Commit(GitCommit),
        RefUpdated(GitRefUpdated),
    }
}

/// A GitHub pull request; a later reading replaces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "github.pull_request", key = ["repository", "number"], replace = newer, time = created_at, quality = intrinsic)]
pub struct GithubPullRequest {
    /// `owner/name`.
    pub repository: String,
    pub number: u64,
    pub title: String,
    pub body: Option<String>,
    /// `open`, `closed` or `merged`.
    pub state: String,
    pub draft: bool,
    pub author: Option<String>,
    pub labels: Vec<String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub merged_at: Option<Timestamp>,
    pub merge_commit: Option<String>,
    /// The branch and commit proposed, and the branch and commit they
    /// target, as GitHub last reported them.
    pub head_ref: String,
    pub head_commit: String,
    pub base_ref: String,
    pub base_commit: String,
    pub url: String,
}

/// A review of a pull request; a later reading (dismissed, edited) replaces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "github.review", key = ["repository", "review_id"], replace = newer, time = submitted_at, quality = intrinsic)]
pub struct GithubReview {
    pub repository: String,
    pub pull_request: u64,
    pub review_id: u64,
    pub author: Option<String>,
    /// `approved`, `changes_requested`, `commented`, `dismissed` or `pending`.
    pub state: String,
    pub body: Option<String>,
    /// The commit the review was made on.
    pub commit: Option<String>,
    pub submitted_at: Option<Timestamp>,
    pub url: Option<String>,
}

/// A check on a commit: a check run (each run, re-runs included, is its own
/// record) or a commit status (one per context, its newest state).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "github.check", key = ["repository", "commit", "reported_as", "name", "check_run"], replace = newer, time = started_at, quality = intrinsic)]
pub struct GithubCheck {
    pub repository: String,
    pub commit: String,
    /// `check_run` or `status`.
    pub reported_as: String,
    /// The check run's name, or the status's context.
    pub name: String,
    /// The check run's ID; absent for a status.
    pub check_run: Option<u64>,
    /// A check run's `queued`, `in_progress` or `completed`; a status's
    /// `pending`, `success`, `failure` or `error`.
    pub status: String,
    /// A completed check run's outcome (`success`, `failure`, `cancelled`,
    /// `skipped`, ...).
    pub conclusion: Option<String>,
    /// The app that reported a check run (`github-actions`).
    pub app: Option<String>,
    /// The workflow run a check run belongs to.
    pub workflow_run: Option<u64>,
    pub description: Option<String>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub url: Option<String>,
}

/// One attempt of a GitHub Actions workflow run; a later reading (queued, in
/// progress, completed) replaces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "github.workflow_run", key = ["repository", "run_id", "attempt"], replace = newer, time = created_at, quality = intrinsic)]
pub struct GithubWorkflowRun {
    pub repository: String,
    pub run_id: u64,
    pub attempt: u64,
    pub run_number: u64,
    pub workflow: Option<String>,
    pub title: Option<String>,
    /// What started it (`push`, `pull_request`, `schedule`, ...).
    pub event: String,
    /// `queued`, `in_progress`, `completed`, ...
    pub status: Option<String>,
    pub conclusion: Option<String>,
    pub head_branch: Option<String>,
    pub head_commit: String,
    pub actor: Option<String>,
    pub created_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    pub url: String,
}

/// A GitHub issue; a later reading replaces it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(type = "github.issue", key = ["repository", "number"], replace = newer, time = created_at, quality = intrinsic)]
pub struct GithubIssue {
    pub repository: String,
    pub number: u64,
    pub title: String,
    pub body: Option<String>,
    /// `open` or `closed`.
    pub state: String,
    /// Why it was closed (`completed`, `not_planned`) or reopened.
    pub state_reason: Option<String>,
    pub author: Option<String>,
    pub labels: Vec<String>,
    pub assignees: Vec<String>,
    pub comments: u64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub closed_at: Option<Timestamp>,
    pub url: String,
}

crate::payload_enum! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum GithubRecord {
        PullRequest(GithubPullRequest),
        Review(GithubReview),
        Check(GithubCheck),
        WorkflowRun(GithubWorkflowRun),
        Issue(GithubIssue),
    }
}

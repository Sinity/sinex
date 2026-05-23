//! Version-control event payloads.
//!
//! Hosts git commit observations and similar VCS events. Sibling providers
//! (Mercurial, SVN) would land here rather than in a new module.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::Timestamp;

/// One commit observation parsed from a git repository's history (#1053).
///
/// Each commit is one event. The `commit_sha` is the full 40-hex SHA-1
/// of the commit object. Combined with `repo_path` it forms a globally-unique
/// occurrence key (forks/mirrors share SHAs, so the repo path is necessary).
///
/// Stats fields (`files_changed_count`, `insertions`, `deletions`) are omitted
/// for merge commits when git cannot attribute them cleanly.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "git", event_type = "commit.created")]
pub struct GitCommitPayload {
    /// Full 40-character hex SHA-1 of the commit.
    pub commit_sha: String,

    /// Absolute path to the repository root on disk.
    pub repo_path: String,

    /// Author display name (from `git log --format=%aN`).
    pub author_name: String,

    /// Author e-mail (from `git log --format=%aE`).
    pub author_email: String,

    /// Committer display name (from `git log --format=%cN`).
    pub committer_name: String,

    /// Committer e-mail (from `git log --format=%cE`).
    pub committer_email: String,

    /// First line of the commit message.
    pub subject: String,

    /// Everything after the subject line (may be empty).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,

    /// Number of files changed. None for merge commits and when
    /// `--shortstat` is unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_changed_count: Option<u32>,

    /// Lines added. None when stats are unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insertions: Option<u32>,

    /// Lines deleted. None when stats are unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletions: Option<u32>,

    /// Number of parent commits. 0 = root commit, 1 = ordinary commit,
    /// 2+ = merge commit.
    pub parent_count: u32,

    /// Author timestamp (seconds since Unix epoch) as recorded in the commit
    /// object. Used as `ts_orig`.
    pub author_timestamp: Timestamp,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::EventPayload;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn declares_source_and_event_type() -> TestResult<()> {
        assert_eq!(GitCommitPayload::SOURCE.as_static_str(), "git");
        assert_eq!(
            GitCommitPayload::EVENT_TYPE.as_static_str(),
            "commit.created"
        );
        Ok(())
    }
}

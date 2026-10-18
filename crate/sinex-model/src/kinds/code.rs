//! Code: commits in local git repositories.
use crate::Timestamp;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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
}

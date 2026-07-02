//! Git commit history parser (#1053).
//!
//! Reads the commit log of a git repository via `git log` and emits one
//! `git`/`commit.created` event per commit.
//!
//! **Adapter choice:** [`StaticFileAdapter`] with the repository root path as
//! the "file". The adapter reads the path from config and hands the raw bytes
//! of that path to the parser. For a directory, the adapter will emit the
//! directory path as `logical_path` on the [`SourceRecord`]. The parser runs
//! `git log` against that path to produce structured commit records.
//!
//! **Anchor:** `ByteRange { start: commit_index, len: 1 }` — zero-based
//! commit index in the topological walk (`git log --topo-order`). Stable as
//! long as the walk order is stable for a given set of commits.
//!
//! **Occurrence key:** `(commit_sha, repo_path)` — the SHA alone is not
//! globally unique because forks and mirrors share object SHAs.
//!
//! **Privacy tier:** Sensitive — commit messages, author names, and e-mail
//! addresses are personal data.

use std::process::Stdio;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command;

use crate::runtime::parser::{MaterialParser, ParserError, ParserResult};
use sinex_macros::SourceMeta;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceId, SourceRecord, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape,
};
use sinex_primitives::temporal::Timestamp;

// ---------------------------------------------------------------------------
// Git log record separator and format
// ---------------------------------------------------------------------------

/// Sentinel string that cannot appear in commit messages.
const RECORD_SEP: &str = "\x1e"; // ASCII record separator (RS)

/// `git log` format string.
///
/// Fields separated by `\x1f` (ASCII unit separator). Chosen because it is
/// not valid in commit metadata and lets us split reliably even when the
/// subject or body contains spaces, colons, or other punctuation.
///
/// Layout:
///   0: commit SHA (40 hex)
///   1: author name
///   2: author e-mail
///   3: committer name
///   4: committer e-mail
///   5: author date (Unix epoch, seconds)
///   6: parent count (number of %P tokens)
///   7: parent SHAs (space-separated, may be empty)
///   8: subject
///   9: body (may span multiple lines; trimmed)
const GIT_FORMAT: &str = "%H\x1f%aN\x1f%aE\x1f%cN\x1f%cE\x1f%at\x1f%P\x1f%s\x1f%b";

// ---------------------------------------------------------------------------
// Raw parsed commit
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RawCommit {
    sha: String,
    author_name: String,
    author_email: String,
    committer_name: String,
    committer_email: String,
    author_ts_secs: i64,
    parent_shas: Vec<String>,
    subject: String,
    body: Option<String>,
}

impl RawCommit {
    fn parent_count(&self) -> u32 {
        self.parent_shas.len() as u32
    }
}

// ---------------------------------------------------------------------------
// Stats line: "N files changed, M insertions(+), K deletions(-)"
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct CommitStats {
    files_changed: Option<u32>,
    insertions: Option<u32>,
    deletions: Option<u32>,
}

fn parse_stats_line(line: &str) -> CommitStats {
    let mut stats = CommitStats::default();
    // Examples:
    //   "1 file changed, 2 insertions(+), 1 deletion(-)"
    //   "3 files changed, 5 insertions(+)"
    //   "2 files changed, 3 deletions(-)"
    for part in line.split(',') {
        let part = part.trim();
        if part.contains("file") {
            stats.files_changed = part.split_whitespace().next().and_then(|n| n.parse().ok());
        } else if part.contains("insertion") {
            stats.insertions = part.split_whitespace().next().and_then(|n| n.parse().ok());
        } else if part.contains("deletion") {
            stats.deletions = part.split_whitespace().next().and_then(|n| n.parse().ok());
        }
    }
    stats
}

// ---------------------------------------------------------------------------
// Parser config + implementation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GitCommitHistoryParserConfig;

/// Parser that runs `git log` against a repository path and emits one
/// [`ParsedEventIntent`] per commit.
#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "git-commit-history",
    namespace = "vcs",
    event_source = "git",
    event_type = "commit.created",
    adapter = "StaticFileAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Uuid5From("(commit_sha, repo_path)"),
    access_scope = AccessScope::StagedExport,
    implementation = "sinexd",
    privacy_context = ProcessingContext::Document,
    resource_profile = ResourceProfile::BoundedFile,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::AppendStream,
    runtime_shape = RuntimeShape::OnDemand,
)]
pub struct GitCommitHistoryParser;

#[async_trait]
impl MaterialParser for GitCommitHistoryParser {
    type Config = GitCommitHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("git-commit-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::StaticFile],
            source_id: SourceId::from_static("git-commit-history"),
            declared_event_types: vec![(
                EventSource::from_static("git"),
                EventType::from_static("commit.created"),
            )],
            privacy_contexts: vec![ProcessingContext::Document],
            sensitivity_hints: Vec::new(),
            description: "Parses a git repository's commit history via \
                `git log` and emits one commit.created event per commit. \
                Captures author/committer identity, subject, body, and \
                per-commit diff stats where available."
                .into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // The logical_path is the repository root directory.
        let repo_path = record
            .logical_path
            .as_ref()
            .map(|p| p.as_str().to_owned())
            .ok_or_else(|| {
                ParserError::Parse(
                    "git parser requires logical_path on SourceRecord (repository root)".into(),
                )
            })?;

        let commits = run_git_log(&repo_path).await?;
        let stats_map = run_git_log_stats(&repo_path, &commits).await?;

        let mut intents = Vec::with_capacity(commits.len());
        for (index, commit) in commits.into_iter().enumerate() {
            let stats = stats_map.get(&commit.sha).cloned().unwrap_or_default();
            let intent = build_intent(commit, stats, index as u64, ctx, &repo_path)?;
            intents.push(intent);
        }
        Ok(intents)
    }
}

// ---------------------------------------------------------------------------
// git log invocation
// ---------------------------------------------------------------------------

async fn run_git_log(repo_path: &str) -> ParserResult<Vec<RawCommit>> {
    // Use record-separator (RS, 0x1e) between commits so we can split cleanly
    // even when the body contains blank lines.
    let format = format!("--format={RECORD_SEP}{GIT_FORMAT}");

    let output = Command::new("git")
        .args([
            "-C",
            repo_path,
            "log",
            "--topo-order",
            "--encoding=UTF-8",
            &format,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| ParserError::Parse(format!("failed to run git log in '{repo_path}': {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ParserError::Parse(format!(
            "git log exited non-zero in '{repo_path}': {stderr}"
        )));
    }

    let raw = String::from_utf8(output.stdout).map_err(|e| {
        ParserError::Parse(format!(
            "git log output is not valid UTF-8 in '{repo_path}': {e}"
        ))
    })?;

    parse_git_log_output(&raw)
}

fn parse_git_log_output(raw: &str) -> ParserResult<Vec<RawCommit>> {
    let mut commits = Vec::new();
    // Split on record separators; skip the empty first chunk.
    for chunk in raw.split(RECORD_SEP).filter(|s| !s.trim().is_empty()) {
        let commit = parse_commit_chunk(chunk)?;
        commits.push(commit);
    }
    Ok(commits)
}

fn parse_commit_chunk(chunk: &str) -> ParserResult<RawCommit> {
    // The chunk is: <SHA>\x1f<aN>\x1f<aE>\x1f<cN>\x1f<cE>\x1f<at>\x1f<parents>\x1f<subject>\x1f<body...>
    // Split on \x1f with a limit of 9 — the last part (body) may span multiple lines.
    let parts: Vec<&str> = chunk.splitn(9, '\x1f').collect();
    if parts.len() < 8 {
        return Err(ParserError::Parse(format!(
            "git log record has only {} fields (expected ≥8): {:?}",
            parts.len(),
            &chunk[..chunk.len().min(120)]
        )));
    }

    let sha = parts[0].trim().to_owned();
    let author_name = parts[1].trim().to_owned();
    let author_email = parts[2].trim().to_owned();
    let committer_name = parts[3].trim().to_owned();
    let committer_email = parts[4].trim().to_owned();
    let author_ts_secs: i64 = parts[5].trim().parse().map_err(|_| {
        ParserError::Parse(format!(
            "invalid author timestamp '{}' for commit {}",
            parts[5].trim(),
            &sha
        ))
    })?;

    let parent_shas: Vec<String> = {
        let p = parts[6].trim();
        if p.is_empty() {
            Vec::new()
        } else {
            p.split_whitespace().map(str::to_owned).collect()
        }
    };

    let subject = parts[7].trim().to_owned();

    let body = if parts.len() > 8 {
        let b = parts[8].trim();
        if b.is_empty() {
            None
        } else {
            Some(b.to_owned())
        }
    } else {
        None
    };

    Ok(RawCommit {
        sha,
        author_name,
        author_email,
        committer_name,
        committer_email,
        author_ts_secs,
        parent_shas,
        subject,
        body,
    })
}

// ---------------------------------------------------------------------------
// Per-commit stats via git show --shortstat
// ---------------------------------------------------------------------------

/// Fetch per-commit diff stats for all commits in a single `git log` call.
///
/// We use `--format=COMMIT:%H` + `--shortstat` so each commit block starts
/// with its SHA and is followed by the stats line (if any). This avoids N
/// individual `git show` invocations.
async fn run_git_log_stats(
    repo_path: &str,
    commits: &[RawCommit],
) -> ParserResult<std::collections::HashMap<String, CommitStats>> {
    if commits.is_empty() {
        return Ok(std::collections::HashMap::new());
    }

    let output = Command::new("git")
        .args([
            "-C",
            repo_path,
            "log",
            "--topo-order",
            "--shortstat",
            "--format=COMMIT:%H",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|e| {
            ParserError::Parse(format!(
                "failed to run git log --shortstat in '{repo_path}': {e}"
            ))
        })?;

    if !output.status.success() {
        // Stats are advisory; if this fails, return empty map.
        return Ok(std::collections::HashMap::new());
    }

    let raw = String::from_utf8_lossy(&output.stdout);
    let mut map = std::collections::HashMap::new();
    let mut current_sha: Option<String> = None;

    for line in raw.lines() {
        if let Some(sha) = line.strip_prefix("COMMIT:") {
            current_sha = Some(sha.trim().to_owned());
        } else if let Some(sha) = &current_sha {
            let trimmed = line.trim();
            if trimmed.contains("file")
                && (trimmed.contains("insertion")
                    || trimmed.contains("deletion")
                    || trimmed.contains("changed"))
            {
                map.insert(sha.clone(), parse_stats_line(trimmed));
            }
        }
    }

    Ok(map)
}

// ---------------------------------------------------------------------------
// Intent builder
// ---------------------------------------------------------------------------

fn build_intent(
    commit: RawCommit,
    stats: CommitStats,
    index: u64,
    ctx: &ParserContext,
    repo_path: &str,
) -> ParserResult<ParsedEventIntent> {
    let ts_orig = Timestamp::from_unix_timestamp(commit.author_ts_secs).ok_or_else(|| {
        ParserError::Parse(format!(
            "author timestamp {} is out of range for commit {}",
            commit.author_ts_secs, commit.sha
        ))
    })?;

    let occurrence_key = OccurrenceKey {
        source_id: SourceId::from_static("git-commit-history"),
        fields: vec![
            ("commit_sha".into(), commit.sha.clone()),
            ("repo_path".into(), repo_path.to_owned()),
        ],
    };

    let payload = serde_json::json!({
        "commit_sha": commit.sha,
        "repo_path": repo_path,
        "author_name": commit.author_name,
        "author_email": commit.author_email,
        "committer_name": commit.committer_name,
        "committer_email": commit.committer_email,
        "subject": commit.subject,
        "body": commit.body,
        "files_changed_count": stats.files_changed,
        "insertions": stats.insertions,
        "deletions": stats.deletions,
        "parent_count": commit.parent_count(),
        "author_timestamp": ts_orig,
    });

    Ok(ParsedEventIntent::builder()
        .source_id(ctx.source_id.clone())
        .parser_id(ParserId::from_static("git-commit-history"))
        .parser_version("1.0.0")
        .event_type(EventType::from_static("commit.created"))
        .event_source(EventSource::from_static("git"))
        .payload(payload)
        .ts_orig(ts_orig)
        .timing(TimingEvidence::Intrinsic {
            field: "author_date".into(),
            confidence: TimingConfidence::Intrinsic,
        })
        .anchor(MaterialAnchor::ByteRange {
            start: index,
            len: 1,
        })
        .occurrence_key(occurrence_key)
        .privacy_context(ProcessingContext::Document)
        .build())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "git_test.rs"]
mod tests;

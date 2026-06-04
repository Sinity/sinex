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

use crate::node_sdk::parser::{MaterialParser, ParserError, ParserResult, StaticFileAdapter};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::parser::{
    InputShapeKind, MaterialAnchor, OccurrenceKey, ParsedEventIntent, ParserContext, ParserId,
    ParserManifest, SourceRecord, SourceId, TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

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
#[derive(Debug, Clone, Default)]
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
// Source contract + binding + registration
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "git-commit-history",
        namespace: "vcs",
        event_types: &[("git", "commit.created")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Uuid5From("(commit_sha, repo_path)"),
        access_policy: "personal_git_history",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:git-commit-history"),
        "git-commit-history",
        "vcs",
    )
    .implementation("sinexd")
    .adapter("StaticFileAdapter")
    .output_event_type("commit.created")
    .privacy_context("Document")
    .material_policy("static_export_file")
    .checkpoint_policy("static_file_cursor")
    .resource_shape("file_reader")
    .source_id("git-commit-history")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::OnDemand)
    .package_impact("git_commit_history_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

crate::register_adapter_ingestor!(
    source_id: "git-commit-history",
    adapter: StaticFileAdapter,
    parser: GitCommitHistoryParser,
);

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::Uuid;
    use sinex_primitives::ids::Id;
    use xtask::sandbox::prelude::*;

    fn test_ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("git-commit-history"),
            source_material_id: Id::new(),
            record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
            operation_id: Uuid::new_v4(),
            job_id: Uuid::new_v4(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record_for(repo_path: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::new(),
            anchor: MaterialAnchor::ByteRange { start: 0, len: 1 },
            bytes: repo_path.as_bytes().to_vec(),
            logical_path: Some(repo_path.into()),
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    // ---------------------------------------------------------------------------
    // git log output parsing (unit tests — no real git required)
    // ---------------------------------------------------------------------------

    /// Minimal single-commit git log output in the wire format we produce.
    const SINGLE_COMMIT: &str = "\x1eabc1234def5678901234567890123456789012345\x1fAlice Foo\x1falice@example.com\x1fBob Bar\x1fbob@example.com\x1f1710000000\x1f\x1ffeat: add widget\x1fThis adds the new widget.\n\nFixes #42.\n";

    /// Three commits including a merge commit (two parents).
    const THREE_COMMITS: &str = "\x1e1111111111111111111111111111111111111111\x1fAlice\x1falice@example.com\x1fAlice\x1falice@example.com\x1f1710000100\x1f2222222222222222222222222222222222222222 3333333333333333333333333333333333333333\x1fMerge branch 'feature'\x1f\x1e2222222222222222222222222222222222222222\x1fAlice\x1falice@example.com\x1fAlice\x1falice@example.com\x1f1710000050\x1f3333333333333333333333333333333333333333\x1fadd feature\x1f\x1e3333333333333333333333333333333333333333\x1fBob\x1fbob@example.com\x1fBob\x1fbob@example.com\x1f1710000000\x1f\x1finitial commit\x1f";

    #[sinex_test]
    async fn parses_single_commit_to_one_intent() -> TestResult<()> {
        let commits = parse_git_log_output(SINGLE_COMMIT).unwrap();
        assert_eq!(commits.len(), 1);
        let c = &commits[0];
        assert_eq!(c.sha, "abc1234def5678901234567890123456789012345");
        assert_eq!(c.author_name, "Alice Foo");
        assert_eq!(c.author_email, "alice@example.com");
        assert_eq!(c.committer_name, "Bob Bar");
        assert_eq!(c.committer_email, "bob@example.com");
        assert_eq!(c.subject, "feat: add widget");
        assert!(c.body.is_some());
        assert_eq!(c.parent_count(), 0);
        Ok(())
    }

    #[sinex_test]
    async fn parses_three_commits_in_order() -> TestResult<()> {
        let commits = parse_git_log_output(THREE_COMMITS).unwrap();
        assert_eq!(commits.len(), 3);
        assert_eq!(commits[0].sha, "1111111111111111111111111111111111111111");
        assert_eq!(commits[1].sha, "2222222222222222222222222222222222222222");
        assert_eq!(commits[2].sha, "3333333333333333333333333333333333333333");
        Ok(())
    }

    #[sinex_test]
    async fn merge_commit_has_two_parents() -> TestResult<()> {
        let commits = parse_git_log_output(THREE_COMMITS).unwrap();
        let merge = &commits[0];
        assert_eq!(merge.parent_count(), 2);
        assert_eq!(merge.subject, "Merge branch 'feature'");
        Ok(())
    }

    #[sinex_test]
    async fn root_commit_has_zero_parents() -> TestResult<()> {
        let commits = parse_git_log_output(THREE_COMMITS).unwrap();
        let root = &commits[2]; // last in topo order = root
        assert_eq!(root.parent_count(), 0);
        assert_eq!(root.subject, "initial commit");
        Ok(())
    }

    #[sinex_test]
    async fn anchor_uses_commit_index() -> TestResult<()> {
        let commits = parse_git_log_output(THREE_COMMITS).unwrap();
        let ctx = test_ctx();
        let intent = build_intent(
            commits.into_iter().next().unwrap(),
            CommitStats::default(),
            7, // arbitrary index
            &ctx,
            "/repo",
        )
        .unwrap();
        assert!(matches!(
            intent.anchor,
            MaterialAnchor::ByteRange { start: 7, len: 1 }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn occurrence_key_contains_sha_and_repo_path() -> TestResult<()> {
        let commits = parse_git_log_output(SINGLE_COMMIT).unwrap();
        let ctx = test_ctx();
        let intent = build_intent(
            commits.into_iter().next().unwrap(),
            CommitStats::default(),
            0,
            &ctx,
            "/realm/project/sinex",
        )
        .unwrap();
        let key = intent.occurrence_key.as_ref().unwrap();
        assert_eq!(key.fields[0].0, "commit_sha");
        assert_eq!(key.fields[0].1, "abc1234def5678901234567890123456789012345");
        assert_eq!(key.fields[1].0, "repo_path");
        assert_eq!(key.fields[1].1, "/realm/project/sinex");
        Ok(())
    }

    #[sinex_test]
    async fn body_is_none_for_empty_body() -> TestResult<()> {
        let commits = parse_git_log_output(THREE_COMMITS).unwrap();
        let ordinary = &commits[1]; // "add feature" has no body
        assert!(ordinary.body.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn body_is_some_for_non_empty_body() -> TestResult<()> {
        let commits = parse_git_log_output(SINGLE_COMMIT).unwrap();
        assert!(commits[0].body.is_some());
        assert!(commits[0].body.as_deref().unwrap().contains("Fixes #42"));
        Ok(())
    }

    #[sinex_test]
    async fn stats_parsing_all_three_fields() -> TestResult<()> {
        let line = "3 files changed, 42 insertions(+), 7 deletions(-)";
        let stats = parse_stats_line(line);
        assert_eq!(stats.files_changed, Some(3));
        assert_eq!(stats.insertions, Some(42));
        assert_eq!(stats.deletions, Some(7));
        Ok(())
    }

    #[sinex_test]
    async fn stats_parsing_insertions_only() -> TestResult<()> {
        let line = "1 file changed, 5 insertions(+)";
        let stats = parse_stats_line(line);
        assert_eq!(stats.files_changed, Some(1));
        assert_eq!(stats.insertions, Some(5));
        assert!(stats.deletions.is_none());
        Ok(())
    }

    #[sinex_test]
    async fn stats_parsing_deletions_only() -> TestResult<()> {
        let line = "2 files changed, 3 deletions(-)";
        let stats = parse_stats_line(line);
        assert_eq!(stats.files_changed, Some(2));
        assert!(stats.insertions.is_none());
        assert_eq!(stats.deletions, Some(3));
        Ok(())
    }

    #[sinex_test]
    async fn invalid_timestamp_returns_error() -> TestResult<()> {
        let bad = "\x1edeadbeef00000000000000000000000000000000\x1fAuthor\x1femail@example.com\x1fCommitter\x1fcmt@example.com\x1fnot-a-number\x1f\x1fsubject\x1f";
        // parse_commit_chunk rejects a non-numeric timestamp eagerly.
        let result = parse_git_log_output(bad);
        assert!(
            result.is_err(),
            "expected Err for non-numeric timestamp, got Ok"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("invalid author timestamp"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Integration test against the actual sinex git repository
    // ---------------------------------------------------------------------------

    #[sinex_test]
    async fn parses_real_repo() -> TestResult<()> {
        let repo_path = env!("CARGO_MANIFEST_DIR");
        // Walk up from crate/sinexd to workspace root
        let workspace_root = std::path::Path::new(repo_path)
            .ancestors()
            .find(|p| p.join(".git").exists())
            .map_or_else(
                || repo_path.to_owned(),
                |p| p.to_str().unwrap_or(repo_path).to_owned(),
            );

        let commits = run_git_log(&workspace_root).await;
        match commits {
            Ok(c) => {
                // We expect at least a few commits.
                assert!(!c.is_empty(), "expected at least one commit");
                // Every commit should have a non-empty SHA.
                for commit in &c {
                    assert_eq!(commit.sha.len(), 40, "SHA must be 40 hex chars");
                }
            }
            Err(e) => {
                // If git is not available in this environment, skip gracefully.
                let msg = e.to_string();
                if msg.contains("git log exited non-zero") || msg.contains("No such file") {
                    // Acceptable — git may not be configured in CI.
                } else {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    #[sinex_test]
    async fn missing_logical_path_returns_error() -> TestResult<()> {
        let mut parser = GitCommitHistoryParser;
        let mut record = record_for("/tmp/nonexistent");
        record.logical_path = None;
        let err = parser.parse_record(record, &test_ctx()).await;
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("logical_path"), "got: {msg}");
        Ok(())
    }
}

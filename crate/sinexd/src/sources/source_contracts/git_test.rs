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

use super::*;
use crate::sandbox::sinex_test;
use tempfile::tempdir;

fn run_git(args: &[&str], cwd: &Path) -> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    assert!(
        output.status.success(),
        "git {} failed: stdout={} stderr={}",
        args.join(" "),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
async fn test_probe_git_state_handles_missing_upstream_without_probe_error()
-> ::xtask::sandbox::TestResult<()> {
    let repo = tempdir()?;
    run_git(&["init", "-q"], repo.path())?;
    run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
    std::fs::write(repo.path().join("README.md"), "hello\n")?;
    run_git(&["add", "README.md"], repo.path())?;
    run_git(&["commit", "-qm", "init"], repo.path())?;

    let git = probe_git_state(repo.path());

    assert_eq!(git.ahead, 0);
    assert_eq!(git.behind, 0);
    assert!(git.last_commit_hash.is_some());
    assert_eq!(git.stash_count, Some(0));
    assert_eq!(git.uncommitted_count, Some(0));
    assert!(git.probe_message.is_none());
    Ok(())
}

#[sinex_test]
async fn test_probe_git_state_reports_non_repo_failures() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::Builder::new()
        .prefix("xtask-nongit-")
        .tempdir_in("/tmp")?;

    let git = probe_git_state(dir.path());

    assert!(!git.dirty);
    assert!(git.last_commit_hash.is_none());
    let probe_message = git
        .probe_message
        .as_deref()
        .unwrap_or_else(|| panic!("expected git probe failure message"));
    assert!(probe_message.contains("git status --porcelain=v2 --branch failed"));
    assert_eq!(git.stash_count, None);
    assert_eq!(git.uncommitted_count, None);
    Ok(())
}

#[sinex_test]
async fn test_parse_git_status_branch_porcelain_extracts_branch_and_upstream_counts()
-> ::xtask::sandbox::TestResult<()> {
    let mut probe_issues = Vec::new();

    assert_eq!(
        parse_git_status_branch_porcelain(
            "# branch.oid abcdef\n# branch.head master\n# branch.upstream origin/master\n# branch.ab +2 -7\n1 .M N... 100644 100644 100644 abcdef abcdef file.txt\n",
            &mut probe_issues,
        ),
        (Some("master".to_string()), true, Some(1), 2, 7)
    );
    assert!(probe_issues.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_parse_git_status_branch_porcelain_reports_invalid_branch_ab_payload()
-> ::xtask::sandbox::TestResult<()> {
    let mut probe_issues = Vec::new();

    assert_eq!(
        parse_git_status_branch_porcelain(
            "# branch.head master\n# branch.ab +2 nope\n",
            &mut probe_issues,
        ),
        (Some("master".to_string()), false, Some(0), 0, 0)
    );
    let message = probe_issues.join("; ");
    assert!(message.contains("git status --porcelain=v2 --branch failed"));
    assert!(message.contains("invalid branch.ab payload: +2 nope"));
    Ok(())
}

#[sinex_test]
async fn test_parse_git_commit_age_mins() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(parse_git_commit_age_mins("100", 100), Some(0));
    assert_eq!(parse_git_commit_age_mins("40", 100), Some(1));
    assert_eq!(
        parse_git_commit_age_mins("0", 60 * 60 * 24 * 3),
        Some(60 * 24 * 3)
    );
    assert_eq!(parse_git_commit_age_mins("200", 100), Some(0));
    assert_eq!(parse_git_commit_age_mins("", 100), None);
    assert_eq!(parse_git_commit_age_mins("garbage", 100), None);
    Ok(())
}

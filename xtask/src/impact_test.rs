use super::*;
use crate::sandbox::prelude::*;
use color_eyre::eyre::eyre;
use tempfile::tempdir;

#[sinex_test]
async fn impact_plan_reuses_exact_proof_for_empty_change_set() -> TestResult<()> {
    let plan = plan_from_changed_files(Vec::new(), Vec::new(), Vec::new())?;

    assert!(plan.can_reuse_exact_proof());
    assert!(plan.scope_args.is_empty());
    assert_eq!(plan.decisions[0].action, ImpactAction::ReuseExactProof);
    Ok(())
}

#[sinex_test]
async fn impact_plan_runs_affected_packages_for_code_changes() -> TestResult<()> {
    let plan = plan_from_changed_files(
        vec!["xtask/src/commands/test.rs".to_string()],
        vec!["xtask".to_string()],
        Vec::new(),
    )?;

    assert_eq!(plan.affected_packages, vec!["xtask"]);
    assert_eq!(plan.scope_args, vec!["-p".to_string(), "xtask".to_string()]);
    assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
    assert!(!plan.accepted_risks.is_empty());
    Ok(())
}

#[sinex_test]
async fn impact_plan_runs_workspace_for_workspace_level_changes() -> TestResult<()> {
    let plan = plan_from_changed_files(
        vec![".config/nextest.toml".to_string()],
        vec!["xtask".to_string()],
        Vec::new(),
    )?;

    assert!(plan.is_workspace());
    assert!(plan.affected_packages.is_empty());
    assert_eq!(plan.decisions[0].action, ImpactAction::RunWorkspace);
    Ok(())
}

#[sinex_test]
async fn impact_plan_uses_test_level_evidence_when_available() -> TestResult<()> {
    let plan = plan_from_changed_files_with_mode(
        vec!["crate/sinexd/src/runtime/stage_as_you_go.rs".to_string()],
        &[FileChangedHunks {
            path: "crate/sinexd/src/runtime/stage_as_you_go.rs".to_string(),
            hunks: vec![ChangedHunk {
                line_start: 10,
                line_end: 10,
            }],
        }],
        &RustItemIndex::default(),
        vec!["sinexd".to_string()],
        vec![ImpactedTest {
            package: Some("sinexd".to_string()),
            test_name: "stage_as_you_go_records_material".to_string(),
            evidence: vec![ImpactEvidence {
                source: ImpactEvidenceSource::CoverageRegion,
                subject: "crate/sinexd/src/runtime/stage_as_you_go.rs".to_string(),
                reason: "covered line range".to_string(),
                line_start: Some(10),
                line_end: Some(20),
            }],
        }],
        Vec::new(),
        ImpactMode::Balanced,
    )?;

    assert_eq!(plan.decisions[0].action, ImpactAction::RunImpactedTests);
    assert_eq!(
        plan.impact_filter.as_deref(),
        Some("test(stage_as_you_go_records_material)")
    );
    assert_eq!(
        plan.scope_args,
        vec![
            "-p".to_string(),
            "sinexd".to_string(),
            "-E".to_string(),
            "test(stage_as_you_go_records_material)".to_string()
        ]
    );
    Ok(())
}

#[sinex_test]
async fn impact_plan_treats_empty_hunks_as_evidence_gap() -> TestResult<()> {
    let plan = plan_from_changed_files_with_mode(
        vec!["xtask/src/impact.rs".to_string()],
        &[],
        &RustItemIndex::default(),
        vec!["xtask".to_string()],
        vec![ImpactedTest {
            package: Some("xtask".to_string()),
            test_name: "impact_manifest_test".to_string(),
            evidence: vec![ImpactEvidence {
                source: ImpactEvidenceSource::CoverageRegion,
                subject: "xtask/src/impact.rs".to_string(),
                reason: "legacy file-level coverage".to_string(),
                line_start: None,
                line_end: None,
            }],
        }],
        Vec::new(),
        ImpactMode::Balanced,
    )?;

    assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
    assert_eq!(plan.evidence_gaps, vec!["xtask/src/impact.rs"]);
    Ok(())
}

#[sinex_test]
async fn impact_plan_falls_back_when_changed_hunk_is_not_covered() -> TestResult<()> {
    let plan = plan_from_changed_files_with_mode(
        vec!["xtask/src/impact.rs".to_string()],
        &[FileChangedHunks {
            path: "xtask/src/impact.rs".to_string(),
            hunks: vec![ChangedHunk {
                line_start: 90,
                line_end: 90,
            }],
        }],
        &RustItemIndex::default(),
        vec!["xtask".to_string()],
        vec![ImpactedTest {
            package: Some("xtask".to_string()),
            test_name: "impact_manifest_test".to_string(),
            evidence: vec![ImpactEvidence {
                source: ImpactEvidenceSource::CoverageRegion,
                subject: "xtask/src/impact.rs".to_string(),
                reason: "covered line range".to_string(),
                line_start: Some(10),
                line_end: Some(20),
            }],
        }],
        Vec::new(),
        ImpactMode::Balanced,
    )?;

    assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
    assert_eq!(plan.evidence_gaps, vec!["xtask/src/impact.rs"]);
    Ok(())
}

#[sinex_test]
async fn changed_hunks_reads_staged_and_unstaged_changes_from_head() -> TestResult<()> {
    let dir = tempdir()?;
    run_git(dir.path(), &["init"])?;
    run_git(
        dir.path(),
        &["config", "user.email", "sinex@example.invalid"],
    )?;
    run_git(dir.path(), &["config", "user.name", "Sinex Test"])?;
    std::fs::create_dir_all(dir.path().join("src"))?;
    std::fs::write(dir.path().join("src/lib.rs"), "fn answer() -> i32 { 1 }\n")?;
    run_git(dir.path(), &["add", "src/lib.rs"])?;
    run_git(dir.path(), &["commit", "-m", "init"])?;

    std::fs::write(dir.path().join("src/lib.rs"), "fn answer() -> i32 { 2 }\n")?;
    let changed_files = vec!["src/lib.rs".to_string()];
    let unstaged = changed_hunks_for_files_in(dir.path(), &changed_files)?;
    run_git(dir.path(), &["add", "src/lib.rs"])?;
    let staged = changed_hunks_for_files_in(dir.path(), &changed_files)?;

    assert_eq!(staged, unstaged);
    assert_eq!(
        staged,
        vec![FileChangedHunks {
            path: "src/lib.rs".to_string(),
            hunks: vec![ChangedHunk {
                line_start: 1,
                line_end: 1,
            }],
        }]
    );
    Ok(())
}

#[sinex_test]
async fn changed_hunks_represents_untracked_rust_files_as_whole_file() -> TestResult<()> {
    let dir = tempdir()?;
    run_git(dir.path(), &["init"])?;
    run_git(
        dir.path(),
        &["config", "user.email", "sinex@example.invalid"],
    )?;
    run_git(dir.path(), &["config", "user.name", "Sinex Test"])?;
    std::fs::write(dir.path().join("README.md"), "init\n")?;
    run_git(dir.path(), &["add", "README.md"])?;
    run_git(dir.path(), &["commit", "-m", "init"])?;

    std::fs::create_dir_all(dir.path().join("src"))?;
    std::fs::write(
        dir.path().join("src/new.rs"),
        "fn one() {}\nfn two() {}\nfn three() {}\n",
    )?;

    let hunks = changed_hunks_for_files_in(dir.path(), &["src/new.rs".to_string()])?;

    assert_eq!(
        hunks,
        vec![FileChangedHunks {
            path: "src/new.rs".to_string(),
            hunks: vec![ChangedHunk {
                line_start: 1,
                line_end: 3,
            }],
        }]
    );
    Ok(())
}

#[sinex_test]
async fn parse_unified_zero_hunks_extracts_new_line_ranges() -> TestResult<()> {
    let hunks = parse_unified_zero_hunks(
        "\
diff --git a/xtask/src/impact.rs b/xtask/src/impact.rs\n\
--- a/xtask/src/impact.rs\n\
+++ b/xtask/src/impact.rs\n\
@@ -10,0 +11,2 @@\n\
+one\n\
+two\n",
    );

    assert_eq!(
        hunks,
        vec![FileChangedHunks {
            path: "xtask/src/impact.rs".to_string(),
            hunks: vec![ChangedHunk {
                line_start: 11,
                line_end: 12,
            }],
        }]
    );
    Ok(())
}

fn run_git(cwd: &std::path::Path, args: &[&str]) -> TestResult<()> {
    let output = std::process::Command::new("git")
        .current_dir(cwd)
        .args(args)
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(eyre!("git {} failed: {stderr}", args.join(" ")));
    }
    Ok(())
}

use super::*;
use crate::sandbox::prelude::*;

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
    let plan = plan_from_changed_files(
        vec!["crate/sinexd/src/runtime/stage_as_you_go.rs".to_string()],
        vec!["sinexd".to_string()],
        vec![ImpactedTest {
            package: Some("sinexd".to_string()),
            test_name: "stage_as_you_go_records_material".to_string(),
            evidence: vec![ImpactEvidence {
                source: ImpactEvidenceSource::CoverageRegion,
                subject: "crate/sinexd/src/runtime/stage_as_you_go.rs".to_string(),
                reason: "covered line range".to_string(),
                line_start: None,
                line_end: None,
            }],
        }],
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
        ImpactMode::Balanced,
    )?;

    assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
    assert_eq!(plan.evidence_gaps, vec!["xtask/src/impact.rs"]);
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

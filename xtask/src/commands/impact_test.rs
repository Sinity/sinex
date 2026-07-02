use super::*;
use crate::impact::{
    IMPACT_COVERAGE_SCHEMA_VERSION, IMPACT_PLANNER_VERSION, ImpactAction, ImpactDecision,
    ImpactMode, ImpactPlan,
};
use crate::sandbox::sinex_test;

#[sinex_test]
async fn exact_test_name_filter_accepts_single_test() -> TestResult<()> {
    assert_eq!(
        exact_test_name_from_filter("test(impact_plan_uses_history)"),
        Some("impact_plan_uses_history".to_string())
    );
    assert_eq!(exact_test_name_from_filter("test(one) | test(two)"), None);
    Ok(())
}

#[sinex_test]
async fn llvm_json_segments_become_hunk_addressable_regions() -> TestResult<()> {
    let root = crate::config::workspace_root();
    let file = root.join("xtask/src/impact.rs");
    let rendered = serde_json::json!({
        "data": [{
            "files": [{
                "filename": file.to_string_lossy(),
                "segments": [
                    [10, 1, 1, true, true, false],
                    [12, 1, 1, true, true, false],
                    [15, 1, 0, true, true, false]
                ]
            }]
        }]
    })
    .to_string();
    let regions = coverage_regions_from_llvm_json(&rendered, "some_test", Some("xtask"), &root)?;
    assert_eq!(regions.len(), 2);
    assert_eq!(regions[0]["file_path"], "xtask/src/impact.rs");
    assert_eq!(regions[0]["line_start"], 10);
    assert_eq!(regions[0]["line_end"], 11);
    assert_eq!(regions[1]["line_start"], 12);
    assert_eq!(regions[1]["line_end"], 14);
    Ok(())
}

#[sinex_test]
async fn impact_audit_sample_zero_does_not_force_broad_run() -> TestResult<()> {
    let plan = test_plan(
        Some("test(targeted_case)".to_string()),
        vec![ImpactDecision {
            action: ImpactAction::RunImpactedTests,
            reason: "targeted proof exists".to_string(),
            subject: Some("1 test(s)".to_string()),
        }],
    );

    let sampled = audit_sample_decisions(&plan, 0);

    assert!(sampled.is_empty());
    assert!(audit_command_for_sample(&plan, &sampled).is_none());
    Ok(())
}

#[sinex_test]
async fn impact_audit_samples_impacted_tests_only_when_requested() -> TestResult<()> {
    let plan = test_plan(
        Some("test(targeted_case)".to_string()),
        vec![ImpactDecision {
            action: ImpactAction::RunImpactedTests,
            reason: "targeted proof exists".to_string(),
            subject: Some("1 test(s)".to_string()),
        }],
    );

    let sampled = audit_sample_decisions(&plan, 1);

    assert_eq!(sampled.len(), 1);
    assert_eq!(sampled[0].action, ImpactAction::RunImpactedTests);
    assert!(audit_command_for_sample(&plan, &sampled).is_some());
    Ok(())
}

fn test_plan(impact_filter: Option<String>, decisions: Vec<ImpactDecision>) -> ImpactPlan {
    ImpactPlan {
        planner_version: IMPACT_PLANNER_VERSION.to_string(),
        mode: ImpactMode::Balanced,
        coverage_schema_version: IMPACT_COVERAGE_SCHEMA_VERSION.to_string(),
        changed: Vec::new(),
        affected_packages: Vec::new(),
        impacted_tests: Vec::new(),
        impact_filter,
        scope_args: Vec::new(),
        decisions,
        accepted_risks: Vec::new(),
        evidence_gaps: Vec::new(),
    }
}

use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn contract_only_report_separates_claims_non_claims_and_checks()
-> ::xtask::sandbox::TestResult<()> {
    let report = build_release_readiness_report("rc", "origin/master", false, |_| {
        unreachable!("contract-only mode must not run checks")
    });

    assert_eq!(report.target, "rc");
    assert_eq!(report.status, ReleaseReadinessStatus::ContractOnly);
    assert!(!report.shipped_claims.is_empty());
    assert!(!report.non_claims.is_empty());
    assert!(!report.caveats.is_empty());
    assert!(!report.generated_artifacts.is_empty());
    assert!(
        report
            .required_checks
            .iter()
            .any(|check| check.id == "changed-strict")
    );
    assert!(report.required_checks.iter().any(|check| {
        check.id == "source-catalog-drift"
            && check
                .command
                .contains("source_catalog_artifact_matches_inventory")
    }));
    assert_eq!(
        report.summary.not_run_check_count,
        report.required_checks.len()
    );
    assert!(!report.summary.ready_for_release);
    Ok(())
}

#[sinex_test]
async fn generated_source_catalog_artifact_points_at_behavior_owner_test()
-> ::xtask::sandbox::TestResult<()> {
    let source_catalog = generated_artifacts()
        .into_iter()
        .find(|artifact| artifact.path == "nixos/modules/source-catalog.generated.json")
        .expect("source catalog generated artifact must be listed");

    assert_eq!(
        source_catalog.validation_command,
        "xtask test -p sinexd -E 'test(source_catalog_artifact_matches_inventory)'"
    );
    Ok(())
}

#[sinex_test]
async fn failing_required_check_blocks_release_readiness() -> ::xtask::sandbox::TestResult<()> {
    let report =
        build_release_readiness_report("rc", "origin/master", true, |check| ReleaseCheckResult {
            id: check.id,
            command: check.command.clone(),
            status: if check.id == "schema-strict-diff" {
                CheckStatus::Failed
            } else {
                CheckStatus::Passed
            },
            detail: "synthetic check result".to_string(),
        });

    assert_eq!(report.status, ReleaseReadinessStatus::Blocked);
    assert_eq!(report.summary.failed_check_count, 1);
    assert!(!report.summary.ready_for_release);
    assert!(report.check_results.iter().any(
        |result| result.id == "schema-strict-diff" && result.status == CheckStatus::Failed
    ));
    Ok(())
}

#[sinex_test]
async fn all_required_checks_pass_makes_release_ready() -> ::xtask::sandbox::TestResult<()> {
    let report =
        build_release_readiness_report("rc", "origin/master", true, |check| ReleaseCheckResult {
            id: check.id,
            command: check.command.clone(),
            status: CheckStatus::Passed,
            detail: "synthetic pass".to_string(),
        });

    assert_eq!(report.status, ReleaseReadinessStatus::Ready);
    assert_eq!(report.summary.failed_check_count, 0);
    assert_eq!(report.summary.not_run_check_count, 0);
    assert!(report.summary.ready_for_release);
    Ok(())
}

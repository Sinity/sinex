use super::{EvidenceKind, MissingEvidencePolicy, TestOutcome, TestRunner, VmOutcomeSummary};

#[test]
fn empty_outcome_report_blocks_vm_suite() {
    let runner = TestRunner::new();

    let error = runner
        .finish()
        .expect_err("an empty VM report must not be treated as behavior evidence");
    assert!(error.to_string().contains("zero test outcomes"));
}

#[test]
fn skipped_outcomes_do_not_fail_vm_suite() {
    let mut runner = TestRunner::new();
    runner.pass("schema exists");
    runner.record(
        "clock skew injection",
        TestOutcome::Skipped,
        "VM lacks clock-setting capability",
    );

    runner
        .finish()
        .expect("declared prerequisite skips should not fail the VM suite");
}

#[test]
fn missing_evidence_blocks_vm_suite() {
    let mut runner = TestRunner::new();
    runner.pass("schema exists");
    runner.record(
        "PID reuse safety",
        TestOutcome::EvidenceMissing,
        "no PID was observed",
    );

    let error = runner
        .finish()
        .expect_err("missing evidence must not be reported as a green VM suite");
    assert!(error.to_string().contains("EVIDENCE-MISSING"));
}

#[test]
fn inconclusive_outcomes_block_vm_suite() {
    let mut runner = TestRunner::new();
    runner.record(
        "zombie reaping",
        TestOutcome::Inconclusive,
        "fault branch raced with normal completion",
    );

    let error = runner
        .finish()
        .expect_err("inconclusive fault tests must not be reported as green");
    assert!(error.to_string().contains("INCONCLUSIVE"));
}

#[test]
fn summary_counts_all_outcome_states() {
    let mut runner = TestRunner::new();
    runner.pass("schema exists");
    runner.fail("pipeline drains", "no events arrived");
    runner.skip("clock skew", "VM lacks clock control");
    runner.inconclusive("zombie reaping", "fault raced with normal completion");
    runner.evidence_missing("PID reuse safety", "no PID was observed");

    let summary = VmOutcomeSummary::from_records(&runner.records);
    assert_eq!(summary.total, 5);
    assert_eq!(summary.passed, 1);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.skipped, 1);
    assert_eq!(summary.inconclusive, 1);
    assert_eq!(summary.evidence_missing, 1);

    let json_line = summary.to_json_line(&runner.records);
    assert!(json_line.starts_with("VM_OUTCOME_SUMMARY "));
    assert!(json_line.contains(r#""outcome":"evidence-missing""#));
}

#[test]
fn observed_required_evidence_records_no_synthetic_outcome() {
    let mut runner = TestRunner::new();
    assert!(runner.require_evidence(
        "db row inserted",
        EvidenceKind::Database,
        true,
        "core.events row unavailable",
        MissingEvidencePolicy::Block,
    ));
    runner.pass("db row inserted");

    let summary = VmOutcomeSummary::from_records(&runner.records);
    assert_eq!(summary.total, 1);
    assert_eq!(summary.evidence_missing, 0);
    runner
        .finish()
        .expect("observed required evidence should allow the VM suite to pass");
}

#[test]
fn missing_required_evidence_records_kind_in_summary() {
    let mut runner = TestRunner::new();
    assert!(!runner.require_evidence(
        "event row visible",
        EvidenceKind::Database,
        false,
        "SELECT COUNT(*) FROM core.events failed",
        MissingEvidencePolicy::Block,
    ));

    let summary = VmOutcomeSummary::from_records(&runner.records);
    assert_eq!(summary.evidence_missing, 1);
    let json_line = summary.to_json_line(&runner.records);
    assert!(json_line.contains(r#""evidence_kind":"database""#));
    assert!(json_line.contains(r#""outcome":"evidence-missing""#));

    let error = runner
        .finish()
        .expect_err("blocking evidence requirements must fail the VM suite");
    assert!(error.to_string().contains("EVIDENCE-MISSING"));
}

#[test]
fn missing_optional_evidence_can_be_declared_as_skip() {
    let mut runner = TestRunner::new();
    assert!(!runner.require_evidence(
        "clock skew injection",
        EvidenceKind::FaultInjection,
        false,
        "CAP_SYS_TIME unavailable in this VM",
        MissingEvidencePolicy::Skip,
    ));
    runner.pass("baseline schema check");

    let summary = VmOutcomeSummary::from_records(&runner.records);
    assert_eq!(summary.skipped, 1);
    assert_eq!(summary.evidence_missing, 0);
    runner
        .finish()
        .expect("declared optional missing evidence should be visible without blocking");
}

//! VM-suite result runner with explicit non-green evidence states.

use color_eyre::eyre::{Result, bail};
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestOutcome {
    Passed,
    Failed,
    Skipped,
    Inconclusive,
    EvidenceMissing,
}

impl TestOutcome {
    fn as_str(self) -> &'static str {
        match self {
            TestOutcome::Passed => "passed",
            TestOutcome::Failed => "failed",
            TestOutcome::Skipped => "skipped",
            TestOutcome::Inconclusive => "inconclusive",
            TestOutcome::EvidenceMissing => "evidence-missing",
        }
    }

    fn blocks_success(self) -> bool {
        matches!(
            self,
            TestOutcome::Failed | TestOutcome::Inconclusive | TestOutcome::EvidenceMissing
        )
    }

    fn label(self) -> &'static str {
        match self {
            TestOutcome::Passed => "PASS",
            TestOutcome::Failed => "FAIL",
            TestOutcome::Skipped => "SKIP",
            TestOutcome::Inconclusive => "INCONCLUSIVE",
            TestOutcome::EvidenceMissing => "EVIDENCE-MISSING",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EvidenceKind {
    Database,
    Nats,
    Process,
    Logs,
    SourceMaterial,
    OutputContract,
    FaultInjection,
    Custom,
}

impl EvidenceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EvidenceKind::Database => "database",
            EvidenceKind::Nats => "nats",
            EvidenceKind::Process => "process",
            EvidenceKind::Logs => "logs",
            EvidenceKind::SourceMaterial => "source-material",
            EvidenceKind::OutputContract => "output-contract",
            EvidenceKind::FaultInjection => "fault-injection",
            EvidenceKind::Custom => "custom",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MissingEvidencePolicy {
    Skip,
    Inconclusive,
    Block,
}

impl MissingEvidencePolicy {
    fn outcome(self) -> TestOutcome {
        match self {
            MissingEvidencePolicy::Skip => TestOutcome::Skipped,
            MissingEvidencePolicy::Inconclusive => TestOutcome::Inconclusive,
            MissingEvidencePolicy::Block => TestOutcome::EvidenceMissing,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TestRecord {
    name: String,
    outcome: TestOutcome,
    reason: Option<String>,
    evidence_kind: Option<EvidenceKind>,
}

pub struct TestRunner {
    records: Vec<TestRecord>,
}

impl TestRunner {
    pub fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    pub fn pass(&mut self, name: &str) {
        self.record(name, TestOutcome::Passed, "");
    }

    pub fn skip(&mut self, name: &str, reason: &str) {
        self.record(name, TestOutcome::Skipped, reason);
    }

    pub fn inconclusive(&mut self, name: &str, reason: &str) {
        self.record(name, TestOutcome::Inconclusive, reason);
    }

    pub fn evidence_missing(&mut self, name: &str, reason: &str) {
        self.record(name, TestOutcome::EvidenceMissing, reason);
    }

    pub fn fail(&mut self, name: &str, reason: &str) {
        self.record(name, TestOutcome::Failed, reason);
    }

    pub fn require_evidence(
        &mut self,
        name: &str,
        kind: EvidenceKind,
        observed: bool,
        missing_reason: &str,
        missing_policy: MissingEvidencePolicy,
    ) -> bool {
        if observed {
            return true;
        }

        let outcome = missing_policy.outcome();
        let reason = format!(
            "required {} evidence missing: {missing_reason}",
            kind.as_str()
        );
        self.record_with_evidence(name, outcome, &reason, Some(kind));
        false
    }

    pub fn record(&mut self, name: &str, outcome: TestOutcome, reason: &str) {
        self.record_with_evidence(name, outcome, reason, None);
    }

    fn record_with_evidence(
        &mut self,
        name: &str,
        outcome: TestOutcome,
        reason: &str,
        evidence_kind: Option<EvidenceKind>,
    ) {
        let label = outcome.label();
        let evidence_suffix = evidence_kind
            .map(|kind| format!(" [{} evidence]", kind.as_str()))
            .unwrap_or_default();
        if outcome.blocks_success() {
            eprintln!("  {label} {name}{evidence_suffix}");
            if !reason.is_empty() {
                eprintln!("       {reason}");
            }
        } else {
            println!("  {label} {name}{evidence_suffix}");
            if !reason.is_empty() {
                println!("       {reason}");
            }
        }

        self.records.push(TestRecord {
            name: name.to_string(),
            outcome,
            reason: (!reason.is_empty()).then(|| reason.to_string()),
            evidence_kind,
        });
    }

    pub fn finish(self) -> Result<()> {
        let summary = VmOutcomeSummary::from_records(&self.records);
        println!("────────────────────────────────────────────");
        println!(
            "{} passed / {} total ({} skipped, {} inconclusive, {} evidence-missing, {} failed)",
            summary.passed,
            summary.total,
            summary.skipped,
            summary.inconclusive,
            summary.evidence_missing,
            summary.failed
        );
        println!("{}", summary.to_json_line(&self.records));

        let skipped = Self::entries_for(&self.records, TestOutcome::Skipped);
        if !skipped.is_empty() {
            println!("Skipped:\n{}", skipped.join("\n"));
        }

        if summary.total == 0 {
            bail!(
                "VM suite produced zero test outcomes; category code returned without behavior evidence"
            );
        }

        let blockers = Self::blocking_entries(&self.records);
        if !blockers.is_empty() {
            bail!(
                "{} non-passing VM test outcome(s):\n{}",
                blockers.len(),
                blockers.join("\n")
            );
        }
        Ok(())
    }

    fn entries_for(records: &[TestRecord], outcome: TestOutcome) -> Vec<String> {
        records
            .iter()
            .filter(|record| record.outcome == outcome)
            .map(format_record)
            .collect()
    }

    fn blocking_entries(records: &[TestRecord]) -> Vec<String> {
        records
            .iter()
            .filter(|record| record.outcome.blocks_success())
            .map(|record| format!("{}: {}", record.outcome.label(), format_record(record)))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct VmOutcomeSummary {
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    inconclusive: usize,
    evidence_missing: usize,
}

impl VmOutcomeSummary {
    fn from_records(records: &[TestRecord]) -> Self {
        Self {
            total: records.len(),
            passed: count(records, TestOutcome::Passed),
            failed: count(records, TestOutcome::Failed),
            skipped: count(records, TestOutcome::Skipped),
            inconclusive: count(records, TestOutcome::Inconclusive),
            evidence_missing: count(records, TestOutcome::EvidenceMissing),
        }
    }

    fn to_json_line(self, records: &[TestRecord]) -> String {
        let items: Vec<_> = records
            .iter()
            .map(|record| {
                json!({
                    "name": &record.name,
                    "outcome": record.outcome.as_str(),
                    "reason": record.reason.as_deref(),
                    "evidence_kind": record.evidence_kind.map(EvidenceKind::as_str),
                })
            })
            .collect();

        format!(
            "VM_OUTCOME_SUMMARY {}",
            json!({
                "total": self.total,
                "passed": self.passed,
                "failed": self.failed,
                "skipped": self.skipped,
                "inconclusive": self.inconclusive,
                "evidence_missing": self.evidence_missing,
                "items": items,
            })
        )
    }
}

fn count(records: &[TestRecord], outcome: TestOutcome) -> usize {
    records
        .iter()
        .filter(|record| record.outcome == outcome)
        .count()
}

fn format_record(record: &TestRecord) -> String {
    match record.reason.as_deref() {
        Some(reason) => format!("{}: {reason}", record.name),
        None => record.name.clone(),
    }
}

#[cfg(test)]
mod tests {
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
}

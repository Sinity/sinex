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
#[path = "runner_test.rs"]
mod tests;

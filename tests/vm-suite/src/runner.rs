//! VM-suite result runner with explicit non-green evidence states.

use color_eyre::eyre::{Result, bail};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestOutcome {
    Skipped,
    Inconclusive,
    EvidenceMissing,
}

pub struct TestRunner {
    passed: Vec<String>,
    failed: Vec<String>,
    skipped: Vec<String>,
    inconclusive: Vec<String>,
    evidence_missing: Vec<String>,
}

impl TestRunner {
    pub fn new() -> Self {
        Self {
            passed: Vec::new(),
            failed: Vec::new(),
            skipped: Vec::new(),
            inconclusive: Vec::new(),
            evidence_missing: Vec::new(),
        }
    }

    pub fn pass(&mut self, name: &str) {
        println!("  PASS {name}");
        self.passed.push(name.to_string());
    }

    pub fn skip(&mut self, name: &str, reason: &str) {
        println!("  SKIP {name}");
        println!("       {reason}");
        self.skipped.push(format!("{name}: {reason}"));
    }

    pub fn inconclusive(&mut self, name: &str, reason: &str) {
        eprintln!("  INCONCLUSIVE {name}");
        eprintln!("               {reason}");
        self.inconclusive.push(format!("{name}: {reason}"));
    }

    pub fn evidence_missing(&mut self, name: &str, reason: &str) {
        eprintln!("  EVIDENCE-MISSING {name}");
        eprintln!("                   {reason}");
        self.evidence_missing.push(format!("{name}: {reason}"));
    }

    pub fn fail(&mut self, name: &str, reason: &str) {
        eprintln!("  FAIL {name}");
        eprintln!("       {reason}");
        self.failed.push(format!("{name}: {reason}"));
    }

    pub fn record(&mut self, name: &str, outcome: TestOutcome, reason: &str) {
        match outcome {
            TestOutcome::Skipped => self.skip(name, reason),
            TestOutcome::Inconclusive => self.inconclusive(name, reason),
            TestOutcome::EvidenceMissing => self.evidence_missing(name, reason),
        }
    }

    pub fn finish(self) -> Result<()> {
        let total = self.passed.len()
            + self.failed.len()
            + self.skipped.len()
            + self.inconclusive.len()
            + self.evidence_missing.len();
        println!("────────────────────────────────────────────");
        println!(
            "{} passed / {} total ({} skipped, {} inconclusive, {} evidence-missing, {} failed)",
            self.passed.len(),
            total,
            self.skipped.len(),
            self.inconclusive.len(),
            self.evidence_missing.len(),
            self.failed.len()
        );
        if !self.skipped.is_empty() {
            println!("Skipped:\n{}", self.skipped.join("\n"));
        }

        let mut blockers = Vec::new();
        blockers.extend(self.failed.iter().cloned());
        blockers.extend(
            self.inconclusive
                .iter()
                .map(|entry| format!("INCONCLUSIVE: {entry}")),
        );
        blockers.extend(
            self.evidence_missing
                .iter()
                .map(|entry| format!("EVIDENCE MISSING: {entry}")),
        );
        if !blockers.is_empty() {
            bail!(
                "{} non-passing VM test outcome(s):\n{}",
                blockers.len(),
                blockers.join("\n")
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{TestOutcome, TestRunner};

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
        assert!(error.to_string().contains("EVIDENCE MISSING"));
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
}

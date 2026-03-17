//! Simple pass/fail test runner with summary output.

use color_eyre::eyre::{Result, bail};

pub struct TestRunner {
    passed: Vec<String>,
    failed: Vec<String>,
}

impl TestRunner {
    pub fn new() -> Self {
        Self {
            passed: Vec::new(),
            failed: Vec::new(),
        }
    }

    pub fn pass(&mut self, name: &str) {
        println!("  ✓ {name}");
        self.passed.push(name.to_string());
    }

    pub fn fail(&mut self, name: &str, reason: &str) {
        eprintln!("  ✗ {name}");
        eprintln!("    {reason}");
        self.failed.push(format!("{name}: {reason}"));
    }

    pub fn finish(self) -> Result<()> {
        let total = self.passed.len() + self.failed.len();
        println!("────────────────────────────────────────────");
        println!("{}/{total} tests passed", self.passed.len());
        if !self.failed.is_empty() {
            bail!(
                "{} test(s) failed:\n{}",
                self.failed.len(),
                self.failed.join("\n")
            );
        }
        Ok(())
    }
}

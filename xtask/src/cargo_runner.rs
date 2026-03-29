//! Test seam for cargo invocations.
//!
//! [`CargoRunner`] abstracts the two streaming cargo calls (`check` and `clippy`)
//! plus the fmt-check call so that [`crate::commands::check::CheckCommand::execute`]
//! can be exercised in unit tests without spawning a real compiler.
//!
//! Production code uses [`RealCargoRunner`] (the default).  Tests swap in
//! [`MockCargoRunner`], which returns pre-configured [`DiagnosticSummary`] values.

use color_eyre::eyre::Result;

use crate::cargo_diagnostics::DiagnosticSummary;
#[cfg(any(test, feature = "sandbox"))]
use parking_lot::Mutex;

// ─── Trait ────────────────────────────────────────────────────────────────────

/// Abstract interface over cargo invocations used by [`CheckCommand`].
///
/// Object-safe: callbacks are `&mut dyn FnMut` so the trait can be stored as
/// `Arc<dyn CargoRunner>` inside [`CommandContext`].
pub trait CargoRunner: Send + Sync {
    /// Run `cargo check` with the given package/workspace args, calling
    /// `on_package_done(n)` each time a package finishes compiling.
    fn run_check_streaming(
        &self,
        args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary>;

    /// Run `cargo clippy` with the given args, same callback contract as above.
    fn run_clippy_streaming(
        &self,
        args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary>;

    /// Run `cargo fmt --all -- --check`.  Returns `Ok(())` on success,
    /// `Err` on formatting violations or process failure.
    fn run_fmt_check(&self) -> Result<()>;
}

// ─── Real implementation ───────────────────────────────────────────────────────

/// Production runner: delegates to [`crate::cargo_diagnostics`] streaming
/// functions and [`crate::process::ProcessBuilder`].
pub struct RealCargoRunner;

impl CargoRunner for RealCargoRunner {
    fn run_check_streaming(
        &self,
        args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary> {
        crate::cargo_diagnostics::run_cargo_check_streaming(args, on_package_done)
    }

    fn run_clippy_streaming(
        &self,
        args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary> {
        crate::cargo_diagnostics::run_cargo_clippy_streaming(args, on_package_done)
    }

    fn run_fmt_check(&self) -> Result<()> {
        crate::process::ProcessBuilder::cargo()
            .args(["fmt", "--all", "--", "--check"])
            .with_description("cargo fmt --check")
            .inherit_output()
            .run_ok()
    }
}

// ─── Mock implementation ───────────────────────────────────────────────────────

/// Test double that returns pre-configured [`DiagnosticSummary`] values.
///
/// Call [`MockCargoRunner::with_check`] / [`MockCargoRunner::with_clippy`] /
/// [`MockCargoRunner::with_fmt`] to set expected responses before handing the
/// runner to a [`CommandContext`].
#[cfg(any(test, feature = "sandbox"))]
pub struct MockCargoRunner {
    /// Response returned by `run_check_streaming`. Defaults to a clean summary.
    pub check_response: DiagnosticSummary,
    /// Response returned by `run_clippy_streaming`. Defaults to a clean summary.
    pub clippy_response: DiagnosticSummary,
    /// Whether `run_fmt_check` should succeed (`true`) or fail (`false`).
    pub fmt_ok: bool,
    /// Records how many times each method was called, for assertion in tests.
    pub calls: Mutex<MockCallCounts>,
}

/// Call counts recorded by [`MockCargoRunner`].
#[cfg(any(test, feature = "sandbox"))]
#[derive(Default, Debug)]
pub struct MockCallCounts {
    pub check: usize,
    pub clippy: usize,
    pub fmt: usize,
}

#[cfg(any(test, feature = "sandbox"))]
impl MockCargoRunner {
    /// Create a runner where every call succeeds with no diagnostics.
    pub fn clean() -> Self {
        Self {
            check_response: DiagnosticSummary {
                errors: 0,
                warnings: 0,
                diagnostics: vec![],
                success: true,
                compiled_packages: Default::default(),
            },
            clippy_response: DiagnosticSummary {
                errors: 0,
                warnings: 0,
                diagnostics: vec![],
                success: true,
                compiled_packages: Default::default(),
            },
            fmt_ok: true,
            calls: Default::default(),
        }
    }

    /// Override the `cargo check` response.
    pub fn with_check(mut self, summary: DiagnosticSummary) -> Self {
        self.check_response = summary;
        self
    }

    /// Override the `cargo clippy` response.
    pub fn with_clippy(mut self, summary: DiagnosticSummary) -> Self {
        self.clippy_response = summary;
        self
    }

    /// Make `run_fmt_check` return `Err` (simulate a formatting violation).
    pub fn with_fmt_fail(mut self) -> Self {
        self.fmt_ok = false;
        self
    }

    /// Snapshot of call counts so far.
    pub fn calls(&self) -> MockCallCounts {
        let guard = self.calls.lock();
        MockCallCounts {
            check: guard.check,
            clippy: guard.clippy,
            fmt: guard.fmt,
        }
    }
}

#[cfg(any(test, feature = "sandbox"))]
impl CargoRunner for MockCargoRunner {
    fn run_check_streaming(
        &self,
        _args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary> {
        self.calls.lock().check += 1;
        // Fire the callback once per compiled package so that progress-reporting
        // code paths in CheckCommand::execute() are exercised. The count matches
        // the pre-configured summary so tests that assert progress counts work.
        for (i, _) in self.check_response.compiled_packages.iter().enumerate() {
            on_package_done(i + 1);
        }
        Ok(self.check_response.clone())
    }

    fn run_clippy_streaming(
        &self,
        _args: &[&str],
        on_package_done: &mut dyn FnMut(usize),
    ) -> Result<DiagnosticSummary> {
        self.calls.lock().clippy += 1;
        for (i, _) in self.clippy_response.compiled_packages.iter().enumerate() {
            on_package_done(i + 1);
        }
        Ok(self.clippy_response.clone())
    }

    fn run_fmt_check(&self) -> Result<()> {
        self.calls.lock().fmt += 1;
        if self.fmt_ok {
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!("fmt check failed (mock)"))
        }
    }
}

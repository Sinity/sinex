//! Shared trybuild runner helpers for sinex-primitives.
//!
//! These tests intentionally use raw `#[test]`: trybuild owns the compiler
//! process and does not need the async sandbox harness. Keep the fixture files
//! individual so diagnostics remain reviewable and nextest keeps the runner
//! test names visible.

pub fn cases() -> trybuild::TestCases {
    trybuild::TestCases::new()
}

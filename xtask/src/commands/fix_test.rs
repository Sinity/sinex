//! Regression coverage for sinex-t7y4: `xtask fix --thorough` is a no-op
//! (silently falls back to normal mode) whenever explicit `-p` packages are
//! passed, contradicting its own doc comment ("Slower but maximal fix
//! coverage" says nothing about requiring an empty package list).

use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
#[ignore = "sinex-t7y4 open: should_run_thorough_fixes ignores --thorough whenever \
            explicit -p packages are passed, instead of running thorough fixes scoped \
            to (or at least including) those packages"]
async fn thorough_mode_is_honored_with_explicit_packages() -> xtask::sandbox::TestResult<()> {
    assert!(
        should_run_thorough_fixes(true, false),
        "xtask fix --thorough -p <pkg> should still run thorough fixes, but \
         should_run_thorough_fixes(thorough=true, packages_empty=false) returned false"
    );
    Ok(())
}

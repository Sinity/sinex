//! Regression coverage for sinex-t7y4: `xtask fix --thorough` is a no-op
//! (silently falls back to normal mode) whenever explicit `-p` packages are
//! passed, contradicting its own doc comment ("Slower but maximal fix
//! coverage" — nothing says it requires an empty package list).

use super::*;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter, Status};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn background_fix_is_rejected_before_planning() -> xtask::sandbox::TestResult<()> {
    let ctx = CommandContext::new(OutputWriter::new(OutputFormat::Silent), true, None, "fix");

    let result = FixCommand::default().execute(&ctx).await?;

    assert_eq!(result.status, Status::Failed);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "XTASK_FIX_BACKGROUND_UNSUPPORTED");
    Ok(())
}

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

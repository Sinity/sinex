use super::*;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;

#[sinex_test]
async fn explicit_packages_do_not_expand_to_affected_scope() -> ::xtask::sandbox::TestResult<()> {
    let cmd = BuildCommand {
        packages: vec!["xtask".to_string()],
        release: false,
        all: false,
        dry_run: false,
    };

    let (packages, scope) = cmd.resolve_execution_plan(None)?;

    assert_eq!(packages, vec!["xtask".to_string()]);
    assert_eq!(scope, WorkloadScope::Packages(vec!["xtask".to_string()]));
    Ok(())
}

#[sinex_test]
async fn explicit_packages_are_sorted_and_deduplicated() -> ::xtask::sandbox::TestResult<()> {
    let cmd = BuildCommand {
        packages: vec![
            "xtask".to_string(),
            "sinex-primitives".to_string(),
            "xtask".to_string(),
        ],
        release: false,
        all: false,
        dry_run: false,
    };

    let (packages, scope) = cmd.resolve_execution_plan(None)?;

    let expected = vec!["sinex-primitives".to_string(), "xtask".to_string()];
    assert_eq!(packages, expected);
    assert_eq!(scope, WorkloadScope::Packages(expected));
    Ok(())
}

#[sinex_test]
async fn background_build_is_rejected_before_planning() -> ::xtask::sandbox::TestResult<()> {
    let ctx = CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        true,
        None,
        "build",
    );
    let result = BuildCommand::default().execute(&ctx).await?;

    assert!(result.is_failure(), "background build must be rejected: {result:?}");
    assert_eq!(result.errors[0].code, "XTASK_BUILD_BACKGROUND_UNSUPPORTED");
    Ok(())
}

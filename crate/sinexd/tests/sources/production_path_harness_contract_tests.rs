use super::{
    _run_case, _run_case_with_directory_entry, _run_case_with_logical_path, AdapterKind,
    ProductionPathCase, missing_obligation_failure, run_production_path_case,
};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn production_path_case_with_no_obligations_is_not_green() -> TestResult<()> {
    let failures = _run_case(
        "weechat.message",
        AdapterKind::AppendOnlyFile,
        b"",
        &[],
        &[],
    )
    .await;

    assert_eq!(
        failures,
        vec![missing_obligation_failure("weechat.message")]
    );
    Ok(())
}

#[sinex_test]
async fn production_path_logical_path_case_with_no_obligations_is_not_green() -> TestResult<()>
{
    let failures = _run_case_with_logical_path(
        "weechat.message",
        AdapterKind::AppendOnlyFile,
        b"",
        "buffer.log",
        &[],
        &[],
    )
    .await;

    assert_eq!(
        failures,
        vec![missing_obligation_failure("weechat.message")]
    );
    Ok(())
}

#[sinex_test]
async fn production_path_directory_entry_case_with_no_obligations_is_not_green()
-> TestResult<()> {
    let failures = _run_case_with_directory_entry(
        "fs.created",
        AdapterKind::FileDrop,
        b"",
        "Downloads/example.txt",
        None,
        &[],
        &[],
    )
    .await;

    assert_eq!(failures, vec![missing_obligation_failure("fs.created")]);
    Ok(())
}

#[sinex_test]
async fn production_path_case_wrapper_surfaces_missing_obligations() -> TestResult<()> {
    let case = ProductionPathCase::new(
        "empty obligation fixture",
        "weechat.message",
        AdapterKind::AppendOnlyFile,
        b"",
        &[],
    )
    .with_obligations(&[]);

    let error = run_production_path_case(case)
        .await
        .expect_err("missing obligations must make the public case wrapper fail");

    assert!(
        error.contains("has no obligations"),
        "unexpected error: {error}"
    );
    Ok(())
}

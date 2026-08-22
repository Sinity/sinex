use super::{
    _run_case, _run_case_with_directory_entry, _run_case_with_logical_path, AdapterKind,
    ProductionPathCase, missing_obligation_failure, run_production_path_case,
};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn adapter_kind_bindings_are_not_parser_only_metadata() -> TestResult<()> {
    let cases = [
        (AdapterKind::AppendOnlyFile, b"fixture line\n".as_slice()),
        (AdapterKind::SqliteRow, b"fixture row".as_slice()),
        (AdapterKind::StaticFile, b"fixture document".as_slice()),
        (AdapterKind::FileDrop, b"fixture drop".as_slice()),
        (
            AdapterKind::Journal,
            b"{\"__CURSOR\":\"fixture\",\"MESSAGE\":\"fixture journal\"}\n".as_slice(),
        ),
        (
            AdapterKind::Dbus,
            b"{\"interface\":\"org.example.Fixture\",\"member\":\"Changed\",\"path\":\"/org/example/Fixture\",\"body_json\":{\"value\":1}}\n".as_slice(),
        ),
        (AdapterKind::Clipboard, b"fixture clipboard\n".as_slice()),
        (AdapterKind::UnixSocket, b"fixture socket\n".as_slice()),
    ];

    for (kind, data) in cases {
        super::fixtures::exercise_adapter_binding(kind, data)
            .await
            .map_err(|error| color_eyre::eyre::eyre!("{} fixture binding: {error}", kind.as_str()))?;
    }
    Ok(())
}

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

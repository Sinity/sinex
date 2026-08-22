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
async fn injected_backends_cannot_fall_back_to_live_services_on_reopen() -> TestResult<()> {
    use sinex_primitives::events::SourceMaterial;
    use sinex_primitives::ids::Id;
    use sinex_primitives::parser::InputShapeAdapter;
    use sinexd::runtime::parser::{
        ClipboardPollingAdapter, ClipboardPollingConfig, DbusBus, DbusMessage,
        DbusStreamAdapter, DbusStreamConfig, MockClipboardBackend, MockDbusBackend,
    };

    let material_id = Id::<SourceMaterial>::new();
    let clipboard = ClipboardPollingAdapter::from_backend(MockClipboardBackend::new([Some(
        "fixture clipboard".to_string(),
    )]));
    let clipboard_config = ClipboardPollingConfig::default();
    let _ = clipboard.open(material_id, &clipboard_config, None).await?;
    let clipboard_error = clipboard
        .open(material_id, &clipboard_config, None)
        .await
        .expect_err("reopening a mocked clipboard adapter must not access arboard");
    assert!(clipboard_error.to_string().contains("refusing live backend fallback"));

    let dbus = DbusStreamAdapter::with_backend(MockDbusBackend::new(vec![DbusMessage {
        interface: "org.example.Fixture".to_string(),
        member: "Changed".to_string(),
        path: "/org/example/Fixture".to_string(),
        sender: None,
        body_json: serde_json::json!({"value": 1}),
    }]));
    let dbus_config = DbusStreamConfig {
        bus: DbusBus::Session,
        match_rules: vec!["type='signal',interface='org.example.Fixture'".to_string()],
    };
    let _ = dbus.open(material_id, &dbus_config, None).await?;
    let dbus_error = dbus
        .open(material_id, &dbus_config, None)
        .await
        .expect_err("reopening a mocked D-Bus adapter must not connect to a live bus");
    assert!(dbus_error.to_string().contains("refusing live backend fallback"));

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

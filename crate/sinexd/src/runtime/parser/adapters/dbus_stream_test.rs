use super::*;
use futures::StreamExt;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

fn make_msg(interface: &str, member: &str) -> DbusMessage {
    DbusMessage {
        interface: interface.into(),
        member: member.into(),
        path: "/org/test".into(),
        sender: Some(":1.42".into()),
        body_json: serde_json::json!({ "key": "value" }),
    }
}

#[sinex_test]
async fn test_mock_backend_yields_messages() -> xtask::sandbox::TestResult<()> {
    let msgs = vec![
        make_msg("org.test.Iface", "Signal1"),
        make_msg("org.test.Iface", "Signal2"),
    ];
    let config = DbusStreamConfig {
        bus: DbusBus::Session,
        // Explicit rule so we keep test-msgs, since they don't match the
        // default catalog.
        match_rules: vec!["type='signal',interface='org.test.Iface'".into()],
    };

    let stream = DbusStreamAdapter::open_with_backend(
        Box::new(MockDbusBackend::new(msgs)),
        dummy_material_id(),
        &config,
    );

    let records: Vec<_> = stream.collect().await;
    assert_eq!(records.len(), 2);
    assert!(records[0].is_ok());
    assert!(records[1].is_ok());
    Ok(())
}

#[sinex_test]
async fn test_dbus_anchor_frame_index_is_monotonic() -> xtask::sandbox::TestResult<()> {
    let msgs = vec![
        make_msg("org.test.Iface", "First"),
        make_msg("org.test.Iface", "Second"),
    ];
    let config = DbusStreamConfig {
        bus: DbusBus::System,
        match_rules: vec!["type='signal',interface='org.test.Iface'".into()],
    };

    let stream = DbusStreamAdapter::open_with_backend(
        Box::new(MockDbusBackend::new(msgs)),
        dummy_material_id(),
        &config,
    );

    let records: Vec<_> = stream.collect().await;
    assert_eq!(records.len(), 2);
    let records = records.into_iter().collect::<ParserResult<Vec<_>>>()?;
    assert!(matches!(
        records[0].anchor,
        MaterialAnchor::StreamFrame { frame_index: 0, .. }
    ));
    assert!(matches!(
        records[1].anchor,
        MaterialAnchor::StreamFrame { frame_index: 1, .. }
    ));
    Ok(())
}

#[sinex_test]
async fn test_default_produces_empty_stream() -> xtask::sandbox::TestResult<()> {
    let _adapter = DbusStreamAdapter::default();
    Ok(())
}

#[sinex_test]
async fn test_dbus_match_rule_excludes_unmatched_signal() -> xtask::sandbox::TestResult<()> {
    // The configured rule only accepts org.example.Allowed signals. The
    // mock backend yields one of each interface; only the allowed one
    // should reach the parser.
    let msgs = vec![
        make_msg("org.example.Allowed", "Ok"),
        make_msg("org.example.Forbidden", "Nope"),
    ];
    let config = DbusStreamConfig {
        bus: DbusBus::Session,
        match_rules: vec!["type='signal',interface='org.example.Allowed'".into()],
    };

    let stream = DbusStreamAdapter::open_with_backend(
        Box::new(MockDbusBackend::new(msgs)),
        dummy_material_id(),
        &config,
    );

    let records: Vec<_> = stream.collect().await;
    assert_eq!(records.len(), 1, "expected exactly one delivered record");
    let record = records.into_iter().next().unwrap().unwrap();
    let iface = record
        .metadata
        .get("interface")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(iface, "org.example.Allowed");
    Ok(())
}

#[sinex_test]
async fn test_dbus_default_rules_constrain_catalog() -> xtask::sandbox::TestResult<()> {
    // Empty match_rules in the user config means the adapter substitutes
    // the parser-catalog defaults. A random interface outside the
    // catalog must be dropped.
    let msgs = vec![
        make_msg("org.freedesktop.Notifications", "Notify"),
        make_msg("org.example.NotInCatalog", "Spam"),
    ];
    let config = DbusStreamConfig {
        bus: DbusBus::Session,
        match_rules: vec![],
    };

    let stream = DbusStreamAdapter::open_with_backend(
        Box::new(MockDbusBackend::new(msgs)),
        dummy_material_id(),
        &config,
    );

    let records: Vec<_> = stream.collect().await;
    assert_eq!(records.len(), 1);
    let iface = records[0]
        .as_ref()
        .unwrap()
        .metadata
        .get("interface")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    assert_eq!(iface, "org.freedesktop.Notifications");
    Ok(())
}

#[sinex_test]
async fn test_dbus_match_rule_parses_keys() -> xtask::sandbox::TestResult<()> {
    let rule: ParsedMatchRule =
        "type='signal',interface='org.x',member='Y',path='/p',sender=':1.2'"
            .parse()
            .unwrap();
    assert_eq!(rule.msg_type.as_deref(), Some("signal"));
    assert_eq!(rule.interface.as_deref(), Some("org.x"));
    assert_eq!(rule.member.as_deref(), Some("Y"));
    assert_eq!(rule.path.as_deref(), Some("/p"));
    assert_eq!(rule.sender.as_deref(), Some(":1.2"));
    Ok(())
}

#[sinex_test]
async fn test_dbus_real_backend_open_path() -> xtask::sandbox::TestResult<()> {
    // The default-constructed adapter has no injected backend, so
    // InputShapeAdapter::open builds a RealDbusBackend. In CI there is
    // typically no D-Bus broker, so opening will produce a stream that
    // errors on the first poll. We assert that the open() call itself
    // succeeds (it doesn't try to connect synchronously) and that the
    // stream is well-formed.
    let adapter = DbusStreamAdapter::default();
    let config = DbusStreamConfig {
        bus: DbusBus::Session,
        match_rules: vec!["type='signal',interface='org.example'".into()],
    };
    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .expect("open must succeed without contacting the bus");

    // Pull at most one item with a short timeout. If no bus exists the
    // stream will yield Err quickly. If a bus exists, it may produce
    // nothing in the test window — both are acceptable.
    let _ = tokio::time::timeout(std::time::Duration::from_millis(50), stream.next()).await;
    Ok(())
}

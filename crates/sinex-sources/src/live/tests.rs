use super::*;

#[devkit::test]
fn options_keep_numbers_and_collect_lists() {
    let o = options(&[
        "every=60".into(),
        "kind=screen".into(),
        "follow=true".into(),
        "roots=/a".into(),
        "roots=/b".into(),
    ])
    .unwrap();
    assert_eq!(
        o,
        serde_json::json!({"every": 60, "kind": "screen", "follow": true, "roots": ["/a", "/b"]})
    );
    assert_eq!(options(&[]).unwrap(), Value::Null);
    assert!(options(&["novalue".into()]).is_err());
}

#[devkit::test]
fn live_bindings_come_from_the_kinds_that_capture() {
    let (kind, b) = binding(
        "clipboard",
        Path::new("/spool/primary"),
        "h",
        serde_json::json!({"selection": "primary"}),
    )
    .unwrap();
    assert_eq!(kind.name, "clipboard");
    assert!(b.name.contains("#primary@h"), "{}", b.name);
    // An export is not a live source.
    assert!(binding("spotify", Path::new("/x"), "h", Value::Null).is_err());
}

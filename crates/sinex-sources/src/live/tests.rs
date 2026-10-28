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

#[devkit::test]
fn rediscovery_applies_exclusions_and_settings_without_removing_spools() {
    const FORMAT: InputFormat = InputFormat {
        capture: Some(|_, stop| {
            stop.wait(Duration::from_secs(100));
            Ok(0)
        }),
        ..crate::sources::file_watch::FORMAT
    };
    let root = tempfile::tempdir().unwrap();
    let spool = root.path().join("spool");
    std::fs::create_dir(&spool).unwrap();
    let kept = spool.join("kept-raw-bytes");
    std::fs::write(&kept, b"synthetic raw data").unwrap();
    let (_, mut binding) = binding("file-watch", &spool, "fixture", Value::Null).unwrap();
    let name = binding.name.clone();
    let mut lives = Vec::new();
    extend(
        &mut lives,
        CapturePlan {
            bindings: vec![(&FORMAT, binding.clone())],
            ..Default::default()
        },
    );
    assert_eq!(lives.len(), 1);
    binding.settings.class = Some(sinex_model::Class::Guarded);
    extend(
        &mut lives,
        CapturePlan {
            bindings: vec![(&FORMAT, binding)],
            ..Default::default()
        },
    );
    assert_eq!(lives[0].binding.name, name);
    assert_eq!(
        lives[0].binding.settings.class,
        Some(sinex_model::Class::Guarded)
    );
    extend(&mut lives, CapturePlan::default());
    assert_eq!(lives.len(), 1, "a missing input must keep its spool reader");
    let source = lives[0].binding.source.clone();
    extend(
        &mut lives,
        CapturePlan {
            bindings: vec![],
            exclude: std::iter::once(source).collect(),
        },
    );
    assert!(lives.is_empty());
    assert_eq!(std::fs::read(kept).unwrap(), b"synthetic raw data");
}

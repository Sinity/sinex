use super::*;

#[devkit::test]
fn every_source_belongs_to_exactly_one_kind() {
    for spec in specs() {
        let owners: Vec<&str> = input_kinds()
            .iter()
            .filter(|k| {
                (k.specs)()
                    .iter()
                    .any(|s| s.name == spec.name && s.parser == spec.parser)
            })
            .map(|k| k.name)
            .collect();
        assert_eq!(
            owners.len(),
            1,
            "{} with {}: read by {owners:?}",
            spec.name,
            spec.parser
        );
    }
}

#[devkit::test]
fn kind_names_are_unique() {
    let mut names: Vec<&str> = input_kinds().iter().map(|k| k.name).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(names.len(), before, "duplicate kind names");
}

#[devkit::test]
fn a_shared_source_name_resolves_by_parser() {
    // The live clipboard and its lake lane are one source read two ways.
    let lake = specs()
        .into_iter()
        .find(|s| s.name == "desktop.clipboard" && s.parser.starts_with("sinnix-lake:"))
        .expect("the lake's clipboard lane");
    let b = crate::Binding::of(lake, std::path::Path::new("/realm"), Settings::new("h"));
    assert_eq!(source_for(&b).unwrap().spec().parser, lake.parser);
    assert_eq!(
        input_kind_of_source(lake.name, lake.parser).unwrap().name,
        "sinnix-lake"
    );
}

#[devkit::test]
fn a_shared_parser_resolves_by_name() {
    let outlook = spec("communication.outlook").expect("Outlook is registered");
    let b = crate::Binding::of(outlook, std::path::Path::new("/x.mbox"), Settings::new("h"));
    assert_eq!(
        input_kind_of_source(&b.source, &b.parser).unwrap().name,
        "outlook"
    );
}

#[devkit::test]
fn every_source_resolves_its_parser_from_a_binding() {
    for spec in specs() {
        let binding = Binding {
            source: spec.name.to_owned(),
            ..binding_for_parser(spec.parser, "h").expect("its parser is registered")
        };
        // Sources reading local times need a zone; give every binding one.
        let binding = Binding {
            settings: Settings {
                zone: Some("UTC".into()),
                options: serde_json::json!({"kind": "screen"}),
                ..binding.settings
            },
            ..binding
        };
        let source = source_for(&binding).expect("source builds");
        assert_eq!(source.spec().parser, spec.parser, "{}", spec.name);
    }
    assert!(binding_for_parser("no-such-parser", "h").is_none());
}

#[devkit::test]
fn the_six_late_sources_are_registered() {
    for name in [
        "desktop.arbtt",
        "terminal.asciinema",
        "communication.outlook",
        "account.samsung",
        "google.location-history",
        "google.fit",
    ] {
        assert!(spec(name).is_some(), "{name} is in the registry");
    }
}

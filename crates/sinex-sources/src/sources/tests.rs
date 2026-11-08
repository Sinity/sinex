use super::*;

#[devkit::test]
fn every_source_belongs_to_exactly_one_format() {
    for spec in specs() {
        let owners: Vec<&str> = formats()
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
fn format_names_are_unique() {
    let mut names: Vec<&str> = formats().iter().map(|k| k.name).collect();
    names.sort_unstable();
    let before = names.len();
    names.dedup();
    assert_eq!(names.len(), before, "duplicate format names");
}

#[devkit::test]
fn a_shared_source_name_resolves_by_parser() {
    // The live clipboard and its lake lane are one source read two ways.
    let lake = specs()
        .into_iter()
        .find(|s| s.name == "desktop.clipboard" && s.parser.starts_with("sinnix-lake:"))
        .expect("the lake's clipboard lane");
    let b =
        sinex_source_core::Binding::of(lake, std::path::Path::new("/realm"), Settings::new("h"));
    assert_eq!(source_for(&b).unwrap().spec().parser, lake.parser);
    assert_eq!(
        format_of_source(lake.name, lake.parser).unwrap().name,
        "sinnix-lake"
    );
}

#[devkit::test]
fn a_shared_parser_resolves_by_name() {
    let outlook = spec("communication.outlook").expect("Outlook is registered");
    let b = sinex_source_core::Binding::of(
        outlook,
        std::path::Path::new("/x.mbox"),
        Settings::new("h"),
    );
    assert_eq!(
        format_of_source(&b.source, &b.parser).unwrap().name,
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
                class: None,
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

#[devkit::test]
fn integrated_source_families_are_registered_with_discovery() {
    for name in [
        "udev",
        "machine-telemetry",
        "freedom",
        "github",
        "beads",
        "agentctl",
        "taskwarrior",
        "messenger-sqlite",
        "reddit-archive",
        "machine-records",
        "router",
    ] {
        let format = format(name).unwrap_or_else(|| panic!("{name} is unregistered"));
        assert!(
            !format.discovery.locations.is_empty(),
            "{name} has no discovery locations"
        );
        assert!(format.plan.is_some(), "{name} has no planner");
        assert!(format.reader.is_some(), "{name} has no reader");
    }
    assert!(format("system-bus").is_none());
    assert!(
        (google_takeout::FORMAT.specs)()
            .iter()
            .any(|spec| spec.parser == google_takeout::assistant::SPEC.parser)
    );
}

#[devkit::test]
fn one_expectation_per_source_name() {
    for spec in specs() {
        for other in specs().into_iter().filter(|other| other.name == spec.name) {
            assert_eq!(
                (other.expect, other.expires_after),
                (spec.expect, spec.expires_after),
                "{} is declared with two expectations ({} and {})",
                spec.name,
                spec.parser,
                other.parser
            );
        }
    }
}

#[devkit::test]
fn expectation_fits_origin() {
    use sinex_source_core::{Expectation, Origin};
    for format in formats() {
        for spec in (format.specs)() {
            if format.on_demand(spec) {
                assert_eq!(
                    spec.expect,
                    Expectation::OnDemand,
                    "{} has no input to watch, so silence means nothing",
                    spec.name
                );
            }
            if spec.expect == Expectation::Export {
                assert!(
                    spec.origin != Origin::Request,
                    "{}: an export is an input the operator takes",
                    spec.name
                );
            }
            for deadline in [
                spec.expect.silent_after(),
                spec.expect.every(),
                spec.expires_after,
            ]
            .into_iter()
            .flatten()
            {
                assert!(!deadline.is_zero(), "{}", spec.name);
            }
        }
    }
}

/// The two silent stalls of 2026-10-27 and 2026-10-29 (Atuin history and the
/// session producer's streams) are exactly the sources whose silence must
/// become visible, so each declares a deadline.
#[devkit::test]
fn continuous_sources_declare_silence() {
    for name in [
        "terminal.atuin-history",
        "terminal.zsh-history",
        "desktop.hyprland",
        "desktop.clipboard",
        "session.integrations",
        "machine.journald",
    ] {
        let spec = spec(name).unwrap_or_else(|| panic!("{name} is declared"));
        assert!(
            spec.expect.silent_after().is_some(),
            "{name} is continuous with a deadline"
        );
    }
    // A provider that expires its own records declares it.
    assert_eq!(
        spec("browser.chromium-visits").unwrap().expires_after,
        Some(sinex_source_core::registry::days(90))
    );
}

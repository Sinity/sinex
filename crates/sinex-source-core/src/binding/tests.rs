use super::*;
use crate::registry::{Driver, SourceSpec};
use sinex_model::Class;

const SPEC: SourceSpec = SourceSpec {
    name: "test.source",
    driver: Driver::ExportArchive,
    excisable: false,
    purpose: "tests",
    class: Class::Open,
    parser: "test",
    parser_version: 1,
    event_types: &[],
    framer: crate::Framer::export(
        |b| (vec![(0, b.len())], b.len()),
        crate::drivers::export_archive::Framing::Whole,
    ),
};

#[devkit::test]
fn bindings_are_named_by_source_input_and_host() {
    let b = Binding::of(&SPEC, Path::new("/x/y.zip"), Settings::new("h"));
    assert_eq!(b.name, "test.source:/x/y.zip@h");
    assert_eq!(b.named("visits").name, "test.source:/x/y.zip#visits@h");
}

#[devkit::test]
fn per_file_reads_a_file_or_a_directorys_kept_files_in_order() {
    let dir = std::env::temp_dir().join(format!("sinex-binding-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["b.zip", "a.zip", "c.txt"] {
        std::fs::write(dir.join(name), b"x").unwrap();
    }
    let zips = per_file(&SPEC, &dir, &Settings::new("h"), false, |p| {
        p.extension().is_some_and(|e| e == "zip")
    });
    let names: Vec<_> = zips
        .iter()
        .map(|b| b.input.file_name().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["a.zip", "b.zip"]);
    let one = per_file(&SPEC, &dir.join("c.txt"), &Settings::new("h"), false, any);
    assert_eq!(one.len(), 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[devkit::test]
fn settings_round_trip_and_keep_a_resolved_zone() {
    let s = Settings {
        class: None,
        zone: Some("Europe/Warsaw".into()),
        ..Settings::new("h")
    };
    let back: Settings = serde_json::from_value(serde_json::to_value(&s).unwrap()).unwrap();
    assert_eq!(back, s);
    assert_eq!(back.zone().unwrap().iana_name(), Some("Europe/Warsaw"));
    let resolved = Settings::new("h").with_zone().unwrap();
    assert!(resolved.zone.is_some());
}

#[devkit::test]
fn removal_readers_require_record_locations_in_saved_memory() {
    use crate::drivers::dir_snapshot::DirState;
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("note.md"), "synthetic note").unwrap();
    let driver = |removed_at_source| DirSnapshot {
        root: directory.path().to_owned(),
        host: "fixture".into(),
        select: |_| true,
        part_bytes: 4096,
        removed_at_source,
    };
    let facts = driver(false)
        .poll(&Value::Null, &mut DirState::default())
        .unwrap()
        .material
        .unwrap()
        .memory
        .unwrap();
    let Reading::Polled(mut reader) = walked(driver(true)) else {
        panic!("directory reader")
    };
    assert!(
        reader
            .restore_memory(&facts)
            .unwrap_err()
            .to_string()
            .contains("removal locations")
    );
    let complete = driver(true)
        .poll(&Value::Null, &mut DirState::default())
        .unwrap()
        .material
        .unwrap()
        .memory
        .unwrap();
    let Reading::Polled(mut reader) = walked(driver(true)) else {
        panic!("directory reader")
    };
    reader.restore_memory(&complete).unwrap();
}

use super::*;
use crate::sources::{self, atuin, bash, google_takeout, messenger, takeout_chrome, zsh};
use std::io::Write;

fn settings() -> Settings {
    let mut settings = Settings::new("fixture-host");
    settings.zone = Some("UTC".into());
    settings
}

fn zip(path: &Path, members: &[(&str, &str)]) {
    let mut writer = zip::ZipWriter::new(std::fs::File::create(path).unwrap());
    for (name, body) in members {
        writer
            .start_file(*name, zip::write::SimpleFileOptions::default())
            .unwrap();
        writer.write_all(body.as_bytes()).unwrap();
    }
    writer.finish().unwrap();
}

/// The member names an archive lists, in its order.
fn listed(path: &Path) -> Vec<String> {
    let mut archive = zip::ZipArchive::new(std::fs::File::open(path).unwrap()).unwrap();
    (0..archive.len())
        .map(|i| archive.by_index(i).unwrap().name().to_owned())
        .collect()
}

fn item(path: &Path) -> Item {
    Item {
        path: path.to_path_buf(),
        parser: None,
    }
}

fn disposition<'a>(file: &'a StagedFile, name: &str) -> &'a Disposition {
    &file
        .members
        .iter()
        .find(|m| m.name == name)
        .unwrap_or_else(|| panic!("{name} is not in the inventory"))
        .disposition
}

fn read_by(source: &str) -> Disposition {
    Disposition::Read {
        sources: vec![source.into()],
    }
}

fn not_interpreted() -> Disposition {
    Disposition::StoredNotInterpreted { candidates: vec![] }
}

/// A Takeout export in three parts, as Google splits a large one: products
/// spread over the parts, members no parser reads, and a part holding only
/// those.
fn multipart_takeout(dir: &Path) -> [PathBuf; 3] {
    let parts = [
        dir.join("takeout-20300101T000000Z-001.zip"),
        dir.join("takeout-20300101T000000Z-002.zip"),
        dir.join("takeout-20300101T000000Z-003.zip"),
    ];
    zip(
        &parts[0],
        &[
            ("Takeout/archive_browser.html", "<html></html>"),
            ("Takeout/Keep/First note.json", r#"{"title":"first"}"#),
            ("Takeout/Chrome/History.json", r#"{"Browser History":[]}"#),
            ("Takeout/Travel/Trips.json", "{}"),
        ],
    );
    zip(
        &parts[1],
        &[
            ("Takeout/Keep/Second note.json", r#"{"title":"second"}"#),
            ("Takeout/Tasks/Tasks.json", r#"{"items":[]}"#),
            (
                "Takeout/Calendar/Personal.ics",
                "BEGIN:VCALENDAR\r\nEND:VCALENDAR\r\n",
            ),
        ],
    );
    zip(&parts[2], &[("Takeout/Drive/Document.pdf", "%PDF-1.7")]);
    parts
}

/// law: sx.stage.inventory-total
#[devkit::test]
fn stage_inventory_total() {
    let dir = tempfile::tempdir().unwrap();
    let parts = multipart_takeout(dir.path());
    let plan = plan(sources::formats(), &[item(dir.path())], &settings()).unwrap();

    assert_eq!(
        plan.files
            .iter()
            .map(|f| f.path.clone())
            .collect::<Vec<_>>(),
        parts.to_vec(),
        "a directory's parts are staged in name order"
    );
    for (file, part) in plan.files.iter().zip(&parts) {
        // Every listed member exactly once, in the archive's order.
        let names: Vec<&str> = file.members.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, listed(part), "{}", part.display());
        assert_eq!(file.container, Some(Container::Zip));
    }

    let first = &plan.files[0];
    assert_eq!(
        disposition(first, "Takeout/Keep/First note.json"),
        &read_by(google_takeout::keep::SPEC.name)
    );
    assert_eq!(
        disposition(first, "Takeout/Chrome/History.json"),
        &read_by(takeout_chrome::SPEC.name)
    );
    assert_eq!(
        disposition(first, "Takeout/Travel/Trips.json"),
        &not_interpreted()
    );
    assert_eq!(
        disposition(first, "Takeout/archive_browser.html"),
        &not_interpreted()
    );
    let FileReading::Parsed { formats } = &first.reading else {
        panic!("{:?}", first.reading)
    };
    let formats: BTreeSet<&str> = formats.iter().map(String::as_str).collect();
    assert_eq!(formats, BTreeSet::from(["takeout", "takeout-chrome"]));
    // Only sources that read a member of this part are bound to it.
    let bound: BTreeSet<&str> = first
        .bindings
        .iter()
        .map(|(_, b)| b.source.as_str())
        .collect();
    assert_eq!(
        bound,
        BTreeSet::from([google_takeout::keep::SPEC.name, takeout_chrome::SPEC.name])
    );

    let second = &plan.files[1];
    assert_eq!(
        disposition(second, "Takeout/Calendar/Personal.ics"),
        &read_by(sources::calendar::SPEC.name)
    );
    assert_eq!(
        disposition(second, "Takeout/Tasks/Tasks.json"),
        &read_by(google_takeout::tasks::SPEC.name)
    );

    // A part no parser reads anything of is kept whole with no parser.
    let third = &plan.files[2];
    assert_eq!(third.reading, FileReading::Unparsed { candidates: vec![] });
    assert!(third.bindings.is_empty());
    assert_eq!(
        disposition(third, "Takeout/Drive/Document.pdf"),
        &not_interpreted()
    );
}

/// law: sx.stage.inventory-total
#[devkit::test]
fn a_failed_binding_marks_exactly_its_members_failed() {
    let dir = tempfile::tempdir().unwrap();
    let parts = multipart_takeout(dir.path());
    let mut plan = plan(sources::formats(), &[item(&parts[0])], &settings()).unwrap();
    let file = &mut plan.files[0];
    file.mark_failed(google_takeout::keep::SPEC.name, "synthetic read error");
    assert_eq!(
        disposition(file, "Takeout/Keep/First note.json"),
        &Disposition::Failed {
            sources: vec![google_takeout::keep::SPEC.name.into()],
            reason: "synthetic read error".into()
        }
    );
    assert_eq!(
        disposition(file, "Takeout/Chrome/History.json"),
        &read_by(takeout_chrome::SPEC.name)
    );
    assert_eq!(file.members.len(), listed(&parts[0]).len());
}

fn atuin_database(path: &Path, columns: &str) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(&format!("CREATE TABLE history ({columns})"))
        .unwrap();
}

const ATUIN_COLUMNS: &str = "id TEXT PRIMARY KEY, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT, deleted_at INTEGER";

/// A copy of `format` under another name that declares `accepts`.
fn declared(
    base: &'static InputFormat,
    name: &'static str,
    accepts: &'static [sinex_source_core::stage::Structure],
) -> &'static InputFormat {
    Box::leak(Box::new(InputFormat {
        name,
        accepts,
        ..*base
    }))
}

fn any_line(line: &[u8]) -> bool {
    !line.is_empty()
}

fn any_member(_: &str) -> bool {
    true
}

/// law: sx.stage.strict-detection
#[devkit::test]
fn stage_strict_detection() {
    let dir = tempfile::tempdir().unwrap();
    let extended = dir.path().join("history-one");
    std::fs::write(&extended, ": 1600000000:0;synthetic command\n").unwrap();
    // Named like a zsh history, shaped like nothing declared.
    let plain = dir.path().join("zsh_history");
    std::fs::write(&plain, "synthetic command\n").unwrap();
    let database = dir.path().join("history.db");
    atuin_database(&database, ATUIN_COLUMNS);
    let other_database = dir.path().join("other.db");
    atuin_database(&other_database, "id TEXT, command TEXT");

    let formats = sources::formats();
    let staged = plan(
        formats,
        &[
            item(&extended),
            item(&plain),
            item(&database),
            item(&other_database),
        ],
        &settings(),
    )
    .unwrap();
    // Exactly one match assigns the parser.
    assert_eq!(
        staged.files[0].reading,
        FileReading::Parsed {
            formats: vec!["zsh".into()]
        }
    );
    assert_eq!(staged.files[0].bindings.len(), 1);
    assert_eq!(staged.files[0].bindings[0].1.source, zsh::SPEC.name);
    // No match: stored with no parser and no candidates, whatever the name;
    // bash history declares no structure and is never assigned by detection.
    assert_eq!(
        staged.files[1].reading,
        FileReading::Unparsed { candidates: vec![] }
    );
    assert!(bash::FORMAT.accepts.is_empty());
    assert_eq!(
        staged.files[2].reading,
        FileReading::Parsed {
            formats: vec!["atuin".into()]
        }
    );
    assert_eq!(staged.files[2].bindings[0].1.source, atuin::SPEC.name);
    assert_eq!(
        staged.files[3].reading,
        FileReading::Unparsed { candidates: vec![] }
    );

    // Two matches: stored with no parser and both listed.
    static LINE: [sinex_source_core::stage::Structure; 1] =
        [sinex_source_core::stage::Structure::FirstLine(any_line)];
    let both = [
        declared(&zsh::FORMAT, "first-synthetic", &LINE),
        declared(&bash::FORMAT, "second-synthetic", &LINE),
    ];
    let staged = plan(&both, &[item(&extended)], &settings()).unwrap();
    assert_eq!(
        staged.files[0].reading,
        FileReading::Unparsed {
            candidates: vec!["first-synthetic".into(), "second-synthetic".into()]
        }
    );
    assert!(staged.files[0].bindings.is_empty());
    assert_eq!(
        disposition(&staged.files[0], "history-one"),
        &Disposition::StoredNotInterpreted {
            candidates: vec!["first-synthetic".into(), "second-synthetic".into()]
        }
    );
}

/// law: sx.stage.strict-detection
#[devkit::test]
fn a_member_two_sources_would_read_is_read_by_neither() {
    let dir = tempfile::tempdir().unwrap();
    let archive = dir.path().join("export.zip");
    zip(
        &archive,
        &[
            ("Takeout/Keep/Contested.json", r#"{"title":"x"}"#),
            ("Takeout/Keep/media/photo.jpg", "jpeg"),
            ("Takeout/Chrome/History.json", r#"{"Browser History":[]}"#),
        ],
    );
    static ANY: [sinex_source_core::stage::Structure; 1] =
        [sinex_source_core::stage::Structure::ArchiveLayout(
            any_member,
        )];
    let formats = [
        &google_takeout::FORMAT,
        &takeout_chrome::FORMAT,
        declared(&messenger::FORMAT, "messenger", &ANY),
    ];
    let staged = plan(&formats, &[item(&archive)], &settings()).unwrap();
    let file = &staged.files[0];
    // Keep's note and Chrome's history both look like messages to the
    // messenger reader, so neither Keep, Chrome nor messenger reads them; the
    // candidates are the formats `--parser` would take.
    assert_eq!(
        disposition(file, "Takeout/Keep/Contested.json"),
        &Disposition::StoredNotInterpreted {
            candidates: vec!["messenger".into(), "takeout".into()]
        }
    );
    assert_eq!(
        disposition(file, "Takeout/Chrome/History.json"),
        &Disposition::StoredNotInterpreted {
            candidates: vec!["messenger".into(), "takeout-chrome".into()]
        }
    );
    assert_eq!(
        disposition(file, "Takeout/Keep/media/photo.jpg"),
        &not_interpreted()
    );
    assert!(file.bindings.is_empty());
    assert_eq!(
        file.reading,
        FileReading::Unparsed {
            candidates: vec![
                "messenger".into(),
                "takeout".into(),
                "takeout-chrome".into()
            ]
        }
    );
}

#[devkit::test]
fn a_named_parser_reads_without_detection() {
    let dir = tempfile::tempdir().unwrap();
    let plain = dir.path().join("history");
    std::fs::write(&plain, "synthetic command\n").unwrap();
    let staged = plan(
        sources::formats(),
        &[Item {
            path: plain.clone(),
            parser: Some("bash".into()),
        }],
        &settings(),
    )
    .unwrap();
    assert_eq!(
        staged.files[0].reading,
        FileReading::Parsed {
            formats: vec!["bash".into()]
        }
    );
    assert_eq!(
        disposition(&staged.files[0], "history"),
        &read_by(bash::SPEC.name)
    );
    let error = plan(
        sources::formats(),
        &[Item {
            path: plain,
            parser: Some("no-such-format".into()),
        }],
        &settings(),
    )
    .unwrap_err();
    assert!(
        error.contains("no format no-such-format; one of"),
        "{error}"
    );
}

#[devkit::test]
fn a_missing_path_is_refused_before_anything_is_planned() {
    let dir = tempfile::tempdir().unwrap();
    let error = plan(
        sources::formats(),
        &[item(&dir.path().join("absent"))],
        &settings(),
    )
    .unwrap_err();
    assert!(
        error.contains("absent: no such file or directory"),
        "{error}"
    );
}

#[devkit::test]
fn extended_history_lines_and_nothing_else() {
    assert!(zsh::extended_line(b": 1600000000:0;ls"));
    assert!(zsh::extended_line(b": 1600000000:12;"));
    assert!(!zsh::extended_line(b": 1600000000;ls"));
    assert!(!zsh::extended_line(b": x:0;ls"));
    assert!(!zsh::extended_line(b"#1600000000"));
    assert!(!zsh::extended_line(b"- cmd: ls"));
}

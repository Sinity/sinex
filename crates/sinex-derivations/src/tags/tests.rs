use super::*;
use sinex_model::event_types::CommandImported;
use sinex_model::event_types::machine::FileChanged;
use sinex_model::{Class, MaterialId, ReadingOrder, RecordLocation, Timestamp};

fn captured<P: EventPayload>(payload: &P, source: &str) -> Event {
    Event::build(
        payload,
        source,
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Guarded,
    )
    .unwrap()
}

fn file(path: &str) -> Event {
    captured(
        &FileChanged {
            received_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            change: "written".into(),
            path: Some(path.into()),
            path_hex: None,
            to: None,
            is_dir: Some(false),
            note: None,
        },
        "machine.file-watch",
    )
}

fn read(event: &Event) -> Vec<Event> {
    Tags.derive(
        Input {
            event,
            material: None,
        },
        &Env::at(Timestamp::now()),
    )
    .unwrap()
}

#[devkit::test]
fn every_matching_rule_is_applied_with_its_parent_and_class() {
    let event = file("/project/main.rs");
    let tags = read(&event);
    let names: Vec<_> = tags
        .iter()
        .map(|tag| tag.payload["tag"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["inferred.file-type.rust", "sys.source.file"]);
    for tag in tags {
        assert_eq!(tag.class, Class::Guarded);
        assert_eq!(
            tag.origin,
            Origin::Derived {
                parents: vec![event.id]
            }
        );
        assert_eq!(tag.ts_orig, event.ts_orig);
        assert!(!tag.payload["rules"].as_array().unwrap().is_empty());
    }
}

#[devkit::test]
fn unknown_extensions_and_dots_in_directories_are_not_file_types() {
    for path in ["/project/file.xyz", "/project.rs/README", "/project/.rs"] {
        let names: Vec<_> = read(&file(path))
            .into_iter()
            .map(|tag| tag.payload["tag"].clone())
            .collect();
        assert_eq!(names, [serde_json::json!("sys.source.file")], "{path}");
    }
}

#[devkit::test]
fn repeats_and_replay_keep_the_tag_identity_and_unknown_time() {
    let event = captured(
        &CommandImported {
            command: "ls".into(),
            hostname: "test".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "terminal.zsh",
    );
    let first = read(&event).remove(0);
    let repeat = read(&event).remove(0);
    assert_eq!(first.record_hash, repeat.record_hash);
    assert_eq!(first.equivalence_key, repeat.equivalence_key);
    assert_eq!(first.ts_orig.at, None);
    let mut replay = event.clone();
    replay.id = sinex_model::EventId::new();
    let reread = read(&replay).remove(0);
    assert_eq!(first.payload, reread.payload);
    assert_eq!(first.equivalence_key, reread.equivalence_key);
    assert_ne!(first.record_hash, reread.record_hash);
}

#[devkit::test]
fn tags_ignore_derived_events_and_the_input_registry_has_no_self_output() {
    let tag = read(&file("/project/main.rs")).remove(0);
    assert!(read(&tag).is_empty());
    assert!(inputs().contains(&"command.imported"));
    assert!(!inputs().contains(&"tag.applied"));
    assert!(!inputs().contains(&"sinex.telemetry"));
}

#[devkit::test]
fn input_order_and_incremental_reads_give_the_same_tags() {
    let events = [file("/p/a.rs"), file("/p/b.md"), file("/p/c.xyz")];
    let normalized = |outputs: Vec<Event>| {
        let mut values: Vec<_> = outputs.into_iter().map(|e| e.payload.to_string()).collect();
        values.sort();
        values
    };
    let full = normalized(events.iter().flat_map(read).collect());
    let shuffled = normalized(
        [&events[2], &events[0], &events[1]]
            .into_iter()
            .flat_map(read)
            .collect(),
    );
    let incremental = normalized(
        events
            .chunks(1)
            .flat_map(|chunk| chunk.iter().flat_map(read))
            .collect(),
    );
    assert_eq!(full, shuffled);
    assert_eq!(full, incremental);
}

#[devkit::test]
fn mime_rules_match_declared_media_types_only() {
    let mut event = file("/p/README");
    for (mime, expected) in [
        ("text/markdown", Some("sys.mime.text-markdown")),
        ("application/pdf", Some("sys.mime.pdf")),
        ("image/png", None),
    ] {
        event.payload["media_type"] = serde_json::json!(mime);
        let tags: Vec<_> = read(&event)
            .into_iter()
            .filter_map(|tag| tag.payload["tag"].as_str().map(str::to_owned))
            .filter(|tag| tag.starts_with("sys.mime."))
            .collect();
        assert_eq!(
            tags,
            expected.into_iter().map(str::to_owned).collect::<Vec<_>>()
        );
    }
}

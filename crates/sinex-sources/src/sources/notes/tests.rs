use super::*;
use crate::drivers::dir_snapshot::{DirState, removed_marker};
use crate::{DrainInput, Limits, drain};
use sinex_model::{MaterialId, ReadingOrder, RecordOutcome};

#[devkit::test]
fn notes_and_their_removal() {
    let owned = tempfile::tempdir().unwrap();
    let root = owned.path().to_path_buf();
    std::fs::create_dir_all(root.join(".obsidian")).unwrap();
    std::fs::write(root.join("a.md"), "# A\n").unwrap();
    std::fs::write(root.join(".obsidian/app.json"), "{}").unwrap();
    let d = driver(root.clone(), "h".into());
    let mut state = DirState::default();
    let parser = NotesParser;
    let run = |a: &crate::drivers::Acquired| {
        let m = a.material.as_ref().unwrap();
        let out = drain(
            &parser,
            DrainInput {
                source: "t",
                class: Class::Open,
                material: MaterialId(uuid::Uuid::nil()),
                bytes: m.record_bytes(),
                records: &m.records,
                carry: Value::Null,
                reading_order: &|_| ReadingOrder(0),
                limits: Limits::default(),
                manifest: &m.manifest,
            },
        )
        .unwrap();
        out.into_iter()
            .flat_map(|c| c.outcomes)
            .map(|o| match o {
                RecordOutcome::Events { events, .. } => events[0].payload.clone(),
                RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
            })
            .collect::<Vec<_>>()
    };
    let acquired = d.poll(&Value::Null, &mut state).unwrap();
    let m = acquired.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    let first = run(&acquired);
    assert_eq!(first.len(), 1, ".obsidian is not a note");
    assert_eq!(first[0]["path"], "a.md");
    assert_eq!(first[0]["text"], "# A\n");
    assert!(first[0]["modified_at"].is_string());
    std::fs::remove_file(root.join("a.md")).unwrap();
    let second = run(&d.poll(&cursor, &mut state).unwrap());
    assert_eq!(second[0]["path"], "a.md");
    assert!(second[0]["removed_at"].is_string());
    assert!(second[0]["text"].is_null());
    assert!(!removed_marker("a.md").is_empty());
}

fn note(text: &str, path: &str) -> NoteDocument {
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({"path":"/vault/MyVault"}));
    let member =
        serde_json::from_value::<sinex_model::Member>(serde_json::json!({"path":path})).unwrap();
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    NotesParser
        .parse(&text.as_bytes(), &NoteIds::default(), &context)
        .unwrap()
        .0
        .remove(0)
}

#[devkit::test]
fn structured_note_keeps_exact_text_and_extracts_front_matter_tags_links_and_native_id() {
    use sinex_model::event_types::media::NoteFrontMatter;
    let text = "---\r\ntitle: Weekly review\r\nid: areas.review.weekly\r\ntags: [review, \"#planning\", review]\r\n---\r\n# Heading #not-a-tag\r\nBody #inline-tag [[Other Note|alias]] [[Other Note#part]] [[Third]].\r\n";
    let p = note(text, "areas/weekly.md");
    assert_eq!(p.text.as_deref(), Some(text));
    assert_eq!(p.size, text.len() as u64);
    assert_eq!(p.title.as_deref(), Some("Weekly review"));
    assert_eq!(p.note_id.as_deref(), Some("areas.review.weekly"));
    assert_eq!(p.tags.unwrap(), ["inline-tag", "planning", "review"]);
    assert_eq!(p.wikilinks.unwrap(), ["Other Note", "Third"]);
    let Some(NoteFrontMatter::Parsed { value }) = p.front_matter else {
        panic!("front matter")
    };
    assert_eq!(
        value["tags"],
        serde_json::json!(["review", "#planning", "review"])
    );
}

#[devkit::test]
fn absent_empty_and_invalid_front_matter_are_distinct_without_losing_the_text() {
    use sinex_model::event_types::media::{NoteFrontMatter, NoteFrontMatterError};
    let empty = note("", "empty.markdown");
    assert_eq!(empty.front_matter, Some(NoteFrontMatter::Absent));
    assert_eq!(empty.title.as_deref(), Some("empty"));
    assert_eq!(empty.tags, Some(vec![]));
    assert_eq!(empty.wikilinks, Some(vec![]));
    assert_eq!(empty.text.as_deref(), Some(""));
    assert_eq!(
        note("---\n---\n", "empty.md").front_matter,
        Some(NoteFrontMatter::Parsed { value: Value::Null })
    );
    assert_eq!(
        note("---\n{}\n---\n", "empty.md").front_matter,
        Some(NoteFrontMatter::Parsed {
            value: serde_json::json!({})
        })
    );
    let bad = "---\ntitle: [private unfinished\n---\nUseful [[note]] #body-tag";
    let p = note(bad, "a.md");
    assert_eq!(p.text.as_deref(), Some(bad));
    assert_eq!(p.wikilinks, Some(vec!["note".into()]));
    let Some(NoteFrontMatter::Invalid {
        kind: NoteFrontMatterError::Yaml,
        ..
    }) = p.front_matter
    else {
        panic!("invalid YAML must stay visible")
    };
    let unclosed = note("---\ntitle: unfinished\n", "a.md");
    assert!(matches!(
        unclosed.front_matter,
        Some(NoteFrontMatter::Invalid {
            kind: NoteFrontMatterError::Unclosed,
            ..
        })
    ));
    assert_eq!(unclosed.tags, None);
    assert_eq!(unclosed.wikilinks, None);
}

#[devkit::test]
fn supplied_scalar_types_and_title_fallbacks_do_not_invent_note_ids() {
    use sinex_model::event_types::media::NoteFrontMatter;
    let p = note(
        "---\nid: '123'\ntags: [42, '42', null, false]\n---\n",
        "file.md",
    );
    assert_eq!(p.note_id.as_deref(), Some("123"));
    assert_eq!(p.tags, Some(vec!["42".into()]));
    let Some(NoteFrontMatter::Parsed { value }) = p.front_matter else {
        panic!("parsed")
    };
    assert_eq!(value["tags"], serde_json::json!([42, "42", null, false]));
    let n = note("---\nid: 123\n---\n", "actual_file.markdown");
    assert_eq!(n.note_id, None);
    assert_eq!(n.title.as_deref(), Some("actual file"));
    assert_eq!(
        note("---\nid: ''\ntitle: ''\n---\n", "last-name.md")
            .note_id
            .as_deref(),
        Some("")
    );
    assert_eq!(
        note("---\nid: areas.some-note\n---\n", "unused.md")
            .title
            .as_deref(),
        Some("some note")
    );
    assert_eq!(
        note("---not a fence\n#tag\n", "actual_file.markdown").front_matter,
        Some(NoteFrontMatter::Absent)
    );
}

#[devkit::test]
fn unsupported_yaml_values_and_expansion_keep_text_with_structural_diagnostics() {
    use sinex_model::event_types::media::{NoteFrontMatter, NoteFrontMatterError};
    for fm in [
        "nested: {1: value}",
        "? [a, b]\n: value",
        "id: 18446744073709551616",
        "id: 00123",
        "id: !!int '18446744073709551616'",
        "id: !custom private-value",
        "id: !!binary SGVsbG8=",
        "file: !include /not/a/real/file",
        "id: .inf",
        "title: first\ntitle: second",
    ] {
        let text = format!("---\n{fm}\n---\nUseful text");
        let p = note(&text, "a.md");
        assert_eq!(p.text.as_deref(), Some(text.as_str()));
        assert!(
            matches!(p.front_matter, Some(NoteFrontMatter::Invalid { .. })),
            "{fm}: {:?}",
            p.front_matter
        );
        if !fm.contains("title: first") {
            assert!(
                matches!(
                    p.front_matter,
                    Some(NoteFrontMatter::Invalid {
                        kind: NoteFrontMatterError::UnsupportedValue,
                        ..
                    })
                ),
                "{fm}: {:?}",
                p.front_matter
            );
        }
        let diagnostic = serde_json::to_string(&p.front_matter).unwrap();
        assert!(!diagnostic.contains("private-value"));
        assert_eq!(p.note_id, None);
    }
    let accepted = note(
        "---\nbase: &label ordinary\ncopy: *label\nid: '18446744073709551616'\n---\n",
        "a.md",
    );
    assert_eq!(accepted.note_id.as_deref(), Some("18446744073709551616"));
    assert_eq!(
        note("---\nid: !!str 00123\n---\n", "a.md")
            .note_id
            .as_deref(),
        Some("00123")
    );

    assert!(matches!(
        accepted.front_matter,
        Some(NoteFrontMatter::Parsed { .. })
    ));
    let repeated = format!(
        "---\na: &a [x,x,x,x]\nb: [{}]\n---\n",
        vec!["*a"; 100].join(",")
    );
    assert!(matches!(
        note(&repeated, "a.md").front_matter,
        Some(NoteFrontMatter::Invalid {
            kind: NoteFrontMatterError::Limit,
            ..
        })
    ));
}

#[devkit::test]
fn payloads_without_metadata_and_removed_notes_do_not_claim_metadata() {
    let old = serde_json::json!({"vault":"v","path":"a.md","text":"","size":0,"modified_at":null,"removed_at":null});
    let payload: NoteDocument = serde_json::from_value(old.clone()).unwrap();
    assert_eq!(payload.front_matter, None);
    assert_eq!(payload.tags, None);
    assert_eq!(serde_json::to_value(payload).unwrap(), old);
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({"path":"/vault/v"}));
    let member = serde_json::from_value::<sinex_model::Member>(
        serde_json::json!({"path":"a.md","removed_at_ns":"1700000000000000000"}),
    )
    .unwrap();
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    let payload = NotesParser
        .parse(&b"".as_slice(), &NoteIds::default(), &context)
        .unwrap()
        .0
        .remove(0);
    assert_eq!(payload.text, None);
    assert_eq!(payload.front_matter, None);
    assert_eq!(payload.title, None);
    assert_eq!(payload.tags, None);
}

#[devkit::test]
fn front_matter_of_any_size_is_parsed() {
    use sinex_model::event_types::media::NoteFrontMatter;
    let text = format!("---\nbody: \"{}\"\n---\ntext\n", "x".repeat(2 << 20));
    assert!(matches!(
        note(&text, "large.md").front_matter,
        Some(NoteFrontMatter::Parsed { .. })
    ));
}

/// Read `path` (with `text`, or as removed when `text` is `None`) under the
/// carried IDs, as a scan does.
fn step(ids: &NoteIds, path: &str, text: Option<&str>) -> (Vec<NoteDocument>, NoteIds) {
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({"path":"/vault/v"}));
    let member = match text {
        Some(_) => serde_json::json!({"path": path}),
        None => serde_json::json!({"path": path, "removed_at_ns": "1700000000000000000"}),
    };
    let member = serde_json::from_value::<sinex_model::Member>(member).unwrap();
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    NotesParser
        .parse(&text.unwrap_or("").as_bytes(), ids, &context)
        .unwrap()
}

fn key(note: &NoteDocument) -> sinex_model::EquivalenceKey {
    use sinex_model::EventPayload;
    match note.key_kind() {
        sinex_model::KeyKind::Fields(fields) => sinex_model::EquivalenceKey::of(
            NoteDocument::EVENT_TYPE,
            fields,
            &serde_json::to_value(note).unwrap(),
        ),
        sinex_model::KeyKind::Location => panic!("notes are keyed"),
    }
}

#[devkit::test]
fn a_renamed_note_keeps_one_identity() {
    let text = "---\nid: projects.alpha\n---\nAlpha\n";
    let (first, ids) = step(&NoteIds::default(), "alpha.md", Some(text));
    // The scan reads the moved file, then finds the old path gone.
    let (moved, ids) = step(&ids, "projects/alpha.md", Some(text));
    let (gone, ids) = step(&ids, "alpha.md", None);
    assert_eq!(key(&first[0]), key(&moved[0]), "one note across the rename");
    assert!(gone.is_empty(), "the old path is not a removal of the note");
    // Deleting it later removes the note under its ID.
    let (deleted, _) = step(&ids, "projects/alpha.md", None);
    assert_eq!(deleted[0].note_id.as_deref(), Some("projects.alpha"));
    assert!(deleted[0].removed_at.is_some());
    assert_eq!(key(&deleted[0]), key(&first[0]));
}

#[devkit::test]
fn a_note_without_an_id_is_its_path() {
    let (first, ids) = step(&NoteIds::default(), "plain.md", Some("text\n"));
    let (renamed, ids) = step(&ids, "other.md", Some("text\n"));
    let (gone, _) = step(&ids, "plain.md", None);
    assert_ne!(key(&first[0]), key(&renamed[0]), "equal text never joins");
    assert_eq!(key(&gone[0]), key(&first[0]));
    assert!(gone[0].removed_at.is_some());
    let (empty, _) = step(&NoteIds::default(), "e.md", Some("---\nid: \"\"\n---\n"));
    assert_eq!(empty[0].note_id.as_deref(), Some(""));
    let (path_keyed, _) = step(&NoteIds::default(), "e.md", Some("x\n"));
    assert_eq!(key(&empty[0]), key(&path_keyed[0]), "an empty ID identifies nothing");
}

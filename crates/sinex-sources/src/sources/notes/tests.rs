use super::*;
use sinex_model::{ReadingOrder, RecordOutcome};
use sinex_source_core::drivers::dir_snapshot::DirState;
use sinex_source_core::{DrainInput, Limits, drain};

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
    let run = |a: &sinex_source_core::drivers::Acquired| {
        let m = a.material.as_ref().unwrap();
        let out = drain(
            &parser,
            DrainInput {
                source: "t",
                class: Class::Open,
                material: m.id,
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
            .flat_map(|o| match o {
                RecordOutcome::Events { events, .. } => events
                    .into_iter()
                    .map(|event| event.payload)
                    .collect::<Vec<_>>(),
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
    let removed = d.poll(&cursor, &mut state).unwrap();
    assert!(
        run(&removed).is_empty(),
        "removal markers carry no invented note text"
    );
    assert_eq!(
        removed.material.unwrap().removed,
        vec![sinex_model::RecordLocation {
            material: m.id,
            anchor: m.records[0].0 as u64,
        }]
    );
}

fn note(text: &str, path: &str) -> NoteDocument {
    note_in_vault(text, path, "/vault/MyVault", "synthetic")
}

fn note_in_vault(text: &str, path: &str, root: &str, host: &str) -> NoteDocument {
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({"path":root,"host":host}));
    let member =
        serde_json::from_value::<sinex_model::Member>(serde_json::json!({"path":path})).unwrap();
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    NotesParser
        .parse(&text.as_bytes(), &(), &context)
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
fn payloads_without_metadata_and_removed_markers_keep_unknown_information() {
    use sinex_model::EventPayload;
    let partial = serde_json::json!({"vault":"v","path":"a.md","text":"","size":0,"modified_at":null,"removed_at":null});
    let payload: NoteDocument = serde_json::from_value(partial.clone()).unwrap();
    assert_eq!(payload.front_matter, None);
    assert_eq!(payload.tags, None);
    assert_eq!(payload.key_kind(), sinex_model::KeyKind::Location);
    assert_eq!(serde_json::to_value(payload).unwrap(), partial);
    let manifest = sinex_model::Manifest::from_json(serde_json::json!({"path":"/vault/v"}));
    let member = serde_json::from_value::<sinex_model::Member>(
        serde_json::json!({"path":"a.md","removed_at_ns":"1700000000000000000"}),
    )
    .unwrap();
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&member),
    };
    let (payloads, ()) = NotesParser.parse(&b"".as_slice(), &(), &context).unwrap();
    assert!(payloads.is_empty());
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

fn key(note: &NoteDocument) -> sinex_model::EquivalenceKey {
    use sinex_model::EventPayload;
    match note.key_kind() {
        sinex_model::KeyKind::Fields(fields) => sinex_model::EquivalenceKey::of(
            NoteDocument::EVENT_TYPE,
            fields,
            &serde_json::to_value(note).unwrap(),
        ),
        sinex_model::KeyKind::Location => panic!("this fixture has a known vault"),
    }
}

#[devkit::test]
fn a_renamed_note_keeps_one_identity() {
    let text = "---\nid: projects.alpha\n---\nAlpha\n";
    let first = note(text, "alpha.md");
    let moved = note(text, "projects/alpha.md");
    assert_eq!(key(&first), key(&moved));
}

#[devkit::test]
fn a_note_without_an_id_is_its_path() {
    let first = note("text\n", "plain.md");
    let renamed = note("text\n", "other.md");
    assert_ne!(key(&first), key(&renamed), "equal text never joins");
    let empty = note("---\nid: \"\"\n---\n", "e.md");
    assert_eq!(empty.note_id.as_deref(), Some(""));
    assert_eq!(key(&empty), key(&note("x\n", "e.md")));
}

#[devkit::test]
fn equally_named_vaults_do_not_join_notes() {
    use sinex_model::EventPayload;
    let text = "---\nid: synthetic\n---\nA note\n";
    let first = note_in_vault(text, "a.md", "/one/MyVault", "one");
    let other_root = note_in_vault(text, "a.md", "/two/MyVault", "one");
    let other_host = note_in_vault(text, "a.md", "/one/MyVault", "two");
    assert_eq!(first.vault, other_root.vault);
    assert_eq!(first.vault, other_host.vault);
    assert_ne!(key(&first), key(&other_root));
    assert_ne!(key(&first), key(&other_host));
    let mut unknown = first;
    unknown.host = None;
    assert_eq!(unknown.key_kind(), sinex_model::KeyKind::Location);
}
#[devkit::test]
fn native_note_paths_stay_distinct_after_reader_memory_and_removal() {
    use std::os::unix::ffi::OsStringExt;
    let directory = tempfile::tempdir().unwrap();
    let raw_name = std::ffi::OsString::from_vec(b"a\xff.md".to_vec());
    std::fs::write(directory.path().join(&raw_name), "same text").unwrap();
    std::fs::write(directory.path().join("a%FF.md"), "same text").unwrap();
    let driver = driver(directory.path().to_owned(), "host".into());
    let mut state = DirState::default();
    let material = driver
        .poll(&Value::Null, &mut state)
        .unwrap()
        .material
        .unwrap();
    let cursor = (material.cursor_at)(material.records.last().unwrap().1).unwrap();
    let outputs = drain(
        &NotesParser,
        DrainInput {
            source: SPEC.name,
            class: Class::Open,
            material: material.id,
            bytes: material.record_bytes(),
            records: &material.records,
            carry: Value::Null,
            reading_order: &|_| ReadingOrder(0),
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    let events: Vec<_> = outputs
        .into_iter()
        .flat_map(|output| output.outcomes)
        .flat_map(|outcome| match outcome {
            RecordOutcome::Events { events, .. } => events,
            RecordOutcome::Failure { reason, .. } => panic!("{reason}"),
        })
        .collect();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["path"], events[1].payload["path"]);
    assert_ne!(events[0].equivalence_key, events[1].equivalence_key);
    for event in &events {
        let declared = <NoteDocument as sinex_model::EventPayload>::declaration()
            .evaluate(&event.payload, event.class)
            .unwrap();
        assert_eq!(declared.equivalence_key, event.equivalence_key);
    }
    assert_ne!(events[0].record_hash, events[1].record_hash);
    let raw_member = material
        .manifest
        .members
        .iter()
        .find(|member| member.details.contains_key("native_path_bytes"))
        .unwrap();
    let location = sinex_model::RecordLocation {
        material: material.id,
        anchor: raw_member.stream_offset,
    };
    let mut restarted = DirState::default();
    restarted
        .remember_saved(material.memory.as_deref().unwrap())
        .unwrap();
    restarted.finish_remembering();
    std::fs::remove_file(directory.path().join(raw_name)).unwrap();
    let removed = driver
        .poll(&cursor, &mut restarted)
        .unwrap()
        .material
        .unwrap();
    assert_eq!(removed.removed, [location]);
    assert_eq!(removed.records.len(), 1);
    let admitted = (removed.cursor_at)(removed.records.last().unwrap().1).unwrap();
    assert!(
        driver
            .poll(&admitted, &mut restarted)
            .unwrap()
            .material
            .is_none()
    );
}

#[devkit::test]
fn native_vault_roots_stay_distinct_from_equal_display_names() {
    use sinex_model::EventPayload;
    use std::os::unix::ffi::OsStringExt;
    let directory = tempfile::tempdir().unwrap();
    let roots = [
        directory.path().join("vault%FF"),
        directory
            .path()
            .join(std::ffi::OsString::from_vec(b"vault\xff".to_vec())),
    ];
    let mut notes = Vec::new();
    for root in roots {
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("note.md"), "---\nid: fixed\n---\nsame text").unwrap();
        let material = driver(root, "host".into())
            .poll(&Value::Null, &mut DirState::default())
            .unwrap()
            .material
            .unwrap();
        let member = &material.manifest.members[0];
        let bytes = material
            .record_bytes()
            .record(material.records[0].0, material.records[0].1)
            .unwrap();
        notes.push(
            NotesParser
                .parse(
                    &bytes.as_ref(),
                    &(),
                    &RecordContext {
                        manifest: &material.manifest,
                        member: Some(member),
                    },
                )
                .unwrap()
                .0
                .remove(0),
        );
    }
    assert_eq!(notes[0].vault_path, notes[1].vault_path);
    let keys: Vec<_> = notes
        .iter()
        .map(|note| {
            let sinex_model::KeyKind::Fields(fields) = note.key_kind() else {
                panic!("note key missing")
            };
            sinex_model::EquivalenceKey::of(
                "note.document",
                fields,
                &serde_json::to_value(note).unwrap(),
            )
        })
        .collect();
    assert_ne!(keys[0], keys[1]);
}

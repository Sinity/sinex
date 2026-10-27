use super::*;
use sinex_model::{EventPayload, KeyKind, Manifest, RecordOutcome};
fn read(format: Format, text: &[u8]) -> Result<Vec<TranscriptCue>, RecordError> {
    TranscriptParser {
        format,
        input: "example-input".into(),
    }
    .parse(
        &text,
        &Carry::default(),
        &RecordContext {
            manifest: &Manifest::default(),
            member: None,
        },
    )
    .map(|p| p.0)
}
#[devkit::test]
fn whisper_keeps_written_decimals_and_supplied_defaults() {
    let rows = read(Format::Whisper, br#"{"segments":[{"text":"same","start":0.0005000,"end":1.23456789,"language":"de","confidence":0,"custom":42},{"text":"same","start":1.23456789,"end":2e1}],"language":"pl","model":"example","recording_started_at":"2026-01-01T00:00:00Z"}"#).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].start_seconds.as_deref(), Some("0.0005000"));
    assert_eq!(rows[0].end_seconds.as_deref(), Some("1.23456789"));
    assert_eq!(rows[1].end_seconds.as_deref(), Some("2e1"));
    assert_eq!((rows[0].start_ms, rows[0].end_ms), (Some(1), Some(1235)));
    assert_eq!(rows[0].language.as_deref(), Some("de"));
    assert_eq!(rows[1].language.as_deref(), Some("pl"));
    assert_eq!(rows[0].confidence, Some(0.0));
    assert_eq!(rows[1].confidence, None);
    assert_eq!(rows[0].extra["custom"], 42);
}
#[devkit::test]
fn large_exact_offsets_survive_without_a_millisecond_estimate() {
    let rows = read(
        Format::Whisper,
        br#"{"segments":[{"text":"x","start":1e99999}]}"#,
    )
    .unwrap();
    assert_eq!(rows[0].start_seconds.as_deref(), Some("1e99999"));
    assert_eq!(rows[0].start_ms, None);
}
#[devkit::test]
fn decimal_millisecond_estimates_are_checked() {
    assert_eq!(decimal_ms("18446744073709551.615", 3).unwrap(), u64::MAX);
    assert_eq!(decimal_ms("1e3", 3).unwrap(), 1_000_000);
    assert_eq!(decimal_ms("1.2300", 3).unwrap(), 1230);
    assert_eq!(decimal_ms("0.30000000000000004", 3).unwrap(), 300);
    assert!(decimal_ms("1.5", 0).is_err());
}
#[devkit::test]
fn malformed_json_is_a_whole_record_failure() {
    for text in [
        br#"[{"text":5}]"#.as_slice(),
        br#"[{"start_ms":0}]"#,
        br#"[{"text":"x","start_ms":4,"end_ms":3}]"#,
        br#"[{"text":"x","start_ms":-1}]"#,
        &[0xff],
    ] {
        assert!(read(Format::Json, text).is_err());
    }
    assert!(
        read(
            Format::Whisper,
            br#"{"segments":[{"text":"x","start":-1}]}"#
        )
        .is_err()
    );
}
#[devkit::test]
fn cues_keep_multiline_text_and_relative_time() {
    for (format, text) in [
        (
            Format::WebVtt,
            "id\n00:01.125 --> 00:02.500 align:start\nŻółw\nline two",
        ),
        (
            Format::Srt,
            "1\n00:00:01,125 --> 00:00:02,500\nŻółw\nline two",
        ),
    ] {
        let rows = read(format, text.as_bytes()).unwrap();
        assert_eq!(rows[0].text, "Żółw\nline two");
        assert_eq!((rows[0].start_ms, rows[0].end_ms), (Some(1125), Some(2500)));
        assert!(rows[0].at.is_none());
    }
    for text in [
        "WEBVTT",
        "NOTE\tcomment\nmore",
        "STYLE\n::cue {}",
        "REGION\nid:example",
    ] {
        assert!(read(Format::WebVtt, text.as_bytes()).unwrap().is_empty());
    }
    assert!(read(Format::WebVtt, b"00:61.000 --> 01:02.000\nx").is_err());
}
#[devkit::test]
fn stable_provider_ids_and_unidentified_outputs_have_their_declared_keys() {
    let rows = read(
        Format::Json,
        br#"[{"id":"stable","text":"same"},{"text":"same"}]"#,
    )
    .unwrap();
    assert!(matches!(rows[0].key_kind(), KeyKind::Fields(_)));
    assert!(matches!(rows[1].key_kind(), KeyKind::Location));
}
#[derive(Default)]
struct Sink(std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>);
impl sinex_source_core::MaterialSink for Sink {
    fn put_file(&self, path: &Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
        self.0.lock().unwrap().insert(name.clone(), bytes.into());
        Ok(name)
    }
}
#[devkit::test]
fn every_import_format_reads_retained_bytes_with_its_registered_parser() {
    use sinex_source_core::MaterialSink;
    let formats = [
        (
            &JSON_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/transcript.json").as_slice(),
            1,
        ),
        (
            &WHISPER_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/whisper.json").as_slice(),
            2,
        ),
        (
            &TEXT_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/transcript.txt").as_slice(),
            2,
        ),
        (
            &WEBVTT_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/captions.vtt").as_slice(),
            2,
        ),
        (
            &SRT_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/captions.srt").as_slice(),
            2,
        ),
        (
            &super::super::ocr_import::JSON_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/ocr.json").as_slice(),
            1,
        ),
        (
            &super::super::ocr_import::TSV_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/ocr.tsv").as_slice(),
            1,
        ),
        (
            &super::super::ocr_import::TEXT_FORMAT,
            include_bytes!("../../../tests/sources/imported-text/ocr.txt").as_slice(),
            2,
        ),
    ];
    for (format, bytes, count) in formats {
        let binding = (format.plan.unwrap())(
            Path::new("/example/missing"),
            &Settings::new("example-host"),
        )
        .unwrap()
        .remove(0);
        let source = (format.source)(&binding).unwrap();
        let sinex_source_core::Reading::Shared(reader) =
            (format.reader.unwrap())(&binding).unwrap()
        else {
            panic!()
        };
        let sink = Sink::default();
        let blob = sink.put(bytes).unwrap();
        let mut carry = Value::Null;
        let mut events = Vec::new();
        sinex_source_core::shared_pass::read(
            sinex_source_core::shared_pass::Input::Sequential(Box::new(std::io::Cursor::new(
                bytes.to_vec(),
            ))),
            blob,
            bytes.len() as u64,
            Default::default(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            vec![reader.route(Value::Null, Class::Guarded)],
            &sink,
            &mut |_, acquired| {
                if let Some(m) = acquired?.material {
                    for batch in source
                        .drain(sinex_source_core::DrainInput {
                            source: source.spec().name,
                            class: Class::Guarded,
                            material: m.id,
                            bytes: m.record_bytes(),
                            records: &m.records,
                            carry: carry.clone(),
                            reading_order: &|_| sinex_model::ReadingOrder(0),
                            limits: Default::default(),
                            manifest: &m.manifest,
                        })
                        .unwrap()
                    {
                        carry = batch.carry;
                        for outcome in batch.outcomes {
                            match outcome {
                                RecordOutcome::Events { events: rows, .. } => events.extend(rows),
                                RecordOutcome::Failure { reason, .. } => {
                                    panic!("{}: {reason}", format.name)
                                }
                            }
                        }
                    }
                }
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(events.len(), count, "{}", format.name);
        assert!(events.iter().all(|e| e.payload["text"] == "Żółw"));
        if format.name == "whisper-json" {
            assert!(events.iter().all(|e| e.payload["model"] == "example-model"));
            let (first, index) = events[0].location().unwrap();
            let (second, next) = events[1].location().unwrap();
            assert_eq!(first, second);
            assert_eq!((index, next), (0, 1));
        }
    }
}
#[devkit::test]
fn whole_json_supports_sealed_routes_and_completed_cursor_restart() {
    use sinex_source_core::{MaterialSink, shared_pass::SharedPass};
    let bytes = include_bytes!("../../../tests/sources/imported-text/whisper.json");
    let sink = Sink::default();
    let blob = sink.put(bytes).unwrap();
    let driver = ExportArchive {
        path: "/example/whisper.json".into(),
        host: "example".into(),
        select: |_| true,
        framing: Framing::Whole,
        part_bytes: 1,
    };
    let mut cursor = Value::Null;
    let mut deliveries = 0;
    for _ in 0..2 {
        sinex_source_core::shared_pass::read(
            sinex_source_core::shared_pass::Input::Sequential(Box::new(std::io::Cursor::new(
                bytes.to_vec(),
            ))),
            blob.clone(),
            bytes.len() as u64,
            Default::default(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            vec![driver.route(cursor.clone(), Class::Sealed)],
            &sink,
            &mut |_, acquired| {
                if let Some(material) = acquired?.material
                    && !material.records.is_empty()
                {
                    deliveries += 1;
                    cursor = (material.cursor_at)(material.records.last().unwrap().1)?;
                    assert_eq!(
                        read(
                            Format::Whisper,
                            &material
                                .record_bytes()
                                .record(material.records[0].0, material.records[0].1)
                                .unwrap()
                        )
                        .unwrap()
                        .len(),
                        2
                    );
                }
                Ok(true)
            },
        )
        .unwrap();
    }
    assert_eq!(deliveries, 1);
}
#[devkit::test]
fn directory_imports_discover_one_path_identity_for_each_file() {
    use sinex_source_core::discovery::DiscoveryContext;
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("nested")).unwrap();
    std::fs::write(temp.path().join("a.json"), b"[]").unwrap();
    std::fs::write(
        temp.path().join("nested/b.json"),
        br#"{"segments":[],"model":"example"}"#,
    )
    .unwrap();
    std::fs::write(temp.path().join("c.txt"), b"text").unwrap();
    let mut context = DiscoveryContext {
        host: "example".into(),
        ..Default::default()
    };
    context
        .inputs
        .insert("transcripts".into(), vec![temp.path().into()]);
    let found = crate::discovery::discover(&context).unwrap();
    let bindings: Vec<_> = found
        .iter()
        .filter(|input| input.source == JSON.name)
        .flat_map(|input| &input.bindings)
        .collect();
    assert_eq!(bindings.len(), 3);
    for parser in [JSON.parser, WHISPER.parser, TEXT.parser] {
        assert_eq!(
            bindings
                .iter()
                .filter(|binding| binding.parser == parser)
                .count(),
            1
        );
    }
    assert!(bindings.iter().all(|binding| binding.input.is_absolute()));
    let names: std::collections::BTreeSet<_> =
        bindings.iter().map(|binding| &binding.name).collect();
    assert_eq!(names.len(), 3);
}

#[devkit::test]
fn named_import_roots_are_validated_and_registered_for_discovery() {
    use sinex_source_core::discovery::DiscoveryContext;
    let temp = tempfile::tempdir().unwrap();
    std::fs::write(
        temp.path().join("example.json"),
        br#"{"segments":[{"text":"same"}]}"#,
    )
    .unwrap();
    let mut context = DiscoveryContext {
        host: "example".into(),
        ..Default::default()
    };
    context
        .inputs
        .insert("transcripts".into(), vec![temp.path().into()]);
    context
        .inputs
        .insert("ocr".into(), vec![temp.path().into()]);
    let found = crate::discovery::discover(&context).unwrap();
    assert_eq!(
        found
            .iter()
            .filter(|i| i.source == WHISPER.name)
            .flat_map(|i| &i.bindings)
            .count(),
        1
    );
    assert_eq!(
        found
            .iter()
            .filter(|i| i.source == super::super::ocr_import::JSON.name)
            .flat_map(|i| &i.bindings)
            .count(),
        1
    );
}

#[devkit::test]
fn archived_provider_ids_are_scoped_to_the_member_path() {
    let parser = TranscriptParser {
        format: Format::Json,
        input: "example.zip".into(),
    };
    let manifest = Manifest::from_json(serde_json::json!({"source_container":"zip"}));
    let read = |path: &str| {
        parser
            .parse(
                &br#"[{"id":"same","text":"same"}]"#.as_slice(),
                &Carry::default(),
                &RecordContext {
                    manifest: &manifest,
                    member: Some(&sinex_model::Member {
                        path: path.into(),
                        ..Default::default()
                    }),
                },
            )
            .unwrap()
            .0
            .remove(0)
    };
    assert_ne!(read("a.json").input, read("b.json").input);
}
#[devkit::test]
fn recording_time_uses_the_written_offset_before_millisecond_rounding() {
    let rows=read(Format::Whisper,br#"{"segments":[{"text":"x","start":0.0005}],"recording_started_at":"2026-01-01T00:00:00Z"}"#).unwrap();
    assert_eq!(
        rows[0].at.unwrap().unix_ns(),
        Timestamp::from_unix_secs(1_767_225_600).unwrap().unix_ns() + 500_000
    );
}

#[devkit::test]
fn reversed_submillisecond_offsets_fail_before_rounding() {
    assert!(
        read(
            Format::Whisper,
            br#"{"segments":[{"text":"x","start":1.0004,"end":1.0003}]}"#
        )
        .is_err()
    );
}

#[devkit::test]
fn offsets_finer_than_a_nanosecond_keep_their_text_without_inventing_a_timestamp() {
    let rows=read(Format::Whisper,br#"{"segments":[{"text":"x","start":0.0000000005}],"recording_started_at":"2026-01-01T00:00:00Z"}"#).unwrap();
    assert_eq!(rows[0].start_seconds.as_deref(), Some("0.0000000005"));
    assert!(rows[0].at.is_none());
}

#[devkit::test]
fn a_header_named_cue_keeps_its_spoken_text() {
    let rows = read(Format::WebVtt, b"WEBVTT\n00:00.000 --> 00:01.000\nspoken").unwrap();
    assert_eq!(rows[0].text, "spoken");
    assert_eq!(rows[0].extra["cue_id"], "WEBVTT");
}

#[devkit::test]
fn an_automatically_discovered_file_has_one_transcript_format() {
    use sinex_source_core::discovery::DiscoveryContext;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("example.json");
    std::fs::write(&path, br#"{"segments":[{"text":"same"}]}"#).unwrap();
    let mut context = DiscoveryContext {
        host: "example".into(),
        ..Default::default()
    };
    context
        .inputs
        .insert("transcripts".into(), vec![path.clone()]);
    let found = crate::discovery::discover(&context).unwrap();
    let bindings: Vec<_> = found
        .iter()
        .filter(|input| input.source == WHISPER.name)
        .flat_map(|input| &input.bindings)
        .collect();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].parser, WHISPER.parser);
    context.inputs.clear();
    context.inputs.insert("transcript-text".into(), vec![path]);
    let found = crate::discovery::discover(&context).unwrap();
    let bindings: Vec<_> = found
        .iter()
        .filter(|input| input.source == TEXT.name)
        .flat_map(|input| &input.bindings)
        .collect();
    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].parser, TEXT.parser);
}

#[devkit::test]
fn imported_text_options_are_typed_and_do_not_enable_models() {
    let input = tempfile::NamedTempFile::new().unwrap();
    let mut settings = sinex_source_core::Settings::new("host");
    for format in [&JSON_FORMAT, &super::super::ocr_import::JSON_FORMAT] {
        settings.options = serde_json::json!({});
        assert!((format.plan.unwrap())(input.path(), &settings).is_ok());
        for options in [serde_json::json!("{}"), serde_json::json!({"run_model": true}), serde_json::json!({"input": "recording"})] {
            settings.options = options;
            assert!((format.plan.unwrap())(input.path(), &settings).is_err());
        }
    }
}

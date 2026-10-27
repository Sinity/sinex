use super::*;
use sinex_model::{EventPayload, KeyKind, Manifest, Member};
fn read(
    format: Format,
    text: &[u8],
    defaults: Value,
) -> Result<Vec<TranscriptSegment>, RecordError> {
    let manifest =
        Manifest::from_json(serde_json::json!({"path":"example.json", "file_metadata": defaults}));
    TranscriptParser {
        format,
        input: "example-input".into(),
    }
    .parse(
        &text,
        &Carry::default(),
        &RecordContext {
            manifest: &manifest,
            member: None,
        },
    )
    .map(|p| p.0)
}
#[devkit::test]
fn json_supplied_times_fields_and_file_defaults() {
    let rows = read(Format::Whisper, br#"{"id":3,"text":"example","start":1.125,"end":2.5,"language":"pl","confidence":0,"custom":42}"#, serde_json::json!({"model":"example-model","external_run":"external-example","language":"en","recording_started_at":"2026-01-01T00:00:00Z"})).unwrap();
    let p = &rows[0];
    assert_eq!((p.start_ms, p.end_ms), (Some(1125), Some(2500)));
    assert_eq!(p.language.as_deref(), Some("pl"));
    assert_eq!(p.model.as_deref(), Some("example-model"));
    assert_eq!(p.confidence, Some(0.0));
    assert_eq!(p.extra["custom"], 42);
    assert_eq!(
        p.at.unwrap().unix_ns(),
        Timestamp(
            time::OffsetDateTime::parse(
                "2026-01-01T00:00:01.125Z",
                &time::format_description::well_known::Rfc3339
            )
            .unwrap()
        )
        .unix_ns()
    );
    assert!(matches!(p.key_kind(), KeyKind::Fields(_)));
}
#[devkit::test]
fn decimal_offsets_are_exact_and_checked() {
    assert_eq!(decimal_ms("18446744073709551.615", 3).unwrap(), u64::MAX);
    assert_eq!(decimal_ms("1e3", 3).unwrap(), 1_000_000);
    assert_eq!(decimal_ms("1.2300", 3).unwrap(), 1230);
    for s in ["-1", "0.0001", "18446744073709551.616", "1e99999", "\"1\""] {
        assert!(decimal_ms(s, 3).is_err(), "{s}");
    }
    let rows = read(
        Format::Whisper,
        br#"{"text":"x","start":18446744073709551.615}"#,
        Value::Null,
    )
    .unwrap();
    assert_eq!(rows[0].start_ms, Some(u64::MAX));
}
#[devkit::test]
fn malformed_input_is_a_record_failure() {
    for text in [
        b"[]".as_slice(),
        br#"{"text":5}"#,
        br#"{"start_ms":0}"#,
        br#"{"text":"x","start_ms":4,"end_ms":3}"#,
        br#"{"text":"x","start_ms":-1}"#,
        &[0xff],
    ] {
        assert!(read(Format::Json, text, Value::Null).is_err());
    }
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
        let rows = read(format, text.as_bytes(), Value::Null).unwrap();
        assert_eq!(rows[0].text, "Żółw\nline two");
        assert_eq!((rows[0].start_ms, rows[0].end_ms), (Some(1125), Some(2500)));
        assert!(rows[0].at.is_none());
        assert!(matches!(rows[0].key_kind(), KeyKind::Location));
    }
    for text in [
        "WEBVTT",
        "NOTE comment\nmore",
        "STYLE\n::cue {}",
        "REGION\nid:example",
    ] {
        assert!(
            read(Format::WebVtt, text.as_bytes(), Value::Null)
                .unwrap()
                .is_empty()
        );
    }
    for text in ["00:01.000 --> 00:00.000\nx", "00:61.000 --> 01:02.000\nx"] {
        assert!(read(Format::WebVtt, text.as_bytes(), Value::Null).is_err());
    }
}
#[devkit::test]
fn equal_plain_lines_remain_distinct_across_carry_restart_and_member_change() {
    let parser = TranscriptParser {
        format: Format::Text,
        input: "example".into(),
    };
    let manifest = Manifest::from_json(serde_json::json!({"path":"example.tar"}));
    let a = Member {
        path: "a.txt".into(),
        ..Default::default()
    };
    let b = Member {
        path: "b.txt".into(),
        ..Default::default()
    };
    let context = RecordContext {
        manifest: &manifest,
        member: Some(&a),
    };
    let (first, carry) = parser
        .parse(&b"same\r\n".as_slice(), &Carry::default(), &context)
        .unwrap();
    let carry: Carry = serde_json::from_slice(&serde_json::to_vec(&carry).unwrap()).unwrap();
    let (second, carry) = parser
        .parse(&b"same\n".as_slice(), &carry, &context)
        .unwrap();
    assert_eq!((first[0].segment_index, second[0].segment_index), (0, 1));
    let (third, _) = parser
        .parse(
            &b"same\n".as_slice(),
            &carry,
            &RecordContext {
                manifest: &manifest,
                member: Some(&b),
            },
        )
        .unwrap();
    assert_eq!(third[0].segment_index, 0);
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
fn all_explicit_formats_read_stored_parts_with_exact_record_citations() {
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
        let mut settings = Settings::new("example-host");
        settings.options = serde_json::json!({"input":"example-input"});
        let binding = (format.plan.unwrap())(Path::new("/example/missing"), &settings)
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
            sinex_model::ObservationFacts::default(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            vec![reader.route(Value::Null, Class::Guarded)],
            &sink,
            &mut |_, acquired| {
                let acquired = acquired?;
                if let Some(m) = &acquired.material
                    && !m.records.is_empty()
                {
                    assert!(m.records.iter().all(|(start, end)| *start < *end));
                    for batch in source
                        .drain(sinex_source_core::DrainInput {
                            source: source.spec().name,
                            class: Class::Guarded,
                            material: m.id,
                            bytes: m.record_bytes(),
                            records: &m.records,
                            carry: carry.clone(),
                            reading_order: &|_| sinex_model::ReadingOrder(0),
                            limits: sinex_source_core::Limits::default(),
                            manifest: &m.manifest,
                        })
                        .unwrap()
                    {
                        carry = batch.carry;
                        for outcome in batch.outcomes {
                            match outcome {
                                sinex_model::RecordOutcome::Events {
                                    events: records, ..
                                } => events.extend(records),
                                sinex_model::RecordOutcome::Failure { reason, .. } => {
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
            assert!(events.iter().all(|e| e.payload["model"] == "example-model" && e.payload["language"] == "pl"));
        }
    }
}

#[devkit::test]
fn json_file_metadata_and_progress_survive_part_restart() {
    use sinex_source_core::{MaterialSink, shared_pass::SharedPass};
    let bytes = include_bytes!("../../../tests/sources/imported-text/whisper.json").to_vec();
    let driver = ExportArchive {
        path: "/example/missing.json".into(),
        host: "example-host".into(),
        select: |_| true,
        framing: JSON_FRAMING,
        part_bytes: 1,
    };
    let sink = Sink::default();
    let blob = sink.put(&bytes).unwrap();
    let mut cursor = Value::Null;
    let mut carry = Value::Null;
    let mut events = Vec::new();
    for pass in 0..2 {
        let mut next_cursor = cursor.clone();
        sinex_source_core::shared_pass::read(
            sinex_source_core::shared_pass::Input::Sequential(Box::new(std::io::Cursor::new(
                bytes.clone(),
            ))),
            blob.clone(),
            bytes.len() as u64,
            sinex_model::ObservationFacts::default(),
            std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            vec![driver.route(cursor.clone(), Class::Guarded)],
            &sink,
            &mut |_, acquired| {
                let acquired = acquired?;
                if let Some(m) = &acquired.material
                    && !m.records.is_empty()
                {
                    let parser = TranscriptParser {
                        format: Format::Whisper,
                        input: "example-input".into(),
                    };
                    for batch in sinex_source_core::drain(
                        &parser,
                        sinex_source_core::DrainInput {
                            source: WHISPER.name,
                            class: Class::Guarded,
                            material: m.id,
                            bytes: m.record_bytes(),
                            records: &m.records,
                            carry: carry.clone(),
                            reading_order: &|_| sinex_model::ReadingOrder(0),
                            limits: sinex_source_core::Limits::default(),
                            manifest: &m.manifest,
                        },
                    )
                    .unwrap()
                    {
                        carry = batch.carry;
                        for outcome in batch.outcomes {
                            match outcome {
                                sinex_model::RecordOutcome::Events { events: rows, .. } => {
                                    events.extend(rows)
                                }
                                sinex_model::RecordOutcome::Failure { reason, .. } => {
                                    panic!("{reason}")
                                }
                            }
                        }
                    }
                    next_cursor = (m.cursor_at)(m.records.last().unwrap().1)?;
                    return Ok(pass != 0);
                }
                Ok(true)
            },
        )
        .unwrap();
        cursor = next_cursor;
        carry = serde_json::from_slice(&serde_json::to_vec(&carry).unwrap()).unwrap();
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload["segment_index"], 0);
    assert_eq!(events[1].payload["segment_index"], 1);
    assert!(events.iter().all(|e| e.payload["language"] == "pl"
        && e.payload["model"] == "example-model"
        && e.payload["external_run"] == "example-run"));
    assert_ne!(events[0].equivalence_key, events[1].equivalence_key);
}

#[devkit::test]
fn webvtt_headers_allow_tabs_and_later_header_named_cues_remain_text() {
    assert!(
        read(Format::WebVtt, b"NOTE\tcomment\nmore", Value::Null)
            .unwrap()
            .is_empty()
    );
    assert!(
        read(Format::WebVtt, b"WEBVTT\tcomment", Value::Null)
            .unwrap()
            .is_empty()
    );
    let parser = TranscriptParser {
        format: Format::WebVtt,
        input: "example".into(),
    };
    let manifest = Manifest::from_json(serde_json::json!({"path":"example.vtt"}));
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let (_, carry) = parser
        .parse(&b"WEBVTT".as_slice(), &Carry::default(), &context)
        .unwrap();
    let (rows, _) = parser
        .parse(
            &b"WEBVTT\n00:00.000 --> 00:01.000\nspoken".as_slice(),
            &carry,
            &context,
        )
        .unwrap();
    assert_eq!(rows[0].text, "spoken");
    assert_eq!(rows[0].extra["cue_id"], "WEBVTT");
    assert!(
        read(
            Format::WebVtt,
            b"60:00.000 --> 61:00.000\nspoken",
            Value::Null
        )
        .is_err()
    );
}

#[devkit::test]
fn changed_file_defaults_change_segment_readings_with_the_same_native_identity() {
    let parser = TranscriptParser {
        format: Format::Whisper,
        input: "example".into(),
    };
    let raw = br#"{"id":0,"text":"same","start":0,"end":1}"#;
    let read = |language| {
        let manifest = Manifest::from_json(
            serde_json::json!({"path":"example.json","file_metadata":{"language":language,"model":"example-model","external_run":"example-run"}}),
        );
        sinex_source_core::drain(
            &parser,
            sinex_source_core::DrainInput {
                source: WHISPER.name,
                class: Class::Guarded,
                material: sinex_model::MaterialId::new(),
                bytes: raw.as_slice().into(),
                records: &[(0, raw.len())],
                carry: Value::Null,
                reading_order: &|_| sinex_model::ReadingOrder(0),
                limits: sinex_source_core::Limits::default(),
                manifest: &manifest,
            },
        )
        .unwrap()
        .into_iter()
        .flat_map(|b| b.outcomes)
        .find_map(|o| match o {
            sinex_model::RecordOutcome::Events { mut events, .. } => events.pop(),
            _ => None,
        })
        .unwrap()
    };
    let first = read("pl");
    let corrected = read("en");
    assert_eq!(first.equivalence_key, corrected.equivalence_key);
    assert_ne!(first.payload["language"], corrected.payload["language"]);
    assert_ne!(first.record_hash, corrected.record_hash);
}

#[devkit::test]
fn imported_json_refuses_sealed_member_staging_before_any_material_delivery() {
    use sinex_source_core::{MaterialSink, shared_pass::SharedPass};
    let bytes = br#"{"segments":[{"text":"synthetic"}],"model":"example"}"#;
    let sink = Sink::default();
    let blob = sink.put(bytes).unwrap();
    let driver = ExportArchive {
        path: "/example/missing.json".into(),
        host: "example".into(),
        select: |_| true,
        framing: JSON_FRAMING,
        part_bytes: 1,
    };
    let mut deliveries = 0;
    let result = sinex_source_core::shared_pass::read(
        sinex_source_core::shared_pass::Input::Sequential(Box::new(std::io::Cursor::new(
            bytes.to_vec(),
        ))),
        blob,
        bytes.len() as u64,
        sinex_model::ObservationFacts::default(),
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        vec![driver.route(Value::Null, Class::Sealed)],
        &sink,
        &mut |_, _| {
            deliveries += 1;
            Ok(true)
        },
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("sealed JSON metadata")
    );
    assert_eq!(deliveries, 0);
    assert_eq!(sink.0.lock().unwrap().len(), 1);
}

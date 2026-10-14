use std::collections::BTreeMap;

use serde_json::json;

use super::*;

fn synthetic_revision() -> (PolylogueRevisionManifest, BTreeMap<i32, Vec<u8>>) {
    let head_value = json!({
        "kind": "session",
        "message_count": 1,
        "native_id": "demo",
        "origin": "claude-code-session",
        "record_id": "claude-code-session:demo",
        "seq": 0,
        "session_id": "claude-code-session:demo"
    });
    let message_value = json!({
        "block_count": 1,
        "kind": "message",
        "message_id": "claude-code-session:demo:n:msg-1",
        "position": 0,
        "variant_index": 0,
        "record_id": "claude-code-session:demo:n:msg-1",
        "seq": 0,
        "session_id": "claude-code-session:demo"
    });
    let block_value = json!({
        "block_id": "claude-code-session:demo:n:msg-1:0",
        "block_type": "tool_result",
        "kind": "block",
        "message_id": "claude-code-session:demo:n:msg-1",
        "record_id": "claude-code-session:demo:n:msg-1:0",
        "seq": 1,
        "session_id": "claude-code-session:demo"
    });
    let mut head = canonical_json_bytes(&head_value).expect("head");
    head.push(b'\n');
    let transcript = [
        canonical_json_bytes(&message_value).expect("message"),
        canonical_json_bytes(&block_value).expect("block"),
    ]
    .into_iter()
    .flat_map(|line| line.into_iter().chain([b'\n']))
    .collect::<Vec<_>>();
    let head_descriptor = PolylogueSegmentDescriptor {
        index: HEAD_SEGMENT_INDEX,
        filename: "head.ndjson".into(),
        sha256: sha256_hex(&head),
        size_bytes: head.len(),
        record_count: 1,
        first_seq: 0,
        last_seq: 0,
    };
    let transcript_descriptor = PolylogueSegmentDescriptor {
        index: 0,
        filename: "seg-00000.ndjson".into(),
        sha256: sha256_hex(&transcript),
        size_bytes: transcript.len(),
        record_count: 2,
        first_seq: 0,
        last_seq: 1,
    };
    let mut segments = BTreeMap::from([(HEAD_SEGMENT_INDEX, head), (0, transcript)]);
    let mut anchors = BTreeMap::new();
    anchors.insert(
        "claude-code-session:demo".into(),
        PolylogueAnchor {
            segment_index: -1,
            line_index: 0,
            seq: 0,
            kind: "session".into(),
            sha256: sha256_hex(&canonical_json_bytes(&head_value).expect("head")),
        },
    );
    anchors.insert(
        "claude-code-session:demo:n:msg-1".into(),
        PolylogueAnchor {
            segment_index: 0,
            line_index: 0,
            seq: 0,
            kind: "message".into(),
            sha256: sha256_hex(&canonical_json_bytes(&message_value).expect("message")),
        },
    );
    anchors.insert(
        "claude-code-session:demo:n:msg-1:0".into(),
        PolylogueAnchor {
            segment_index: 0,
            line_index: 1,
            seq: 1,
            kind: "block".into(),
            sha256: sha256_hex(&canonical_json_bytes(&block_value).expect("block")),
        },
    );
    let mut counts = BTreeMap::new();
    counts.insert("session".into(), 1);
    counts.insert("message".into(), 1);
    counts.insert("block".into(), 1);
    let joined = segments.remove(&HEAD_SEGMENT_INDEX).expect("head");
    let mut content = joined.clone();
    content.extend_from_slice(segments.get(&0).expect("transcript"));
    segments.insert(HEAD_SEGMENT_INDEX, joined);
    (
        PolylogueRevisionManifest {
            protocol_version: PROTOCOL_VERSION.into(),
            semantics_version: SEMANTICS_VERSION,
            origin_vocabulary_version: ORIGIN_VOCABULARY_VERSION,
            origin_vocabulary_digest: ORIGIN_VOCABULARY_DIGEST.into(),
            session_id: "claude-code-session:demo".into(),
            origin: "claude-code-session".into(),
            native_id: "demo".into(),
            revision_id: sha256_hex(&content),
            superseded_revision_id: None,
            content_digest: PolylogueContentDigest {
                polylogue_sha256: sha256_hex(&content),
                canonicalizer_version: CANONICALIZER_VERSION,
                size_bytes: content.len(),
                media_type: "application/x-ndjson; charset=utf-8".into(),
                sinex_cas_digest: None,
                provider_digest: None,
            },
            head_segment: head_descriptor,
            segments: vec![transcript_descriptor],
            expected_record_counts: counts,
            anchors,
            sequence_rule: "two seq spaces".into(),
            completeness: "complete".into(),
            fidelity_gaps: Vec::new(),
            revision_created_at: None,
        },
        segments,
    )
}

#[test]
fn verifies_and_resolves_anchored_revision() {
    let (manifest, segments) = synthetic_revision();
    manifest
        .verify(&segments)
        .expect("synthetic protocol revision verifies");
    let resolved = manifest
        .resolve_anchor(&segments, "claude-code-session:demo:n:msg-1:0")
        .expect("block anchor resolves");
    assert_eq!(resolved.kind, PolylogueRecordKind::Block);
    assert_eq!(resolved.value["block_type"], "tool_result");
    assert_eq!(resolved.original_bytes, resolved.canonical_bytes);
    assert_eq!(
        resolved.original_sha256,
        sha256_hex(&resolved.original_bytes)
    );
    assert_eq!(
        manifest
            .anchor_byte(&segments, "claude-code-session:demo:n:msg-1:0")
            .unwrap(),
        (segments[&0]
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap()
            .len()
            + 1) as i64
    );
}

#[test]
fn checked_in_polylogue_producer_fixture_matches_every_anchor() {
    let manifest = PolylogueRevisionManifest::from_json(include_bytes!(
        "../../../tests/fixtures/polylogue/v1/small-session/manifest.json"
    ))
    .expect("real Polylogue producer manifest");
    assert_eq!(
        manifest.origin_vocabulary_version,
        ORIGIN_VOCABULARY_VERSION
    );
    assert_eq!(manifest.origin_vocabulary_digest, ORIGIN_VOCABULARY_DIGEST);
    let segments = BTreeMap::from([
        (
            -1,
            include_bytes!(
                "../../../tests/fixtures/polylogue/v1/small-session/segments/head.ndjson"
            )
            .to_vec(),
        ),
        (
            0,
            include_bytes!(
                "../../../tests/fixtures/polylogue/v1/small-session/segments/seg-00000.ndjson"
            )
            .to_vec(),
        ),
        (
            1,
            include_bytes!(
                "../../../tests/fixtures/polylogue/v1/small-session/segments/seg-00001.ndjson"
            )
            .to_vec(),
        ),
        (
            2,
            include_bytes!(
                "../../../tests/fixtures/polylogue/v1/small-session/segments/seg-00002.ndjson"
            )
            .to_vec(),
        ),
    ]);
    manifest
        .verify(&segments)
        .expect("Rust consumer verifies Python producer bytes");
    let fixture_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/polylogue/v1/small-session/segments");
    let paths = std::iter::once(&manifest.head_segment)
        .chain(&manifest.segments)
        .map(|descriptor| (descriptor.index, fixture_root.join(&descriptor.filename)))
        .collect();
    let offsets = manifest
        .verify_files(&paths)
        .expect("producer files stream verify");
    for record_id in manifest.anchors.keys() {
        assert_eq!(
            offsets[record_id],
            manifest.anchor_byte(&segments, record_id).unwrap()
        );
    }
    assert_eq!(manifest.semantics_version, SEMANTICS_VERSION);
    assert_eq!(
        manifest.expected_record_counts.len(),
        POLYLOGUE_VOCABULARY.len()
    );
    for (record_id, anchor) in &manifest.anchors {
        let resolved = manifest
            .resolve_anchor(&segments, record_id)
            .expect("anchor resolves");
        assert_eq!(resolved.kind.as_str(), anchor.kind);
        assert_eq!(sha256_hex(&resolved.canonical_bytes), anchor.sha256);
        assert_eq!(
            sha256_hex(&resolved.original_bytes),
            resolved.original_sha256
        );
        assert_eq!(
            &segments[&anchor.segment_index]
                [manifest.anchor_byte(&segments, record_id).unwrap() as usize..]
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap()[..],
            &resolved.original_bytes
        );
    }
}

#[test]
fn rejects_duplicate_keys_and_unsupported_values_before_value_loss() {
    for raw in [
        br#"{"outer":{"key":1,"key":2}}"#.as_slice(),
        br#"{"seq":18446744073709551616}"#.as_slice(),
        br#"{"seq":-9223372036854775809}"#.as_slice(),
        br#"{"bad":"\ud800"}"#.as_slice(),
    ] {
        assert!(parse_strict_json(raw).is_err(), "must reject {raw:?}");
    }
    assert!(PolylogueRevisionManifest::from_json(br#"{"anchors":{},"anchors":{}}"#).is_err());
    assert!(parse_strict_json(br#"{"seq":18446744073709551615}"#).is_ok());
}

#[test]
fn python_canonical_helper_matches_rust_at_numeric_and_unicode_boundaries() {
    let produced = include_bytes!("../../../tests/fixtures/polylogue/v1/canonical_boundaries.json");
    let parsed = parse_strict_json(produced).expect("Python canonical boundary vector parses");
    assert_eq!(
        parsed["large_integer"].as_u64(),
        Some(9_007_199_254_740_993)
    );
    assert_eq!(parsed["u64_max"].as_u64(), Some(u64::MAX));
    assert_eq!(
        canonical_json_bytes(&parsed).unwrap(),
        produced[..produced.len() - 1]
    );
}

#[test]
fn raw_original_can_differ_from_canonical_record_without_changing_anchor_identity() {
    let composed = br#"{"text":"caf\u00e9"}"#;
    let decomposed = br#"{"text":"cafe\u0301"}"#;
    assert_ne!(sha256_hex(composed), sha256_hex(decomposed));
    assert_ne!(
        canonical_json_bytes(&parse_strict_json(composed).unwrap()).unwrap(),
        canonical_json_bytes(&parse_strict_json(decomposed).unwrap()).unwrap()
    );

    let (mut manifest, mut segments) = synthetic_revision();
    let head = segments.get_mut(&HEAD_SEGMENT_INDEX).unwrap();
    head.insert(1, b' ');
    manifest.head_segment.sha256 = sha256_hex(head);
    manifest.head_segment.size_bytes = head.len();
    let mut joined = head.clone();
    joined.extend_from_slice(&segments[&0]);
    manifest.revision_id = sha256_hex(&joined);
    manifest.content_digest.polylogue_sha256 = manifest.revision_id.clone();
    manifest.content_digest.size_bytes = joined.len();
    manifest
        .verify(&segments)
        .expect("raw whitespace does not change canonical anchor");
    let resolved = manifest
        .resolve_anchor(&segments, &manifest.session_id)
        .unwrap();
    assert_ne!(resolved.original_bytes, resolved.canonical_bytes);
    assert_ne!(
        resolved.original_sha256,
        manifest.anchors[&manifest.session_id].sha256
    );
}

#[test]
fn mutations_fail_closed() {
    let (manifest, mut segments) = synthetic_revision();
    segments.get_mut(&0).expect("segment")[0] ^= 1;
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.origin_vocabulary_digest = "stale".into();
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.expected_record_counts.insert("message".into(), 2);
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.revision_id = "stale".into();
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.segments[0].first_seq = 4;
    assert!(manifest.verify(&segments).is_err());

    let (manifest, mut segments) = synthetic_revision();
    segments.insert(1, b"{}\n".to_vec());
    assert!(manifest.verify(&segments).is_err());

    let (manifest, mut segments) = synthetic_revision();
    segments.remove(&0);
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.anchors.remove("claude-code-session:demo:n:msg-1");
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.content_digest.canonicalizer_version += 1;
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.semantics_version -= 1;
    assert!(manifest.verify(&segments).is_err());

    let (mut manifest, segments) = synthetic_revision();
    manifest.head_segment.first_seq = u64::MAX;
    assert!(manifest.verify(&segments).is_err());
}

#[test]
fn anchor_resolution_rejects_incompatible_manifest_versions() {
    let record_id = "claude-code-session:demo:n:msg-1";
    let mut mutations: Vec<Box<dyn Fn(&mut PolylogueRevisionManifest)>> = vec![
        Box::new(|manifest| manifest.protocol_version.push_str("-unknown")),
        Box::new(|manifest| manifest.semantics_version += 1),
        Box::new(|manifest| manifest.origin_vocabulary_version += 1),
        Box::new(|manifest| manifest.origin_vocabulary_digest = "stale".into()),
        Box::new(|manifest| manifest.content_digest.canonicalizer_version += 1),
    ];
    for mutate in mutations.drain(..) {
        let (mut manifest, segments) = synthetic_revision();
        mutate(&mut manifest);
        assert!(manifest.verify(&segments).is_err());
        assert!(manifest.resolve_anchor(&segments, record_id).is_err());
        assert!(manifest.anchor_byte(&segments, record_id).is_err());
    }
}

#[test]
fn vocabulary_maps_every_protocol_kind_to_a_non_legacy_event() {
    assert_eq!(POLYLOGUE_VOCABULARY.len(), 7);
    let registry: Value = serde_json::from_str(include_str!("../../../schemas/v1/registry.json"))
        .expect("schema registry");
    let registered = registry["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["source"] == "integration.polylogue")
        .collect::<Vec<_>>();
    assert_eq!(registered.len(), POLYLOGUE_VOCABULARY.len());
    for entry in POLYLOGUE_VOCABULARY {
        assert_eq!(entry.event_type, entry.kind.event_type());
        assert!(!entry.consumer.is_empty());
        assert!(
            registered
                .iter()
                .any(|schema| schema["event_type"] == entry.event_type)
        );
    }
}

#[test]
fn opaque_unicode_keys_and_values_remain_distinct() {
    let raw = "{\"é\":\"é\",\"e\u{301}\":\"e\u{301}\"}";
    let value = parse_strict_json(raw.as_bytes()).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 2);
    assert_ne!(value["é"], value["e\u{301}"]);
    let canonical = canonical_json_bytes(&value).unwrap();
    assert_eq!(parse_strict_json(&canonical).unwrap(), value);
}

// Update only byte commitments: canonical anchors remain independent witnesses.
fn recommit_bytes(manifest: &mut PolylogueRevisionManifest, segments: &BTreeMap<i32, Vec<u8>>) {
    let mut joined = Sha256::new();
    let mut size = 0;
    for descriptor in std::iter::once(&mut manifest.head_segment).chain(&mut manifest.segments) {
        let raw = &segments[&descriptor.index];
        descriptor.sha256 = sha256_hex(raw);
        descriptor.size_bytes = raw.len();
        joined.update(raw);
        size += raw.len();
    }
    manifest.revision_id = format!("{:x}", joined.finalize());
    manifest.content_digest.polylogue_sha256 = manifest.revision_id.clone();
    manifest.content_digest.size_bytes = size;
}

fn stream_with_tiny_buffers(
    manifest: &PolylogueRevisionManifest,
    segments: &BTreeMap<i32, Vec<u8>>,
) -> Result<BTreeMap<String, i64>> {
    manifest.verify_readers(segments, |raw| {
        Ok(BufReader::with_capacity(1, Cursor::new(raw)))
    })
}

#[test]
fn streamed_offsets_match_resolved_original_bytes_across_small_read_buffers() {
    let (mut manifest, mut segments) = synthetic_revision();
    segments.get_mut(&0).unwrap().insert(1, b' ');
    recommit_bytes(&mut manifest, &segments);
    let offsets = stream_with_tiny_buffers(&manifest, &segments).unwrap();
    for (record_id, anchor) in &manifest.anchors {
        assert_eq!(
            offsets[record_id],
            manifest.anchor_byte(&segments, record_id).unwrap()
        );
        let raw = &segments[&anchor.segment_index];
        let resolved = manifest.resolve_anchor(&segments, record_id).unwrap();
        assert_eq!(
            raw[offsets[record_id] as usize..]
                .split(|byte| *byte == b'\n')
                .next()
                .unwrap(),
            resolved.original_bytes
        );
    }
}

#[test]
fn streaming_rejects_framing_duplicate_keys_ids_and_unsupported_sequences() {
    let invalid_segments = [
        // Missing final LF, interior empty line, and an actually empty segment.
        b"{}".to_vec(),
        b"\n{}\n".to_vec(),
        Vec::new(),
        br#"{"kind":"message","seq":0,"record_id":"x","nested":{"a":1,"a":2}}
"#
        .to_vec(),
        br#"{"kind":"message","seq":18446744073709551616,"record_id":"x"}
"#
        .to_vec(),
        br#"{"kind":"message","seq":0.0,"record_id":"x"}
"#
        .to_vec(),
        br#"{"kind":"message","seq":0,"record_id":"same"}
{"kind":"block","seq":1,"record_id":"same"}
"#
        .to_vec(),
    ];
    for raw in invalid_segments {
        let (mut manifest, mut segments) = synthetic_revision();
        segments.insert(0, raw);
        recommit_bytes(&mut manifest, &segments);
        assert!(stream_with_tiny_buffers(&manifest, &segments).is_err());
    }
    let (manifest, mut segments) = synthetic_revision();
    segments.get_mut(&0).unwrap()[0] ^= 1;
    assert!(stream_with_tiny_buffers(&manifest, &segments).is_err());
}

#[test]
fn streaming_checks_session_identity_and_semantic_counts_after_recommitment() {
    for field in ["session_id", "origin", "native_id", "message_count"] {
        let (mut manifest, mut segments) = synthetic_revision();
        let raw = &segments[&HEAD_SEGMENT_INDEX];
        let mut session = parse_strict_json(&raw[..raw.len() - 1]).unwrap();
        session[field] = if field == "message_count" {
            json!(2)
        } else {
            json!("other")
        };
        let mut replacement = canonical_json_bytes(&session).unwrap();
        manifest
            .anchors
            .get_mut(&manifest.session_id)
            .unwrap()
            .sha256 = sha256_hex(&replacement);
        replacement.push(b'\n');
        segments.insert(HEAD_SEGMENT_INDEX, replacement);
        recommit_bytes(&mut manifest, &segments);
        assert!(
            stream_with_tiny_buffers(&manifest, &segments).is_err(),
            "changed {field}"
        );
    }
    let (mut manifest, mut segments) = synthetic_revision();
    let raw = &segments[&0];
    let lines = raw.split(|byte| *byte == b'\n').collect::<Vec<_>>();
    let mut message = parse_strict_json(lines[0]).unwrap();
    message["block_count"] = json!(2);
    let mut replacement = canonical_json_bytes(&message).unwrap();
    manifest
        .anchors
        .get_mut("claude-code-session:demo:n:msg-1")
        .unwrap()
        .sha256 = sha256_hex(&replacement);
    replacement.push(b'\n');
    replacement.extend_from_slice(lines[1]);
    replacement.push(b'\n');
    segments.insert(0, replacement);
    recommit_bytes(&mut manifest, &segments);
    assert!(stream_with_tiny_buffers(&manifest, &segments).is_err());
}

#[test]
fn file_verification_reports_missing_and_mutated_segment_files() {
    let (manifest, segments) = synthetic_revision();
    let directory = tempfile::tempdir().unwrap();
    let paths = segments
        .iter()
        .map(|(index, raw)| {
            let path = directory.path().join(format!("segment-{index}.ndjson"));
            std::fs::write(&path, raw).unwrap();
            (*index, path)
        })
        .collect::<BTreeMap<_, _>>();
    let offsets = manifest.verify_files(&paths).unwrap();
    assert_eq!(
        offsets,
        stream_with_tiny_buffers(&manifest, &segments).unwrap()
    );
    std::fs::write(&paths[&0], b"{}\n").unwrap();
    let error = manifest.verify_files(&paths).unwrap_err();
    assert_eq!(error.kind(), crate::error::SinexErrorKind::Validation);
    assert!(!error.is_retryable());
    std::fs::remove_file(&paths[&0]).unwrap();
    let error = manifest.verify_files(&paths).unwrap_err();
    assert_eq!(error.kind(), crate::error::SinexErrorKind::Io);
    assert!(error.is_retryable());
    assert!(error.to_string().contains("cannot open segment"), "{error}");
}

#[test]
fn streamed_read_failure_is_retryable_unavailability() {
    struct UnavailableReader;
    impl std::io::Read for UnavailableReader {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("synthetic read failure"))
        }
    }
    impl BufRead for UnavailableReader {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            Err(std::io::Error::other("synthetic read failure"))
        }
        fn consume(&mut self, _: usize) {}
    }
    let (manifest, segments) = synthetic_revision();
    let error = manifest
        .verify_readers(&segments, |_| Ok(UnavailableReader))
        .unwrap_err();
    assert_eq!(error.kind(), crate::error::SinexErrorKind::Io);
    assert!(error.is_retryable());
    assert!(
        error.to_string().contains("cannot read segment -1"),
        "{error}"
    );
}

fn checked_producer_revision() -> (PolylogueRevisionManifest, BTreeMap<i32, Vec<u8>>) {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/polylogue/v1/small-session");
    let manifest =
        PolylogueRevisionManifest::from_json(&std::fs::read(root.join("manifest.json")).unwrap())
            .unwrap();
    let segments = std::iter::once(&manifest.head_segment)
        .chain(&manifest.segments)
        .map(|descriptor| {
            (
                descriptor.index,
                std::fs::read(root.join("segments").join(&descriptor.filename)).unwrap(),
            )
        })
        .collect();
    (manifest, segments)
}

// Rebuild all byte and canonical commitments so semantic tests cannot pass by
// merely detecting stale digests or anchors.
fn recommit_records(manifest: &mut PolylogueRevisionManifest, segments: &BTreeMap<i32, Vec<u8>>) {
    manifest.anchors.clear();
    manifest.expected_record_counts.clear();
    for descriptor in std::iter::once(&mut manifest.head_segment).chain(&mut manifest.segments) {
        let raw = &segments[&descriptor.index];
        let lines = raw[..raw.len() - 1]
            .split(|byte| *byte == b'\n')
            .collect::<Vec<_>>();
        descriptor.record_count = lines.len();
        for (line_index, line) in lines.into_iter().enumerate() {
            let value = parse_strict_json(line).unwrap();
            let record_id = value["record_id"].as_str().unwrap().to_owned();
            let kind = value["kind"].as_str().unwrap().to_owned();
            let seq = value["seq"].as_u64().unwrap();
            if line_index == 0 {
                descriptor.first_seq = seq;
            }
            descriptor.last_seq = seq;
            *manifest
                .expected_record_counts
                .entry(kind.clone())
                .or_default() += 1;
            manifest.anchors.insert(
                record_id,
                PolylogueAnchor {
                    segment_index: descriptor.index,
                    line_index,
                    seq,
                    kind,
                    sha256: sha256_hex(&canonical_json_bytes(&value).unwrap()),
                },
            );
        }
    }
    recommit_bytes(manifest, segments);
}

fn mutate_record(
    segments: &mut BTreeMap<i32, Vec<u8>>,
    record_id: &str,
    mut mutate: impl FnMut(&mut Value),
) {
    let mut found = false;
    for raw in segments.values_mut() {
        let mut replacement = Vec::new();
        for line in raw[..raw.len() - 1].split(|byte| *byte == b'\n') {
            let mut record = parse_strict_json(line).unwrap();
            if record["record_id"].as_str() == Some(record_id) {
                mutate(&mut record);
                found = true;
            }
            replacement.extend_from_slice(&canonical_json_bytes(&record).unwrap());
            replacement.push(b'\n');
        }
        *raw = replacement;
    }
    assert!(found, "synthetic record {record_id} exists");
}

#[test]
fn recomputed_orphan_block_cannot_disappear_from_message_count_closure() {
    let (mut manifest, mut segments) = synthetic_revision();
    mutate_record(
        &mut segments,
        "claude-code-session:demo:n:msg-1:0",
        |record| {
            record["message_id"] = json!("claude-code-session:demo:n:absent");
        },
    );
    mutate_record(
        &mut segments,
        "claude-code-session:demo:n:msg-1",
        |record| {
            record["block_count"] = json!(0);
        },
    );
    recommit_records(&mut manifest, &segments);
    let error = stream_with_tiny_buffers(&manifest, &segments).unwrap_err();
    assert!(
        error.to_string().contains("absent or later message owner"),
        "{error}"
    );
    assert!(!error.is_retryable());
}

#[test]
fn each_owned_kind_rejects_recomputed_absent_or_later_message_owners() {
    for kind in ["block", "attachment", "session_event"] {
        let (original, original_segments) = checked_producer_revision();
        let (record_id, anchor) = original
            .anchors
            .iter()
            .find(|(_, anchor)| anchor.kind == kind)
            .unwrap();
        for owner in [
            "claude-code-session:demo-session-1:n:absent",
            "claude-code-session:demo-session-1:n:msg-6",
        ] {
            let mut manifest = original.clone();
            let mut segments = original_segments.clone();
            mutate_record(&mut segments, record_id, |record| {
                record[if kind == "session_event" {
                    "source_message_id"
                } else {
                    "message_id"
                }] = json!(owner);
            });
            // The session event already follows msg-6. Move it before its owner
            // with fresh sequences when testing the later-owner case.
            if kind == "session_event" && owner.ends_with("msg-6") {
                let raw = segments.get_mut(&anchor.segment_index).unwrap();
                let mut records = raw[..raw.len() - 1]
                    .split(|byte| *byte == b'\n')
                    .map(|line| parse_strict_json(line).unwrap())
                    .collect::<Vec<_>>();
                records.swap(2, 3);
                *raw = records
                    .into_iter()
                    .enumerate()
                    .flat_map(|(index, mut record)| {
                        record["seq"] = json!(8 + index);
                        canonical_json_bytes(&record)
                            .unwrap()
                            .into_iter()
                            .chain([b'\n'])
                    })
                    .collect();
            }
            recommit_records(&mut manifest, &segments);
            let error = manifest.verify(&segments).unwrap_err();
            assert!(
                error.to_string().contains("absent or later message owner"),
                "{kind}: {error}"
            );
        }
    }
}

#[test]
fn all_seven_kinds_reject_recomputed_wrong_session_identity() {
    for kind in POLYLOGUE_VOCABULARY.map(|entry| entry.kind.as_str()) {
        let (mut manifest, mut segments) = checked_producer_revision();
        let record_id = manifest
            .anchors
            .iter()
            .find(|(_, anchor)| anchor.kind == kind)
            .unwrap()
            .0
            .clone();
        mutate_record(&mut segments, &record_id, |record| {
            record[if kind == "lineage" {
                "src_session_id"
            } else {
                "session_id"
            }] = json!("different-session");
        });
        recommit_records(&mut manifest, &segments);
        let error = manifest.verify(&segments).unwrap_err();
        assert!(
            error.to_string().contains("different session"),
            "{kind}: {error}"
        );
    }
}

#[test]
fn all_seven_kind_shapes_reject_recomputed_unknown_missing_and_mistyped_fields() {
    let fields = [
        ("session", "message_count", json!("one"), "native_id"),
        ("lineage", "confidence", json!(true), "src_session_id"),
        ("usage", "input_tokens", json!(false), "session_id"),
        (
            "message",
            "usage",
            json!({"input_tokens": true}),
            "position",
        ),
        ("block", "tool_result_is_error", json!(1), "block_id"),
        ("attachment", "byte_count", json!("zero"), "message_id"),
        ("session_event", "payload", json!([]), "session_id"),
    ];
    for (kind, field, wrong_value, required) in fields {
        for mutation in 0..3 {
            let (mut manifest, mut segments) = checked_producer_revision();
            let record_id = manifest
                .anchors
                .iter()
                .find(|(_, anchor)| anchor.kind == kind)
                .unwrap()
                .0
                .clone();
            mutate_record(&mut segments, &record_id, |record| match mutation {
                0 => {
                    record["undeclared_field"] = json!(null);
                }
                1 => {
                    record[field] = wrong_value.clone();
                }
                _ => {
                    record.as_object_mut().unwrap().remove(required);
                }
            });
            recommit_records(&mut manifest, &segments);
            let error = manifest.verify(&segments).unwrap_err();
            assert_eq!(
                error.kind(),
                crate::error::SinexErrorKind::Validation,
                "{kind}: {error}"
            );
            assert!(
                error.to_string().contains("record field")
                    || error.to_string().contains("record has no"),
                "{kind}: {error}"
            );
        }
    }
}

#[test]
fn optional_fields_and_opaque_objects_keep_their_declared_contracts() {
    let (mut manifest, mut segments) = checked_producer_revision();
    let session_id = manifest.session_id.clone();
    mutate_record(&mut segments, &session_id, |record| {
        record.as_object_mut().unwrap().remove("title");
        record["metadata"] = json!({"e\u{301}": {"opaque": true}, "é": [null, 1, "exact"]});
    });
    let message_id = manifest
        .anchors
        .iter()
        .find(|(_, anchor)| anchor.kind == "message")
        .unwrap()
        .0
        .clone();
    mutate_record(&mut segments, &message_id, |record| {
        record["text"] = Value::Null;
        record["usage"] = json!({"input_tokens": null, "output_tokens": 0});
    });
    let event_id = manifest
        .anchors
        .iter()
        .find(|(_, anchor)| anchor.kind == "session_event")
        .unwrap()
        .0
        .clone();
    mutate_record(&mut segments, &event_id, |record| {
        record["source_message_id"] = Value::Null;
        record["payload"] = json!({"opaque": {"undeclared_field": [false]}});
    });
    recommit_records(&mut manifest, &segments);
    manifest.verify(&segments).unwrap();
}

#[test]
fn recomputed_message_identity_ordinals_and_block_identity_must_agree() {
    for field in ["message_id", "position", "block_id"] {
        let (mut manifest, mut segments) = checked_producer_revision();
        let kind = if field == "block_id" {
            "block"
        } else {
            "message"
        };
        let record_id = manifest
            .anchors
            .iter()
            .find(|(_, anchor)| anchor.kind == kind)
            .unwrap()
            .0
            .clone();
        mutate_record(&mut segments, &record_id, |record| {
            record[field] = if field == "position" {
                json!(1)
            } else {
                json!("inconsistent")
            };
        });
        recommit_records(&mut manifest, &segments);
        let error = manifest.verify(&segments).unwrap_err();
        assert!(
            error.to_string().contains("identity is inconsistent")
                || error.to_string().contains("duplicate message ordinal"),
            "{field}: {error}"
        );
    }
}

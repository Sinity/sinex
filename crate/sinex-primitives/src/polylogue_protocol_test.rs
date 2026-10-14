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

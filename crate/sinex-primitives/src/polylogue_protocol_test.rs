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
}

#[test]
fn vocabulary_maps_every_protocol_kind_to_a_non_legacy_event() {
    assert_eq!(POLYLOGUE_VOCABULARY.len(), 7);
    for entry in POLYLOGUE_VOCABULARY {
        assert_eq!(entry.event_type, entry.kind.event_type());
        assert!(!entry.event_type.ends_with("session_indexed"));
        assert!(!entry.consumer.is_empty());
    }
}

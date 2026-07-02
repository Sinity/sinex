use super::*;
use crate::SinexError;
use crate::domain::{EventSource, EventType};
use crate::parser::{FieldSource, FieldSpec, FieldType, InputFormat};
use crate::parser::{ParserId, SourceId};
use crate::privacy::ProcessingContext;
use crate::rpc::sources::{CaveatSeverity, caveat_codes};
use serde_json::json;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn test_from_json_simple() -> xtask::sandbox::TestResult<()> {
    let value = json!({"name": "Alice", "age": 30});
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.format, "json");
    assert_eq!(fp.keys, vec!["/age", "/name"]); // sorted
    assert_eq!(fp.type_map["/name"], "string");
    assert_eq!(fp.type_map["/age"], "integer");
    Ok(())
}

#[sinex_test]
async fn test_from_json_with_nulls() -> xtask::sandbox::TestResult<()> {
    let value = json!({"name": "Bob", "email": null});
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.keys, vec!["/email", "/name"]);
    assert_eq!(fp.type_map["/email"], "null");
    Ok(())
}

#[sinex_test]
async fn test_from_json_with_mixed_types() -> xtask::sandbox::TestResult<()> {
    let value = json!({
        "text": "hello",
        "count": 42,
        "active": true,
        "nested": { "key": "value" },
        "items": [1, 2, 3],
        "nullable": null
    });
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.type_map["/text"], "string");
    assert_eq!(fp.type_map["/count"], "integer");
    assert_eq!(fp.type_map["/active"], "boolean");
    assert_eq!(fp.type_map["/nested"], "object");
    assert_eq!(fp.type_map["/nested/key"], "string");
    assert_eq!(fp.type_map["/items"], "array");
    assert_eq!(fp.type_map["/nullable"], "null");
    Ok(())
}

#[sinex_test]
async fn test_from_json_uses_nested_json_pointer_paths() -> xtask::sandbox::TestResult<()> {
    let value = json!({
        "message": {
            "content": "hello",
            "meta/with~escape": {
                "tokens": 12
            }
        },
        "session_id": "abc"
    });
    let fp = SourceRecordFingerprint::from_json(&value);

    assert!(fp.keys.contains(&"/message/content".to_string()));
    assert!(fp.keys.contains(&"/message/meta~1with~0escape".to_string()));
    assert!(
        fp.keys
            .contains(&"/message/meta~1with~0escape/tokens".to_string())
    );
    assert_eq!(fp.type_map["/message/content"], "string");
    assert_eq!(fp.type_map["/message/meta~1with~0escape/tokens"], "integer");
    Ok(())
}

#[sinex_test]
async fn test_fingerprint_stability() -> xtask::sandbox::TestResult<()> {
    let value1 = json!({"z": 1, "a": "x", "m": 3.15});
    let value2 = json!({"a": "x", "m": 3.15, "z": 1});

    let fp1 = SourceRecordFingerprint::from_json(&value1);
    let fp2 = SourceRecordFingerprint::from_json(&value2);

    assert_eq!(fp1.keys, fp2.keys);
    assert_eq!(fp1.type_map, fp2.type_map);
    assert_eq!(fp1.hash(), fp2.hash());
    Ok(())
}

#[sinex_test]
async fn test_fingerprint_different_when_keys_change() -> xtask::sandbox::TestResult<()> {
    let value1 = json!({"name": "Alice", "age": 30});
    let value2 = json!({"name": "Alice", "age": 30, "city": "NYC"});

    let fp1 = SourceRecordFingerprint::from_json(&value1);
    let fp2 = SourceRecordFingerprint::from_json(&value2);

    assert_ne!(fp1.hash(), fp2.hash());
    Ok(())
}

#[sinex_test]
async fn test_fingerprint_different_when_types_change() -> xtask::sandbox::TestResult<()> {
    let value1 = json!({"count": 42});
    let value2 = json!({"count": "42"});

    let fp1 = SourceRecordFingerprint::from_json(&value1);
    let fp2 = SourceRecordFingerprint::from_json(&value2);

    assert_ne!(fp1.hash(), fp2.hash());
    Ok(())
}

#[sinex_test]
async fn test_from_jsonl_bytes_records_row_object_keys() -> xtask::sandbox::TestResult<()> {
    let fp = SourceRecordFingerprint::from_jsonl_bytes(
        br#"{"entry_id":1,"entry_created_at":"2026-01-01 00:00:00","content":"a"}

{"entry_id":2,"entry_created_at":"2026-01-02 00:00:00","votes_score":3}
"#,
    )?;

    assert_eq!(fp.format, "jsonl");
    assert!(fp.keys.contains(&"/[]/entry_id".to_string()));
    assert!(fp.keys.contains(&"/[]/entry_created_at".to_string()));
    assert!(fp.keys.contains(&"/[]/content".to_string()));
    assert!(fp.keys.contains(&"/[]/votes_score".to_string()));
    assert_eq!(fp.type_map["/[]/entry_id"], "integer");
    assert_eq!(fp.type_map["/[]/entry_created_at"], "string");
    assert_eq!(fp.type_map["/[]/votes_score"], "integer");
    Ok(())
}

#[sinex_test]
async fn test_from_csv_bytes_infers_header_shape() -> xtask::sandbox::TestResult<()> {
    let fp =
        SourceRecordFingerprint::from_csv_bytes(b"id,name,active,score\n42,Alice,true,98.5\n")?;

    assert_eq!(fp.format, "csv");
    assert_eq!(fp.keys, vec!["active", "id", "name", "score"]);
    assert_eq!(fp.type_map["id"], "integer");
    assert_eq!(fp.type_map["name"], "string");
    assert_eq!(fp.type_map["active"], "boolean");
    assert_eq!(fp.type_map["score"], "number");
    Ok(())
}

#[sinex_test]
async fn test_from_tsv_bytes_detects_missing_and_extra_columns()
-> xtask::sandbox::TestResult<()> {
    let fp = SourceRecordFingerprint::from_tsv_bytes(b"id\tname\n42\tAlice\tunexpected\n")?;

    assert_eq!(fp.format, "tsv");
    assert_eq!(fp.keys, vec!["__extra_2", "id", "name"]);
    assert_eq!(fp.type_map["__extra_2"], "string");
    Ok(())
}

#[sinex_test]
async fn test_from_csv_bytes_handles_empty_input() -> xtask::sandbox::TestResult<()> {
    let fp = SourceRecordFingerprint::from_csv_bytes(b"  \n\t")?;

    assert_eq!(fp.format, "csv");
    assert!(fp.keys.is_empty());
    assert!(fp.type_map.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_csv_drift_reports_header_and_type_changes() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.csv");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);
    let fp1 = SourceRecordFingerprint::from_csv_bytes(b"id,name,score\n42,Alice,98.5\n")?;
    let fp2 = SourceRecordFingerprint::from_csv_bytes(b"id,full_name,score\n42,Alice,high\n")?;

    acc.observe(&fp1);
    let drift = acc.observe(&fp2).expect("csv shape drift should emit");

    assert_eq!(drift.format, "csv");
    assert_eq!(drift.added_keys, vec!["full_name"]);
    assert_eq!(drift.removed_keys, vec!["name"]);
    assert_eq!(
        drift.type_changes,
        vec![(
            "score".to_string(),
            "number".to_string(),
            "string".to_string()
        )]
    );
    Ok(())
}

#[cfg(feature = "rusqlite")]
#[sinex_test]
async fn test_from_sqlite_connection_fingerprints_table_columns()
-> xtask::sandbox::TestResult<()> {
    let conn = rusqlite::Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE history (
            id INTEGER PRIMARY KEY,
            command TEXT NOT NULL,
            ts_ms INTEGER
        )",
        [],
    )?;

    let fp = SourceRecordFingerprint::from_sqlite_connection(&conn)?;

    assert_eq!(fp.format, "sqlite_schema");
    assert!(fp.keys.contains(&"table:history".to_string()));
    assert!(fp.keys.contains(&"history.command".to_string()));
    assert_eq!(fp.type_map["history.command"], "text;not_null=true;pk=0");
    assert_eq!(fp.type_map["history.id"], "integer;not_null=false;pk=1");
    Ok(())
}

#[cfg(feature = "rusqlite")]
#[sinex_test]
async fn test_sqlite_schema_drift_reports_column_change() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.sqlite");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);

    let conn1 = rusqlite::Connection::open_in_memory()?;
    conn1.execute(
        "CREATE TABLE history (id INTEGER PRIMARY KEY, command TEXT)",
        [],
    )?;
    let conn2 = rusqlite::Connection::open_in_memory()?;
    conn2.execute(
        "CREATE TABLE history (
            id INTEGER PRIMARY KEY,
            command BLOB,
            exit_code INTEGER
        )",
        [],
    )?;

    let fp1 = SourceRecordFingerprint::from_sqlite_connection(&conn1)?;
    let fp2 = SourceRecordFingerprint::from_sqlite_connection(&conn2)?;
    acc.observe(&fp1);
    let drift = acc
        .observe(&fp2)
        .expect("sqlite schema shape drift should emit");

    assert_eq!(drift.format, "sqlite_schema");
    assert_eq!(drift.added_keys, vec!["history.exit_code"]);
    assert_eq!(
        drift.type_changes,
        vec![(
            "history.command".to_string(),
            "text;not_null=false;pk=0".to_string(),
            "blob;not_null=false;pk=0".to_string()
        )]
    );
    Ok(())
}

#[sinex_test]
async fn test_drift_accumulator_first_observation() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source);

    let fp = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));
    let event = acc.observe(&fp);

    assert!(event.is_none());
    assert_eq!(acc.last_seen_hash(), Some(fp.hash()));
    Ok(())
}

#[sinex_test]
async fn test_drift_accumulator_same_fingerprint() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source);

    let fp = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

    // First observation.
    acc.observe(&fp);

    // Second observation: identical fingerprint.
    let event = acc.observe(&fp);
    assert!(event.is_none());
    Ok(())
}

#[sinex_test]
async fn test_drift_accumulator_detects_drift() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(2) // Low threshold for testing.
        .with_cooldown_secs(0);

    let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

    // Observe fp1.
    acc.observe(&fp1);
    assert_eq!(acc.record_count_since_last_emit, 1);

    // Observe fp1 again (identical).
    acc.observe(&fp1);
    assert_eq!(acc.record_count_since_last_emit, 2);

    // Observe fp2 (drift, and rate limit clears).
    let event = acc.observe(&fp2);
    assert!(event.is_some());

    let drift = event.unwrap();
    assert_eq!(drift.added_keys, vec!["/name".to_string()]);
    assert!(drift.removed_keys.is_empty());
    assert!(drift.type_changes.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_drift_accumulator_respects_record_count_limit() -> xtask::sandbox::TestResult<()>
{
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(100)
        .with_cooldown_secs(0);

    let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));

    acc.observe(&fp1);

    // Only 1 record observed; need 100 before emitting drift.
    let event = acc.observe(&fp2);
    assert!(event.is_none());
    Ok(())
}

#[sinex_test]
async fn test_drift_accumulator_respects_cooldown() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(1)
        .with_cooldown_secs(1000); // 1000 seconds between emissions.

    let fp1 = SourceRecordFingerprint::from_json(&json!({"id": 1}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "test"}));
    let fp3 = SourceRecordFingerprint::from_json(&json!({"id": 1})); // back to fp1 shape

    acc.observe(&fp1);

    // First drift: emitted (no prior emit).
    let event1 = acc.observe(&fp2);
    assert!(event1.is_some());

    // Second drift (back to fp1 shape): not emitted due to cooldown.
    acc.record_count_since_last_emit = 0; // Reset counter.
    let event2 = acc.observe(&fp3);
    assert!(event2.is_none());
    Ok(())
}

#[sinex_test]
async fn test_drift_event_construction() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source.clone())
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);

    let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": "x"}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 1, "c": true}));

    acc.observe(&fp1);
    let event = acc.observe(&fp2).unwrap();

    assert_eq!(event.source_id, source);
    assert_eq!(event.added_keys, vec!["/c"]);
    assert_eq!(event.removed_keys, vec!["/b"]);
    // "a" should have no type change (integer -> integer).
    assert!(event.type_changes.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_drift_event_type_changes() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);

    let fp1 = SourceRecordFingerprint::from_json(&json!({"count": 42}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"count": "42"}));

    acc.observe(&fp1);
    let event = acc.observe(&fp2).unwrap();

    assert!(event.added_keys.is_empty());
    assert!(event.removed_keys.is_empty());
    assert_eq!(event.type_changes.len(), 1);
    assert_eq!(
        event.type_changes[0],
        (
            "/count".to_string(),
            "integer".to_string(),
            "string".to_string()
        )
    );
    Ok(())
}

#[sinex_test]
async fn test_fingerprint_diff_matches_drift_payload() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let fp1 = SourceRecordFingerprint::from_json(&json!({"count": 42, "name": "old"}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"count": "42", "enabled": true}));

    let drift = SourceRecordFingerprint::diff(source.clone(), &fp1, &fp2)
        .expect("different fingerprints should report drift");

    assert_eq!(drift.source_id, source);
    assert_eq!(drift.previous_hash, fp1.hash());
    assert_eq!(drift.current_hash, fp2.hash());
    assert_eq!(drift.added_keys, vec!["/enabled"]);
    assert_eq!(drift.removed_keys, vec!["/name"]);
    assert_eq!(
        drift.type_changes,
        vec![(
            "/count".to_string(),
            "integer".to_string(),
            "string".to_string()
        )]
    );
    assert!(
        SourceRecordFingerprint::diff(SourceId::from_static("test.unit"), &fp1, &fp1).is_none()
    );
    Ok(())
}

#[sinex_test]
async fn drift_readiness_caveats_classify_advisory_and_degraded_shapes()
-> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");

    let additive = SourceRecordFingerprint::diff(
        source.clone(),
        &SourceRecordFingerprint::from_json(&json!({"id": 1})),
        &SourceRecordFingerprint::from_json(&json!({"id": 1, "optional": true})),
    )
    .ok_or_else(|| SinexError::validation("additive drift expected"))?;
    let additive_caveats = additive.readiness_caveats();
    assert_eq!(additive_caveats.len(), 1);
    assert_eq!(additive_caveats[0].code, caveat_codes::SOURCE_SHAPE_CHANGED);
    assert_eq!(additive_caveats[0].severity, CaveatSeverity::Info);
    assert!(
        additive_caveats[0]
            .evidence_ref
            .as_deref()
            .is_some_and(|reference| reference.starts_with("drift:"))
    );

    let degraded = SourceRecordFingerprint::diff(
        source,
        &SourceRecordFingerprint::from_json(&json!({"id": 1, "name": "old"})),
        &SourceRecordFingerprint::from_json(&json!({"id": "1"})),
    )
    .ok_or_else(|| SinexError::validation("degraded drift expected"))?;
    let degraded_caveats = degraded.readiness_caveats();
    let codes = degraded_caveats
        .iter()
        .map(|caveat| caveat.code.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        codes,
        vec![
            caveat_codes::PARSER_FIELD_TYPE_CHANGED,
            caveat_codes::PARSER_REQUIRED_FIELD_MISSING
        ]
    );
    assert!(
        degraded_caveats
            .iter()
            .all(|caveat| caveat.severity == CaveatSeverity::Degraded)
    );

    let required_caveats =
        degraded.readiness_caveats_with_required_fields(&["/name".to_string()]);
    assert!(
        required_caveats.iter().any(|caveat| {
            caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                && caveat.severity == CaveatSeverity::Blocking
        }),
        "required input removal should block readiness: {required_caveats:?}"
    );

    let spec = DeclarativeParserSpec {
        parser_id: ParserId::from_static("test-parser"),
        parser_version: "1.0.0".to_string(),
        source_id: SourceId::from_static("test.unit"),
        event_source: EventSource::from_static("test"),
        event_type: EventType::from_static("test.event"),
        default_privacy_context: ProcessingContext::Metadata,
        input_format: InputFormat::Json,
        fields: vec![FieldSpec {
            name: "name".to_string(),
            source: FieldSource::JsonPointer {
                pointer: "/name".to_string(),
            },
            field_type: FieldType::String,
            required: true,
            default: None,
            skip_payload: false,
            privacy_context: None,
            sensitivity: Vec::new(),
            occurrence_key: false,
            timestamp: None,
            suppress_if: None,
            carry: None,
            transform: None,
            validate: None,
        }],
        discriminator: None,
    };
    let spec_caveats = degraded.readiness_caveats_for_declarative_parser(&spec);
    assert!(
        spec_caveats.iter().any(|caveat| {
            caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
                && caveat.severity == CaveatSeverity::Blocking
        }),
        "declarative required input removal should block readiness: {spec_caveats:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_drift_event_to_payload() -> xtask::sandbox::TestResult<()> {
    let source = SourceId::from_static("test.unit");
    let mut acc = DriftAccumulator::new(source)
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);

    let fp1 = SourceRecordFingerprint::from_json(&json!({"x": 1}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"x": 1, "y": 2}));

    acc.observe(&fp1);
    let event = acc.observe(&fp2).unwrap();

    let payload = event.to_payload();
    assert!(payload.is_object());
    assert_eq!(payload["format"], "json");
    assert_eq!(payload["added_keys"], serde_json::json!(["/y"]));
    Ok(())
}

#[sinex_test]
async fn test_empty_record() -> xtask::sandbox::TestResult<()> {
    let value = json!({});
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.format, "json");
    assert!(fp.keys.is_empty());
    assert!(fp.type_map.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_array_top_level() -> xtask::sandbox::TestResult<()> {
    let value = json!([1, 2, 3]);
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.format, "json");
    assert!(fp.keys.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_top_level_array_of_objects_records_element_keys() -> xtask::sandbox::TestResult<()>
{
    let value = json!([
        {
            "ts": "2026-01-01T00:00:00Z",
            "ms_played": 1000,
            "track": { "uri": "spotify:track:1" }
        },
        {
            "ts": "2026-01-01T00:01:00Z",
            "ms_played": "2000",
            "platform": "linux"
        }
    ]);
    let fp = SourceRecordFingerprint::from_json(&value);

    assert_eq!(fp.format, "json");
    assert_eq!(
        fp.keys,
        vec![
            "/[]/ms_played",
            "/[]/platform",
            "/[]/track",
            "/[]/track/uri",
            "/[]/ts"
        ]
    );
    assert_eq!(fp.type_map["/[]/ts"], "string");
    assert_eq!(fp.type_map["/[]/ms_played"], "mixed");
    assert_eq!(fp.type_map["/[]/track/uri"], "string");
    Ok(())
}

// -----------------------------------------------------------------------
// Coverage gaps filled (#1100 substrate hardening)
// -----------------------------------------------------------------------

#[sinex_test]
async fn from_record_falls_back_to_binary_for_non_json() -> xtask::sandbox::TestResult<()> {
    use crate::Id;
    use crate::parser::{MaterialAnchor, SourceRecord};
    let record = SourceRecord {
        material_id: Id::from_uuid(uuid::Uuid::nil()),
        anchor: MaterialAnchor::ByteRange { start: 0, len: 8 },
        bytes: b"not-json".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let fp = SourceRecordFingerprint::from_record(&record);
    assert_eq!(fp.format, "binary");
    assert!(fp.keys.is_empty());
    assert!(fp.type_map.is_empty());
    // Non-empty hash computed over the opaque bytes.
    assert!(!fp.hash().is_empty());
    Ok(())
}

#[sinex_test]
async fn drift_record_count_resets_after_emission() -> xtask::sandbox::TestResult<()> {
    // After a drift event fires, record_count_since_last_emit must reset
    // so that a third schema requires another emit_every_n_records before
    // emitting again. Without this contract, every record after a drift
    // would re-emit.
    use crate::parser::SourceId;
    let mut acc = DriftAccumulator::new(SourceId::from_static("test.unit"))
        .with_emit_every_n_records(1)
        .with_cooldown_secs(0);
    let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": 2}));
    let fp3 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": 2, "c": 3}));
    let _ = acc.observe(&fp1); // baseline
    let drift = acc.observe(&fp2);
    assert!(drift.is_some(), "first drift after baseline should emit");
    // After emit, next observation with a NEW schema should re-emit only
    // when count threshold is met again. With emit_every_n_records=1,
    // observing fp3 (a different schema) should fire a new event.
    let drift_again = acc.observe(&fp3);
    assert!(
        drift_again.is_some(),
        "subsequent drift past emit_every_n_records should fire"
    );
    Ok(())
}

#[sinex_test]
async fn drift_hash_stable_for_same_schema_under_value_changes()
-> xtask::sandbox::TestResult<()> {
    // Two records with the same field set + types but different values
    // must produce the same fingerprint hash, so DriftAccumulator does
    // not flap on every record.
    let fp1 = SourceRecordFingerprint::from_json(&json!({"a": 1, "b": "x"}));
    let fp2 = SourceRecordFingerprint::from_json(&json!({"a": 999, "b": "y"}));
    assert_eq!(fp1.hash(), fp2.hash());
    assert_eq!(fp1.keys, fp2.keys);
    Ok(())
}

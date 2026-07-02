use super::*;
use crate::Event;
use crate::repositories::events::StreamBatchRow;
use serde_json::json;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::EventId;
use sinex_primitives::{Id, Timestamp, Uuid};
use xtask::sandbox::sinex_test;

fn minimal_row() -> StreamBatchRow {
    StreamBatchRow {
        id: Uuid::now_v7(),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.event"),
        ts_orig: Timestamp::now(),
        ts_quality: None,
        host: sinex_primitives::domain::HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        source_material_id: None,
        anchor_byte: None,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
    }
}

fn row_fields(row: &StreamBatchRow) -> Vec<String> {
    let mut buf = Vec::new();
    row.write_copy_row(&mut buf).expect("write_copy_row failed");
    let s = String::from_utf8(buf).expect("non-UTF-8 output");
    // Strip the trailing newline before splitting
    let trimmed = s.trim_end_matches('\n');
    trimmed.split('\t').map(str::to_string).collect()
}

/// The COPY format must have exactly one field per authoritative writable event column.
#[sinex_test]
async fn produces_exactly_declared_field_count() -> ::xtask::sandbox::TestResult<()> {
    let fields = row_fields(&minimal_row());
    assert_eq!(
        fields.len(),
        event_copy_column_count(),
        "Expected {} tab-separated fields, got {}:\n{fields:?}",
        event_copy_column_count(),
        fields.len()
    );
    Ok(())
}

/// Row must end with a newline — required by Postgres COPY text protocol.
#[sinex_test]
async fn row_ends_with_newline() -> ::xtask::sandbox::TestResult<()> {
    let mut buf = Vec::new();
    minimal_row().write_copy_row(&mut buf).unwrap();
    assert_eq!(*buf.last().unwrap(), b'\n', "Row must end with newline");
    Ok(())
}

/// Null optional fields must emit the `\N` sentinel.
#[sinex_test]
async fn null_optionals_write_null_sentinel() -> ::xtask::sandbox::TestResult<()> {
    let fields = row_fields(&minimal_row());
    for event in [
        Events::SourceMaterialId,
        Events::AnchorByte,
        Events::OffsetStart,
        Events::OffsetEnd,
        Events::OffsetKind,
        Events::SourceEventIds,
        Events::PayloadSchemaId,
        Events::ModuleRunId,
        Events::AnchorPayloadHash,
        Events::AssociatedBlobIds,
        Events::TemporalPolicy,
        Events::SemanticsVersion,
        Events::ScopeKey,
        Events::EquivalenceKey,
        Events::CreatedByOperationId,
        Events::AutomatonModel,
    ] {
        let idx = event_copy_column_index(event);
        assert_eq!(
            fields[idx], "\\N",
            "Field {idx} should be \\N for None, got {:?}",
            fields[idx]
        );
    }
    Ok(())
}

#[sinex_test]
async fn missing_event_ts_orig_is_rejected() -> ::xtask::sandbox::TestResult<()> {
    let event = Event::<JsonValue> {
        id: Some(Id::new()),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.event"),
        payload: json!({"ok": true}),
        ts_orig: None,
        ts_quality: None,
        host: sinex_primitives::domain::HostName::from_static("localhost"),
        module_run_id: None,
        payload_schema_id: None,
        anchor_payload_hash: None,
        provenance: crate::Provenance::Material {
            id: Id::new(),
            anchor_byte: 0,
            offset_start: None,
            offset_end: None,
            offset_kind: sinex_primitives::events::builder::OffsetKind::Byte,
        },
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
    };

    let mut buf = Vec::new();
    let error = event
        .write_copy_row(&mut buf)
        .expect_err("missing ts_orig must be rejected");
    assert!(error.to_string().contains("missing ts_orig"));
    Ok(())
}

/// Tabs inside a field value must be escaped to `\t` so Postgres doesn't
/// mistake them for field delimiters.
#[sinex_test]
async fn tab_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
    let mut row = minimal_row();
    row.payload = json!({"k": "v\tw"});
    let fields = row_fields(&row);
    let payload_field = &fields[event_copy_column_index(Events::Payload)];
    assert!(
        !payload_field.contains('\t'),
        "Literal tab must be escaped in payload"
    );
    assert!(
        payload_field.contains("\\t"),
        "Escaped \\t must appear in payload, got: {payload_field:?}"
    );
    Ok(())
}

/// Newlines inside a field value must be escaped to `\n`.
#[sinex_test]
async fn newline_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
    let mut row = minimal_row();
    row.payload = json!({"k": "line1\nline2"});
    let fields = row_fields(&row);
    let payload_field = &fields[event_copy_column_index(Events::Payload)];
    assert!(
        !payload_field.contains('\n'),
        "Literal newline must be escaped"
    );
    assert!(payload_field.contains("\\n"), "Escaped \\n must appear");
    Ok(())
}

/// Backslashes must be doubled.
#[sinex_test]
async fn backslash_in_source_is_doubled() -> ::xtask::sandbox::TestResult<()> {
    let mut row = minimal_row();
    row.payload = json!({"path": "C:\\Users\\test"});
    let fields = row_fields(&row);
    // "C:\\Users\\test" → JSON string "C:\Users\test" → COPY escaped "C:\\Users\\test"
    assert!(
        fields[event_copy_column_index(Events::Payload)].contains("\\\\"),
        "Backslash should be doubled in COPY output"
    );
    Ok(())
}

/// UUID arrays must use Postgres `{uuid1,uuid2}` format.
#[sinex_test]
async fn uuid_arrays_use_postgres_brace_format() -> ::xtask::sandbox::TestResult<()> {
    let id1: EventId = Id::new();
    let id2: EventId = Id::new();
    let u1 = Uuid::new_v4();
    let u2 = Uuid::new_v4();
    let mut row = minimal_row();
    row.source_event_ids = Some(vec![id1, id2]);
    row.associated_blob_ids = Some(vec![u1, u2]);

    let fields = row_fields(&row);
    let sei = &fields[event_copy_column_index(Events::SourceEventIds)];
    let abi = &fields[event_copy_column_index(Events::AssociatedBlobIds)];

    assert!(
        sei.starts_with('{') && sei.ends_with('}'),
        "source_event_ids must be {{...}}"
    );
    assert!(
        abi.starts_with('{') && abi.ends_with('}'),
        "associated_blob_ids must be {{...}}"
    );
    assert!(
        sei.contains(&id1.to_uuid().to_string()),
        "source_event_ids must contain id1"
    );
    assert!(
        sei.contains(&id2.to_uuid().to_string()),
        "source_event_ids must contain id2"
    );
    assert!(
        abi.contains(&u1.to_string()),
        "associated_blob_ids must contain u1"
    );
    Ok(())
}

/// Numeric fields (anchor_byte, ts_orig_subnano, …) must be plain digits, not `\N`.
#[sinex_test]
async fn numeric_fields_are_written_as_digits() -> ::xtask::sandbox::TestResult<()> {
    let mut row = minimal_row();
    row.source_material_id = Some(Id::new());
    row.anchor_byte = Some(42);
    row.offset_start = Some(0);
    row.offset_end = Some(100);

    let fields = row_fields(&row);
    assert_ne!(
        fields[event_copy_column_index(Events::SourceMaterialId)],
        "\\N",
        "source_material_id should not be \\N"
    );
    assert_eq!(
        fields[event_copy_column_index(Events::AnchorByte)],
        "42",
        "anchor_byte should be '42'"
    );
    assert_eq!(
        fields[event_copy_column_index(Events::OffsetStart)],
        "0",
        "offset_start should be '0'"
    );
    assert_eq!(
        fields[event_copy_column_index(Events::OffsetEnd)],
        "100",
        "offset_end should be '100'"
    );
    Ok(())
}

/// The ID in field 0 must be a valid UUID (36-char hyphenated form) because
/// the staging table uses UUID columns and the INSERT SELECT applies `::uuid`.
#[sinex_test]
async fn id_is_written_as_uuid_native() -> ::xtask::sandbox::TestResult<()> {
    let id = Uuid::now_v7();
    let mut row = minimal_row();
    row.id = id;
    let fields = row_fields(&row);
    // UUID has 36 chars (8-4-4-4-12 + 4 hyphens)
    assert_eq!(
        fields[event_copy_column_index(Events::Id)].len(),
        36,
        "ID field should be UUID (36 chars), got {:?}",
        fields[event_copy_column_index(Events::Id)]
    );
    assert!(
        fields[event_copy_column_index(Events::Id)].contains('-'),
        "UUID must contain hyphens"
    );
    // Must round-trip through Uuid
    let parsed: Uuid = fields[event_copy_column_index(Events::Id)]
        .parse()
        .expect("id field must be parseable as UUID");
    assert_eq!(parsed, id, "UUID must match original Uuid's UUID");
    Ok(())
}

/// Carriage returns must be escaped to `\r` (exercises the slow fallback path).
#[sinex_test]
async fn carriage_return_in_payload_is_escaped() -> ::xtask::sandbox::TestResult<()> {
    let mut row = minimal_row();
    row.payload = json!({"k": "line1\r\nline2"});
    let fields = row_fields(&row);
    let payload_field = &fields[event_copy_column_index(Events::Payload)];
    assert!(
        !payload_field.contains('\r'),
        "Literal \\r must be escaped in payload"
    );
    assert!(
        payload_field.contains("\\r"),
        "Escaped \\r must appear in payload, got: {payload_field:?}"
    );
    assert!(
        payload_field.contains("\\n"),
        "Escaped \\n must appear alongside \\r"
    );
    Ok(())
}

/// Verify escape_copy_str directly for edge cases.
#[sinex_test]
async fn escape_copy_str_unit_tests() -> ::xtask::sandbox::TestResult<()> {
    let mut buf = Vec::new();

    // No specials — bulk copy
    super::escape_copy_str(&mut buf, "hello world");
    assert_eq!(buf, b"hello world");

    // Tab + newline
    buf.clear();
    super::escape_copy_str(&mut buf, "a\tb\nc");
    assert_eq!(buf, b"a\\tb\\nc");

    // Backslash
    buf.clear();
    super::escape_copy_str(&mut buf, "C:\\Users");
    assert_eq!(buf, b"C:\\\\Users");

    // \r triggers slow path
    buf.clear();
    super::escape_copy_str(&mut buf, "a\rb");
    assert_eq!(buf, b"a\\rb");

    // Mixed \r\n\t\\
    buf.clear();
    super::escape_copy_str(&mut buf, "\t\r\n\\");
    assert_eq!(buf, b"\\t\\r\\n\\\\");
    Ok(())
}

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
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
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
        Events::ProductClass,
        Events::ClaimSupport,
        Events::DerivationDeclarationId,
        Events::DerivationEpochId,
        Events::DerivationLaneId,
        Events::AdjudicationEventId,
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
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
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

/// COPY column contract (sinex-8cr.2): `query_as_insert_columns_match_copy_contract`
/// (persistence_test.rs) already proves the COPY column *names* line up with
/// the authoritative schema. That test alone would stay green even if the
/// writer silently emitted `\N` for every derivation-control-plane field
/// forever (the sinex-0vx.4 bug this bead fixes) — it only checks list
/// parity, never field *values*. This test proves the COPY text-protocol
/// writer round-trips non-null `product_class`/`claim_support` (plus the
/// other four derivation columns) for both `StreamBatchRow` and
/// `Event<JsonValue>`, the two `ToPostgresCopy` implementors.
#[sinex_test]
async fn copy_column_contract() -> ::xtask::sandbox::TestResult<()> {
    use sinex_primitives::derivation::{
        ClaimSupport, ClaimTemporalQuality, SourceCoverage, SupportLevel,
    };

    let claim_support = ClaimSupport::unreviewed(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        3,
        1,
        2,
        0,
    );
    let claim_support_json = serde_json::to_value(&claim_support)?;
    let declaration_id = "test.declaration".to_string();
    let epoch_id = Uuid::now_v7();
    let lane_id = Uuid::now_v7();
    let adjudication_id = Uuid::now_v7();

    // StreamBatchRow side.
    let mut row = minimal_row();
    row.product_class = Some("canonical_derived_event".to_string());
    row.claim_support = Some(claim_support_json.clone());
    row.derivation_declaration_id = Some(declaration_id.clone());
    row.derivation_epoch_id = Some(epoch_id);
    row.derivation_lane_id = Some(lane_id);
    row.adjudication_event_id = Some(adjudication_id);

    let row_field_values = row_fields(&row);
    assert_eq!(
        row_field_values[event_copy_column_index(Events::ProductClass)],
        "canonical_derived_event"
    );
    let decoded_support: serde_json::Value = serde_json::from_str(
        &row_field_values[event_copy_column_index(Events::ClaimSupport)],
    )?;
    assert_eq!(decoded_support, claim_support_json);
    assert_eq!(
        row_field_values[event_copy_column_index(Events::DerivationDeclarationId)],
        declaration_id
    );
    assert_eq!(
        row_field_values[event_copy_column_index(Events::DerivationEpochId)].parse::<Uuid>()?,
        epoch_id
    );
    assert_eq!(
        row_field_values[event_copy_column_index(Events::DerivationLaneId)].parse::<Uuid>()?,
        lane_id
    );
    assert_eq!(
        row_field_values[event_copy_column_index(Events::AdjudicationEventId)].parse::<Uuid>()?,
        adjudication_id
    );

    // Event<JsonValue> side — same six fields, same assertions.
    let product_class = sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent;
    let event = Event::<JsonValue> {
        id: Some(Id::new()),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.event"),
        payload: json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
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
        product_class: Some(product_class),
        claim_support: Some(claim_support),
        derivation_declaration_id: Some(declaration_id.clone()),
        derivation_epoch_id: Some(epoch_id),
        derivation_lane_id: Some(lane_id),
        adjudication_event_id: Some(adjudication_id),
    };

    let mut buf = Vec::new();
    event.write_copy_row(&mut buf)?;
    let s = String::from_utf8(buf).expect("non-UTF-8 output");
    let event_fields: Vec<String> = s.trim_end_matches('\n').split('\t').map(str::to_string).collect();

    assert_eq!(
        event_fields[event_copy_column_index(Events::ProductClass)],
        "canonical_derived_event"
    );
    let decoded_event_support: serde_json::Value =
        serde_json::from_str(&event_fields[event_copy_column_index(Events::ClaimSupport)])?;
    assert_eq!(decoded_event_support, claim_support_json);
    assert_eq!(
        event_fields[event_copy_column_index(Events::DerivationDeclarationId)],
        declaration_id
    );
    assert_eq!(
        event_fields[event_copy_column_index(Events::DerivationEpochId)].parse::<Uuid>()?,
        epoch_id
    );
    assert_eq!(
        event_fields[event_copy_column_index(Events::DerivationLaneId)].parse::<Uuid>()?,
        lane_id
    );
    assert_eq!(
        event_fields[event_copy_column_index(Events::AdjudicationEventId)].parse::<Uuid>()?,
        adjudication_id
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

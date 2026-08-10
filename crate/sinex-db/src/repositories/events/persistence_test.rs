use super::*;
use crate::repositories::DbPoolExt;
use serde_json::json;
use sinex_primitives::Result;
use sinex_primitives::domain::{EventType, HostName};
use sinex_primitives::events::{EquivalenceKey, EventId};
use std::sync::Arc;
use xtask::sandbox::sinex_test;

fn base_stream_batch_row() -> Result<StreamBatchRow> {
    Ok(StreamBatchRow {
        id: Uuid::now_v7(),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.event")?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        source_material_id: None,
        anchor_byte: None,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        associated_blob_ids: None,
        anchor_payload_hash: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
        content_hash: None,
    })
}

fn base_record() -> EventRecord {
    let ts = Timestamp::now();
    let subnano = ts.nanosecond() as i32;
    EventRecord {
        id: uuid::Uuid::now_v7(),
        source: "test.source".to_string(),
        event_type: "test.event".to_string(),
        host: "localhost".to_string(),
        payload: json!({"ok": true}),
        ts_orig: ts,
        ts_orig_subnano: Some(subnano),
        ts_coided: Timestamp::now(),
        ts_persisted: Timestamp::now(),
        source_material_id: None,
        anchor_byte: None,
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        anchor_payload_hash: None,
        associated_blob_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
        content_hash: None,
    }
}

#[sinex_test]
async fn missing_provenance_is_rejected() -> Result<()> {
    let record = base_record();
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("missing provenance"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_requires_anchor() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("anchor"));
    Ok(())
}

#[sinex_test]
async fn valid_material_provenance_passes() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    record.anchor_byte = Some(42);
    assert!(record.try_to_event().is_ok());
    Ok(())
}

#[sinex_test]
async fn invalid_material_offset_kind_is_rejected() -> Result<()> {
    let mut record = base_record();
    record.source_material_id = Some(uuid::Uuid::now_v7());
    record.anchor_byte = Some(42);
    record.offset_kind = Some("mystery".to_string());
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("invalid offset kind"));
    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_requires_non_empty_sources() -> Result<()> {
    let mut record = base_record();
    record.source_event_ids = Some(vec![]);
    let err = record.try_to_event().expect_err("should fail");
    assert!(format!("{err}").contains("source_event_ids"));
    Ok(())
}

#[sinex_test]
async fn synthesis_operation_lineage_round_trips_from_record() -> Result<()> {
    let mut record = base_record();
    let parent_id = uuid::Uuid::now_v7();
    let operation_id = uuid::Uuid::now_v7();
    record.source_event_ids = Some(vec![parent_id]);
    record.created_by_operation_id = Some(operation_id);

    let event = record.try_to_event()?;

    match &event.provenance {
        crate::models::Provenance::Derived {
            source_event_ids,
            operation_id: provenance_operation_id,
        } => {
            assert_eq!(
                source_event_ids.as_slice(),
                &[sinex_primitives::events::EventId::from_uuid(parent_id)]
            );
            assert_eq!(
                provenance_operation_id.as_ref().map(Id::to_uuid),
                Some(operation_id)
            );
        }
        other => panic!("expected derived provenance, got {other:?}"),
    }
    assert_eq!(event.created_by_operation_id, Some(operation_id));

    Ok(())
}

#[sinex_test]
async fn mismatched_operation_lineage_is_rejected() -> Result<()> {
    let parent_id = Id::<Event<JsonValue>>::new();
    let provenance_operation_id = Id::<sinex_primitives::events::builder::OperationMarker>::new();
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.event")?,
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        module_run_id: None,
        payload_schema_id: None,
        provenance: crate::models::Provenance::from_derived([
            sinex_primitives::events::EventId::from_uuid(parent_id.to_uuid()),
        ])
        .expect("single parent should produce derived provenance")
        .with_operation(provenance_operation_id),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: Some(uuid::Uuid::now_v7()),
        automaton_model: None,
        anchor_payload_hash: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };

    let err = resolved_created_by_operation_id(&event).expect_err("should fail");
    assert!(format!("{err}").contains("operation lineage mismatch"));

    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_query_builder_for_small_material_batches()
-> Result<()> {
    let batch = vec![base_stream_batch_row()?];
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::QueryBuilder)
    );
    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_copy_for_large_material_batches() -> Result<()> {
    let batch = (0..COPY_BATCH_THRESHOLD)
        .map(|_| base_stream_batch_row())
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::Copy)
    );
    Ok(())
}

#[sinex_test]
async fn stream_batch_insert_strategy_prefers_synthesis_for_parent_batches() -> Result<()> {
    let mut row = base_stream_batch_row()?;
    row.source_event_ids = Some(vec![EventId::from_uuid(Uuid::now_v7())]);
    let batch = vec![row];
    assert_eq!(
        EventRepository::stream_batch_insert_strategy(&batch),
        Some(StreamBatchInsertStrategy::Derived)
    );
    Ok(())
}

#[sinex_test]
async fn event_storage_lane_targets_are_explicit() -> Result<()> {
    assert_eq!(EventStorageLane::Activity.table_name(), "core.events");
    assert_eq!(
        EventStorageLane::Reflection.table_name(),
        "reflection.events"
    );
    Ok(())
}

#[sinex_test]
async fn derived_insert_rejects_non_live_parent(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let missing_parent = Id::<Event<JsonValue>>::new();
    let provenance =
        crate::models::Provenance::from_derived([EventId::from_uuid(missing_parent.to_uuid())])
            .expect("expected derived provenance");
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.source")?,
        event_type: EventType::new("test.derived")?,
        host: HostName::from_static("localhost"),
        payload: json!({"derived": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        module_run_id: None,
        payload_schema_id: None,
        provenance,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        anchor_payload_hash: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };

    let error = match EventRepository::new(ctx.pool()).insert(event).await {
        Ok(_) => {
            panic!("derived insert unexpectedly accepted a missing parent");
        }
        Err(error) => error,
    };

    assert!(
        format!("{error}").contains("non-live source_event_ids"),
        "unexpected error: {error}"
    );

    Ok(())
}

#[sinex_test]
async fn copy_staging_probe_avoids_repeated_create_notice() -> Result<()> {
    let probe_sql = EventRepository::copy_staging_exists_sql();
    assert!(
        probe_sql.contains("pg_temp.sinex_batch_staging"),
        "staging-table probe must be scoped to the current connection's temp schema"
    );

    let create_sql = EventRepository::copy_staging_create_sql("id UUID");
    assert!(
        !create_sql.contains("IF NOT EXISTS"),
        "COPY staging setup must not use CREATE IF NOT EXISTS; PostgreSQL emits a NOTICE \
             on every reused temp table and sqlx forwards it to journald"
    );
    assert!(
        create_sql.contains("CREATE TEMP TABLE sinex_batch_staging"),
        "COPY staging table should still be a connection-local temporary table"
    );

    Ok(())
}

/// Drift guard: the column set used in the `query_as!` single-insert sites
/// must equal the COPY-contract column set (`EVENT_COPY_COLUMNS`). The
/// `query_as!` macro verifies individual column types at compile time but
/// does NOT verify that every column in the COPY contract appears in the
/// VALUES list, so set-drift can go undetected until runtime. This test
/// catches that by asserting the same names appear in both. (#1575)
#[sinex_test]
async fn query_as_insert_columns_match_copy_contract() -> Result<()> {
    // These are the 30 columns listed in both `insert` and `insert_with_tx`
    // `query_as!` sites. If those sites ever gain or lose a column, this
    // constant must be updated — which forces an explicit review of whether
    // EVENT_COPY_COLUMNS was updated too.
    let query_as_columns: std::collections::BTreeSet<String> = [
        "id",
        "source",
        "event_type",
        "host",
        "payload",
        "ts_orig",
        "ts_orig_subnano",
        "ts_quality",
        "module_run_id",
        "payload_schema_id",
        "source_event_ids",
        "source_material_id",
        "offset_start",
        "offset_end",
        "offset_kind",
        "anchor_byte",
        "associated_blob_ids",
        "temporal_policy",
        "semantics_version",
        "scope_key",
        "equivalence_key",
        "created_by_operation_id",
        "automaton_model",
        "anchor_payload_hash",
        // Derivation control plane (sinex-0vx.4 / W1): explicit NULL in both
        // query_as! sites (see the comments there) — not yet settable via
        // this write path, but must appear in the INSERT column list to stay
        // in lockstep with EVENT_COPY_COLUMNS (this test's whole point).
        "product_class",
        "claim_support",
        "derivation_declaration_id",
        "derivation_epoch_id",
        "derivation_lane_id",
        "adjudication_event_id",
        // sinex-w1w7: admission-time content hash.
        "content_hash",
    ]
    .iter()
    .map(std::string::ToString::to_string)
    .collect();

    // Parse the SSOT column list from EVENT_COPY_COLUMNS.
    let copy_sql = event_copy_column_list_sql();
    let copy_columns: std::collections::BTreeSet<String> =
        copy_sql.split(", ").map(|s| s.trim().to_string()).collect();

    // BTreeSet comparison gives a sorted diff in the assertion message.
    assert_eq!(
        query_as_columns, copy_columns,
        "query_as! single-insert column set diverges from EVENT_COPY_COLUMNS (the SSOT). \
             Add the missing column to both the query_as! INSERT and EVENT_COPY_COLUMNS, \
             or remove it from both."
    );
    Ok(())
}

/// Register a `derivation.product_declarations` row so
/// `derivation.enforce_event_product_declaration()` accepts a test event that
/// declares `product_class`. The `sinexd` startup reconciler
/// (`sinexd::automata::product_declarations`, sinex-x79t) only runs from
/// `Supervisor::run`, not inside this sandbox, so tests below the `sinexd`
/// crate that persist a non-null `product_class` still seed their own row.
async fn seed_product_declaration(
    pool: &sqlx::PgPool,
    declaration_id: &str,
    product_class: sinex_primitives::derivation::DerivedProductClass,
    output_source: &str,
    output_event_type: &str,
) -> Result<()> {
    sqlx::query!(
        r#"
        INSERT INTO derivation.product_declarations (
            declaration_id, owner, product_class, write_surface,
            output_source, output_event_type, semantics_version,
            input_eligibility, default_claim_support, verification_command
        ) VALUES (
            $1, 'sinex-8cr.2-test', $2, 'derived_output',
            $3, $4, 'v1', 'default_canonical_input', '{}'::jsonb, 'true'
        )
        ON CONFLICT (declaration_id) DO NOTHING
        "#,
        declaration_id,
        product_class.as_str(),
        output_source,
        output_event_type,
    )
    .execute(pool)
    .await
    .map_err(|e| sinex_primitives::SinexError::database("seed product declaration").with_source(e))?;
    Ok(())
}

/// Round-trip proof for the "Single insert" + "EventRecord, conversion" AC
/// lines (sinex-8cr.2): a concrete non-default `product_class`/`claim_support`
/// survives `EventRepository::insert` unchanged, both in the `RETURNING`
/// value and in an independent read-back via `get_by_id`. Before this bead,
/// `insert`/`insert_with_tx` bound `None::<..>` unconditionally for these six
/// columns regardless of what the caller set on `Event<T>` — this test fails
/// against that code (the fields silently come back `None`).
#[sinex_test]
async fn event_product_metadata_persists(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    use sinex_primitives::derivation::{
        ClaimSupport, ClaimTemporalQuality, DerivedProductClass, SourceCoverage, SupportLevel,
    };

    let material_id = ctx
        .create_source_material(Some("event-product-metadata-material"))
        .await?;

    let declaration_id = "sinex.test.event_product_metadata_persists";
    let product_class = DerivedProductClass::CanonicalDerivedEvent;
    seed_product_declaration(
        ctx.pool(),
        declaration_id,
        product_class,
        "test.product.single",
        "test.event.product_metadata_persists",
    )
    .await?;

    let claim_support = ClaimSupport::unreviewed(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        2,
        1,
        1,
        0,
    );

    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("test.product.single")?,
        event_type: EventType::new("test.event.product_metadata_persists")?,
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        module_run_id: None,
        payload_schema_id: None,
        provenance: crate::models::Provenance::from_material(material_id, 0, None, None),
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        product_class: Some(product_class),
        claim_support: Some(claim_support.clone()),
        derivation_declaration_id: Some(declaration_id.to_string()),
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    };

    let inserted = EventRepository::new(ctx.pool()).insert(event).await?;
    let event_id = inserted.id.expect("inserted event should have id");

    assert_eq!(inserted.product_class, Some(product_class));
    assert_eq!(inserted.claim_support.as_ref(), Some(&claim_support));
    assert_eq!(
        inserted.derivation_declaration_id.as_deref(),
        Some(declaration_id)
    );
    assert_eq!(inserted.derivation_epoch_id, None);
    assert_eq!(inserted.derivation_lane_id, None);
    assert_eq!(inserted.adjudication_event_id, None);

    let retrieved = ctx
        .pool()
        .events()
        .get_by_id(event_id)
        .await?
        .expect("event should be retrievable by id");
    assert_eq!(retrieved.product_class, Some(product_class));
    assert_eq!(retrieved.claim_support.as_ref(), Some(&claim_support));
    assert_eq!(
        retrieved.derivation_declaration_id.as_deref(),
        Some(declaration_id)
    );

    Ok(())
}

/// Round-trip proof for the "QueryBuilder batch" and "stream batch" AC lines
/// (sinex-8cr.2), each a separate `QueryBuilder`-VALUES construction site:
/// `insert_batch` (the `Vec<Event<T>>` API/replay path,
/// `insert_batch_unnest_in_tx`) and `insert_stream_batch` (the `StreamBatchRow`
/// event_engine ingestion path, `execute_batch_insert`). Both batches carry
/// two rows with *different* `product_class` values plus a null-`product_class`
/// row — proof against an off-by-one/wrong-row bind-order bug that a
/// single-row test would not catch.
#[sinex_test]
async fn batch_product_metadata(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    use sinex_primitives::derivation::{
        ClaimSupport, ClaimTemporalQuality, DerivedProductClass, SourceCoverage, SupportLevel,
    };

    let claim_support = ClaimSupport::unreviewed(
        SupportLevel::Convergent,
        SourceCoverage::Partial,
        ClaimTemporalQuality::InferredMtime,
        4,
        2,
        1,
        1,
    );

    // --- QueryBuilder batch path: insert_batch -> insert_batch_unnest_in_tx ---
    let material_id = ctx
        .create_source_material(Some("batch-product-metadata-material"))
        .await?;

    seed_product_declaration(
        ctx.pool(),
        "sinex.test.batch_metadata.canonical",
        DerivedProductClass::CanonicalDerivedEvent,
        "test.product.batch",
        "test.event.batch.canonical",
    )
    .await?;
    seed_product_declaration(
        ctx.pool(),
        "sinex.test.batch_metadata.claim",
        DerivedProductClass::AnalysisClaim,
        "test.product.batch",
        "test.event.batch.claim",
    )
    .await?;

    let make_event = |event_type: &str,
                       product_class: Option<DerivedProductClass>,
                       declaration_id: Option<&str>|
     -> Result<Event<JsonValue>> {
        Ok(Event {
            id: Some(Id::new()),
            source: EventSource::new("test.product.batch")?,
            event_type: EventType::new(event_type)?,
            host: HostName::from_static("localhost"),
            payload: json!({"ok": true}),
            ts_orig: Some(Timestamp::now()),
            ts_quality: None,
            module_run_id: None,
            payload_schema_id: None,
            provenance: crate::models::Provenance::from_material(material_id, 0, None, None),
            anchor_payload_hash: None,
            associated_blob_ids: None,
            temporal_policy: None,
            semantics_version: None,
            scope_key: None,
            equivalence_key: None,
            created_by_operation_id: None,
            automaton_model: None,
            product_class,
            claim_support: product_class.map(|_| claim_support.clone()),
            derivation_declaration_id: declaration_id.map(str::to_string),
            derivation_epoch_id: None,
            derivation_lane_id: None,
            adjudication_event_id: None,
        })
    };

    let events = vec![
        make_event(
            "test.event.batch.canonical",
            Some(DerivedProductClass::CanonicalDerivedEvent),
            Some("sinex.test.batch_metadata.canonical"),
        )?,
        make_event(
            "test.event.batch.claim",
            Some(DerivedProductClass::AnalysisClaim),
            Some("sinex.test.batch_metadata.claim"),
        )?,
        make_event("test.event.batch.none", None, None)?,
    ];
    let expected: Vec<(Id<Event<JsonValue>>, Option<DerivedProductClass>)> = events
        .iter()
        .map(|e| (e.id.expect("test event has id"), e.product_class))
        .collect();

    let inserted = EventRepository::new(ctx.pool()).insert_batch(events).await?;
    assert_eq!(inserted.len(), expected.len());

    for (id, expected_class) in &expected {
        let row = inserted
            .iter()
            .find(|e| e.id.as_ref() == Some(id))
            .unwrap_or_else(|| panic!("insert_batch result missing event {id:?}"));
        assert_eq!(
            row.product_class, *expected_class,
            "insert_batch product_class mismatch for event {id:?}"
        );

        let retrieved = ctx
            .pool()
            .events()
            .get_by_id(*id)
            .await?
            .unwrap_or_else(|| panic!("batch event {id:?} should be retrievable by id"));
        assert_eq!(
            retrieved.product_class, *expected_class,
            "persisted product_class mismatch for batch event {id:?}"
        );
    }

    // --- Stream batch path: insert_stream_batch -> execute_batch_insert ---
    let stream_material_id = ctx
        .create_source_material(Some("batch-product-metadata-stream-material"))
        .await?;

    seed_product_declaration(
        ctx.pool(),
        "sinex.test.batch_metadata.stream.canonical",
        DerivedProductClass::CanonicalDerivedEvent,
        "test.product.batch.stream",
        "test.event.batch.stream.canonical",
    )
    .await?;
    seed_product_declaration(
        ctx.pool(),
        "sinex.test.batch_metadata.stream.claim",
        DerivedProductClass::AnalysisClaim,
        "test.product.batch.stream",
        "test.event.batch.stream.claim",
    )
    .await?;

    let make_stream_row = |event_type: &str,
                            product_class: Option<DerivedProductClass>,
                            declaration_id: Option<&str>|
     -> Result<StreamBatchRow> {
        Ok(StreamBatchRow {
            id: Uuid::now_v7(),
            source: EventSource::new("test.product.batch.stream")?,
            event_type: EventType::new(event_type)?,
            ts_orig: Timestamp::now(),
            ts_quality: None,
            host: HostName::from_static("localhost"),
            payload: json!({"ok": true}),
            source_material_id: Some(stream_material_id),
            anchor_byte: Some(0),
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
            product_class: product_class.map(|p| p.to_string()),
            claim_support: product_class
                .map(|_| serde_json::to_value(&claim_support))
                .transpose()
                .map_err(|e| {
                    sinex_primitives::SinexError::database("serialize test claim_support")
                        .with_source(e)
                })?,
            derivation_declaration_id: declaration_id.map(str::to_string),
            derivation_epoch_id: None,
            derivation_lane_id: None,
            adjudication_event_id: None,
            content_hash: None,
        })
    };

    let stream_specs = [
        (
            "test.event.batch.stream.canonical",
            Some(DerivedProductClass::CanonicalDerivedEvent),
            Some("sinex.test.batch_metadata.stream.canonical"),
        ),
        (
            "test.event.batch.stream.claim",
            Some(DerivedProductClass::AnalysisClaim),
            Some("sinex.test.batch_metadata.stream.claim"),
        ),
    ];
    let mut stream_rows = Vec::new();
    let mut stream_expected: Vec<(Uuid, Option<DerivedProductClass>)> = Vec::new();
    for (event_type, product_class, declaration_id) in stream_specs {
        let row = make_stream_row(event_type, product_class, declaration_id)?;
        stream_expected.push((row.id, product_class));
        stream_rows.push(row);
    }

    let stream_result = EventRepository::new(ctx.pool())
        .insert_stream_batch(&stream_rows)
        .await?;
    assert_eq!(stream_result.inserted_count, stream_rows.len());

    for (uuid, expected_class) in stream_expected {
        let retrieved = ctx
            .pool()
            .events()
            .get_by_id(Id::from_uuid(uuid))
            .await?
            .unwrap_or_else(|| panic!("stream batch event {uuid} should be retrievable by id"));
        assert_eq!(
            retrieved.product_class, expected_class,
            "persisted product_class mismatch for stream batch event {uuid}"
        );
    }

    Ok(())
}

/// Reproduces sinex-audit-equivrace: two writers admitting the same
/// occurrence (shared `equivalence_key`) concurrently, neither having
/// observed the other's live row yet, both calling `insert_stream_batch_into`
/// at (as close as `tokio::sync::Barrier` can force) the same instant.
///
/// This is the actual physical conflict point, not a simulation of it:
/// admission's `resolve_equivalence` check (`sinexd::event_engine::admission`)
/// runs well before a batch reaches this function, so two concurrent
/// admissions racing the same key both legitimately observe "no live row"
/// upstream and both arrive here believing they are `Fresh` — exactly what
/// this test drives directly, skipping the upstream admission layer
/// entirely so the test exercises the real DB-level race deterministically
/// rather than depending on scheduling luck across the whole pipeline.
///
/// Before `lock_and_filter_equivalence_conflicts` (added by this bead), both
/// inserts would land: `core.events` is a `TimescaleDB` hypertable and
/// cannot carry a unique index on `equivalence_key` alone (only indexes that
/// include the partition key `id` are permitted, and `id` differs by
/// definition between the two racers), so nothing at the DB layer rejected
/// the second writer. This test asserts the invariant directly: exactly one
/// of the two racing rows ends up live, never two, never zero.
#[sinex_test]
async fn concurrent_equivalence_key_race_yields_exactly_one_live_row(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("equivrace-race-test"))
        .await?;
    let key = format!("equivrace-{}", Uuid::now_v7());

    let mut row_a = base_stream_batch_row()?;
    row_a.source_material_id = Some(material_id);
    row_a.anchor_byte = Some(0);
    row_a.equivalence_key = Some(EquivalenceKey::new(key.clone()));

    let mut row_b = base_stream_batch_row()?;
    row_b.source_material_id = Some(material_id);
    row_b.anchor_byte = Some(1);
    row_b.equivalence_key = Some(EquivalenceKey::new(key.clone()));

    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let pool_a = ctx.pool.clone();
    let barrier_a = Arc::clone(&barrier);
    let handle_a = tokio::spawn(async move {
        barrier_a.wait().await;
        pool_a
            .events()
            .insert_stream_batch_into(EventStorageLane::Activity, std::slice::from_ref(&row_a))
            .await
    });

    let pool_b = ctx.pool.clone();
    let barrier_b = Arc::clone(&barrier);
    let handle_b = tokio::spawn(async move {
        barrier_b.wait().await;
        pool_b
            .events()
            .insert_stream_batch_into(EventStorageLane::Activity, std::slice::from_ref(&row_b))
            .await
    });

    let result_a = handle_a
        .await
        .map_err(|e| SinexError::unknown(format!("racer A task panicked: {e}")))??;
    let result_b = handle_b
        .await
        .map_err(|e| SinexError::unknown(format!("racer B task panicked: {e}")))??;

    let total_inserted = result_a.inserted_count + result_b.inserted_count;
    assert_eq!(
        total_inserted, 1,
        "exactly one of the two racing writers should win the equivalence_key \
         occurrence; got {total_inserted} inserted (result_a={result_a:?}, \
         result_b={result_b:?})"
    );

    let live_count = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE equivalence_key = $1",
        key
    )
    .fetch_one(ctx.pool())
    .await
    .map_err(|e| SinexError::database("count live rows for equivalence_key").with_source(e))?
    .unwrap_or(0);

    assert_eq!(
        live_count, 1,
        "expected exactly one live core.events row for the raced equivalence_key, found {live_count}"
    );

    Ok(())
}

/// sinex-f8v: equivalence-key dedupe must resolve against the SAME physical
/// lane the candidate event belongs to. Before the fix,
/// `find_live_by_equivalence_key`/`find_live_by_equivalence_keys` were
/// hardcoded to `core.events`, so a reflection-lane occurrence's live row
/// (in `reflection.events`) was invisible to the lookup and every replayed
/// reflection event resolved `Fresh` forever, duplicating without bound.
///
/// This test inserts a reflection-lane row carrying an `equivalence_key` and
/// proves the repository lookup — called with `EventStorageLane::Reflection`
/// — actually finds it. Changing `find_live_by_equivalence_key`'s hardcoded
/// table back to `core.events` (or removing the `lane` parameter) makes this
/// assertion fail with `None` instead of `Some`.
#[sinex_test]
async fn reflection_lane_equivalence_key_dedupe_is_scoped_to_reflection_events(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("f8v-reflection-equiv-test"))
        .await?;
    let key = format!("f8v-reflection-{}", Uuid::now_v7());

    let mut row = base_stream_batch_row()?;
    row.source_material_id = Some(material_id);
    row.anchor_byte = Some(0);
    row.equivalence_key = Some(EquivalenceKey::new(key.clone()));

    let inserted = ctx
        .pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Reflection, std::slice::from_ref(&row))
        .await?;
    assert_eq!(inserted.inserted_count, 1);

    let found = ctx
        .pool()
        .events()
        .find_live_by_equivalence_key(&key, EventStorageLane::Reflection)
        .await?;
    assert_eq!(
        found.map(|r| r.id),
        Some(row.id),
        "reflection-lane equivalence_key lookup must find the reflection.events \
         row, not report it missing"
    );

    let found_via_batch = ctx
        .pool()
        .events()
        .find_live_by_equivalence_keys(&[key.clone()], EventStorageLane::Reflection)
        .await?;
    assert_eq!(
        found_via_batch.into_iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![row.id],
        "batch form must likewise resolve against reflection.events"
    );

    // A same-keyed lookup scoped to the WRONG lane must not see this row —
    // dedup is a same-lane concept (sinex-f8v cross-lane policy).
    let found_wrong_lane = ctx
        .pool()
        .events()
        .find_live_by_equivalence_key(&key, EventStorageLane::Activity)
        .await?;
    assert!(
        found_wrong_lane.is_none(),
        "activity-lane lookup must not see a reflection-lane row sharing the same key"
    );

    Ok(())
}

/// sinex-f8v: activity-lane equivalence-key dedupe behavior is unchanged by
/// threading `lane` through — this is the AC's explicit "activity-lane
/// behavior unchanged" requirement, proven directly rather than assumed.
#[sinex_test]
async fn activity_lane_equivalence_key_dedupe_still_works(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("f8v-activity-equiv-test"))
        .await?;
    let key = format!("f8v-activity-{}", Uuid::now_v7());

    let mut row = base_stream_batch_row()?;
    row.source_material_id = Some(material_id);
    row.anchor_byte = Some(0);
    row.equivalence_key = Some(EquivalenceKey::new(key.clone()));

    ctx.pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Activity, std::slice::from_ref(&row))
        .await?;

    let found = ctx
        .pool()
        .events()
        .find_live_by_equivalence_key(&key, EventStorageLane::Activity)
        .await?;
    assert_eq!(found.map(|r| r.id), Some(row.id));

    Ok(())
}

/// sinex-f8v cross-lane parent policy, positive case: a reflection-derived
/// event may reference a LIVE REFLECTION-LANE parent. Before the fix,
/// `ensure_source_event_ids_are_live` always checked `core.events`, so this
/// legitimate same-lane reference was falsely rejected as "non-live" for
/// every reflection-derived event with reflection-lane parents.
#[sinex_test]
async fn reflection_derived_insert_accepts_live_reflection_parent(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("f8v-reflection-parent-test"))
        .await?;

    let mut parent_row = base_stream_batch_row()?;
    parent_row.source_material_id = Some(material_id);
    parent_row.anchor_byte = Some(0);
    let parent_id = parent_row.id;

    ctx.pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Reflection, std::slice::from_ref(&parent_row))
        .await?;

    // Every derived event (source_event_ids set) must declare a
    // product_class backed by a registered `derivation.product_declarations`
    // row, and a declared product_class requires claim_support — unrelated
    // to sinex-f8v, but required by the schema's derivation control-plane
    // enforcement for ANY derived insert to succeed.
    let declaration_id = "sinex-f8v-test.reflection_derived_insert_accepts_live_reflection_parent";
    seed_product_declaration(
        ctx.pool(),
        declaration_id,
        sinex_primitives::derivation::DerivedProductClass::CanonicalDerivedEvent,
        "test.source",
        "test.event",
    )
    .await?;

    let mut derived_row = base_stream_batch_row()?;
    derived_row.source_event_ids = Some(vec![EventId::from_uuid(parent_id)]);
    derived_row.product_class = Some("canonical_derived_event".to_string());
    derived_row.claim_support = Some(json!({}));
    derived_row.derivation_declaration_id = Some(declaration_id.to_string());

    let result = ctx
        .pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Reflection, std::slice::from_ref(&derived_row))
        .await?;
    assert_eq!(
        result.inserted_count, 1,
        "reflection-derived event must accept its live reflection-lane parent"
    );

    Ok(())
}

/// sinex-f8v cross-lane parent policy, negative case: a reflection-derived
/// event referencing a parent that only exists in `core.events` (the
/// activity lane) must be explicitly rejected, not accidentally
/// misclassified as live via the wrong table. This is the "cross-lane
/// parent policy is explicit, tested, and documented" half of the AC — the
/// chosen policy is same-lane-only parents.
#[sinex_test]
async fn reflection_derived_insert_rejects_core_lane_parent(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("f8v-cross-lane-parent-test"))
        .await?;

    let mut activity_parent_row = base_stream_batch_row()?;
    activity_parent_row.source_material_id = Some(material_id);
    activity_parent_row.anchor_byte = Some(0);
    let activity_parent_id = activity_parent_row.id;

    ctx.pool()
        .events()
        .insert_stream_batch_into(
            EventStorageLane::Activity,
            std::slice::from_ref(&activity_parent_row),
        )
        .await?;

    let mut derived_row = base_stream_batch_row()?;
    derived_row.source_event_ids = Some(vec![EventId::from_uuid(activity_parent_id)]);

    let error = ctx
        .pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Reflection, std::slice::from_ref(&derived_row))
        .await
        .expect_err(
            "reflection-derived event referencing an activity-lane-only parent must be rejected",
        );

    assert!(
        format!("{error}").contains("non-live source_event_ids"),
        "unexpected error: {error}"
    );

    Ok(())
}

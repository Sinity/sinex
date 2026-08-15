use super::*;
use crate::models::Blob;
use crate::repositories::DbPoolExt;
use crate::repositories::events::{EventStorageLane, StreamBatchRow};
use serde_json::json;
use sinex_primitives::domain::{EventType, HostName, ModuleKind, ModuleName};
use std::sync::Arc;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn realtime_capture_uses_typed_byte_offset_kind() -> ::xtask::sandbox::TestResult<()> {
    let entry = TemporalLedgerEntry::realtime_capture(uuid::Uuid::now_v7(), 42, Timestamp::now());

    assert_eq!(entry.offset_kind, OffsetKind::Byte);
    Ok(())
}

#[sinex_test]
async fn source_readiness_rejects_failed_runtime_with_historical_output(
    ctx: xtask::sandbox::TestContext,
) -> ::xtask::sandbox::TestResult<()> {
    let source = "hdnv-source-readiness";
    let material_id = ctx.create_source_material(Some(source)).await?;
    sqlx::query!(
        "UPDATE raw.source_material_registry SET source_identifier = $2 WHERE id = $1",
        material_id.to_uuid(),
        source,
    )
    .execute(ctx.pool())
    .await?;
    let manifest = ctx
        .pool
        .state()
        .register_module(
            &ModuleName::new(source),
            ModuleKind::Source,
            "test",
            Some("source readiness regression"),
        )
        .await?;
    let run = ctx
        .pool
        .state()
        .start_module_run(
            manifest.id,
            "source-readiness-test",
            "failed-instance",
            "test-host",
            None,
            None,
        )
        .await?;

    let mut output = race_test_event_row(material_id.to_uuid())?;
    output.module_run_id = Some(run.id.to_uuid());
    ctx.pool()
        .events()
        .insert_stream_batch_into(EventStorageLane::Activity, std::slice::from_ref(&output))
        .await?;

    let failed_at = time::OffsetDateTime::now_utc() - time::Duration::hours(2);
    sqlx::query!(
        r#"
        UPDATE core.runs
        SET status = 'failed', started_at = $2, last_heartbeat_at = $2
        WHERE id = $1
        "#,
        run.id.to_uuid(),
        failed_at,
    )
    .execute(ctx.pool())
    .await?;

    let readiness = ctx
        .pool()
        .source_materials()
        .get_source_readiness(source, None, Some(300))
        .await?
        .expect("registered source should have a readiness row");

    assert_eq!(
        readiness.status,
        SourceReadinessStatus::Error,
        "historical material/output must not mask a failed current source run"
    );
    assert_eq!(readiness.evidence["runtime"]["run_status"], "failed");
    assert_eq!(
        readiness.evidence["runtime"]["liveness_status"],
        "Unhealthy"
    );
    Ok(())
}

fn race_test_blob(checksum: &str) -> Blob {
    Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash(format!("content-{checksum}"))
        .original_filename("tombstonerace.bin".to_string())
        .size_bytes(4)
        .mime_type("application/octet-stream".to_string())
        .checksum_blake3(checksum.to_string())
        .build()
}

fn race_test_event_row(material_uuid: uuid::Uuid) -> sinex_primitives::Result<StreamBatchRow> {
    Ok(StreamBatchRow {
        id: Uuid::now_v7(),
        source: sinex_primitives::domain::EventSource::new("test.source")?,
        event_type: EventType::new("test.event")?,
        ts_orig: Timestamp::now(),
        host: HostName::from_static("localhost"),
        payload: json!({"ok": true}),
        source_material_id: Some(material_uuid.into()),
        anchor_byte: Some(0),
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

/// Reproduces the delete-on-tombstone material-row TOCTOU race
/// (sinex-audit-tombstonerace) and asserts the fix
/// (`delete_material_if_orphan` baking the orphan recheck into the DELETE
/// statement) closes it.
///
/// Racer A runs the exact call `handle_tombstone_approve`
/// (`crate::sinexd::api::handlers::lifecycle`) now makes:
/// `materials_repo.delete_material_if_orphan(material_id)`. Racer B is the
/// real live write path that used to race the old drop-then-delete ordering:
/// a concurrent `core.events` insert with `source_material_id` pointing at
/// the same material -- exactly what a slow-arriving multi-slice material
/// event, or a JetStream redelivery that was still pending `MaterialReadySet`
/// at scan time, looks like.
///
/// Before the fix, `find_orphan_materials` (a stale recheck) was followed
/// much later by an unconditional `content_store.drop_content` and only then
/// an unconditional `delete_material`: a redelivered event landing in that
/// window left the CAS blob dropped even though the material row survived
/// (correctly blocked by the real `core.events.source_material_id` FK) with a
/// live event still pointing at it.
///
/// With the fix, the orphan recheck is baked into the same DELETE statement
/// that removes the row, so a caller only learns it is safe to drop the
/// associated blob when the row is actually confirmed gone. This test
/// asserts the only two possible outcomes are both safe:
/// - Racer B's insert lands first (or wins the race): racer A's delete finds
///   a live reference and deletes nothing -- the row AND its attached blob
///   survive untouched.
/// - Racer A's delete lands first: the row is gone, and the real
///   `core.events.source_material_id` foreign key then rejects racer B's
///   insert outright (the production admission pipeline NAKs/retries/DLQs
///   this case via `MaterialReadySet`) -- never a live event silently
///   pointing at a row (or blob) that no longer exists.
#[sinex_test]
async fn concurrent_event_insert_race_never_orphans_deleted_material(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool.clone();

    let material_id = ctx
        .create_source_material(Some("tombstonerace-material"))
        .await?;
    let material_uuid = material_id.to_uuid();

    // Attach a CAS blob to the material, mirroring what delete-on-tombstone
    // inspects via `optional_blob_id` before deciding whether to drop content.
    let checksum = format!("blake3-{}", uuid::Uuid::now_v7());
    let blob = pool.blobs().insert(race_test_blob(&checksum)).await?;
    let blob_uuid = blob.id.to_uuid();
    sqlx::query!(
        "UPDATE raw.source_material_registry SET optional_blob_id = $1 WHERE id = $2",
        blob_uuid,
        material_uuid
    )
    .execute(&pool)
    .await
    .map_err(|e| SinexError::database("attach blob to material").with_source(e))?;

    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    // Racer A: the fixed delete-on-tombstone material delete.
    let pool_a = pool.clone();
    let barrier_a = Arc::clone(&barrier);
    let handle_a = tokio::spawn(async move {
        barrier_a.wait().await;
        pool_a
            .source_materials()
            .delete_material_if_orphan(sinex_primitives::Id::from_uuid(material_uuid))
            .await
    });

    // Racer B: a redelivered/slow-arriving event referencing the same
    // material via the real stream-batch insert path.
    let pool_b = pool.clone();
    let barrier_b = Arc::clone(&barrier);
    let handle_b = tokio::spawn(async move {
        barrier_b.wait().await;
        let row = race_test_event_row(material_uuid)?;
        pool_b
            .events()
            .insert_stream_batch_into(EventStorageLane::Activity, std::slice::from_ref(&row))
            .await
    });

    let delete_outcome = handle_a.await.map_err(|e| {
        SinexError::unknown(format!("racer A (delete-on-tombstone) panicked: {e}"))
    })??;
    let insert_result = handle_b
        .await
        .map_err(|e| SinexError::unknown(format!("racer B (redelivered event) panicked: {e}")))?;

    let material_row_exists = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM raw.source_material_registry WHERE id = $1) AS "exists!""#,
        material_uuid
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database("check material row survived").with_source(e))?;

    let blob_row_exists = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM core.blobs WHERE id = $1) AS "exists!""#,
        blob_uuid
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database("check blob row survived").with_source(e))?;

    match (delete_outcome, insert_result.is_ok()) {
        (None, _) => {
            // Racer A found a live (or about-to-be-live) reference and
            // deleted nothing. The row -- and, critically, its attached blob,
            // which no caller should have touched -- must both survive.
            assert!(
                material_row_exists,
                "delete_material_if_orphan returned None (nothing deleted) but the \
                 material row is gone"
            );
            assert!(
                blob_row_exists,
                "material row survived a blocked delete, but its attached blob was \
                 dropped anyway -- this is exactly the sinex-audit-tombstonerace bug"
            );
        }
        (Some(_), true) => {
            panic!(
                "impossible outcome: delete_material_if_orphan deleted the material \
                 row AND the concurrent event insert (which requires that same row \
                 to exist via the core.events.source_material_id foreign key) also \
                 succeeded -- a live event now points at a deleted material row"
            );
        }
        (Some(_), false) => {
            // Racer A's delete won: the row (and, by extension, its blob) is
            // confirmed truly orphaned, so it's now safe for the caller to
            // drop the blob content. Racer B's insert correctly failed
            // against the real FK instead of silently landing a dangling
            // reference.
            assert!(
                !material_row_exists,
                "delete_material_if_orphan reported a deletion but the row is still \
                 present"
            );
        }
    }

    Ok(())
}

use super::*;
use crate::models::SourceMaterial;
use crate::repositories::DbPoolExt;
use serde_json::json;
use std::sync::Arc;
use xtask::sandbox::sinex_test;

fn race_test_blob(checksum: &str) -> Blob {
    Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash(format!("content-{checksum}"))
        .original_filename("cas-refcheck-toctou.bin".to_string())
        .size_bytes(4)
        .mime_type("application/octet-stream".to_string())
        .checksum_blake3(checksum.to_string())
        .build()
}

#[sinex_test]
async fn associated_blob_reference_uses_indexed_containment(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let material_id = ctx
        .create_source_material(Some("blob-reference-index-test"))
        .await?;
    let blob = ctx
        .pool
        .blobs()
        .insert(race_test_blob(&format!("index-{}", uuid::Uuid::now_v7())))
        .await?;
    let blob_uuid = blob.id.to_uuid();

    let mut insert = sqlx::QueryBuilder::<sqlx::Postgres>::new(
        "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig, source_material_id, anchor_byte, associated_blob_ids) ",
    );
    insert.push_values(0..200, |mut row, offset| {
        row.push_bind(uuid::Uuid::now_v7())
            .push_bind("blob-reference-test")
            .push_bind("blob.reference")
            .push_bind("test-host")
            .push_bind(json!({"offset": offset}))
            .push_bind(sinex_primitives::Timestamp::now())
            .push_bind(material_id.to_uuid())
            .push_bind(i64::from(offset))
            .push_bind(vec![blob_uuid]);
    });
    insert.build().execute(&ctx.pool).await?;
    sqlx::query("ANALYZE core.events")
        .execute(&ctx.pool)
        .await?;

    let mut conn = ctx.pool.acquire().await?;
    sqlx::query("SET enable_seqscan = OFF")
        .execute(&mut *conn)
        .await?;
    let plan = sqlx::query(
        "EXPLAIN (FORMAT JSON) SELECT id FROM core.events \
         WHERE associated_blob_ids IS NOT NULL
           AND associated_blob_ids @> ARRAY[$1]::uuid[]",
    )
    .bind(blob_uuid)
    .fetch_one(&mut *conn)
    .await?;
    sqlx::query("RESET enable_seqscan")
        .execute(&mut *conn)
        .await?;
    let plan_json: serde_json::Value = sqlx::Row::try_get(&plan, 0)?;
    assert!(
        plan_json
            .to_string()
            .contains("ix_events_associated_blob_ids"),
        "the production containment predicate must use the associated-blob GIN index: {plan_json}"
    );

    assert!(
        ctx.pool
            .blobs()
            .is_referenced_excluding_material(blob.id, uuid::Uuid::now_v7())
            .await?,
        "a live event's associated blob must prevent deletion"
    );

    Ok(())
}

/// Reproduces the delete-on-tombstone TOCTOU race (sinex-audit-cas-refcheck-toctou)
/// and asserts the fix (`lock_by_id_for_update` + recheck + delete inside one
/// transaction) closes it.
///
/// Racer A runs the exact sequence `handle_tombstone_approve`
/// (`crate::sinexd::api::handlers::lifecycle`) runs inside `pool.with_transaction`:
/// lock the blob row `FOR UPDATE`, recheck `is_referenced_excluding_material`,
/// then delete. Racer B is the real live write path that used to race it: a
/// concurrent `INSERT INTO raw.source_material_registry` with `optional_blob_id`
/// pointing at the same blob -- exactly what
/// `ContentStoreManager::check_dedup`'s caller does via
/// `SourceMaterialRegistration::with_blob_id` when a concurrent `stage
/// --with-bytes` call dedups onto this blob's checksum.
///
/// Before the fix, `is_referenced_excluding_material` and `delete_by_id` were
/// two unguarded round-trips: racer B's INSERT could land in the window
/// between them, and `optional_blob_id`'s `ON DELETE SET NULL` FK action would
/// then silently null racer B's brand-new reference with no error once racer A
/// deleted the row -- racer B's material row would end up live, believing it
/// deduped, while the CAS content was already gone.
///
/// With the fix, taking `FOR UPDATE` on the blob row makes the FK's implicit
/// `FOR KEY SHARE` check (run when racer B's INSERT executes) block until
/// racer A's transaction resolves. This test asserts the only two possible
/// outcomes are both safe: either racer A backs off (found the row still
/// referenced under the lock, or lost the row-lock race and found it gone) and
/// the blob survives, or racer A deletes the blob and racer B's INSERT then
/// fails outright with a foreign-key violation -- never a live
/// `raw.source_material_registry` row whose `optional_blob_id` was silently
/// nulled while pointing at already-deleted content.
#[sinex_test]
async fn concurrent_material_registration_race_never_orphans_deleted_blob(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let pool = ctx.pool.clone();

    // The existing material that already references the blob and is the one
    // delete-on-tombstone is (incorrectly, per the race) about to conclude has
    // zero remaining references once it excludes itself.
    let existing_material_id = ctx
        .create_source_material(Some("cas-refcheck-toctou-existing"))
        .await?;
    let existing_material_uuid = existing_material_id.to_uuid();

    let checksum = format!("blake3-{}", uuid::Uuid::now_v7());
    let blob = pool.blobs().insert(race_test_blob(&checksum)).await?;
    let blob_id = blob.id;
    let blob_uuid = blob_id.to_uuid();

    sqlx::query!(
        "UPDATE raw.source_material_registry SET optional_blob_id = $1 WHERE id = $2",
        blob_uuid,
        existing_material_uuid
    )
    .execute(&pool)
    .await
    .map_err(|e| SinexError::database("attach blob to existing material").with_source(e))?;

    let new_material_id = Id::<SourceMaterial>::new();
    let new_material_uuid = new_material_id.to_uuid();

    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    // Racer A: delete-on-tombstone's check-then-delete sequence, run exactly
    // as `handle_tombstone_approve` runs it.
    let pool_a = pool.clone();
    let barrier_a = Arc::clone(&barrier);
    let handle_a = tokio::spawn(async move {
        barrier_a.wait().await;
        pool_a
            .with_transaction(async |tx| {
                let Some(_locked) = pool_a
                    .blobs()
                    .lock_by_id_for_update(&mut **tx, blob_id)
                    .await?
                else {
                    return Ok(false);
                };

                if pool_a
                    .blobs()
                    .is_referenced_excluding_material_with_executor(
                        &mut **tx,
                        blob_id,
                        existing_material_uuid,
                    )
                    .await?
                {
                    // Referenced elsewhere under the lock -- back off, exactly
                    // like handle_tombstone_approve does.
                    return Ok(false);
                }

                // Real CAS content deletion is outside the scope of this
                // repository-level test; the invariant under test is entirely
                // about the core.blobs row + the concurrent FK-guarded insert.
                let deleted = pool_a
                    .blobs()
                    .delete_by_id_with_executor(&mut **tx, blob_id)
                    .await?;
                Ok(deleted)
            })
            .await
    });

    // Racer B: the real dedup write path -- a concurrent INSERT into
    // raw.source_material_registry whose optional_blob_id references the same
    // blob (what ContentStoreManager's caller does via
    // SourceMaterialRegistration::with_blob_id after check_dedup finds a hit).
    let pool_b = pool.clone();
    let barrier_b = Arc::clone(&barrier);
    let handle_b = tokio::spawn(async move {
        barrier_b.wait().await;
        sqlx::query!(
            r#"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, optional_blob_id)
            VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', $3)
            "#,
            new_material_uuid,
            format!("cas-refcheck-toctou-new-{new_material_uuid}"),
            blob_uuid
        )
        .execute(&pool_b)
        .await
    });

    let deleted = handle_a.await.map_err(|e| {
        SinexError::unknown(format!("racer A (delete-on-tombstone) panicked: {e}"))
    })??;
    let insert_result = handle_b
        .await
        .map_err(|e| SinexError::unknown(format!("racer B (dedup insert) panicked: {e}")))?;

    let blob_row_exists = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM core.blobs WHERE id = $1) AS "exists!""#,
        blob_uuid
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database("check blob row survived").with_source(e))?;

    let new_material_row_exists = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM raw.source_material_registry WHERE id = $1) AS "exists!""#,
        new_material_uuid
    )
    .fetch_one(&pool)
    .await
    .map_err(|e| SinexError::database("check new material row").with_source(e))?;

    match (deleted, insert_result.is_ok()) {
        (true, true) => {
            // Exactly the bug this test guards against: the blob was deleted
            // AND the concurrent dedup insert still reports success -- meaning
            // a live material row now silently points at deleted content.
            panic!(
                "TOCTOU race reproduced: blob {blob_uuid} was deleted by racer A \
                 AND racer B's concurrent material registration ({new_material_uuid}) \
                 succeeded referencing it (blob_row_exists={blob_row_exists}, \
                 new_material_row_exists={new_material_row_exists})"
            );
        }
        (true, false) => {
            // Racer A won the lock race and deleted the blob; racer B's insert
            // correctly failed (FK violation on the now-gone row) instead of
            // landing an orphaned reference.
            assert!(
                !blob_row_exists,
                "blob row should be gone: racer A reported a successful delete"
            );
            assert!(
                !new_material_row_exists,
                "a failed INSERT must never leave a row behind"
            );
        }
        (false, _) => {
            // Racer A backed off (found the row still referenced under the
            // lock, or the row was already gone) -- the blob must still exist,
            // and if racer B's insert succeeded its row must be consistent
            // with a live, undeleted blob.
            assert!(
                blob_row_exists,
                "blob row must survive when delete-on-tombstone backs off"
            );
            if insert_result.is_ok() {
                assert!(
                    new_material_row_exists,
                    "racer B's successful insert must leave its row behind"
                );
            }
        }
    }

    Ok(())
}

/// sinex-ldyx (finding #1): the blake3 INSERT branch in `insert_with_executor`
/// only arbitrates `ON CONFLICT (checksum_blake3)`, but `core.blobs` also
/// carries a second unique index, `uk_blobs_annex_backend_content_hash`
/// (`annex_backend`, `content_hash`), that this branch does not guard at
/// all. Re-inserting content already stored WITHOUT a blake3 checksum, this
/// time WITH one computed, hits the unguarded second index and raises a raw
/// `23505` instead of being handled gracefully like the checksum_blake3
/// conflict path.
#[sinex_test]
#[ignore = "sinex-ldyx open: the blake3 INSERT branch only arbitrates ON CONFLICT \
            (checksum_blake3), leaving uk_blobs_annex_backend_content_hash unguarded -- a \
            same-content re-insert that newly carries a blake3 checksum hits a raw 23505 \
            instead of updating the existing row"]
async fn reinserting_with_newly_computed_blake3_does_not_raise_raw_conflict(
    ctx: TestContext,
) -> TestResult<()> {
    let repo = ctx.pool.blobs();

    let without_blake3 = Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash("ldyx-shared-content-hash".to_string())
        .original_filename("ldyx-annex-collision.bin".to_string())
        .size_bytes(4)
        .mime_type("application/octet-stream".to_string())
        .build();
    repo.insert(without_blake3).await?;

    let with_blake3 = Blob::builder()
        .storage_backend("SHA256E".to_string())
        .content_hash("ldyx-shared-content-hash".to_string())
        .original_filename("ldyx-annex-collision.bin".to_string())
        .size_bytes(4)
        .mime_type("application/octet-stream".to_string())
        .checksum_blake3("ldyx-newly-computed-blake3".to_string())
        .build();

    let result = repo.insert(with_blake3).await;
    assert!(
        result.is_ok(),
        "re-inserting the same (annex_backend, content_hash) with a newly-computed blake3 \
         checksum should update the existing row, not raise a raw unique-violation from the \
         unguarded uk_blobs_annex_backend_content_hash index: {result:?}"
    );

    Ok(())
}

//! Regression test for sinex-audit-docchunks-replay-redaction-bypass.
//!
//! `core.fn_document_projection()` (the `AFTER INSERT ON core.events`
//! trigger) projects `document.chunked` events into `core.document_chunks`.
//! Before the fix, that branch used `ON CONFLICT (document_id, chunk_index)
//! DO NOTHING` — since `document_id` is deterministic and `chunk_index` is
//! stable, a replayed `document.chunked` event for the same chunk always
//! collided on the composite PK and was silently dropped. That meant a
//! replay triggered by tightening a privacy/redaction rule would correctly
//! write a properly-redacted `document.chunked` row to `core.events`, but
//! `core.document_chunks.text` (the exact column FTS/trigram search reads
//! and returns verbatim) would keep the *original*, under-redacted text
//! forever.
//!
//! This test drives the real trigger (raw `INSERT INTO core.events`, not a
//! direct `core.document_chunks` write) to prove the projection itself
//! self-heals: a second `document.chunked` event for the same
//! `(document_id, chunk_index)` with different text must overwrite the
//! stale row, not be dropped.
//!
//! Anti-vacuity: reverting the trigger's `ON CONFLICT ... DO UPDATE` back to
//! `DO NOTHING` makes `document_chunks_replay_overwrites_stale_text` fail —
//! the final `text` would still read `"original under-redacted text SSN
//! 123-45-6789"` instead of the redacted replacement.

use sinex_primitives::Uuid;
use sqlx::PgPool;
use time::OffsetDateTime;
use xtask::sandbox::prelude::*;

/// Inserts a source-material row (satisfies the FK on
/// `core.events.source_material_id`) and returns its id.
async fn seed_material(pool: &PgPool, source_identifier: &str) -> TestResult<Uuid> {
    let material_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type,
             start_time, end_time, total_bytes)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'realtime', $3, $4, 1024)
        ",
    )
    .bind(material_id)
    .bind(source_identifier)
    .bind(OffsetDateTime::now_utc())
    .bind(OffsetDateTime::now_utc())
    .execute(pool)
    .await?;
    Ok(material_id)
}

/// Inserts one `core.events` row with material provenance, driving the real
/// `trg_document_projection` trigger.
async fn seed_document_event(
    pool: &PgPool,
    material_id: Uuid,
    anchor_byte: i64,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<Uuid> {
    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.events
            (id, source, event_type, payload, ts_orig, host,
             source_material_id, anchor_byte)
        VALUES ($1::uuid, 'document-parser', $2, $3::jsonb, NOW(), 'test-host',
                $4::uuid, $5)
        ",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(payload.to_string())
    .bind(material_id)
    .bind(anchor_byte)
    .execute(pool)
    .await?;
    Ok(event_id)
}

/// Replaying a `document.chunked` event for the same `(document_id,
/// chunk_index)` with corrected (redacted) text must overwrite
/// `core.document_chunks.text`, not be silently dropped.
#[sinex_test]
async fn document_chunks_replay_overwrites_stale_text(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let document_id = Uuid::now_v7();
    let natural_key = format!("notes/replay-redaction-{document_id}");

    // 1. `document.parsed` creates the parent `core.documents` row.
    let parsed_material = seed_material(pool, &format!("parsed-material-{document_id}")).await?;
    seed_document_event(
        pool,
        parsed_material,
        0,
        "document.parsed",
        serde_json::json!({
            "document_id": document_id,
            "kind": "dendron_markdown",
            "natural_key": natural_key,
            "extraction_version": 1,
            "chunk_count": 1,
            "text_byte_len": 40,
            "side_data": {},
        }),
    )
    .await?;

    // 2. Original `document.chunked` — under-redacted text (an SSN leaked
    //    through because the privacy rule hadn't caught this pattern yet).
    let original_text = "original under-redacted text SSN 123-45-6789";
    let chunk_material = seed_material(pool, &format!("chunk-material-{document_id}")).await?;
    seed_document_event(
        pool,
        chunk_material,
        0,
        "document.chunked",
        serde_json::json!({
            "document_id": document_id,
            "chunk_index": 0,
            "text": original_text,
            "byte_offset_start": 0,
            "byte_offset_end": original_text.len(),
            "source_anchor_start": null,
            "source_anchor_end": null,
        }),
    )
    .await?;

    let stored_text: String = sqlx::query_scalar(
        "SELECT text FROM core.document_chunks WHERE document_id = $1 AND chunk_index = 0",
    )
    .bind(document_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        stored_text, original_text,
        "sanity check: initial projection should store the original text"
    );

    // 3. Operator tightens the privacy rule and replays the affected source
    //    material: `core.events` correctly gets a *new*, properly-redacted
    //    `document.chunked` event for the exact same `(document_id,
    //    chunk_index)` occurrence (deterministic document_id, stable
    //    chunk_index — this is exactly what replay produces).
    let redacted_text = "original under-redacted text SSN [REDACTED]";
    let replay_material = seed_material(pool, &format!("chunk-material-replay-{document_id}")).await?;
    seed_document_event(
        pool,
        replay_material,
        0,
        "document.chunked",
        serde_json::json!({
            "document_id": document_id,
            "chunk_index": 0,
            "text": redacted_text,
            "byte_offset_start": 0,
            "byte_offset_end": redacted_text.len(),
            "source_anchor_start": null,
            "source_anchor_end": null,
        }),
    )
    .await?;

    // 4. The projection must reflect the NEW (redacted) text — not the
    //    stale original. This is the exact bug: `ON CONFLICT ... DO
    //    NOTHING` would leave `stored_text == original_text` here, keeping
    //    the leaked SSN searchable forever.
    let row: (String, i64, i64) = sqlx::query_as(
        "SELECT text, byte_offset_start, byte_offset_end \
         FROM core.document_chunks WHERE document_id = $1 AND chunk_index = 0",
    )
    .bind(document_id)
    .fetch_one(pool)
    .await?;
    let (stored_text, byte_offset_start, byte_offset_end) = row;

    assert_eq!(
        stored_text, redacted_text,
        "replayed document.chunked must overwrite stale chunk text, not be dropped \
         (sinex-audit-docchunks-replay-redaction-bypass)"
    );
    assert!(
        !stored_text.contains("123-45-6789"),
        "leaked SSN must not remain searchable after replay: {stored_text}"
    );
    assert_eq!(byte_offset_start, 0);
    assert_eq!(byte_offset_end, redacted_text.len() as i64);

    // Only one row should exist for this occurrence — this is an upsert,
    // not an accumulating insert.
    let row_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core.document_chunks WHERE document_id = $1",
    )
    .bind(document_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(row_count, 1, "replay must upsert in place, not accumulate rows");

    Ok(())
}

/// A fresh `document.chunked` event for a *different* `chunk_index` on the
/// same document must still insert normally (the fix must not turn the
/// insert path into an update-only no-op for genuinely new chunks).
#[sinex_test]
async fn document_chunks_new_chunk_index_still_inserts(ctx: TestContext) -> TestResult<()> {
    let pool = &ctx.pool;
    let document_id = Uuid::now_v7();
    let natural_key = format!("notes/new-chunk-{document_id}");

    let parsed_material = seed_material(pool, &format!("parsed-material-{document_id}")).await?;
    seed_document_event(
        pool,
        parsed_material,
        0,
        "document.parsed",
        serde_json::json!({
            "document_id": document_id,
            "kind": "dendron_markdown",
            "natural_key": natural_key,
            "extraction_version": 1,
            "chunk_count": 2,
            "text_byte_len": 20,
            "side_data": {},
        }),
    )
    .await?;

    for chunk_index in 0..2i32 {
        let text = format!("chunk body {chunk_index}");
        let material = seed_material(
            pool,
            &format!("chunk-material-{document_id}-{chunk_index}"),
        )
        .await?;
        seed_document_event(
            pool,
            material,
            0,
            "document.chunked",
            serde_json::json!({
                "document_id": document_id,
                "chunk_index": chunk_index,
                "text": text,
                "byte_offset_start": 0,
                "byte_offset_end": text.len(),
                "source_anchor_start": null,
                "source_anchor_end": null,
            }),
        )
        .await?;
    }

    let row_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.document_chunks WHERE document_id = $1")
            .bind(document_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(row_count, 2, "distinct chunk_index values must both be inserted");

    Ok(())
}

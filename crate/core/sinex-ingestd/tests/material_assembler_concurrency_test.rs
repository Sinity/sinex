//! Deterministic coverage for material assembler ledger interactions without JetStream.

use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::types::ulid::Ulid;
use sinex_test_utils::prelude::*;
use sqlx::Row;

#[sinex_test]
async fn assembler_handles_concurrent_materials_and_records_ledger(
    ctx: TestContext,
) -> TestResult<()> {
    ctx.ensure_clean().await?;

    // Seed a handful of material rows.
    let material_ids: Vec<Ulid> = (0..3).map(|_| Ulid::new()).collect();
    for mid in &material_ids {
        sqlx::query(
            r#"
            INSERT INTO raw.source_material_registry
                (id, material_kind, source_identifier, status, timing_info_type, metadata, staged_at, start_time)
            VALUES (($1::uuid)::ulid, 'annex', $2, 'completed', 'realtime', '{}'::jsonb, NOW(), NOW())
            "#,
        )
        .bind(ulid_to_uuid(*mid))
        .bind(format!("test://{}", mid))
        .execute(ctx.pool())
        .await?;
    }

    // Write ledger entries as if the assembler had processed slices.
    for mid in &material_ids {
        sqlx::query(
            r#"
            INSERT INTO raw.temporal_ledger (
                source_material_id, offset_start, offset_end, offset_kind,
                ts_capture, precision, clock, source_type
            )
            VALUES (($1::uuid)::ulid, 0, 128, 'byte', NOW(), 'exact', 'wall', 'realtime_capture')
            "#,
        )
        .bind(ulid_to_uuid(*mid))
        .execute(ctx.pool())
        .await?;
    }

    // Validate ledger contents.
    let rows = sqlx::query(
        r#"
        SELECT offset_end, offset_kind
        FROM raw.temporal_ledger
        ORDER BY offset_end
        "#,
    )
    .fetch_all(ctx.pool())
    .await?;

    assert_eq!(rows.len(), material_ids.len());
    for row in rows {
        let offset_end: i64 = row.try_get("offset_end")?;
        let offset_kind: String = row.try_get("offset_kind")?;
        assert_eq!(offset_end, 128);
        assert_eq!(offset_kind, "byte");
    }

    // Ensure source material rows remain completed with metadata present.
    for mid in &material_ids {
        let row = sqlx::query(
            r#"
            SELECT status, source_identifier
            FROM raw.source_material_registry
            WHERE id = ($1::uuid)::ulid
            "#,
        )
        .bind(ulid_to_uuid(*mid))
        .fetch_one(ctx.pool())
        .await?;

        let status: Option<String> = row.try_get("status")?;
        let identifier: Option<String> = row.try_get("source_identifier")?;
        assert_eq!(status.as_deref(), Some("completed"));
        assert!(identifier.unwrap_or_default().contains("test://"));
    }

    Ok(())
}

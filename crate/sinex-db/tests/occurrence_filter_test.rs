//! Integration tests for [`sinex_db::build_occurrence_filter`] (#1050).
//!
//! The DB-built filter must produce key strings that compare *equal* to
//! keys produced in-memory via
//! [`sinex_primitives::parser::occurrence_key_string`]. These tests cover
//! the wrapper-correctness regression caught in review: a caller building
//! the filter from DB and then querying via `occurrence_key_string` must
//! get a cache *hit*, not a 100% miss.

use sinex_db::build_occurrence_filter;
use sinex_primitives::parser::{
    OccurrenceKey, SourceUnitId, occurrence_key_string,
};
use sqlx::types::Uuid;
use time::OffsetDateTime;
use xtask::sandbox::prelude::*;

/// Insert a single event referencing a synthetic source-material row.
/// Mirrors the helper in `repositories_continuity_seams.rs`.
async fn seed_event(
    pool: &DbPool,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> TestResult<()> {
    let material_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO raw.source_material_registry
            (id, material_kind, source_identifier, status, timing_info_type,
             start_time, end_time, total_bytes)
        VALUES ($1::uuid, 'annex', $2, 'completed', 'wall_clock',
                $3, $4, 1024)
        ",
    )
    .bind(material_id)
    .bind(format!("occfilter-{material_id}"))
    .bind(OffsetDateTime::now_utc())
    .bind(OffsetDateTime::now_utc())
    .execute(pool)
    .await?;

    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.events
            (id, source, event_type, payload, ts_orig, host,
             source_material_id, anchor_byte)
        VALUES ($1::uuid, $2, $3, $4::jsonb, NOW(), 'test-host',
                $5::uuid, 0)
        ",
    )
    .bind(event_id)
    .bind(source)
    .bind(event_type)
    .bind(payload.to_string())
    .bind(material_id)
    .execute(pool)
    .await?;
    Ok(())
}

#[sinex_test]
async fn db_filter_key_matches_in_memory_key(ctx: TestContext) -> TestResult<()> {
    // Two events with identical occurrence-key fields; the filter should
    // contain one canonical key string that equals what
    // `occurrence_key_string` produces in-memory.
    let payload = serde_json::json!({
        "track_uri": "spotify:track:abc123",
        "started_at": "2024-01-15T08:00:00Z",
        "played_ms": "240000",
    });
    seed_event(ctx.pool(), "spotify-occfilter", "track.played", payload.clone()).await?;
    seed_event(ctx.pool(), "spotify-occfilter", "track.played", payload).await?;

    let filter = build_occurrence_filter(
        ctx.pool(),
        "spotify-occfilter",
        "track.played",
        "spotify-extended-history",
        &["track_uri", "started_at", "played_ms"],
    )
    .await?;

    let expected = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("spotify-extended-history"),
        fields: vec![
            ("track_uri".into(), "spotify:track:abc123".into()),
            ("started_at".into(), "2024-01-15T08:00:00Z".into()),
            ("played_ms".into(), "240000".into()),
        ],
    };
    let key_str = occurrence_key_string(&expected);

    assert_eq!(filter.len(), 1, "two identical payloads -> one distinct key");
    assert!(
        filter.contains(&key_str),
        "DB-built filter must contain the canonical in-memory key. \
         expected={key_str:?}"
    );
    Ok(())
}

#[sinex_test]
async fn db_filter_escapes_pipe_in_payload_values(ctx: TestContext) -> TestResult<()> {
    // A track name containing `|` must be escaped in both the DB-built key
    // and the in-memory key, and they must compare equal.
    let payload = serde_json::json!({
        "track_name": "Foo|bar",
        "started_at": "2024-01-15T08:00:00Z",
    });
    seed_event(ctx.pool(), "spotify-occfilter-esc", "track.played", payload).await?;

    let filter = build_occurrence_filter(
        ctx.pool(),
        "spotify-occfilter-esc",
        "track.played",
        "spotify-extended-history",
        &["track_name", "started_at"],
    )
    .await?;

    let expected = OccurrenceKey {
        source_unit_id: SourceUnitId::from_static("spotify-extended-history"),
        fields: vec![
            ("track_name".into(), "Foo|bar".into()),
            ("started_at".into(), "2024-01-15T08:00:00Z".into()),
        ],
    };
    let key_str = occurrence_key_string(&expected);
    assert!(
        filter.contains(&key_str),
        "DB escaping must match in-memory escaping. expected={key_str:?}"
    );
    Ok(())
}

#[sinex_test]
async fn db_filter_rejects_invalid_field_identifiers(ctx: TestContext) -> TestResult<()> {
    // SQL-injection sink defense: a hostile field name must be rejected
    // before any SQL is built. The validator returns an error.
    let result = build_occurrence_filter(
        ctx.pool(),
        "any-source",
        "any.event",
        "any-unit",
        &["legit_field", "'; DROP TABLE events; --"],
    )
    .await;
    assert!(
        result.is_err(),
        "hostile field identifier must be rejected by validate_pg_identifier"
    );
    Ok(())
}

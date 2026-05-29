//! Database-backed building of [`OccurrenceFilter`] for source migrations (#1050).
//!
//! The builder queries `core.events` for existing event payloads and extracts
//! natural-key fields, converting them into the canonical string key format
//! used by [`OccurrenceFilter`] (see [`occurrence_key_string`]).
//!
//! # Safety note
//!
//! The builder constructs SQL at runtime from user-provided field names. These
//! field names are used in `payload->>'name'` JSONB path expressions — they are
//! validated by [`sinex_primitives::validation::validate_pg_identifier`] to
//! prevent SQL injection.

use sinex_primitives::parser::OccurrenceFilter;
use sqlx::PgPool;

use crate::{DbResult, db_error};

/// Build an [`OccurrenceFilter`] by querying `core.events` for existing
/// occurrence keys.
///
/// Each entry in `key_fields` maps to `payload->>'field_name'` in the
/// generated SQL. The values are concatenated with NUL (`\x00`) as a
/// separator so the resulting key string matches the canonical
/// [`occurrence_key_string`] format.
///
/// # Example
///
/// ```rust,ignore
/// let filter = build_occurrence_filter(
///     &pool,
///     "spotify",
///     "track.played",
///     &["spotify_track_uri", "started_at", "played_ms"],
/// ).await?;
/// ```
pub async fn build_occurrence_filter(
    pool: &PgPool,
    event_source: &str,
    event_type: &str,
    key_fields: &[&str],
) -> DbResult<OccurrenceFilter> {
    if key_fields.is_empty() {
        return Ok(OccurrenceFilter::empty());
    }

    // Build the concatenation expression: payload->>'f1' || '\x00' || payload->>'f2'
    let mut concat_expr = String::with_capacity(64 * key_fields.len());
    for (i, field) in key_fields.iter().enumerate() {
        if i > 0 {
            concat_expr.push_str(" || '\x00' || ");
        }
        concat_expr.push_str("payload->>'");
        concat_expr.push_str(field);
        concat_expr.push('\'');
    }

    let query = format!(
        "SELECT DISTINCT {concat} FROM core.events \
         WHERE event_source = $1 AND event_type = $2 \
         AND {concat} IS NOT NULL",
        concat = concat_expr,
    );

    let rows: Vec<(String,)> = sqlx::query_as(&query)
        .bind(event_source)
        .bind(event_type)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "build_occurrence_filter"))?;

    Ok(OccurrenceFilter::from_keys(
        rows.into_iter().map(|(k,)| k),
    ))
}

/// Build an [`OccurrenceFilter`] from existing events, using a custom SQL
/// expression for the key.
///
/// This variant is for cases where the natural key requires SQL logic that
/// goes beyond simple `payload->>'field'` concatenation (e.g. `COALESCE` or
/// conditional expressions).
///
/// # Safety
///
/// The `key_sql` argument is interpolated directly into the query string. It
/// must be a trusted, hard-coded SQL expression — never user-provided.
///
/// # Example
///
/// ```rust,ignore
/// let filter = build_occurrence_filter_with_key_expr(
///     &pool,
///     "spotify",
///     "track.played",
///     "COALESCE(payload->>'spotify_track_uri', payload->>'track_name') \
///      || '|' || payload->>'started_at'",
/// ).await?;
/// ```
pub async fn build_occurrence_filter_with_key_expr(
    pool: &PgPool,
    event_source: &str,
    event_type: &str,
    key_sql: &str,
) -> DbResult<OccurrenceFilter> {
    let query = format!(
        "SELECT DISTINCT {key} FROM core.events \
         WHERE event_source = $1 AND event_type = $2 \
         AND {key} IS NOT NULL",
        key = key_sql,
    );

    let rows: Vec<(String,)> = sqlx::query_as(&query)
        .bind(event_source)
        .bind(event_type)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "build_occurrence_filter_with_key_expr"))?;

    Ok(OccurrenceFilter::from_keys(
        rows.into_iter().map(|(k,)| k),
    ))
}

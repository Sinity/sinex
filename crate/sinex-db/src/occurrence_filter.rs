//! Database-backed building of [`OccurrenceFilter`] for source migrations (#1050).
//!
//! The builder queries `core.events` for existing event payloads and extracts
//! natural-key fields, converting them into the canonical string key format
//! used by [`OccurrenceFilter`] (see
//! [`sinex_primitives::parser::occurrence_key_string`]).
//!
//! # Safety note
//!
//! The builder constructs SQL at runtime from caller-provided field names.
//! These field names are used in `payload->>'name'` JSONB path expressions and
//! are validated up-front by
//! [`sinex_primitives::validation::validate_pg_identifier`] to prevent SQL
//! injection. Invalid identifiers cause [`build_occurrence_filter`] to
//! return an error before any SQL is built.

use sinex_primitives::parser::OccurrenceFilter;
use sinex_primitives::validation::validate_pg_identifier;
use sqlx::PgPool;

use crate::{DbResult, db_error};

/// Build an [`OccurrenceFilter`] by querying `core.events` for existing
/// occurrence keys, returned in the canonical
/// [`occurrence_key_string`](sinex_primitives::parser::occurrence_key_string)
/// format so the resulting strings compare equal to in-memory keys produced
/// from a parser's [`OccurrenceKey`](sinex_primitives::parser::OccurrenceKey).
///
/// The generated SQL emits one string per distinct payload combination, of
/// shape:
///
/// ```text
/// <source_unit_id>|<f1>=<v1>|<f2>=<v2>|...
/// ```
///
/// The order of `key_fields` is preserved verbatim into the generated key, so
/// it MUST match the order in which the parser pushes fields onto
/// `OccurrenceKey::fields` for keys to compare equal.
///
/// Backslash, `|`, and `=` inside payload values are escaped (`\\`, `\|`,
/// `\=`) using the same scheme as
/// [`occurrence_key_string`](sinex_primitives::parser::occurrence_key_string).
/// Field *names* are required to be valid `PostgreSQL` identifiers (no `|`, `=`,
/// or `\` possible) and so are emitted verbatim.
///
/// # Errors
///
/// Returns an error if any entry in `key_fields` fails
/// [`validate_pg_identifier`], or if the underlying query fails.
///
/// # Example
///
/// ```rust,ignore
/// let filter = build_occurrence_filter(
///     &pool,
///     "spotify",
///     "track.played",
///     "spotify-extended-history",
///     &["track_uri", "started_at", "played_ms"],
/// ).await?;
/// ```
pub async fn build_occurrence_filter(
    pool: &PgPool,
    event_source: &str,
    event_type: &str,
    source_unit_id: &str,
    key_fields: &[&str],
) -> DbResult<OccurrenceFilter> {
    // SQL-injection safety: every interpolated field name must be a valid
    // PostgreSQL identifier. The validator rejects whitespace, quotes, and
    // any non-`[a-zA-Z_][a-zA-Z0-9_]*` shape — including the bytes that
    // could break out of the `payload->>'…'` literal.
    for field in key_fields {
        validate_pg_identifier(field, "occurrence-key field")?;
    }

    if key_fields.is_empty() {
        return Ok(OccurrenceFilter::empty());
    }

    // Build the canonical key expression:
    //   $3 || '|' ||
    //     'f1=' || sinex_pg_escape_occ(payload->>'f1') ||
    //     '|' || 'f2=' || sinex_pg_escape_occ(payload->>'f2') || ...
    //
    // We perform the escaping in SQL with REPLACE so the materialized key
    // matches what `occurrence_key_string` produces in Rust. The order of
    // REPLACEs matters: `\` first (so the escapes we introduce in the next
    // two steps are not themselves escaped again), then `|` and `=`.
    //
    // `sinex_pg_escape_occ` is inlined as a subexpression — we don't create
    // a function. Spelled out:
    //   replace(replace(replace(payload->>'f', '\', '\\'), '|', '\|'), '=', '\=')
    fn escape_expr(field: &str) -> String {
        format!(
            "replace(replace(replace(coalesce(payload->>'{field}', ''), \
             '\\', '\\\\'), '|', '\\|'), '=', '\\=')",
        )
    }

    let mut key_expr = String::with_capacity(128 + 96 * key_fields.len());
    // Source-unit-id prefix is supplied as a parameter, not interpolated.
    // We still have to escape it inside SQL via the same REPLACE chain so an
    // adversarial id with `|` or `=` keeps the canonical form.
    key_expr.push_str("replace(replace(replace($3::text, '\\', '\\\\'), '|', '\\|'), '=', '\\=')");
    for field in key_fields {
        key_expr.push_str(" || '|' || ");
        // Field name is a validated pg identifier — safe to emit verbatim.
        key_expr.push('\'');
        key_expr.push_str(field);
        key_expr.push_str("=' || ");
        key_expr.push_str(&escape_expr(field));
    }

    let query = format!(
        "SELECT DISTINCT {key_expr} FROM core.events \
         WHERE source = $1 AND event_type = $2"
    );

    let rows: Vec<(String,)> = sqlx::query_as(&query)
        .bind(event_source)
        .bind(event_type)
        .bind(source_unit_id)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "build_occurrence_filter"))?;

    Ok(OccurrenceFilter::from_keys(rows.into_iter().map(|(k,)| k)))
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
        "SELECT DISTINCT {key_sql} FROM core.events \
         WHERE source = $1 AND event_type = $2 \
         AND {key_sql} IS NOT NULL",
    );

    let rows: Vec<(String,)> = sqlx::query_as(&query)
        .bind(event_source)
        .bind(event_type)
        .fetch_all(pool)
        .await
        .map_err(|e| db_error(e, "build_occurrence_filter_with_key_expr"))?;

    Ok(OccurrenceFilter::from_keys(rows.into_iter().map(|(k,)| k)))
}

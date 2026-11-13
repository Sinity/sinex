//! Structural state-run bounds: retain refreshes until the first changed state.
use super::*;
pub(super) async fn bounds(
    conn: &mut PgConnection,
    kinds: &[Kind],
    start: OffsetDateTime,
    end: OffsetDateTime,
    scope_field: &str,
    subject_fields: &[&str],
    now: OffsetDateTime,
) -> Result<(OffsetDateTime, OffsetDateTime)> {
    let (types, contains) = kind_arrays(kinds);
    let subjects: Vec<String> = subject_fields.iter().map(|field| (*field).into()).collect();
    // Scope count must not multiply history evaluation. In particular, an
    // uncaptured scope has one fallback identity per event.
    let row = sqlx::query_file!(
        "src/derivations/sql/state_transition_bounds.sql",
        &types,
        &contains,
        start,
        end,
        scope_field,
        &subjects,
        now,
    )
    .fetch_one(&mut *conn)
    .await?;
    let from = row.start.unwrap_or(start);
    let until = row.finish.unwrap_or(end);
    // Include the authoritative closing transition in an exclusive-end read.
    let until = Timestamp::from_unix_ns(Timestamp(until).unix_ns() + 1_000)
        .map_err(|error| crate::Error::Invalid(error.to_string()))?
        .0;
    Ok((from, until))
}

#[cfg(test)]
mod tests;

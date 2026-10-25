//! Reading event payloads by field, for code that keeps its current state as
//! events and reads it back (the media library). The caller names the event
//! type and the payload path; this module knows neither. "Live" is the live
//! reading of each keyed record (`core.live_equivalence`).
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;

/// Which live readings to read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordStates {
    /// Only records present at their source (not removed or withdrawn).
    Present,
    /// Present or removed.
    Any,
}

impl RecordStates {
    fn present_only(self) -> bool {
        self == RecordStates::Present
    }
}

fn path(p: &[&str]) -> Vec<String> {
    p.iter().map(|s| (*s).to_owned()).collect()
}

/// Live payloads of `event_type` whose text at `field` (a payload path) is
/// one of `values`, in commit order.
pub async fn live_where(
    conn: &mut PgConnection,
    event_type: &str,
    field: &[&str],
    values: &[String],
    state: RecordStates,
) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT e.payload AS "payload!"
           FROM core.events e JOIN core.live_equivalence le ON le.event_id = e.id
           WHERE e.event_type = $1 AND e.payload #>> $2::text[] = ANY($3::text[])
             AND (NOT $4 OR e.record_state = 'present')
           ORDER BY e.commit_seq"#,
        event_type,
        &path(field),
        values,
        state.present_only()
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Live payloads of `event_type` whose array at `field` holds any of
/// `values`, in commit order.
pub async fn live_containing(
    conn: &mut PgConnection,
    event_type: &str,
    field: &[&str],
    values: &[String],
    state: RecordStates,
) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT e.payload AS "payload!"
           FROM core.events e JOIN core.live_equivalence le ON le.event_id = e.id
           WHERE e.event_type = $1 AND (e.payload #> $2::text[]) ?| $3::text[]
             AND (NOT $4 OR e.record_state = 'present')
           ORDER BY e.commit_seq"#,
        event_type,
        &path(field),
        values,
        state.present_only()
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Every live payload of `event_type`, in commit order.
pub async fn live_all(
    conn: &mut PgConnection,
    event_type: &str,
    state: RecordStates,
) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT e.payload AS "payload!"
           FROM core.events e JOIN core.live_equivalence le ON le.event_id = e.id
           WHERE e.event_type = $1 AND (NOT $2 OR e.record_state = 'present')
           ORDER BY e.commit_seq"#,
        event_type,
        state.present_only()
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// Every reading of `event_type` with `field` equal to `value`, live and
/// archived, in commit order: a record's history.
pub async fn history(
    conn: &mut PgConnection,
    event_type: &str,
    field: &[&str],
    value: &str,
) -> Result<Vec<Value>> {
    Ok(sqlx::query_scalar!(
        r#"SELECT e.payload AS "payload!"
           FROM core.events e
           WHERE e.event_type = $1 AND e.payload #>> $2::text[] = $3
           ORDER BY e.commit_seq"#,
        event_type,
        &path(field),
        value
    )
    .fetch_all(&mut *conn)
    .await?)
}

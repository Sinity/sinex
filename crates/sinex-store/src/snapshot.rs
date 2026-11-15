//! Reads behind a consumer's current-state snapshot: the in-scope readings at
//! one commit position with their lifecycle state at that position, and the
//! checks a later page repeats against current tables.
use crate::Result;
use sinex_model::KindRef;
use sqlx::PgConnection;

/// One reading at the snapshot position. Its body is loaded with
/// [`crate::confirmed::rows`].
#[derive(Debug, sqlx::FromRow)]
pub struct Reading {
    pub id: uuid::Uuid,
    pub commit_seq: i64,
    pub class: String,
    pub archived: bool,
    pub lifecycle_position: Option<i64>,
    /// Archived at or before the position by a change whose position is not
    /// recorded: the snapshot cannot state when.
    pub history_unknown: bool,
}

/// The selection a snapshot reads: these kinds, in these classes, at this
/// commit position.
pub struct Selection<'a> {
    pub position: i64,
    pub kinds: &'a [String],
    pub classes: &'a [String],
}

/// Up to `limit` readings after `after`, in commit order then ID.
pub async fn readings(
    conn: &mut PgConnection,
    selection: &Selection<'_>,
    after: Option<(i64, uuid::Uuid)>,
    limit: i64,
) -> Result<Vec<Reading>> {
    Ok(sqlx::query_as("SELECT e.id,e.commit_seq,e.class,coalesce(h.archived,false) AS archived,h.commit_seq AS lifecycle_position,(h.commit_seq IS NULL AND (EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id AND a.archived_seq<=$1) OR coalesce((SELECT NOT c.archived FROM audit.archive_changes c WHERE c.event_id=e.id AND c.commit_seq>$1 ORDER BY c.commit_seq LIMIT 1),false))) AS history_unknown FROM core.events e LEFT JOIN LATERAL (SELECT c.archived,c.commit_seq FROM audit.archive_changes c WHERE c.event_id=e.id AND c.commit_seq<=$1 ORDER BY c.commit_seq DESC LIMIT 1) h ON true WHERE e.event_type=ANY($2) AND e.commit_seq<=$1 AND ($3::bigint IS NULL OR (e.commit_seq,e.id)>($3,$4::uuid)) AND e.class=ANY($6) ORDER BY e.commit_seq,e.id LIMIT $5")
        .bind(selection.position)
        .bind(selection.kinds)
        .bind(after.map(|after| after.0))
        .bind(after.map(|after| after.1))
        .bind(limit)
        .bind(selection.classes)
        .fetch_all(conn)
        .await?)
}

/// Whether a reading of these kinds at or below `position` was tombstoned
/// after it, so pages already delivered no longer match the store.
pub async fn tombstoned_since(
    conn: &mut PgConnection,
    position: i64,
    kinds: &[String],
) -> Result<bool> {
    Ok(sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE commit_seq>$1 AND event_type=ANY($2) AND (original_commit_seq<=$1 OR original_commit_seq IS NULL))")
        .bind(position)
        .bind(kinds)
        .fetch_one(conn)
        .await?)
}

/// Whether every kind version is registered with exactly this definition.
pub async fn kinds_registered(
    conn: &mut PgConnection,
    kinds: &[(KindRef, String)],
) -> Result<bool> {
    let refs = kinds
        .iter()
        .map(|(kind, _)| kind.clone())
        .collect::<Vec<_>>();
    let registered = sinex_schema::kinds::load(conn, &refs).await?;
    Ok(kinds.iter().all(|(kind, hash)| {
        registered
            .get(kind)
            .is_some_and(|definition| definition.definition_hash() == *hash)
    }))
}

#[cfg(test)]
mod tests;

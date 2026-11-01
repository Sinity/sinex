use sqlx::PgConnection;

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(conn).await.unwrap()
}

/// Readings, erased skeletons and visibility changes share global positions.
/// A tombstone keeps the original event slot and adds its lifecycle position.
/// An event's position is on its row; the commit log holds only positions
/// that admitted no event.
pub(crate) async fn assert_commit_log(conn: &mut PgConnection) {
    let shared = count(
        conn,
        "SELECT count(*) FROM (
           SELECT commit_seq FROM (SELECT commit_seq FROM core.events
             UNION ALL SELECT original_commit_seq FROM core.event_tombstones) p
           GROUP BY commit_seq HAVING count(*) > 1) shared",
    )
    .await;
    assert_eq!(
        shared, 0,
        "one original position per retained reading or tombstone skeleton"
    );
    let invalid = count(
        conn,
        "SELECT count(*) FROM (SELECT commit_seq FROM core.events
             UNION ALL SELECT original_commit_seq FROM core.event_tombstones
             UNION ALL SELECT commit_seq FROM core.commit_log) p
         WHERE p.commit_seq IS NULL OR p.commit_seq < 1
           OR p.commit_seq > (SELECT value FROM core.counters WHERE name='commit_seq')",
    )
    .await;
    assert_eq!(invalid, 0, "every position is a real allocated one");
    let marker_events = count(
        conn,
        "SELECT count(*) FROM core.commit_log c WHERE
           EXISTS(SELECT 1 FROM core.events e WHERE e.commit_seq=c.commit_seq)
           OR EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.original_commit_seq=c.commit_seq)",
    )
    .await;
    assert_eq!(
        marker_events, 0,
        "positions in the commit log never stand for events"
    );
    let invalid_history = count(
        conn,
        "WITH RECURSIVE replaced_descendants(commit_seq,event_id) AS (
             SELECT a.commit_seq,edge.child FROM audit.archive_changes a
             JOIN core.event_children edge ON edge.parent=a.event_id
             WHERE a.reason='newer_reading' AND a.archived
             UNION
             SELECT d.commit_seq,edge.child FROM replaced_descendants d
             JOIN core.event_children edge ON edge.parent=d.event_id)
         SELECT count(*) FROM audit.archive_changes a
         WHERE a.commit_seq < 1
            OR a.commit_seq > (SELECT value FROM core.counters WHERE name='commit_seq')
            OR ((EXISTS(SELECT 1 FROM core.events e WHERE e.commit_seq=a.commit_seq)
                 OR EXISTS(SELECT 1 FROM core.event_tombstones t WHERE t.original_commit_seq=a.commit_seq))
                AND a.reason <> 'newer_reading'
                AND NOT (a.reason='recomputed' AND a.archived
                    AND EXISTS(SELECT 1 FROM replaced_descendants d
                               WHERE d.commit_seq=a.commit_seq AND d.event_id=a.event_id)))",
    )
    .await;
    assert_eq!(
        invalid_history, 0,
        "only replacements and their exact recomputed descendants may share the replacement Event position"
    );
    let projection = count(
        conn,
        "WITH latest AS (SELECT DISTINCT ON(event_id) event_id,archived FROM audit.archive_changes
             ORDER BY event_id,commit_seq DESC)
         SELECT count(*) FROM latest l FULL JOIN audit.archived a USING(event_id)
         WHERE coalesce(l.archived,false) <> (a.event_id IS NOT NULL)
           AND NOT EXISTS (SELECT 1 FROM core.event_tombstones t WHERE t.event_id = coalesce(l.event_id, a.event_id))",
    )
    .await;
    assert_eq!(
        projection, 0,
        "visibility history and current archive state agree"
    );
}

pub(crate) async fn marker_count(conn: &mut PgConnection) -> i64 {
    count(conn, "SELECT count(*) FROM core.commit_log").await
}

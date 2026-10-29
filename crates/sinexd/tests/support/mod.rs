use sqlx::PgConnection;

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(conn).await.unwrap()
}

/// Readings, erased skeletons and visibility changes share global positions.
/// Excision keeps the original event slot and adds its lifecycle position.
pub(crate) async fn assert_commit_log(conn: &mut PgConnection) {
    let events = count(
        conn,
        "SELECT (SELECT count(*) FROM core.events)+(SELECT count(*) FROM core.event_tombstones)",
    )
    .await;
    let logged = count(
        conn,
        "SELECT count(*) FROM core.commit_log WHERE event_id IS NOT NULL",
    )
    .await;
    assert_eq!(
        logged, events,
        "one original event-bearing commit row per retained reading or excised skeleton"
    );
    let disagree = count(
        conn,
        "SELECT count(*) FROM
         (SELECT id,event_type,commit_seq FROM core.events
          UNION ALL SELECT event_id,event_type,original_commit_seq FROM core.event_tombstones) e FULL JOIN
         (SELECT * FROM core.commit_log WHERE event_id IS NOT NULL) c ON c.event_id=e.id
         WHERE e.id IS NULL OR c.event_id IS NULL
            OR (e.event_type,e.commit_seq) IS DISTINCT FROM (c.event_type,c.commit_seq)",
    )
    .await;
    assert_eq!(disagree, 0, "event IDs, types and positions agree exactly");
    let invalid = count(conn,
        "SELECT count(*) FROM core.commit_log c WHERE
           (c.event_id IS NULL) <> (c.event_type IS NULL)
           OR c.commit_seq < 1
           OR c.commit_seq > (SELECT value FROM core.counters WHERE name='commit_seq')
           OR (c.event_id IS NULL AND EXISTS(SELECT 1 FROM core.events e WHERE e.commit_seq=c.commit_seq))").await;
    assert_eq!(
        invalid, 0,
        "typed metadata markers have real positions and never stand for Events"
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
         SELECT count(*) FROM audit.archive_changes a LEFT JOIN core.commit_log c USING(commit_seq)
         WHERE a.commit_seq < 1
            OR a.commit_seq > (SELECT value FROM core.counters WHERE name='commit_seq')
            OR (c.event_id IS NOT NULL AND a.reason <> 'newer_reading'
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
         WHERE coalesce(l.archived,false) <> (a.event_id IS NOT NULL)",
    )
    .await;
    assert_eq!(
        projection, 0,
        "visibility history and current archive state agree"
    );
}

pub(crate) async fn marker_count(conn: &mut PgConnection) -> i64 {
    count(
        conn,
        "SELECT count(*) FROM core.commit_log WHERE event_id IS NULL",
    )
    .await
}

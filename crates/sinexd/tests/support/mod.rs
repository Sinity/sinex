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
        "SELECT count(*) FROM audit.archive_changes a LEFT JOIN core.commit_log c USING(commit_seq)
         WHERE a.commit_seq < 1
            OR a.commit_seq > (SELECT value FROM core.counters WHERE name='commit_seq')
            OR (a.reason <> 'newer_reading' AND c.event_id IS NOT NULL)",
    )
    .await;
    assert_eq!(
        invalid_history, 0,
        "visibility transitions use their own positions; replacements may share an Event position"
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

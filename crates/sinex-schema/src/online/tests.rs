use super::*;
use crate::{Applied, VERSION, apply};
use sqlx::{Connection, Executor};
use std::collections::BTreeSet;

/// Each derived event's parents, by insertion number; events 1 to 6 are
/// recorded, 7 to 14 derived, so ids ascend with the number.
const LINEAGE: &[(i32, &[i32])] = &[
    (7, &[1, 2]),
    (8, &[7]),
    (9, &[3, 7]),
    (10, &[8, 9]),
    (11, &[5]),
    (12, &[11, 4]),
    (13, &[10]),
    (14, &[6, 13]),
];

async fn version_two(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("../tests/version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../tests/version_two.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
}

/// A version-two store with the lineage above and two published copies,
/// upgraded; returns the event ids in insertion order.
async fn upgraded_with_lineage(conn: &mut PgConnection) -> Vec<String> {
    version_two(conn).await;
    let mut tx = conn.begin().await.unwrap();
    tx.execute(
        "CREATE TEMPORARY TABLE numbered ON COMMIT DROP AS SELECT n, uuidv7() AS id FROM generate_series(1, 14) n ORDER BY n",
    )
    .await
    .unwrap();
    let parents: Vec<String> = LINEAGE
        .iter()
        .map(|(child, parents)| {
            format!(
                "({child}, ARRAY[{}])",
                parents
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect();
    tx.execute(
        format!(
            "INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,parents,ts_quality,ts_orig,commit_seq,payload,reading_order,record_hash,class)
             SELECT n.id, 'command.imported', 'test', '00000000-0000-7000-8000-000000000001'::uuid,
               CASE WHEN l.child IS NULL THEN uuidv7() END, CASE WHEN l.child IS NULL THEN n.n END,
               (SELECT array_agg(p.id ORDER BY p.n) FROM numbered p WHERE p.n = ANY (l.parents)),
               'unknown', NULL, n.n, '{{}}', 1, decode(repeat('01',32),'hex'), 'open'
             FROM numbered n LEFT JOIN (VALUES {}) l(child, parents) ON l.child = n.n
             ORDER BY n.n",
            parents.join(", ")
        )
        .as_str(),
    )
    .await
    .unwrap();
    let ids: Vec<String> = sqlx::query_scalar("SELECT id::text FROM numbered ORDER BY n")
        .fetch_all(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    conn.execute(
        "INSERT INTO core.published (event_id, stream_seq) SELECT id, commit_seq FROM core.events WHERE commit_seq IN (1, 7)",
    )
    .await
    .unwrap();
    assert_eq!(
        apply(conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    ids
}

/// Every event's children and parents through the walk functions.
async fn walks(conn: &mut PgConnection) -> Vec<(String, Vec<String>, Vec<String>)> {
    sqlx::query_as(
        "SELECT e.id::text,
           ARRAY(SELECT c.child::text FROM core.event_children_of(e.id) c ORDER BY 1),
           ARRAY(SELECT p.parent::text FROM core.event_parents_of(e.id) p ORDER BY 1)
         FROM core.events e ORDER BY e.id",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap()
}

/// The walks the lineage defines, computed without the database.
fn expected_walks(ids: &[String]) -> Vec<(String, Vec<String>, Vec<String>)> {
    let id = |n: i32| ids[usize::try_from(n - 1).unwrap()].clone();
    let mut rows: Vec<_> = (1..=14)
        .map(|n| {
            let children: BTreeSet<String> = LINEAGE
                .iter()
                .filter(|(_, parents)| parents.contains(&n))
                .map(|(child, _)| id(*child))
                .collect();
            let parents: BTreeSet<String> = LINEAGE
                .iter()
                .find(|(child, _)| *child == n)
                .map(|(_, parents)| parents.iter().map(|p| id(*p)).collect())
                .unwrap_or_default();
            (
                id(n),
                children.into_iter().collect(),
                parents.into_iter().collect(),
            )
        })
        .collect();
    rows.sort();
    rows
}

async fn edges(conn: &mut PgConnection) -> Vec<(String, String)> {
    sqlx::query_as("SELECT parent::text, child::text FROM core.event_children ORDER BY 1, 2")
        .fetch_all(&mut *conn)
        .await
        .unwrap()
}

async fn catalog(conn: &mut PgConnection) -> Vec<String> {
    crate::tests::catalog(conn).await.unwrap()
}

#[devkit::test]
async fn work_after_start_resumes_in_order_and_walks_answer_as_filled(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await;
    let ids = upgraded_with_lineage(&mut conn).await;
    let expected = expected_walks(&ids);
    assert_eq!(remaining(&mut conn).await.unwrap(), Task::ALL);
    assert!(
        edges(&mut conn).await.is_empty(),
        "the upgrade fills no edge"
    );
    assert_eq!(walks(&mut conn).await, expected);

    assert_eq!(
        step(&mut conn).await.unwrap(),
        Progress::Finished {
            task: Task::PublishedKey
        }
    );
    assert_eq!(
        step(&mut conn).await.unwrap(),
        Progress::Finished {
            task: Task::FeedIndex
        }
    );
    // A preceding build committed edges through event 9. The replacement
    // must honor that prefix, without changing fallback answers.
    sqlx::query("INSERT INTO core.event_children(parent,child) SELECT p.parent,e.id FROM core.events e CROSS JOIN LATERAL unnest(e.parents) p(parent) WHERE e.id<=$1::text::uuid ON CONFLICT DO NOTHING")
        .bind(&ids[8]).execute(&mut conn).await.unwrap();
    sqlx::query("UPDATE sinex_schema.event_children_fill SET filled_through=$1::text::uuid")
        .bind(&ids[8])
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(walks(&mut conn).await, expected);
    assert_eq!(edges(&mut conn).await.len(), 5);

    // A restart resumes after event 9.
    drop(conn);
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mark: String =
        sqlx::query_scalar("SELECT filled_through::text FROM sinex_schema.event_children_fill")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(mark, ids[8]);
    assert_eq!(remaining(&mut conn).await.unwrap(), [Task::EventChildren]);
    // An edge admission already wrote is not written again.
    conn.execute(
        format!(
            "INSERT INTO core.event_children VALUES ('{}', '{}')",
            ids[7], ids[9]
        )
        .as_str(),
    )
    .await
    .unwrap();
    assert_eq!(walks(&mut conn).await, expected);
    // The complete remaining set streams in one pass, including edges that
    // admission already wrote.
    assert_eq!(
        step(&mut conn).await.unwrap(),
        Progress::Finished {
            task: Task::EventChildren
        }
    );
    assert_eq!(walks(&mut conn).await, expected);
    finish(&mut conn).await.unwrap();
    assert_eq!(walks(&mut conn).await, expected);
    let lineage: Vec<(String, String)> = sqlx::query_as(
        "SELECT p::text, e.id::text FROM core.events e CROSS JOIN LATERAL unnest(e.parents) p ORDER BY 1, 2",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(edges(&mut conn).await, lineage);
    assert!(remaining(&mut conn).await.unwrap().is_empty());
    assert_eq!(catalog(&mut conn).await, fresh);
    assert_eq!(step(&mut conn).await.unwrap(), Progress::Done);
}

async fn index_oid(conn: &mut PgConnection, name: &str) -> Option<i64> {
    sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(name)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

async fn invalidate(conn: &mut PgConnection, name: &str) {
    sqlx::query("UPDATE pg_index SET indisvalid = false WHERE indexrelid = to_regclass($1)")
        .bind(name)
        .execute(&mut *conn)
        .await
        .unwrap();
}

#[devkit::test]
async fn interrupted_index_builds_are_rebuilt_and_finished_ones_kept(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await;
    upgraded_with_lineage(&mut conn).await;
    // An interrupted concurrent build leaves an invalid index under the name.
    conn.execute("CREATE UNIQUE INDEX published_pairs_pkey ON core.published (event_id, stream_seq, stream_instance)")
        .await
        .unwrap();
    invalidate(&mut conn, "core.published_pairs_pkey").await;
    let invalid = index_oid(&mut conn, "core.published_pairs_pkey").await;
    assert_eq!(
        step(&mut conn).await.unwrap(),
        Progress::Finished {
            task: Task::PublishedKey
        }
    );
    assert_ne!(
        index_oid(&mut conn, "core.published_pairs_pkey").await,
        invalid
    );
    let key: String = sqlx::query_scalar("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'core.published'::regclass AND contype = 'p'")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(key, "PRIMARY KEY (event_id, stream_seq, stream_instance)");

    // A chunk-by-chunk build that stopped leaves the index invalid.
    conn.execute(crate::tables::EVENTS_FEED).await.unwrap();
    invalidate(&mut conn, "core.events_feed").await;
    let invalid = index_oid(&mut conn, "core.events_feed").await;
    assert_eq!(
        step(&mut conn).await.unwrap(),
        Progress::Finished {
            task: Task::FeedIndex
        }
    );
    assert_ne!(index_oid(&mut conn, "core.events_feed").await, invalid);

    // A build that finished just before its task ended is kept.
    conn.execute(begin(Task::FeedIndex).join("; ").as_str())
        .await
        .unwrap();
    let built = index_oid(&mut conn, "core.events_feed").await;
    finish(&mut conn).await.unwrap();
    assert_eq!(index_oid(&mut conn, "core.events_feed").await, built);
    let invalid: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname IN ('core', '_timescaledb_internal') AND NOT i.indisvalid",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(invalid, 0);
    assert_eq!(catalog(&mut conn).await, fresh);
}

/// A session that still holds the work, such as a stopped daemon's backend
/// finishing an index build, is waited for.
#[devkit::test]
async fn a_step_waits_while_another_session_holds_the_work(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    upgraded_with_lineage(&mut conn).await;
    let mut other = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(WORK_LOCK)
        .execute(&mut other)
        .await
        .unwrap();
    assert_eq!(step(&mut conn).await.unwrap(), Progress::Waiting);
    assert_eq!(remaining(&mut conn).await.unwrap(), Task::ALL);
    drop(other);
    // The lock ends with the session.
    finish(&mut conn).await.unwrap();
    assert!(remaining(&mut conn).await.unwrap().is_empty());
}

async fn plan(conn: &mut PgConnection, query: &str) -> String {
    conn.execute("SET enable_seqscan = off").await.unwrap();
    let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {query}"))
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    conn.execute("RESET enable_seqscan").await.unwrap();
    plan.join("\n")
}

/// Feed pages read the chunks in commit order without the feed index and
/// through it once it exists, with identical results.
#[devkit::test]
async fn readers_use_other_indexes_until_event_indexes_exist(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    upgraded_with_lineage(&mut conn).await;
    assert_eq!(index_oid(&mut conn, "core.events_feed").await, None);
    assert_eq!(index_oid(&mut conn, "core.events_coverage").await, None);
    let feed =
        "SELECT id::text FROM core.events WHERE commit_seq > 3 ORDER BY commit_seq, id LIMIT 5";
    let unindexed = plan(&mut conn, feed).await;
    assert!(!unindexed.contains("events_feed"), "{unindexed}");
    let before: Vec<String> = sqlx::query_scalar(feed).fetch_all(&mut conn).await.unwrap();
    finish(&mut conn).await.unwrap();
    let indexed = plan(&mut conn, feed).await;
    assert!(indexed.contains("events_feed"), "{indexed}");
    let after: Vec<String> = sqlx::query_scalar(feed).fetch_all(&mut conn).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(after.len(), 5);
}

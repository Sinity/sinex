use super::*;
use sinex_model::lifecycle::Act;
use sqlx::Executor;

/// A reading stored before the upgrade and tombstoned before the fill reached
/// it keeps its parent edges, as it would have on a filled store, and chain
/// walks during the fill find it through its parents.
#[devkit::test]
async fn a_tombstone_during_the_edge_fill_keeps_its_edges(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = keyed("source", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![parent.clone()])).await;
    let mut child = keyed("derived", 2, 2, 1);
    child.origin = Origin::Derived {
        parents: vec![parent.id],
    };
    admit_one(&mut conn, &batch(vec![child.clone()])).await;
    let mut grandchild = keyed("derived", 3, 3, 1);
    grandchild.origin = Origin::Derived {
        parents: vec![child.id],
    };
    admit_one(&mut conn, &batch(vec![grandchild.clone()])).await;
    let filled: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT parent, child FROM core.event_children ORDER BY parent, child")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    super::equivalence::version_two_keys(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded {
            from: 2,
            to: sinex_schema::VERSION
        }
    );
    let edges = "SELECT parent, child FROM core.event_children ORDER BY parent, child";
    let none: Vec<(Uuid, Uuid)> = sqlx::query_as(edges).fetch_all(&mut conn).await.unwrap();
    assert!(none.is_empty(), "the upgrade leaves the edges to the fill");

    // The archive walks the chain from the child through the pending walk.
    let archived =
        crate::lifecycle::move_events(&mut conn, Act::Archive, vec![child.id], &NoDerivations)
            .await;
    assert!(
        matches!(archived, crate::lifecycle::Ended::Ok { .. }),
        "{archived:?}"
    );
    let chain: Vec<Uuid> =
        sqlx::query_scalar("SELECT event_id FROM audit.archived ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let mut expected = vec![child.id.0, grandchild.id.0];
    expected.sort();
    assert_eq!(chain, expected, "the walk found the grandchild");
    let tombstoned =
        crate::lifecycle::move_events(&mut conn, Act::Tombstone, vec![child.id], &NoDerivations)
            .await;
    assert!(
        matches!(tombstoned, crate::lifecycle::Ended::Ok { .. }),
        "{tombstoned:?}"
    );
    let gone: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE id = ANY($1)")
        .bind(&expected)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(gone, 0);

    sinex_schema::online::finish(&mut conn).await.unwrap();
    let after: Vec<(Uuid, Uuid)> = sqlx::query_as(edges).fetch_all(&mut conn).await.unwrap();
    assert_eq!(after, filled);
}

/// The fill can pause after taking its tombstone lock without blocking a
/// new admission. A tombstone waits for that pass, then keeps the same edges.
#[devkit::test]
async fn admission_and_tombstone_keep_edges_while_the_retained_pass_is_blocked(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let parent = keyed("concurrent-source", 1, 1, 1);
    admit_one(&mut conn, &batch(vec![parent.clone()])).await;
    let mut child = keyed("concurrent-derived", 2, 2, 1);
    child.origin = Origin::Derived {
        parents: vec![parent.id],
    };
    admit_one(&mut conn, &batch(vec![child.clone()])).await;
    super::equivalence::version_two_keys(&mut conn).await;
    conn.execute("UPDATE sinex_schema.applied SET version=2")
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    for _ in 0..2 {
        sinex_schema::online::step(&mut conn).await.unwrap();
    }
    assert_eq!(
        sinex_schema::online::remaining(&mut conn).await.unwrap(),
        [sinex_schema::online::Task::EventChildren]
    );
    let mut blocker = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut blocker)
        .await
        .unwrap();
    let mut barrier = blocker.begin().await.unwrap();
    sqlx::query("SELECT filled_through FROM sinex_schema.event_children_fill FOR UPDATE")
        .execute(&mut *barrier)
        .await
        .unwrap();
    let mut fill = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let fill_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut fill)
        .await
        .unwrap();
    let mut fill_task = tokio::spawn(async move { sinex_schema::online::finish(&mut fill).await });
    let mut monitor = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    loop {
        let blocked: bool = sqlx::query_scalar("SELECT $1=ANY(pg_blocking_pids($2))")
            .bind(blocker_pid)
            .bind(fill_pid)
            .fetch_one(&mut monitor)
            .await
            .unwrap();
        if blocked {
            break;
        }
        tokio::select! {
            result=&mut fill_task=>panic!("fill ended before its mark barrier: {result:?}"),
            _=tokio::task::yield_now()=>{}
        }
    }
    let mut grandchild = keyed("concurrent-derived", 3, 3, 1);
    grandchild.origin = Origin::Derived {
        parents: vec![child.id],
    };
    admit_one(&mut conn, &batch(vec![grandchild.clone()])).await;
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM core.event_children WHERE parent=$1 AND child=$2)"
        )
        .bind(child.id.0)
        .bind(grandchild.id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap()
    );
    let archived =
        crate::lifecycle::move_events(&mut conn, Act::Archive, vec![child.id], &NoDerivations)
            .await;
    assert!(
        matches!(archived, crate::lifecycle::Ended::Ok { .. }),
        "{archived:?}"
    );
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let tombstone =
        crate::lifecycle::move_events(&mut conn, Act::Tombstone, vec![child.id], &NoDerivations);
    tokio::pin!(tombstone);
    loop {
        tokio::select! {
            result=&mut tombstone=>panic!("tombstone passed the fill's lock: {result:?}"),
            blocked=sqlx::query_scalar::<_,bool>("SELECT $1=ANY(pg_blocking_pids($2))")
                .bind(fill_pid).bind(writer_pid).fetch_one(&mut monitor)=> {
                if blocked.unwrap() {break}
            }
        }
    }
    barrier.commit().await.unwrap();
    let ended = tombstone.await;
    assert!(
        matches!(ended, crate::lifecycle::Ended::Ok { .. }),
        "{ended:?}"
    );
    fill_task.await.unwrap().unwrap();
    let mut after = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    let mut edges: Vec<(Uuid, Uuid)> =
        sqlx::query_as("SELECT parent,child FROM core.event_children ORDER BY parent,child")
            .fetch_all(&mut after)
            .await
            .unwrap();
    let mut expected = vec![(parent.id.0, child.id.0), (child.id.0, grandchild.id.0)];
    edges.sort();
    expected.sort();
    assert_eq!(edges, expected);
}

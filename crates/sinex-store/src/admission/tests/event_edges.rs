use super::*;
use sinex_model::lifecycle::Act;

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

use super::*;
use sqlx::{Connection, Executor};

async fn seed(conn: &mut PgConnection) -> Vec<String> {
    conn.execute("WITH prefix AS MATERIALIZED (SELECT left(uuidv7()::text,24) AS value),
        numbered AS (SELECT n,(value||lpad(to_hex(n),12,'0'))::uuid AS id,
          (value||'000000000001')::uuid AS root FROM prefix CROSS JOIN generate_series(1,8194)n)
        INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,parents,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        SELECT id,'command.imported','synthetic-edges',uuidv7(),CASE WHEN n=1 THEN uuidv7() END,
          CASE WHEN n=1 THEN 1 END,CASE WHEN n>1 THEN ARRAY[root,root] END,
          'unknown',n,'{}',1,decode(repeat('01',32),'hex'),'open' FROM numbered").await.unwrap();
    for statement in begin() {
        conn.execute(statement.as_str()).await.unwrap();
    }
    sqlx::query_scalar("SELECT id::text FROM core.events ORDER BY id")
        .fetch_all(conn)
        .await
        .unwrap()
}

async fn count_edges(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core.event_children")
        .fetch_one(conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn one_pass_keeps_duplicates_as_exact_sets_and_resumes_a_committed_prefix(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = crate::catalog(&mut conn).await.unwrap();
    let ids = seed(&mut conn).await;
    sqlx::query("INSERT INTO core.event_children(parent,child) SELECT p.parent,e.id FROM core.events e CROSS JOIN LATERAL unnest(e.parents) p(parent) WHERE e.id<=$1::text::uuid ON CONFLICT DO NOTHING")
        .bind(&ids[2]).execute(&mut conn).await.unwrap();
    sqlx::query("UPDATE sinex_schema.event_children_fill SET filled_through=$1::text::uuid")
        .bind(&ids[2])
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(count_edges(&mut conn).await, 2);
    conn.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        crate::online::step(&mut conn).await.unwrap(),
        crate::online::Progress::Finished {
            task: Task::EventChildren
        }
    );
    let edges: Vec<(String, String)> =
        sqlx::query_as("SELECT parent::text,child::text FROM core.event_children ORDER BY child")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        edges,
        ids[1..]
            .iter()
            .map(|child| (ids[0].clone(), child.clone()))
            .collect::<Vec<_>>()
    );
    assert_eq!(crate::catalog(&mut conn).await.unwrap(), fresh);
}

#[devkit::test]
async fn failed_prefix_commit_rolls_back_edges_and_keeps_fallback_answers(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let ids = seed(&mut conn).await;
    conn.execute("CREATE FUNCTION pg_temp.fail_edge_cut() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic cut failure'; END $$;
        CREATE TRIGGER fail_edge_cut BEFORE UPDATE ON sinex_schema.event_children_fill FOR EACH ROW EXECUTE FUNCTION pg_temp.fail_edge_cut()")
        .await.unwrap();
    assert!(fill(&mut conn).await.is_err());
    assert_eq!(count_edges(&mut conn).await, 0);
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.event_children_of($1::text::uuid)")
            .bind(&ids[0])
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(visible, 8193);
    conn.execute("DROP TRIGGER fail_edge_cut ON sinex_schema.event_children_fill")
        .await
        .unwrap();
    assert_eq!(fill(&mut conn).await.unwrap(), 8193);
    assert_eq!(count_edges(&mut conn).await, 8193);
    finish(&mut conn).await.unwrap();
}

#[devkit::test]
async fn prepared_fallback_walk_survives_each_finish_commit_and_restart(
    db: devkit::pg::TestDatabase,
) {
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    let ids = seed(&mut writer).await;
    let mut reader = PgConnection::connect(&db.url(None)).await.unwrap();
    reader
        .execute("SET plan_cache_mode=force_generic_plan")
        .await
        .unwrap();
    let query = "SELECT child::text FROM core.event_children_of($1::text::uuid) ORDER BY child";
    let expected = ids[1..].to_vec();
    let before: Vec<String> = sqlx::query_scalar(query)
        .bind(&ids[0])
        .fetch_all(&mut reader)
        .await
        .unwrap();
    assert_eq!(before, expected);
    fill(&mut writer).await.unwrap();
    let copied: Vec<String> = sqlx::query_scalar(query)
        .bind(&ids[0])
        .fetch_all(&mut reader)
        .await
        .unwrap();
    assert_eq!(copied, expected);
    install_walks(&mut writer).await.unwrap();
    let installed: Vec<String> = sqlx::query_scalar(query)
        .bind(&ids[0])
        .fetch_all(&mut reader)
        .await
        .unwrap();
    assert_eq!(installed, expected);
    writer.close().await.unwrap();
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        fill(&mut writer).await.unwrap(),
        0,
        "committed prefix is not copied twice"
    );
    finish(&mut writer).await.unwrap();
    let retired: Vec<String> = sqlx::query_scalar(query)
        .bind(&ids[0])
        .fetch_all(&mut reader)
        .await
        .unwrap();
    assert_eq!(retired, expected);
    assert!(
        !crate::online::pending(&mut writer, Task::EventChildren)
            .await
            .unwrap()
    );
}

#[devkit::test]
async fn an_initial_nil_prefix_needs_no_native_timestamp_lower_bound(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    seed(&mut conn).await;
    conn.execute("UPDATE sinex_schema.event_children_fill SET filled_through='00000000-0000-0000-0000-000000000000'")
        .await.unwrap();
    assert_eq!(fill(&mut conn).await.unwrap(), 8193);
    assert_eq!(count_edges(&mut conn).await, 8193);
    finish(&mut conn).await.unwrap();
}

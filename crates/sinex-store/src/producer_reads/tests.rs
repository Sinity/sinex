use super::*;
use sqlx::{Connection, Executor};

async fn tuple_work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.ingress_frames'::regclass")
        .fetch_one(conn).await.unwrap()
}

/// Run the real storage APIs, so this work oracle follows their SQL changes.
async fn reads(conn: &mut PgConnection, ids: &[Uuid]) -> (serde_json::Value, Vec<i64>) {
    let mut work = Vec::new();
    let before = tuple_work(conn).await;
    let mut owners = material_owners(conn, ids).await.unwrap();
    work.push(tuple_work(conn).await - before);
    owners.sort();
    let before = tuple_work(conn).await;
    let unknown = family_unknown(conn, "p", ids, "family-a").await.unwrap();
    work.push(tuple_work(conn).await - before);
    let before = tuple_work(conn).await;
    let family = material_family(conn, ids[0]).await.unwrap();
    work.push(tuple_work(conn).await - before);
    let before = tuple_work(conn).await;
    let mut sources = material_sources(conn, ids).await.unwrap();
    work.push(tuple_work(conn).await - before);
    sources.sort();
    let before = tuple_work(conn).await;
    let mut registered = materials_registered_by(conn, "p", ids).await.unwrap();
    work.push(tuple_work(conn).await - before);
    registered.sort();
    let before = tuple_work(conn).await;
    let refusals = crate::diagnostics::ingress_refusals(conn).await.unwrap();
    work.push(tuple_work(conn).await - before);
    let before = tuple_work(conn).await;
    let errors = crate::diagnostics::producer_errors(conn).await.unwrap();
    work.push(tuple_work(conn).await - before);
    (
        serde_json::json!({"owners":owners,"unknown":unknown,"family":family,
        "sources":sources,"registered":registered,"refusals":refusals,"errors":errors}),
        work,
    )
}

#[devkit::test]
async fn receipt_authority_and_diagnostics_ignore_unrelated_successful_history(
    db: devkit::pg::TestDatabase,
) {
    let mut setup = PgConnection::connect(&db.url(None)).await.unwrap();
    setup.execute("INSERT INTO core.ingress_frames(producer,frame,material_id,outcome,family,source_name,source_version)
        SELECT 'p','success-'||n,uuidv7(),'{\"result\":\"admitted\"}','unrelated','unrelated.source',1
        FROM generate_series(1,8192)n").await.unwrap();
    let ids = [Uuid::from_u128(1), Uuid::from_u128(2)];
    sqlx::query("INSERT INTO core.ingress_frames(producer,frame,material_id,outcome,refusal,family,write_kind_prefixes,source_name,source_version,received_at) VALUES
        ('p','unknown-origin',$1,'{\"result\":\"refused\",\"kind\":\"malformed\",\"source\":null,\"event_types\":[]}','old unknown',NULL,NULL,NULL,NULL,now()-interval '2 days'),
        ('p','first-owner',$1,'{\"result\":\"admitted\"}',NULL,'family-a',ARRAY['example.'],'example.source',3,now()),
        ('q','other-owner',$1,'{\"result\":\"admitted\"}',NULL,'family-b',ARRAY['other.'],'other.source',4,now()),
        ('p','recent-refusal',$2,'{\"result\":\"refused\",\"kind\":\"malformed\",\"source\":null,\"event_types\":[]}','bad shape',NULL,NULL,NULL,NULL,now()-interval '1 hour'),
        ('p','split-kind',NULL,'{\"result\":\"refused\",\"kind\":\"invalid\",\"source\":\"example.source\",\"event_types\":[\"example.a\",\"example.b\"]}','bad content',NULL,NULL,NULL,NULL,now()),
        ('p','last-malformed',NULL,'{\"result\":\"refused\",\"kind\":\"malformed\",\"source\":null,\"event_types\":[]}','last shape',NULL,NULL,NULL,NULL,now()-interval '2 days'),
        ('q','q-malformed',NULL,'{\"result\":\"refused\",\"kind\":\"malformed\",\"source\":\"other.source\",\"event_types\":[\"example.c\"]}','q shape',NULL,NULL,NULL,NULL,now())")
        .bind(ids[0]).bind(ids[1]).execute(&mut setup).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM core.ingress_frames WHERE refusal IS NULL AND family='unrelated'"
        )
        .fetch_one(&mut setup)
        .await
        .unwrap(),
        8192
    );
    setup.execute("ANALYZE core.ingress_frames").await.unwrap();
    setup.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_indexonlyscan=off")
        .await
        .unwrap();
    let (indexed, work) = reads(&mut tx, &ids).await;
    assert!(
        work.iter().all(|&rows| rows <= 32),
        "receipt tuple work {work:?}"
    );
    // Repeated calls also exercise PostgreSQL's generic prepared plans.
    for _ in 0..7 {
        let (again, work) = reads(&mut tx, &ids).await;
        assert_eq!(again, indexed);
        assert!(
            work.iter().all(|&rows| rows <= 32),
            "prepared receipt tuple work {work:?}"
        );
    }
    assert_eq!(
        indexed["owners"],
        serde_json::json!([[ids[0], "family-a"], [ids[0], "family-b"]])
    );
    assert_eq!(indexed["unknown"], true);
    assert_eq!(
        indexed["family"],
        serde_json::json!(["family-a", ["example."]])
    );
    assert_eq!(
        indexed["sources"],
        serde_json::json!([
            [ids[0], null, null],
            [ids[0], "example.source", 3],
            [ids[0], "other.source", 4],
            [ids[1], null, null]
        ])
    );
    assert_eq!(indexed["registered"], serde_json::json!([ids[0]]));
    assert_eq!(
        indexed["refusals"],
        serde_json::json!([
        {"producer":"p","source":"example.source","event_type":"example.a","refusal_kind":"invalid","frames":1},
        {"producer":"p","source":"example.source","event_type":"example.b","refusal_kind":"invalid","frames":1},
        {"producer":"p","source":null,"event_type":null,"refusal_kind":"malformed","frames":1},
        {"producer":"q","source":"other.source","event_type":"example.c","refusal_kind":"malformed","frames":1}])
    );
    assert_eq!(
        indexed["errors"],
        serde_json::json!([
        {"producer":"p","frame":"last-malformed","reason":"last shape"},
        {"producer":"q","frame":"q-malformed","reason":"q shape"}])
    );
    assert!(material_family(&mut tx, ids[1]).await.unwrap().is_none());
    assert!(
        !family_unknown(&mut tx, "p", &[Uuid::from_u128(3)], "family-a")
            .await
            .unwrap()
    );
    tx.execute("DROP INDEX core.ingress_frames_material, core.ingress_frames_refused_at, core.ingress_frames_malformed_producer")
        .await.unwrap();
    let (unindexed, control) = reads(&mut tx, &ids).await;
    assert_eq!(
        unindexed, indexed,
        "indexes preserve unknown/source/family and refusal semantics"
    );
    for i in [0, 2, 3, 5, 6] {
        assert!(
            control[i] >= 8192,
            "unindexed positive control {i} visited {} tuples",
            control[i]
        );
    }
    tx.rollback().await.unwrap();
}

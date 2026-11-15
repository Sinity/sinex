use super::*;
use sqlx::{Connection, Executor};

/// Run the real storage APIs over one fixture.
async fn reads(conn: &mut PgConnection, ids: &[Uuid]) -> serde_json::Value {
    let mut owners = material_owners(conn, ids).await.unwrap();
    owners.sort();
    let unknown = family_unknown(conn, "p", ids, "family-a").await.unwrap();
    let family = material_family(conn, ids[0]).await.unwrap();
    let mut sources = material_sources(conn, ids).await.unwrap();
    sources.sort();
    let mut registered = materials_registered_by(conn, "p", ids).await.unwrap();
    registered.sort();
    let refusals = crate::diagnostics::ingress_refusals(conn).await.unwrap();
    let errors = crate::diagnostics::producer_errors(conn).await.unwrap();
    serde_json::json!({"owners":owners,"unknown":unknown,"family":family,
        "sources":sources,"registered":registered,"refusals":refusals,"errors":errors})
}

#[devkit::test]
async fn receipt_authority_and_diagnostics_report_only_relevant_frames(
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
    setup.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    let found = reads(&mut tx, &ids).await;
    assert_eq!(
        found["owners"],
        serde_json::json!([[ids[0], "family-a"], [ids[0], "family-b"]])
    );
    assert_eq!(found["unknown"], true);
    assert_eq!(
        found["family"],
        serde_json::json!(["family-a", ["example."]])
    );
    assert_eq!(
        found["sources"],
        serde_json::json!([
            [ids[0], null, null],
            [ids[0], "example.source", 3],
            [ids[0], "other.source", 4],
            [ids[1], null, null]
        ])
    );
    assert_eq!(found["registered"], serde_json::json!([ids[0]]));
    assert_eq!(
        found["refusals"],
        serde_json::json!([
        {"producer":"p","source":"example.source","event_type":"example.a","refusal_kind":"invalid","frames":1},
        {"producer":"p","source":"example.source","event_type":"example.b","refusal_kind":"invalid","frames":1},
        {"producer":"p","source":null,"event_type":null,"refusal_kind":"malformed","frames":1},
        {"producer":"q","source":"other.source","event_type":"example.c","refusal_kind":"malformed","frames":1}])
    );
    assert_eq!(
        found["errors"],
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
    tx.rollback().await.unwrap();
}

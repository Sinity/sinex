use super::*;

#[devkit::test]
async fn wide_ordinals_upgrade_preserves_compressed_records_views_and_grants(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    let fixture = include_str!("../preserved_rows.sql")
        .replace(
            "anchor,ts_quality,commit_seq",
            "anchor,output_index,ts_quality,commit_seq",
        )
        .replace(
            "000000000013',i,'unknown',i,",
            "000000000013',i,32767,'unknown',i,",
        );
    assert!(fixture.contains("i,32767,'unknown'"));
    sqlx::raw_sql(&fixture).execute(&mut conn).await.unwrap();
    let before_ordinal = upgrades::LANDED_VERSION
        + i32::try_from(
            upgrades::NAMED
                .iter()
                .position(|name| *name == "output_ordinals")
                .unwrap(),
        )
        .unwrap();
    for from in 2..before_ordinal {
        let mut tx = conn.begin().await.unwrap();
        for statement in &upgrades::upgrades()[usize::try_from(from).unwrap()] {
            tx.execute(statement.as_str()).await.unwrap();
        }
        sqlx::query("INSERT INTO sinex_schema.applied(version) VALUES($1)")
            .bind(from + 1)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    crate::kinds::upgrade_historical(&mut conn, &crate::kinds::Owners::default())
        .await
        .unwrap();
    crate::description::Description::before_output_ordinals()
        .apply(&mut conn, &crate::kinds::Owners::default())
        .await
        .unwrap();
    crate::kinds::version_tests::assert_historical_and_current(&mut conn).await;
    conn.execute("CREATE VIEW public.ordinal_citations WITH (security_barrier=true) AS SELECT id,output_index FROM core.visible_events").await.unwrap();
    conn.execute("GRANT SELECT ON public.ordinal_citations TO sinex_reader WITH GRANT OPTION")
        .await
        .unwrap();
    conn.execute("COMMENT ON VIEW public.ordinal_citations IS 'synthetic ordinal citations'")
        .await
        .unwrap();
    conn.execute("GRANT SELECT (output_index) ON public.ordinal_citations TO sinex_writer")
        .await
        .unwrap();
    conn.execute("COMMENT ON COLUMN public.ordinal_citations.output_index IS 'unchanged ordinal'")
        .await
        .unwrap();
    let views:Vec<serde_json::Value>=sqlx::query_scalar("SELECT to_jsonb(v) FROM (SELECT schemaname,viewname,viewowner,definition FROM pg_views WHERE schemaname IN ('core','event_types','public') ORDER BY 1,2) v").fetch_all(&mut conn).await.unwrap();
    let legacy_max: i16 = sqlx::query_scalar("SELECT max(output_index) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(legacy_max, i16::MAX);
    let citation_sql = "SELECT 'sinex:record/'||material_id||'/'||anchor||'/'||event_type||'/'||output_index FROM core.events WHERE material_id IS NOT NULL ORDER BY id";
    let old_citations: Vec<String> = sqlx::query_scalar(citation_sql)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(old_citations.len(), 3);
    assert!(
        old_citations
            .iter()
            .all(|citation| citation.ends_with("/32767"))
    );
    let expected = stored_rows(&mut conn).await;
    let compressed: i64 =
        sqlx::query_scalar("SELECT count(compress_chunk(c)) FROM show_chunks('core.events') c")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(compressed > 0);
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: before_ordinal,
            to: VERSION
        }
    );
    assert_eq!(stored_rows(&mut conn).await, expected);
    crate::kinds::version_tests::assert_historical_and_current(&mut conn).await;
    let current_citations: Vec<String> = sqlx::query_scalar(citation_sql)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(current_citations, old_citations);
    let after:Vec<serde_json::Value>=sqlx::query_scalar("SELECT to_jsonb(v) FROM (SELECT schemaname,viewname,viewowner,definition FROM pg_views WHERE schemaname IN ('core','event_types','public') ORDER BY 1,2) v").fetch_all(&mut conn).await.unwrap();
    assert_eq!(after, views);
    let still_compressed:i64=sqlx::query_scalar("SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema='core' AND hypertable_name='events' AND is_compressed").fetch_one(&mut conn).await.unwrap();
    assert_eq!(still_compressed, compressed);
    let privilege:(bool,bool)=sqlx::query_as("SELECT has_table_privilege('sinex_reader','public.ordinal_citations','SELECT'),has_table_privilege('sinex_reader','public.ordinal_citations','SELECT WITH GRANT OPTION')").fetch_one(&mut conn).await.unwrap();
    assert_eq!(privilege, (true, true));
    let metadata:(Option<Vec<String>>,Option<String>)=sqlx::query_as("SELECT reloptions,obj_description(oid,'pg_class') FROM pg_class WHERE oid='public.ordinal_citations'::regclass").fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        metadata,
        (
            Some(vec!["security_barrier=true".into()]),
            Some("synthetic ordinal citations".into())
        )
    );
    let column_access:(bool,bool)=sqlx::query_as("SELECT has_column_privilege('sinex_writer','public.ordinal_citations','output_index','SELECT'),has_table_privilege('sinex_writer','public.ordinal_citations','SELECT')").fetch_one(&mut conn).await.unwrap();
    assert_eq!(column_access, (true, false));
    let column_comment: Option<String> =
        sqlx::query_scalar("SELECT col_description('public.ordinal_citations'::regclass,2)")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(column_comment.as_deref(), Some("unchanged ordinal"));
    let output_type:String=sqlx::query_scalar("SELECT data_type FROM information_schema.columns WHERE table_schema='public' AND table_name='ordinal_citations' AND column_name='output_index'").fetch_one(&mut conn).await.unwrap();
    assert_eq!(output_type, "bigint");
}

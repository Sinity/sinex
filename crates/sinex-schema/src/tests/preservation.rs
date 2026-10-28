use super::*;

async fn version_two(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("version_two.sql"))
        .execute(conn)
        .await
        .unwrap();
}

async fn populated(conn: &mut PgConnection) {
    sqlx::raw_sql(include_str!("version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("preserved_rows.sql"))
        .execute(conn)
        .await
        .unwrap();
}

async fn stored_rows(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    let compact: bool = sqlx::query_scalar("SELECT data_type='bytea' FROM information_schema.columns WHERE table_schema='core' AND table_name='live_equivalence' AND column_name='equivalence_key'")
        .fetch_one(&mut *conn).await.unwrap();
    let tables: Vec<(String,String)> = sqlx::query_as("SELECT schemaname,tablename FROM pg_tables WHERE schemaname=ANY($1) AND schemaname<>'sinex_schema' AND tablename<>'admission_stage' ORDER BY 1,2")
        .bind(schemas()).fetch_all(&mut *conn).await.unwrap();
    let mut result = Vec::new();
    for (schema, table) in tables {
        let name = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
        let query = match table.as_str() {
            "live_equivalence" if compact => format!(
                "SELECT to_jsonb(t)||jsonb_build_object('equivalence_key',core.decode_equivalence_key(equivalence_key)) FROM {name} t ORDER BY 1"
            ),
            "event_types" if schema == "registry" => format!(
                "SELECT to_jsonb(t) FROM {name} t WHERE name NOT IN ('operator.mark', 'query.saved') ORDER BY 1"
            ),
            "counters" => {
                format!("SELECT to_jsonb(t) FROM {name} t WHERE name <> 'blob_changes' ORDER BY 1")
            }
            "search_text" => {
                format!("SELECT to_jsonb(t)-'trigram_indexed' FROM {name} t ORDER BY 1")
            }
            _ => format!("SELECT to_jsonb(t) FROM {name} t ORDER BY 1"),
        };
        let rows: Vec<serde_json::Value> = sqlx::query_scalar(&query)
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        result.push(serde_json::json!({"table": name,"rows": rows}));
    }
    result
}

#[devkit::test]
async fn every_upgrade_keeps_populated_records_and_matches_fresh_definitions(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    version_two(&mut conn).await;
    populated(&mut conn).await;
    let expected = stored_rows(&mut conn).await;
    for from in 2..VERSION {
        let mut tx = conn.begin().await.unwrap();
        for statement in &upgrades::upgrades()[from as usize] {
            tx.execute(statement.as_str()).await.unwrap();
        }
        sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
            .bind(from + 1)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            stored_rows(&mut conn).await,
            expected,
            "upgrade from {from}"
        );
    }
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Current { version: VERSION }
    );
}

#[devkit::test]
async fn whole_chain_rolls_back_when_its_last_upgrade_fails(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    populated(&mut conn).await;
    let expected = stored_rows(&mut conn).await;
    let old_catalog = catalog(&mut conn).await.unwrap();
    conn.execute("CREATE INDEX published_pairs_pkey ON core.consumer_positions(commit_seq)")
        .await
        .unwrap();
    assert!(apply(&mut conn).await.is_err());
    conn.execute("DROP INDEX core.published_pairs_pkey")
        .await
        .unwrap();
    assert_eq!(stored_rows(&mut conn).await, expected);
    assert_eq!(catalog(&mut conn).await.unwrap(), old_catalog);
    let version: i32 = sqlx::query_scalar("SELECT max(version) FROM sinex_schema.applied")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(version, 2);
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 2,
            to: VERSION
        }
    );
    assert_eq!(stored_rows(&mut conn).await, expected);
}

#[devkit::test]
async fn chain_refuses_writer_and_newer_schema_before_changing_rows(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    version_two(&mut conn).await;
    populated(&mut conn).await;
    let expected = stored_rows(&mut conn).await;
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    for from in 2..VERSION {
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(WRITER_LOCK)
            .execute(&mut writer)
            .await
            .unwrap();
        assert!(
            matches!(apply(&mut conn).await, Err(Error::WriterRunning)),
            "from {from}"
        );
        assert_eq!(stored_rows(&mut conn).await, expected);
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(WRITER_LOCK)
            .execute(&mut writer)
            .await
            .unwrap();
        let mut tx = conn.begin().await.unwrap();
        for statement in &upgrades::upgrades()[from as usize] {
            tx.execute(statement.as_str()).await.unwrap();
        }
        sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
            .bind(from + 1)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    sqlx::query("INSERT INTO sinex_schema.applied (version) VALUES ($1)")
        .bind(VERSION + 1)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        matches!(require_current(&mut conn).await, Err(Error::Version {found}) if found==VERSION+1)
    );
    assert!(matches!(apply(&mut conn).await, Err(Error::Version {found}) if found==VERSION+1));
    assert_eq!(stored_rows(&mut conn).await, expected);
}

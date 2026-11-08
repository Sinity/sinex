use super::*;
use serde_json::Value;
use sqlx::Executor;

/// Six event chunks, each with distinct IDs in one millisecond. A
/// non-searchable event shares that millisecond, and one search row is orphaned.
async fn fixture(conn: &mut PgConnection, per_chunk: i32) -> Vec<(String, bool)> {
    sqlx::raw_sql(include_str!("../../tests/version_four.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../../tests/version_two.sql"))
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.execute(
        format!("CREATE TEMP TABLE numbered AS
         SELECT n, (substr(h,1,8)||'-'||substr(h,9,4)||'-7000-8000-'||lpad(to_hex(n+1),12,'0'))::uuid AS id
         FROM (SELECT n, lpad(to_hex(1800000000000::bigint+(n/{per_chunk})*864000000::bigint),12,'0') AS h
               FROM generate_series(0,{last}) n) q;
         INSERT INTO core.events (id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
         SELECT id, CASE WHEN n%3=0 THEN 'command.imported' ELSE 'journal.entry' END,
           'test', uuidv7(), uuidv7(), 0, 'unknown', n+1, '{{}}', 1, decode(repeat('01',32),'hex'), 'open'
         FROM numbered ORDER BY n;
         INSERT INTO core.search_text (event_id,text) SELECT id, 'fixture '||n FROM numbered WHERE n<>7;
         INSERT INTO core.search_text (event_id,text)
         SELECT (substr(id::text,1,24)||'ffffffffffff')::uuid, 'orphan' FROM numbered WHERE n={last};", last=6*per_chunk-1).as_str(),
    )
    .await
    .unwrap();
    crate::apply(conn).await.unwrap();
    let expected = sqlx::query_as(
        "SELECT s.event_id::text, coalesce(n.n%3=0,true) FROM core.search_text s
         LEFT JOIN numbered n ON n.id=s.event_id ORDER BY s.event_id",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    let chunks: i64 = sqlx::query_scalar("SELECT count(*) FROM show_chunks('core.events')")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(chunks >= 6, "the fixture must exercise chunk exclusion");
    expected
}

fn assert_selected_id_lookup(plan: &Value) {
    fn walk(node: &Value, excluding: bool, joined: bool, scans: &mut usize) {
        let kind = node["Node Type"].as_str().unwrap_or("");
        let independent = node["Parent Relationship"].as_str() == Some("InitPlan")
            || node["Subplan Name"].as_str() == Some("CTE selected_events");
        let excluding = (!independent && excluding)
            || node["Runtime Exclusion"].as_bool() == Some(true)
            || node["Startup Exclusion"].as_bool() == Some(true);
        let joined = (!independent && joined) || matches!(kind, "Hash Join" | "Merge Join");
        if node["Alias"]
            .as_str()
            .unwrap_or("")
            .starts_with("rewrite_event")
        {
            assert!(
                excluding && !joined,
                "event lookup lost chunk exclusion: {node}"
            );
            let append = matches!(kind, "Append" | "Merge Append")
                || node["Custom Plan Provider"].as_str() == Some("ChunkAppend");
            if !append {
                *scans += 1;
                let condition = ["Filter", "Index Cond", "Recheck Cond", "Vectorized Filter"]
                    .iter()
                    .map(|key| node[*key].to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                assert!(
                    condition.contains("ANY"),
                    "event lookup lost exact IDs: {node}"
                );
            }
        }
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                walk(child, excluding, joined, scans);
            }
        }
    }
    let mut scans = 0;
    walk(&plan[0]["Plan"], false, false, &mut scans);
    assert!(scans > 0, "the plan must fetch event identities: {plan}");
}

fn assert_compressed_work(plan: &Value, compressed: &[(String, String, String)]) {
    fn walk(
        node: &Value,
        compressed: &[(String, String, String)],
        batches: &mut f64,
        decompressed: &mut f64,
    ) {
        let loops = node["Actual Loops"].as_f64().unwrap_or(0.0);
        if compressed.iter().any(|(schema, table, _)| {
            node["Schema"].as_str() == Some(schema.as_str())
                && node["Relation Name"].as_str() == Some(table.as_str())
        }) {
            *batches += node["Actual Rows"].as_f64().unwrap_or(0.0) * loops;
        }
        if node["Custom Plan Provider"].as_str() == Some("ColumnarScan") {
            *decompressed += (node["Actual Rows"].as_f64().unwrap_or(0.0)
                + node["Rows Removed by Filter"].as_f64().unwrap_or(0.0))
                * loops;
        }
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                walk(child, compressed, batches, decompressed);
            }
        }
    }
    let (mut batches, mut decompressed) = (0.0, 0.0);
    walk(
        &plan[0]["Plan"],
        compressed,
        &mut batches,
        &mut decompressed,
    );
    eprintln!("compressed rewrite work: row_groups={batches}, decompressed_rows={decompressed}");
    assert!(
        batches > 0.0 && batches <= 4.0,
        "a four-ID page must fetch only its compressed row groups: {batches}, {plan}"
    );
    assert!(
        decompressed <= 4000.0,
        "the ID predicate must run after row-group pruning, not a whole chunk: {decompressed}, {plan}"
    );
}

#[devkit::test]
async fn search_rewrite_uses_bounded_event_lookups_in_custom_and_generic_plans(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let expected = fixture(&mut conn, 8192).await;
    // Every chunk must contain many compressed row groups, so chunk exclusion
    // alone cannot satisfy the physical-work assertion.
    // Compression relation names differ across template clones and fresh schemas.
    // Match the catalog's physical relation identity to verbose EXPLAIN nodes.
    let compressed: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text, format('%I.%I', n.nspname, c.relname)
         FROM _timescaledb_catalog.compression_settings s
         JOIN pg_class c ON c.oid=s.compress_relid
         JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE s.relid IN (SELECT show_chunks('core.events'))",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        compressed.len(),
        6,
        "every fixture chunk must be compressed"
    );
    for (_, _, table) in &compressed {
        let batches: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(batches >= 6, "fixture needs multiple compressed row groups");
    }
    // These session settings exercise both prepared-plan choices and make a
    // history-wide hash join attractive; production changes no planner setting.
    conn.execute("SET enable_nestloop=off").await.unwrap();
    for resumed in [false, true] {
        conn.execute(
            format!(
                "PREPARE rewrite(text,text,text[]) AS {}",
                rewrite_statement(resumed)
            )
            .as_str(),
        )
        .await
        .unwrap();
        for mode in ["force_custom_plan", "force_generic_plan"] {
            conn.execute(format!("SET plan_cache_mode={mode}").as_str())
                .await
                .unwrap();
            let mark = if resumed {
                format!("'{}'", expected[0].0)
            } else {
                "NULL".into()
            };
            let mut tx = conn.begin().await.unwrap();
            let plan: Value = sqlx::query_scalar(&format!(
                "EXPLAIN (ANALYZE, VERBOSE, FORMAT JSON) EXECUTE rewrite({mark},'{}',ARRAY['journal.entry'])",
                expected[3].0
            ))
            .fetch_one(&mut *tx)
            .await
            .unwrap();
            tx.rollback().await.unwrap();
            assert_selected_id_lookup(&plan);
            eprintln!("prepared rewrite plan: resumed={resumed}, mode={mode}");
            assert_compressed_work(&plan, &compressed);
        }
        conn.execute("DEALLOCATE rewrite").await.unwrap();
    }
}

/// law: online-work-resumes
#[devkit::test]
async fn search_rewrite_preserves_exact_ids_and_rolls_back_mark_with_flags(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let expected = fixture(&mut conn, 8).await;
    conn.execute(
        "CREATE FUNCTION pg_temp.fail_mark() RETURNS trigger LANGUAGE plpgsql AS
           $$ BEGIN RAISE EXCEPTION 'fixture mark failure'; END $$;
         CREATE TRIGGER fail_mark BEFORE UPDATE ON sinex_schema.search_trigram_rewrite
           FOR EACH ROW EXECUTE FUNCTION pg_temp.fail_mark()",
    )
    .await
    .unwrap();
    assert!(rewrite_batch(&mut conn, 3).await.is_err());
    // SQLx transaction Drop queues rollback; this read sends it before checking.
    let state: (Option<String>, i64) = sqlx::query_as(
        "SELECT rewritten_through::text, (SELECT count(*) FROM core.search_text WHERE NOT trigram_indexed)
         FROM sinex_schema.search_trigram_rewrite",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(state, (None, 0));
    conn.execute("DROP TRIGGER fail_mark ON sinex_schema.search_trigram_rewrite")
        .await
        .unwrap();
    assert_eq!(rewrite_batch(&mut conn, 3).await.unwrap(), Some(2));
    let changed: Vec<String> = sqlx::query_scalar(
        "SELECT event_id::text FROM core.search_text WHERE NOT trigram_indexed ORDER BY event_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(changed, vec![expected[1].0.clone(), expected[2].0.clone()]);
    drop(conn);
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mark: String = sqlx::query_scalar(
        "SELECT rewritten_through::text FROM sinex_schema.search_trigram_rewrite",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(mark, expected[2].0);
    while rewrite_batch(&mut conn, 3).await.unwrap().is_some() {}
    let actual: Vec<(String, bool)> = sqlx::query_as(
        "SELECT event_id::text, trigram_indexed FROM core.search_text ORDER BY event_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(actual, expected);
}

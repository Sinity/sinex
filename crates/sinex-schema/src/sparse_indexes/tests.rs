use super::*;
use sqlx::{Connection, Executor};

async fn settings(conn: &mut PgConnection) -> serde_json::Value {
    sqlx::query_scalar("SELECT jsonb_build_object('segmentby',segmentby,'orderby',orderby,'index',\"index\") FROM _timescaledb_catalog.compression_settings WHERE relid='core.events'::regclass")
        .fetch_one(conn).await.unwrap()
}

async fn rows(conn: &mut PgConnection) -> Vec<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(e) FROM core.events e ORDER BY e.id")
        .fetch_all(conn)
        .await
        .unwrap()
}

async fn missing(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar(&format!(
        "SELECT count(*) FROM ({}) pending",
        pending_query()
    ))
    .fetch_one(conn)
    .await
    .unwrap()
}

/// Count compressed batch tuples actually passed to decompression, rather
/// than the aggregate's one output row or a LIMIT's returned suffix.
async fn batch_work(conn: &mut PgConnection) -> (u64, serde_json::Value) {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT relname FROM pg_class WHERE oid IN (
          SELECT compress_relid FROM _timescaledb_catalog.compression_settings
          WHERE relid IN (SELECT show_chunks('core.events')) AND compress_relid IS NOT NULL)",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let plan:serde_json::Value=sqlx::query_scalar(
        "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT sum(e.commit_seq) FROM core.events e WHERE e.commit_seq>8190 AND e.commit_seq<=8192")
        .fetch_one(conn).await.unwrap();
    fn visited(value: &serde_json::Value, names: &[String]) -> u64 {
        match value {
            serde_json::Value::Object(object) => {
                let own = object
                    .get("Relation Name")
                    .and_then(|name| name.as_str())
                    .filter(|name| names.iter().any(|candidate| candidate.as_str() == *name))
                    .map(|_| {
                        object
                            .get("Actual Rows")
                            .and_then(|n| n.as_u64())
                            .unwrap_or(0)
                            * object
                                .get("Actual Loops")
                                .and_then(|n| n.as_u64())
                                .unwrap_or(0)
                    })
                    .unwrap_or(0);
                own + object
                    .values()
                    .map(|child| visited(child, names))
                    .sum::<u64>()
            }
            serde_json::Value::Array(values) => {
                values.iter().map(|child| visited(child, names)).sum()
            }
            _ => 0,
        }
    }
    (visited(&plan, &names), plan)
}

async fn compressed_predecessor(conn: &mut PgConnection) {
    // The old chunks retain an additional configured bloom that the current
    // hypertable setting alone no longer mentions.
    conn.execute("ALTER TABLE core.events SET (timescaledb.compress_index='bloom(material_id,anchor),bloom(material_id),bloom(anchor),bloom(run_id)');
        INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
        SELECT ((CASE WHEN n<=4096 THEN '01800000-0000-7000-8000-' ELSE '01810000-0000-7000-8000-' END)||lpad(to_hex(n),12,'0'))::uuid,
          'command.imported','synthetic-sparse',uuidv7(),uuidv7(),n,'unknown',n,jsonb_build_object('n',n),1,decode(repeat('01',32),'hex'),'open'
        FROM generate_series(1,8192)n").await.unwrap();
    sqlx::query("SELECT compress_chunk(chunk) FROM show_chunks('core.events') chunks(chunk)")
        .execute(&mut *conn)
        .await
        .unwrap();
    conn.execute("ALTER TABLE core.events SET (timescaledb.compress_index='bloom(material_id,anchor),bloom(material_id),bloom(anchor)')")
        .await.unwrap();
}

#[devkit::test]
async fn native_sparse_rebuild_preserves_objects_values_and_resumes_per_chunk(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    compressed_predecessor(&mut conn).await;
    let before = rows(&mut conn).await;
    assert_eq!(before.len(), 8192);
    let layout = settings(&mut conn).await;
    conn.execute(configuration().as_str()).await.unwrap();
    let configured = settings(&mut conn).await;
    assert_eq!(configured["segmentby"], layout["segmentby"]);
    assert_eq!(configured["orderby"], layout["orderby"]);
    let objects = configured["index"].as_array().unwrap();
    for (kind, column) in [
        ("bloom", serde_json::json!(["material_id", "anchor"])),
        ("bloom", serde_json::json!("material_id")),
        ("bloom", serde_json::json!("anchor")),
        ("bloom", serde_json::json!("run_id")),
        ("minmax", serde_json::json!("commit_seq")),
    ] {
        assert!(
            objects
                .iter()
                .any(|object| object["type"] == kind && object["column"] == column),
            "{configured}"
        );
    }
    let (unpruned, old_plan) = batch_work(&mut conn).await;
    assert!(
        unpruned >= 4,
        "unpruned compressed-batch positive control: {old_plan}"
    );
    assert_eq!(missing(&mut conn).await, 2);
    assert_eq!(
        crate::online::remaining(&mut conn).await.unwrap(),
        [crate::online::Task::FeedIndex]
    );
    let chunk: String = sqlx::query_scalar(&format!(
        "SELECT chunk FROM ({}) pending ORDER BY relid LIMIT 1",
        pending_query()
    ))
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let mut failed = conn.begin().await.unwrap();
    sqlx::query("SELECT _timescaledb_functions.rebuild_sparse_index($1::regclass)")
        .bind(&chunk)
        .execute(&mut *failed)
        .await
        .unwrap();
    assert_eq!(missing(&mut failed).await, 1);
    failed.rollback().await.unwrap();
    assert_eq!(
        missing(&mut conn).await,
        2,
        "native metadata and population roll back together"
    );
    assert_eq!(rows(&mut conn).await, before);
    assert_eq!(
        crate::online::step(&mut conn).await.unwrap(),
        crate::online::Progress::Advanced {
            task: crate::online::Task::FeedIndex
        }
    );
    assert_eq!(missing(&mut conn).await, 1, "one native chunk per step");
    conn.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        crate::online::step(&mut conn).await.unwrap(),
        crate::online::Progress::Finished {
            task: crate::online::Task::FeedIndex
        }
    );
    assert_eq!(missing(&mut conn).await, 0);
    let (pruned, new_plan) = batch_work(&mut conn).await;
    assert!(
        pruned > 0 && pruned <= 2 && pruned < unpruned,
        "compressed batches {unpruned} -> {pruned}; old: {old_plan}; new: {new_plan}"
    );
    assert_eq!(sqlx::query_scalar::<_,Option<i64>>("SELECT sum(commit_seq)::bigint FROM core.events WHERE commit_seq>8190 AND commit_seq<=8192")
        .fetch_one(&mut conn).await.unwrap(),Some(16383));
    assert_eq!(rows(&mut conn).await, before);
    assert!(!step(&mut conn).await.unwrap());
    assert!(
        crate::online::remaining(&mut conn)
            .await
            .unwrap()
            .is_empty()
    );
    // Configuration order is not a reason to rewrite already populated
    // metadata. Native settings are compared by required objects.
    conn.execute("ALTER TABLE core.events SET (timescaledb.compress_index='minmax(commit_seq),bloom(run_id),bloom(anchor),bloom(material_id),bloom(material_id,anchor)')")
        .await.unwrap();
    assert!(!pending(&mut conn).await.unwrap());
}

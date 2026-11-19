use super::*;
use sqlx::{Connection, Executor};

#[devkit::test]
async fn copy_fields_bound_compressed_lookup_to_matching_candidates(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let kind = sinex_schema::COPY_FIELDS_EVENT_TYPE;
    sqlx::query(
        "WITH readings AS (
           INSERT INTO core.events (id,event_type,source,run_id,material_id,record_offset,ts_quality,commit_seq,payload,reading_order,record_hash)
           SELECT uuidv7(),$1,'fixture.copy',uuidv7(),uuidv7(),0,'unknown',n,
             jsonb_build_object('content','bytes-'||n,'path','/fixture/'||n),1,decode(repeat('01',32),'hex')
           FROM generate_series(1,4097)n RETURNING id,payload,commit_seq
         ), lookup AS (
           INSERT INTO core.library_copy_fields (event_id,content_hash,path_hash)
           SELECT id,sha256(convert_to(CASE WHEN commit_seq=2 THEN 'bytes-1' ELSE payload->>'content' END,'UTF8')),
                     sha256(convert_to(payload->>'path','UTF8')) FROM readings
         ) INSERT INTO core.live_equivalence (source,event_type,equivalence_key,event_id,reading_order,record_hash)
         SELECT 'fixture.copy',$1,core.encode_equivalence_key(id::text),id,1,decode(repeat('01',32),'hex') FROM readings",
    )
    .bind(kind)
    .execute(&mut conn)
    .await
    .unwrap();
    conn.execute("SELECT compress_chunk(c) FROM show_chunks('core.events') c; ANALYZE core.library_copy_fields")
        .await.unwrap();

    for (field, selector, input, index, candidates) in [
        (
            CopyField::Content,
            "content",
            "bytes-1",
            "library_copy_fields_content",
            2.0,
        ),
        (
            CopyField::Path,
            "path",
            "/fixture/1",
            "library_copy_fields_path",
            1.0,
        ),
    ] {
        let values = [input.to_owned()];
        let old = live_where(&mut conn, kind, &[selector], &values, RecordStates::Any)
            .await
            .unwrap();
        assert_eq!(
            old,
            [serde_json::json!({"content":"bytes-1","path":"/fixture/1"})]
        );
        assert_eq!(live_copies(&mut conn, field, &values).await.unwrap(), old);

        let sql = include_str!("live_copies.sql");
        let plan: Value =
            sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {sql}"))
                .bind(selector)
                .bind(&values[..])
                .bind(kind)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(plan[0]["Plan"]["Actual Rows"].as_f64(), Some(1.0));
        assert_eq!(index_visits(&plan[0]["Plan"], index), candidates);
    }
}

fn index_visits(node: &Value, index: &str) -> f64 {
    let own = if node["Index Name"] == index {
        node["Actual Rows"].as_f64().unwrap() * node["Actual Loops"].as_f64().unwrap()
    } else {
        0.0
    };
    own + node["Plans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|child| index_visits(child, index))
        .sum::<f64>()
}

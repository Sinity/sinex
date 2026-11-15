use super::*;
use sqlx::{Connection, Executor, Row};

#[devkit::test]
async fn header_batches_preserve_order_duplicates_and_classes_on_compressed_history(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01800000-0000-7000-8000-'||lpad(to_hex(n),12,'0'))::uuid,
        'command.imported','synthetic-header',uuidv7(),uuidv7(),n,'unknown',1,'{}',1,decode(repeat('01',32),'hex'),
        CASE n%3 WHEN 0 THEN 'guarded' WHEN 1 THEN 'sealed' ELSE 'open' END
      FROM generate_series(1,8448)n").await.unwrap();
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut conn)
        .await
        .unwrap();
    let logical_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(logical_rows, 8448);
    let mut ids: Vec<Uuid> = (8193..=8448)
        .rev()
        .map(|n| Uuid::parse_str(&format!("01800000-0000-7000-8000-{n:012x}")).unwrap())
        .collect();
    ids.push(ids[0]);
    let missing = Uuid::parse_str("01800000-0000-7000-8000-00000000dead").unwrap();
    ids.push(missing);
    let mut tx = conn.begin().await.unwrap();
    {
        let headers = <EventReference as EventProjection>::load(&mut tx, &ids)
            .await
            .unwrap();
        assert_eq!(
            headers.iter().map(|row| row.id).collect::<Vec<_>>(),
            ids[..ids.len() - 1]
        );
        for row in headers {
            let n = row.id.as_u128() & 0xffff;
            assert_eq!(row.event_type, "command.imported");
            assert_eq!(row.commit_seq, 1);
            assert_eq!(
                row.class,
                match n % 3 {
                    0 => "guarded",
                    1 => "sealed",
                    _ => "open",
                }
            );
        }
    }

    let unique = &ids[..256];
    sqlx::query("INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason) SELECT n,id,2,true,'archive' FROM unnest($1::uuid[]) WITH ORDINALITY selected(id,n)")
        .bind(unique).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason) VALUES(257,$1,3,false,'restore'),(258,$2,2,true,'archive'),(259,$3,2,true,'archive')")
        .bind(ids[0]).bind(missing).bind(Uuid::parse_str("01800000-0000-7000-8000-00000000beef").unwrap()).execute(&mut *tx).await.unwrap();
    sqlx::query("INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id,commit_seq,original_commit_seq,class) VALUES($1,'synthetic-header','command.imported',$2,4,1,'guarded')")
        .bind(missing).bind(Uuid::now_v7()).execute(&mut *tx).await.unwrap();
    let archive_ids: Vec<i64> = (1..=259).collect();
    let mut events = ids.clone();
    events.push(Uuid::parse_str("01800000-0000-7000-8000-00000000beef").unwrap());
    {
        let rows = sqlx::query(include_str!("../archive_headers.sql"))
            .bind(&archive_ids)
            .bind(&events)
            .fetch_all(&mut *tx)
            .await
            .unwrap();
        assert_eq!(rows.len(), 259);
        let rows: BTreeMap<i64, _> = rows
            .into_iter()
            .map(|row| (row.get("change_seq"), row))
            .collect();
        assert_eq!(rows[&257].get::<Uuid, _>("event_id"), ids[0]);
        assert!(!rows[&257].get::<bool, _>("archived"));
        assert_eq!(
            rows[&258].get::<Option<String>, _>("class").as_deref(),
            Some("guarded")
        );
        assert_eq!(
            rows[&258].get::<Option<String>, _>("event_type").as_deref(),
            Some("command.imported")
        );
        assert!(rows[&259].get::<Option<String>, _>("class").is_none());
        assert!(rows[&259].get::<Option<String>, _>("event_type").is_none());
    }
    tx.rollback().await.unwrap();
}

use super::*;
use serde_json::{Value, json};
use sqlx::types::Uuid;

pub(super) async fn search_before_pieces(conn: &mut PgConnection) {
    conn.execute(include_str!(
        "../../tests/fixtures/search-before-pieces.sql"
    ))
    .await
    .unwrap();
}

async fn store(
    conn: &mut PgConnection,
    event_type: &str,
    class: &str,
    position: i64,
    payload: Value,
) -> Uuid {
    let id = sinex_model::EventId::new().0;
    sqlx::query(
        "INSERT INTO core.events(id,event_type,source,run_id,material_id,record_offset,class,ts_quality,commit_seq,payload,reading_order,record_hash)
         VALUES ($1,$2,'fixture',$1,$1,0,$3,'unknown',$4,$5,0,decode(repeat('00',32),'hex'))",
    )
    .bind(id)
    .bind(event_type)
    .bind(class)
    .bind(position)
    .bind(payload)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query("INSERT INTO core.search_text(event_id,text) VALUES ($1,'harbor lights beacon')")
        .bind(id)
        .execute(conn)
        .await
        .unwrap();
    id
}

#[devkit::test]
async fn search_upgrade_keeps_indexed_text_and_resumes_from_its_saved_position(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let fresh = catalog(&mut conn).await.unwrap();
    search_before_pieces(&mut conn).await;
    conn.execute("UPDATE sinex_schema.applied SET version = 54")
        .await
        .unwrap();
    let journal = store(
        &mut conn,
        "journal.entry",
        "open",
        1,
        json!({"message": "Harbor lights", "unit": "beacon", "hostname": "unindexed"}),
    )
    .await;
    let command = store(
        &mut conn,
        "command.imported",
        "guarded",
        2,
        json!({"command": "Harbor lights beacon"}),
    )
    .await;
    for (kind, class, position) in [
        ("terminal.output", "open", 3),
        ("journal.entry", "sealed", 4),
        ("journal.entry", "open", 6),
    ] {
        store(
            &mut conn,
            kind,
            class,
            position,
            json!({"message": "harbor lights beacon"}),
        )
        .await;
    }
    conn.execute("INSERT INTO core.search_position(commit_seq) VALUES (5)")
        .await
        .unwrap();
    let previous: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.search_text WHERE tsv @@ phraseto_tsquery('simple','harbor lights') AND event_id = ANY($1)",
    )
    .bind(&[journal, command][..])
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(previous, 2);

    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: 54,
            to: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    let mut indexed: Vec<(Uuid, String, bool)> = sqlx::query_as(
        "SELECT event_id,text,trigram FROM core.search_text
         WHERE tsv @@ phraseto_tsquery('simple','harbor lights')
           AND tsv @@ plainto_tsquery('simple','beacon')",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    indexed.sort_unstable_by_key(|row| row.0);
    let mut expected = vec![
        (journal, "harbor lights beacon".into(), false),
        (command, "harbor lights beacon".into(), true),
    ];
    expected.sort_unstable_by_key(|row| row.0);
    assert_eq!(indexed, expected);
    let (position, pieces, events): (i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT commit_seq FROM core.search_position),
                (SELECT count(*) FROM core.search_text),
                (SELECT count(*) FROM core.events)",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!((position, pieces, events), (5, 2, 5));
}

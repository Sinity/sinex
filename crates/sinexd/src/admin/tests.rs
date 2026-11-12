use serde_json::{Value, json};
use sinex_source_core::drivers::append_file::{AppendFile, AppendState, lines};
use sinex_source_core::drivers::live_stream::LiveStream;
use sinex_source_core::{Framer, Reader};
use sqlx::Connection;
use std::io::Write;

#[devkit::test]
async fn source_cursor_upgrade_resumes_at_eof_and_reads_only_appended_bytes(
    db: devkit::pg::TestDatabase,
) {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("000000000001.ndjson");
    std::fs::write(&file, b"a\nb\n").unwrap();
    let driver = AppendFile {
        path: file.clone(),
        host: "fixture".into(),
        framer: Framer::incremental(lines),
    };
    let first = driver
        .poll(&Value::Null, &mut AppendState::default())
        .unwrap();
    let material = first.material.unwrap();
    let mut old = (material.cursor_at)(4).unwrap();
    old.as_object_mut().unwrap().remove("ctime_ns");
    old.as_object_mut().unwrap().remove("looked_ns");
    assert!(driver.poll(&old, &mut AppendState::default()).is_err());
    let wrapped = json!({"segment":1,"inner":old,"part_base":0,"last_material":material.id});
    let carry = json!({"pending":"synthetic retained parser state"});
    let mut existing = old.clone();
    existing["ctime_ns"] = json!(123);
    let unrelated = json!({"offset":7,"page":"synthetic"});
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    for (binding, cursor) in [
        ("file", &old),
        ("stream", &wrapped),
        ("existing", &existing),
        ("unrelated", &unrelated),
    ] {
        sqlx::query(
            "INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES($1,$2,$3,17)",
        )
        .bind(binding)
        .bind(cursor)
        .bind(&carry)
        .execute(&mut conn)
        .await
        .unwrap();
    }
    // Reconstruct the search layout present before the cursor step; later
    // upgrades must also run from their actual preceding stored format.
    sqlx::raw_sql(
        "DROP VIEW core.visible_search_trigram;
         DROP TABLE core.search_trigram;
         DROP VIEW core.visible_search_text;
         ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true;
         CREATE INDEX search_text_trigram ON core.search_text USING gin(text gin_trgm_ops);
         CREATE VIEW core.visible_search_text AS SELECT s.event_id,s.text,s.tsv,s.trigram_indexed
           FROM core.search_text s JOIN core.events e ON e.id=s.event_id
           WHERE core.class_visible(e.class);",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    sqlx::query("UPDATE sinex_schema.applied SET version=$1")
        .bind(sinex_schema::version_before("append_file_stat_metadata").unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    sinex_schema::apply(&mut conn).await.unwrap();
    let mut upgraded = Vec::new();
    for (binding, original) in [("file", &old), ("stream", &wrapped)] {
        let (cursor, saved_carry, sequence): (Value, Value, i64) =
            sqlx::query_as("SELECT cursor,carry,committed_seq FROM raw.positions WHERE binding=$1")
                .bind(binding)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        let mut retained = cursor.clone();
        let inner = if binding == "stream" {
            &mut retained["inner"]
        } else {
            &mut retained
        };
        assert_eq!(inner["ctime_ns"], json!(0));
        assert_eq!(inner["looked_ns"], json!(0));
        inner.as_object_mut().unwrap().remove("ctime_ns");
        inner.as_object_mut().unwrap().remove("looked_ns");
        assert_eq!(&retained, original);
        assert_eq!(saved_carry, carry);
        assert_eq!(sequence, 17);
        upgraded.push(cursor);
    }
    let preserved: Value =
        sqlx::query_scalar("SELECT cursor FROM raw.positions WHERE binding='existing'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(preserved["ctime_ns"], json!(123));
    assert_eq!(preserved["looked_ns"], json!(0));
    let untouched: Value =
        sqlx::query_scalar("SELECT cursor FROM raw.positions WHERE binding='unrelated'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(untouched, unrelated);
    let mut stream = LiveStream {
        spool: root.path().to_owned(),
        input: sinex_model::InputRef {
            kind: "socket".into(),
            locator: "synthetic".into(),
            host: "fixture".into(),
        },
    };
    assert!(
        driver
            .poll(&upgraded[0], &mut AppendState::default())
            .unwrap()
            .material
            .is_none()
    );
    assert!(stream.poll(&upgraded[1]).unwrap().material.is_none());
    std::fs::OpenOptions::new()
        .append(true)
        .open(&file)
        .unwrap()
        .write_all(b"c\n")
        .unwrap();
    let appended = driver
        .poll(&upgraded[0], &mut AppendState::default())
        .unwrap();
    let streamed = stream.poll(&upgraded[1]).unwrap();
    for (acquired, expected_order) in [(appended, 4), (streamed, (1_i64 << 40) | 4)] {
        assert_eq!(acquired.revision, sinex_model::RevisionChange::Appended);
        let next = acquired.material.unwrap();
        assert_eq!(next.records, vec![(0, 2)]);
        assert_eq!(next.part_index, 1);
        assert_eq!(next.previous_part, Some(material.id));
        assert_eq!(
            (next.reading_order)(0),
            sinex_model::ReadingOrder(expected_order)
        );
        let bytes = match next.stored.unwrap() {
            sinex_source_core::drivers::StoredBytes::FramedFile(file) => {
                let mut bytes = [0; 2];
                std::os::unix::fs::FileExt::read_exact_at(file.as_ref(), &mut bytes, 0).unwrap();
                bytes.to_vec()
            }
            sinex_source_core::drivers::StoredBytes::FramedMemory(bytes) => bytes.as_ref().clone(),
            other => panic!("append fixture did not retain its framed bytes: {other:?}"),
        };
        assert_eq!(bytes, b"c\n");
    }
}

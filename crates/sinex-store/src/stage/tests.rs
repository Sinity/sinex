//! Rows streamed through PostgreSQL COPY read back field for field.
use super::*;
use sinex_model::{
    Class, EventId, MaterialId, ReadingOrder, RecordLocation, Timestamp,
    event_types::CommandImported,
};
use sqlx::{Connection, Row};

fn event(n: i64, payload: serde_json::Value) -> Event {
    let mut e = Event::build(
        &CommandImported {
            command: "synthetic copy".into(),
            hostname: "synthetic".into(),
            started_at: Some(Timestamp::from_unix_secs(1_700_000_000 + n).unwrap()),
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "synthetic",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: n as u64,
            },
            output_index: 3,
        },
        ReadingOrder(n),
        [n as u8; 32],
        Class::Guarded,
    )
    .unwrap();
    e.payload = payload;
    e
}

#[devkit::test]
async fn streaming_copy_round_trips_json_and_every_escaped_field(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut deep = serde_json::json!({"empty_array":[],"empty_object":{}});
    for _ in 0..64 {
        deep = serde_json::json!([deep]);
    }
    let edge = format!(
        "{}ą🦀{}",
        "x".repeat(JSON_STRING_BYTES - 1),
        ['"', '\\', '\t', '\n', '\r', '\0', '\u{0008}', '\u{001f}']
            .into_iter()
            .collect::<String>()
    );
    let mut map = serde_json::Map::new();
    map.insert(
        edge.repeat(3),
        serde_json::json!([
            edge.repeat(5),
            null,
            true,
            false,
            -0.0,
            i64::MIN,
            u64::MAX,
            1e-300,
            1e300,
            deep
        ]),
    );
    let mut first = event(1, serde_json::Value::Object(map));
    first.source = format!(
        "{}N{}{}{}{}🦀",
        char::from(92),
        char::from(9),
        char::from(10),
        char::from(13),
        char::from(92)
    )
    .repeat(20000);
    first.event_type = format!("synthetic.{}", edge.repeat(4).replace('\0', ""));
    first.equivalence_key = Some(sinex_model::EquivalenceKey(
        format!("{}N{}🦀", char::from(92), char::from(10)).repeat(20000),
    ));
    first.origin = Origin::Derived {
        parents: (0..4097).map(|_| EventId(uuid::Uuid::now_v7())).collect(),
    };
    let mut second = event(
        2,
        serde_json::json!({"literal":format!("{}N",char::from(92)),"empty":"","null":null}),
    );
    second.equivalence_key = None;
    let events = [first, second];
    let mut tx = conn.begin().await.unwrap();
    sqlx::raw_sql("CREATE TEMP TABLE copy_wire (LIKE core.admission_stage INCLUDING ALL); ALTER TABLE copy_wire ALTER COLUMN payload TYPE text USING payload::text").execute(&mut *tx).await.unwrap();
    {
        let copy = tx
            .copy_in_raw(&format!("COPY copy_wire ({COLUMNS}) FROM STDIN"))
            .await
            .unwrap();
        let mut encoder = CopyText::new(copy);
        for (i, e) in events.iter().enumerate() {
            encoder.row(i as i64, e).await.unwrap();
        }
        encoder.finish().await.unwrap();
    }
    let rows=sqlx::query("SELECT ord,id,event_type,source,material_id,anchor,output_index,payload,equivalence_key,parents,reading_order,record_hash,class FROM copy_wire ORDER BY ord")
        .fetch_all(&mut *tx).await.unwrap();
    for (i, (row, e)) in rows.iter().zip(&events).enumerate() {
        assert_eq!(row.get::<i64, _>("ord"), i as i64);
        assert_eq!(row.get::<uuid::Uuid, _>("id"), e.id.0);
        assert_eq!(row.get::<String, _>("event_type"), e.event_type);
        assert_eq!(row.get::<String, _>("source"), e.source);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&row.get::<String, _>("payload")).unwrap(),
            e.payload
        );
        assert_eq!(
            row.get::<Option<String>, _>("equivalence_key"),
            e.equivalence_key.as_ref().map(|k| k.0.clone())
        );
        assert_eq!(row.get::<Vec<u8>, _>("record_hash"), e.record_hash);
        assert_eq!(row.get::<i64, _>("reading_order"), e.reading_order.0);
        assert_eq!(row.get::<String, _>("class"), e.class.as_db());
        match &e.origin {
            Origin::Derived { parents } => {
                assert_eq!(
                    row.get::<Vec<uuid::Uuid>, _>("parents"),
                    parents.iter().map(|p| p.0).collect::<Vec<_>>()
                );
                assert_eq!(row.get::<i64, _>("output_index"), 0);
                assert!(row.get::<Option<uuid::Uuid>, _>("material_id").is_none());
            }
            Origin::Record {
                location,
                output_index,
            } => {
                assert!(row.get::<Option<Vec<uuid::Uuid>>, _>("parents").is_none());
                assert_eq!(
                    row.get::<Option<uuid::Uuid>, _>("material_id"),
                    Some(location.material.0)
                );
                assert_eq!(
                    row.get::<Option<i64>, _>("anchor"),
                    Some(location.anchor as i64)
                );
                assert_eq!(row.get::<i64, _>("output_index"), *output_index as i64);
            }
        }
    }
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn streaming_copy_admits_one_huge_field_and_many_rows(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut events = vec![event(
        1,
        serde_json::json!({"huge":"\"\\\n🦀".repeat(1<<20)}),
    )];
    events
        .extend((2..258).map(|n| event(n, serde_json::json!({"text":"synthetic ".repeat(2048)}))));
    let mut tx = conn.begin().await.unwrap();
    {
        let copy = tx
            .copy_in_raw(&format!("COPY core.admission_stage ({COLUMNS}) FROM STDIN"))
            .await
            .unwrap();
        let mut encoder = CopyText::new(copy);
        for (i, e) in events.iter().enumerate() {
            encoder.row(i as i64, e).await.unwrap();
        }
        encoder.finish().await.unwrap();
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.admission_stage")
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
        events.len() as i64
    );
    let payload = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT payload FROM core.admission_stage WHERE ord=0",
    )
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    assert_eq!(payload, events[0].payload);
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn streaming_copy_encoding_and_database_errors_rollback_and_retry(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let good = event(1, serde_json::json!({"text":"valid".repeat(COPY_BYTES)}));
    for invalid in 0..3 {
        let mut bad = event(2, serde_json::json!({"text":"unfinished"}));
        if invalid == 0 {
            bad.replace = None;
        } else if invalid == 1 {
            bad.source = format!("invalid{}text", char::from(0));
        } else {
            bad.payload = serde_json::json!({"nul":char::from(0).to_string()});
        }
        let mut tx = conn.begin().await.unwrap();
        let error = copy(&mut tx, &[(0, &good), (1, &bad)]).await.unwrap_err();
        if invalid == 0 {
            assert!(error.to_string().contains("no replacement rule"), "{error}");
        } else {
            assert!(matches!(error, Error::Database(_)), "{error}");
        }
        tx.rollback().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.admission_stage")
                .fetch_one(&mut conn)
                .await
                .unwrap(),
            0
        );
        let mut tx = conn.begin().await.unwrap();
        copy(&mut tx, &[(0, &good)]).await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, uuid::Uuid>("SELECT id FROM core.admission_stage")
                .fetch_one(&mut *tx)
                .await
                .unwrap(),
            good.id.0
        );
        tx.rollback().await.unwrap();
    }
}

#[devkit::test]
async fn canceled_streaming_copy_releases_transaction_and_writer_for_exact_retry(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut observer = PgConnection::connect(&url).await.unwrap();
    sqlx::raw_sql("CREATE TABLE public.copy_mutation(ord bigint); CREATE FUNCTION public.hold_copy_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.copy_mutation VALUES(NEW.ord); PERFORM pg_advisory_xact_lock(927163); RETURN NEW; END $$; CREATE TRIGGER hold_copy_row AFTER INSERT ON core.admission_stage FOR EACH ROW WHEN (NEW.ord=0) EXECUTE FUNCTION public.hold_copy_row()")
        .execute(&mut observer).await.unwrap();
    let mut blocker = PgConnection::connect(&url).await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(927163)")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut writer = crate::AdmissionContext::connect(&url).await.unwrap();
    let pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .unwrap();
    let first = event(1, serde_json::json!({"text":"first"}));
    let retry = first.clone();
    let copying = tokio::spawn(async move {
        let large = event(2, serde_json::json!({"large":"\"\\\n🦀".repeat(2<<20)}));
        let mut tx = writer.begin().await.unwrap();
        copy(tx.connection(), &[(0, &first), (1, &large)])
            .await
            .unwrap();
        tx.commit().await.unwrap();
    });
    tokio::time::timeout(std::time::Duration::from_secs(5),async { loop {
        let held=sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')").bind(pid).fetch_one(&mut observer).await.unwrap();
        if held {break;} tokio::task::yield_now().await;
    }}).await.expect("real COPY blocked after transaction mutation");
    assert!(!copying.is_finished());
    copying.abort();
    assert!(copying.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let exists = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1)",
            )
            .bind(pid)
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if !exists {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned COPY backend released on disconnection");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM public.copy_mutation")
            .fetch_one(&mut observer)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.admission_stage")
            .fetch_one(&mut observer)
            .await
            .unwrap(),
        0
    );
    sqlx::query("SELECT pg_advisory_unlock(927163)")
        .execute(&mut blocker)
        .await
        .unwrap();
    let mut fresh = crate::AdmissionContext::connect(&url).await.unwrap();
    let mut tx = fresh.begin().await.unwrap();
    copy(tx.connection(), &[(0, &retry)]).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, uuid::Uuid>("SELECT id FROM core.admission_stage")
            .fetch_one(&mut *fresh)
            .await
            .unwrap(),
        retry.id.0
    );
    fresh.close().await.unwrap();
}

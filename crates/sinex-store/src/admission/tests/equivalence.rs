use super::*;

fn keys() -> Vec<String> {
    let digest = "a7".repeat(32);
    vec![
        digest.clone(),
        format!("hash:{digest}"),
        format!("sealed:{digest}"),
        digest.to_uppercase(),
        format!("{digest}\n"),
        format!("hash:{digest}\r\n"),
        format!("hash:{}", digest.to_uppercase()),
        String::new(),
        "zażółć 🦀".into(),
        "03a7".into(),
    ]
}

#[devkit::test]
async fn every_key_form_keeps_its_identity_and_public_spelling(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let keys = keys();
    let events: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let mut event = keyed("s", i as u8, 1, 1);
            event.equivalence_key = Some(sinex_model::EquivalenceKey(key.clone()));
            event
        })
        .collect();
    assert_eq!(
        admit_one(&mut conn, &batch(events.clone())).await.admitted,
        keys.len()
    );
    let encoded: Vec<(String,Vec<u8>)> = sqlx::query_as("SELECT core.decode_equivalence_key(equivalence_key),equivalence_key FROM core.live_equivalence ORDER BY equivalence_key")
        .fetch_all(&mut conn).await.unwrap();
    assert_eq!(encoded.len(), keys.len());
    for (key, bytes) in &encoded {
        assert!(keys.contains(key));
        if bytes[0] < 3 {
            assert_eq!(bytes.len(), 33);
        }
    }
    for event in &events {
        let reference = sinex_model::query::Reference::Key {
            source: "s".into(),
            event_type: event.event_type.into(),
            key: event.equivalence_key.as_ref().unwrap().0.clone(),
        };
        // Resolve through the public query path as well as classification.
        let shown = crate::query::show(&mut conn, &reference).await.unwrap();
        assert!(shown.to_string().contains(&event.id.to_string()));
    }
    assert_eq!(
        admit_one(&mut conn, &batch(events)).await.repeats,
        keys.len()
    );
}

pub(super) async fn version_two_keys(conn: &mut PgConnection) {
    sqlx::query("ALTER TABLE core.live_equivalence ALTER COLUMN equivalence_key TYPE text USING core.decode_equivalence_key(equivalence_key)")
        .execute(&mut *conn).await.unwrap();
    sqlx::query(
        "DROP FUNCTION core.encode_equivalence_key(text),core.decode_equivalence_key(bytea)",
    )
    .execute(&mut *conn)
    .await
    .unwrap();
}

async fn rows(conn: &mut PgConnection) -> Vec<Value> {
    let tables: Vec<(String,String)> = sqlx::query_as("SELECT schemaname,tablename FROM pg_tables WHERE schemaname=ANY($1) AND schemaname<>'sinex_schema' AND tablename<>'admission_stage' ORDER BY 1,2")
        .bind(sinex_schema::schemas()).fetch_all(&mut *conn).await.unwrap();
    let mut rows = Vec::new();
    for (schema, table) in tables {
        let name = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            table.replace('"', "\"\"")
        );
        let query = if table == "live_equivalence" {
            format!(
                "SELECT to_jsonb(t)||jsonb_build_object('equivalence_key',core.decode_equivalence_key(t.equivalence_key)) FROM {name} t ORDER BY 1"
            )
        } else {
            format!("SELECT to_jsonb(t) FROM {name} t ORDER BY 1")
        };
        let data: Vec<Value> = sqlx::query_scalar(&query)
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        rows.push(json!({"table": name,"rows": data}));
    }
    rows
}

#[devkit::test]
async fn populated_key_upgrade_preserves_all_rows_and_matches_fresh_schema(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let events: Vec<_> = keys()
        .iter()
        .enumerate()
        .map(|(i, key)| {
            let mut event = keyed("s", i as u8, 1, 1);
            event.equivalence_key = Some(sinex_model::EquivalenceKey(key.clone()));
            event
        })
        .collect();
    admit_one(&mut conn, &batch(events.clone())).await;
    let mut newer = events[0].clone();
    newer.id = EventId::new();
    newer.reading_order = ReadingOrder(2);
    newer.record_hash = [5; 32];
    admit_one(&mut conn, &batch(vec![newer])).await;
    let mut hide = batch(vec![]);
    hide.archive.push(ArchiveRequest::Hide {
        target: events[1].id,
        operation: OperationId::new(),
    });
    admit_one(&mut conn, &hide).await;
    let before = rows(&mut conn).await;
    let catalog = sinex_schema::catalog(&mut conn).await.unwrap();
    version_two_keys(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut conn).await.unwrap(),
        sinex_schema::Applied::Upgraded { from: 2, to: 3 }
    );
    assert_eq!(rows(&mut conn).await, before);
    assert_eq!(sinex_schema::catalog(&mut conn).await.unwrap(), catalog);
}

#[devkit::test]
async fn interrupted_key_upgrade_rolls_back_functions_and_rows(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut conn = PgConnection::connect(&url).await.unwrap();
    sqlx::query("INSERT INTO core.live_equivalence VALUES ('s','command.executed',core.encode_equivalence_key($1),$2,7,$3)")
        .bind("hash:".to_owned()+&"a7".repeat(32)).bind(Uuid::now_v7()).bind(vec![5u8;32])
        .execute(&mut conn).await.unwrap();
    version_two_keys(&mut conn).await;
    sqlx::query("UPDATE sinex_schema.applied SET version=2")
        .execute(&mut conn)
        .await
        .unwrap();
    let before: Vec<Value> = sqlx::query_scalar("SELECT to_jsonb(t) FROM core.live_equivalence t")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let mut block = conn.begin().await.unwrap();
    sqlx::query("LOCK TABLE core.live_equivalence IN ACCESS SHARE MODE")
        .execute(&mut *block)
        .await
        .unwrap();
    let mut upgrading = PgConnection::connect(&url).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut upgrading)
        .await
        .unwrap();
    let task = tokio::spawn(async move { sinex_schema::apply(&mut upgrading).await });
    let mut observer = PgConnection::connect(&url).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)",
            )
            .bind(pid)
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(pid)
        .execute(&mut observer)
        .await
        .unwrap();
    assert!(task.await.unwrap().is_err());
    block.rollback().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT max(version) FROM sinex_schema.applied")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        2
    );
    assert!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT to_regprocedure('core.encode_equivalence_key(text)')::text"
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
        .is_none()
    );
    assert_eq!(
        sqlx::query_scalar::<_, Value>("SELECT to_jsonb(t) FROM core.live_equivalence t")
            .fetch_all(&mut conn)
            .await
            .unwrap(),
        before
    );
}

#[devkit::test]
async fn keyed_and_location_readings_are_disjoint_even_when_the_text_matches(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut located = keyed("s", 1, 1, 1);
    located.equivalence_key = None;
    let Origin::Record {
        location,
        output_index,
    } = located.origin
    else {
        unreachable!()
    };
    let mut keyed = keyed("s", 2, 2, 2);
    keyed.equivalence_key = Some(sinex_model::EquivalenceKey(format!(
        "{}:{}:{}",
        location.material.0, location.anchor, output_index
    )));
    let b = batch(vec![located.clone(), keyed.clone()]);
    let outcome = admit_one(&mut conn, &b).await;
    assert_eq!((outcome.admitted, outcome.superseded), (2, 0));
    let before = crate::query::current_seq(&mut conn).await.unwrap();
    let outcome = admit_one(&mut conn, &b).await;
    assert_eq!(
        (outcome.admitted, outcome.repeats, outcome.commit_seq),
        (0, 2, None)
    );
    assert_eq!(crate::query::current_seq(&mut conn).await.unwrap(), before);
    let stage: i64 = sqlx::query_scalar("SELECT count(*) FROM core.admission_stage")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(stage, 0);
    let mut hide = batch(vec![]);
    hide.archive.push(ArchiveRequest::Hide {
        target: located.id,
        operation: OperationId::new(),
    });
    admit_one(&mut conn, &hide).await;
    let outcome = admit_one(&mut conn, &b).await;
    assert_eq!((outcome.hidden, outcome.repeats), (1, 1));
}

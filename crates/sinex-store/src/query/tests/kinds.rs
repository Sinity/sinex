use super::*;
use sinex_model::{KindDeclaration, KindRef};

async fn register(
    conn: &mut PgConnection,
    version: u64,
    property: serde_json::Value,
) -> KindDeclaration {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: "example.point".into(),
            version,
        },
        "example".into(),
    );
    d.payload_schema = Some(
        json!({"type":"object","properties":{"value":property,"at":{"type":"string","format":"date-time"}},"required":["value","at"]}),
    );
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    d
}

async fn points(conn: &mut PgConnection, d: &KindDeclaration) {
    let events = (1..=3)
        .map(|value| {
            Event::submitted(
                d.kind.clone(),
                json!({"value":value,"at":"2026-10-27T14:00:00+02:00"}),
                "example".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(0),
                [0; 32],
                Class::Open,
            )
        })
        .collect();
    let b = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn continuation_keeps_its_field_types_when_another_version_registers(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let d = register(&mut conn, 9, json!({"type":"integer"})).await;
    points(&mut conn, &d).await;
    let text = "events where event_type = example.point sort payload.value fields payload.value, payload.at";
    let first = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await;
    assert_eq!(first.rows[0]["payload.value"], json!(1));
    assert_eq!(
        first.rows[0]["payload.at"],
        json!("2026-10-27T12:00:00+00:00")
    );
    register(&mut conn, 10, json!({"type":"string"})).await;
    let next = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(1),
            cursor: first.page.cursor,
        },
    )
    .await;
    assert_eq!(next.rows[0]["payload.value"], json!(2));
}

#[devkit::test]
async fn timestamp_extraction_checks_calendar_dates_and_schemaless_mixed_values(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let valid: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT core.payload_time('2026-10-27T14:00:00+02:00')")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(valid.unwrap().unix_timestamp(), 1793102400);
    let lowercase: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT core.payload_time('2026-10-27t12:00:00z')")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(lowercase.unwrap().unix_timestamp(), 1793102400);

    for text in [
        "2026-02-30T12:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-01-01T25:00:00Z",
        "2026-10-27T12:00:00Z trailing",
        "infinity",
    ] {
        let parsed: Option<time::OffsetDateTime> =
            sqlx::query_scalar("SELECT core.payload_time($1)")
                .bind(text)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert!(parsed.is_none(), "{text}");
    }
}

#[devkit::test]
async fn mixed_version_fields_keep_numeric_comparisons_and_aggregates(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let integer = register(&mut conn, 9, json!({"type":"integer"})).await;
    points(&mut conn, &integer).await;
    register(&mut conn, 10, json!({"type":"string"})).await;
    let summed = ask(
        &mut conn,
        "events where event_type = example.point and payload.value >= 2 sum(payload.value)",
    )
    .await;
    assert_eq!(summed.total, 1);
    assert_eq!(summed.rows[0]["sum_payload_value"], json!(5));
    let counted = ask(
        &mut conn,
        "events where event_type = example.point count_distinct(payload.value)",
    )
    .await;
    assert_eq!(counted.rows[0]["count_distinct_payload_value"], json!(3));
    let dates = ask(
        &mut conn,
        "events where event_type = example.point and day(payload.at) = 2026-10-27 count",
    )
    .await;
    assert_eq!(dates.rows[0]["count"], json!(3));
}

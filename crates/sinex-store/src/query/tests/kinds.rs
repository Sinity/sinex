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

async fn points(conn: &mut crate::AdmissionContext, d: &KindDeclaration) {
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
    tx.admit(&b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn continuation_keeps_its_field_types_when_another_version_registers(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = register(&mut *conn, 9, json!({"type":"integer"})).await;
    points(&mut conn, &d).await;
    let text = "events where event_type = example.point sort payload.value fields payload.value, payload.at";
    let first = ask_page(
        &mut *conn,
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
    register(&mut *conn, 10, json!({"type":"string"})).await;
    let next = ask_page(
        &mut *conn,
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
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let valid: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT core.payload_time('2026-10-27T14:00:00+02:00')")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(valid.unwrap().unix_timestamp(), 1793102400);
    let lowercase: Option<time::OffsetDateTime> =
        sqlx::query_scalar("SELECT core.payload_time('2026-10-27t12:00:00z')")
            .fetch_one(&mut *conn)
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
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        assert!(parsed.is_none(), "{text}");
    }
}

#[devkit::test]
async fn dynamic_json_uses_projected_row_types_without_losing_scalar_spelling(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = register(&mut conn, 19, json!({})).await;
    let values = [
        ("leading", json!("001")),
        ("one", json!(1)),
        ("string42", json!("42")),
        ("number42", json!(42)),
        ("yes", json!(true)),
        ("stringtrue", json!("true")),
        ("no", json!(false)),
        ("null", Json::Null),
        ("object", json!({"n":42})),
        ("array", json!([42])),
        ("literalstar", json!("a*b")),
        ("nearstar", json!("axxb")),
    ];
    let events = values
        .into_iter()
        .map(|(label, value)| {
            Event::submitted(
                d.kind.clone(),
                json!({"value":value,"label":label,"at":"2026-10-27T12:00:00Z"}),
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
    let batch = Batch {
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
    tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    for (condition, want) in [
        ("payload.value = 001", vec!["leading", "one"]),
        ("payload.value = \"001\"", vec!["leading", "one"]),
        ("payload.value = 42", vec!["number42", "string42"]),
        ("payload.value = true", vec!["stringtrue", "yes"]),
        ("payload.value ^ 001", vec!["leading"]),
        ("payload.value ~ 001", vec!["leading"]),
        ("payload.value = a*b", vec!["literalstar"]),
        (
            "NOT (payload.value = xyz)",
            vec![
                "leading",
                "literalstar",
                "nearstar",
                "string42",
                "stringtrue",
            ],
        ),
        ("NOT (payload.missing = xyz)", vec![]),
    ] {
        let text = format!("events WHERE event_type = example.point AND {condition}");
        let answer = ask(&mut conn, &text).await;
        let mut actual: Vec<_> = answer
            .rows
            .iter()
            .map(|row| row["payload"]["label"].as_str().unwrap())
            .collect();
        actual.sort_unstable();
        assert_eq!(actual, want, "{text}");
    }
    crate::privacy::add_rule(
        &mut conn,
        &crate::privacy::NewRule {
            name: "project-current-values".into(),
            source: "example".into(),
            event_type: Some("example.point".into()),
            field: Some("/value".into()),
            matcher: "pattern".into(),
            matcher_arg: Some("42".into()),
            class: Class::Open,
            action: crate::privacy::Action::Mask,
            author: "test".into(),
        },
    )
    .await
    .unwrap();
    crate::privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    let original = ask(
        &mut conn,
        "events WHERE event_type = example.point AND payload.value = 42",
    )
    .await;
    assert_eq!(
        original.total, 0,
        "raw numeric values cannot bypass current transformed JSON rows"
    );
    let masked = ask(
        &mut conn,
        "events WHERE event_type = example.point AND payload.value = \"[masked]\"",
    )
    .await;
    let labels: std::collections::BTreeSet<_> = masked
        .rows
        .iter()
        .map(|row| row["payload"]["label"].as_str().unwrap())
        .collect();
    assert!(
        labels.contains("number42"),
        "the projected string controls the comparison type of an originally numeric value"
    );
}

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

async fn mixed_payloads(conn: &mut crate::AdmissionContext) -> [Uuid; 3] {
    let declarations: Vec<_> = [
        ("example.integer", json!({"type":"integer"})),
        ("example.text", json!({"type":["string","null"]})),
    ]
    .into_iter()
    .map(|(name, value)| {
        let mut declaration = KindDeclaration::schemaless(
            KindRef {
                name: name.into(),
                version: 1,
            },
            "example".into(),
        );
        declaration.payload_schema = Some(json!({
            "type":"object",
            "properties":{"value":value,"at":{"type":"string","format":"date-time"}},
            "required":["value","at"]
        }));
        declaration.time_rule = Some(sinex_model::kinds::TimeRule::FirstPresent {
            fields: vec!["at".into()],
            quality: sinex_model::TimeQuality::Captured,
        });
        declaration
    })
    .collect();
    let mut tx = conn.begin().await.unwrap();
    sinex_schema::kinds::register(&mut tx, "example", &declarations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let events: Vec<_> = [(0, json!(1)), (1, json!("1")), (1, Json::Null)]
        .into_iter()
        .enumerate()
        .map(|(index, (kind, value))| {
            Event::submitted(
                declarations[kind].kind.clone(),
                json!({"value":value,"at":format!("2026-09-14T08:0{index}:00Z")}),
                "example".into(),
                Origin::Record {
                    location: RecordLocation {
                        material: MaterialId::new(),
                        anchor: 0,
                    },
                    output_index: 0,
                },
                ReadingOrder(0),
                [index as u8; 32],
                Class::Open,
            )
        })
        .collect();
    let ids = events.iter().map(|event| event.id.0).collect::<Vec<_>>();
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
    ids.try_into().unwrap()
}

async fn selection_before_kind_filter(db: devkit::pg::TestDatabase, grouped: bool, joined: bool) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = mixed_payloads(&mut conn).await;
    // The oracle partitions and orders the original JSONB, never a text cast.
    let (selection, oracle) = if grouped {
        (
            "events | sort by when, id | group by payload.value | first 1",
            "SELECT id,event_type,ts_orig FROM (SELECT id,event_type,ts_orig,row_number() OVER (PARTITION BY payload->'value' ORDER BY ts_orig,id) AS chosen FROM core.events) rows WHERE chosen=1",
        )
    } else {
        (
            "events | sort by payload.value, when, id | limit 1",
            "SELECT id,event_type,ts_orig FROM core.events ORDER BY payload->'value' ASC NULLS LAST,ts_orig,id LIMIT 1",
        )
    };
    let original: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT id FROM ({oracle}) selected ORDER BY ts_orig,id"
    ))
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let unfiltered = ask(
        &mut conn,
        &format!("{selection} | sort by when, id | fields id"),
    )
    .await;
    assert_eq!(unfiltered.total as usize, original.len());
    assert_eq!(
        column(&unfiltered, "id"),
        original.iter().map(|id| json!(id)).collect::<Vec<_>>()
    );
    let expected: Vec<Uuid> = sqlx::query_scalar(&format!(
        "SELECT id FROM ({oracle}) selected WHERE event_type='example.text' ORDER BY ts_orig,id"
    ))
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    assert_eq!(
        expected,
        if grouped {
            vec![ids[1], ids[2]]
        } else {
            vec![ids[2]]
        }
    );
    let selected = format!("{selection} | where event_type = example.text");
    let query = if joined {
        format!(
            "events where id = {} | join ({selected}) on event_type = event_type | sort by match.when, match.id | fields match.id as id",
            ids[1]
        )
    } else {
        format!("{selected} | sort by when, id | fields id")
    };
    let answer = ask(&mut conn, &query).await;
    assert_eq!(answer.total as usize, expected.len(), "{query}");
    assert_eq!(
        answer.rows,
        expected
            .iter()
            .map(|id| json!({"id":id}))
            .collect::<Vec<_>>(),
        "{query}"
    );
}

#[devkit::test]
async fn later_kind_filter_preserves_group_first_json_partitions(db: devkit::pg::TestDatabase) {
    selection_before_kind_filter(db, true, false).await;
}

#[devkit::test]
async fn later_kind_filter_preserves_sort_limit_json_order(db: devkit::pg::TestDatabase) {
    selection_before_kind_filter(db, false, false).await;
}

#[devkit::test]
async fn joined_later_kind_filter_preserves_group_first_json_partitions(
    db: devkit::pg::TestDatabase,
) {
    selection_before_kind_filter(db, true, true).await;
}

#[devkit::test]
async fn joined_later_kind_filter_preserves_sort_limit_json_order(db: devkit::pg::TestDatabase) {
    selection_before_kind_filter(db, false, true).await;
}

#[devkit::test]
async fn continuation_keeps_its_field_types_when_another_version_registers(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let d = register(&mut conn, 9, json!({"type":"integer"})).await;
    points(&mut conn, &d).await;
    let text = "events where event_type = example.point | sort by payload.value | fields payload.value, payload.at";
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
async fn dynamic_json_uses_stored_row_types_without_losing_scalar_spelling(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: "example.point".into(),
            version: 19,
        },
        "example".into(),
    );
    d.payload_schema = Some(json!({
        "type":"object",
        "properties":{"value":{},"missing":{},"at":{"type":"string","format":"date-time"}},
        "required":["value","at"]
    }));
    let mut tx = conn.begin().await.unwrap();
    // The field is declared but absent from every row, so comparison is Unknown.
    sinex_schema::kinds::register(&mut tx, "example", std::slice::from_ref(&d))
        .await
        .unwrap();
    tx.commit().await.unwrap();
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
            name: "mask-new-values".into(),
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
    let late = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![Event::submitted(
                d.kind.clone(),
                json!({"value":42,"label":"late42","at":"2026-10-27T12:00:00Z"}),
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
            )],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&late, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    let mut stored: Vec<_> = ask(
        &mut conn,
        "events WHERE event_type = example.point AND payload.value = 42",
    )
    .await
    .rows
    .iter()
    .map(|row| row["payload"]["label"].as_str().unwrap().to_owned())
    .collect();
    stored.sort_unstable();
    assert_eq!(
        stored,
        ["number42", "string42"],
        "a new rule applies at admission and never reaches stored events"
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
    assert_eq!(
        labels,
        ["late42"].into(),
        "the masked string controls the comparison type of an originally numeric value"
    );
}

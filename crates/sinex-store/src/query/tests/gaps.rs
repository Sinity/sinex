use super::*;

#[devkit::test]
async fn fill_keeps_missing_buckets_unknown_and_composes_with_paging_and_filters(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    let text = "commands where when in 2026-09 group by day(when) count fill";
    let answer = ask(&mut conn, text).await;
    assert_eq!(answer.total, 30);
    let recorded = answer
        .rows
        .iter()
        .find(|r| r["day"] == "2026-09-14")
        .unwrap();
    assert_eq!(recorded["count"], 5);
    assert_eq!(recorded["coverage"], "recorded");
    assert!(
        answer
            .rows
            .iter()
            .filter(|r| r["day"] != "2026-09-14")
            .all(|r| r["count"].is_null() && r["coverage"] == "no_data")
    );
    let filtered = ask(&mut conn, &format!("{text} where count > 0")).await;
    assert_eq!(filtered.total, 1);
    let first = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(3),
            cursor: None,
        },
    )
    .await;
    let second = ask_page(
        &mut conn,
        text,
        Page {
            size: Some(3),
            cursor: first.page.cursor,
        },
    )
    .await;
    assert_eq!(second.rows[0]["day"], "2026-09-04");
    let dimensions = ask(
        &mut conn,
        "commands where when in 2026-09 group by day(when), cwd count fill",
    )
    .await;
    assert_eq!(dimensions.total, 120);
}

#[devkit::test]
async fn typed_payload_aggregates_preserve_unknown_and_sort_as_numbers(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    let a = ask(&mut conn,"events where event_type = command.executed sum(payload.exit_code) count(payload.exit_code)").await;
    assert_eq!(a.rows[0]["sum_payload_exit_code"], 2);
    assert_eq!(a.rows[0]["count_payload_exit_code"], 4);
    let a = ask(&mut conn,"events where event_type = command.executed fields payload.exit_code sort payload.exit_code desc").await;
    assert_eq!(a.rows[0]["payload.exit_code"], 1);
    assert!(a.rows.last().unwrap()["payload.exit_code"].is_null());
}

#[devkit::test]
async fn source_views_distinguish_unknown_continuity_and_drift_from_no_changes(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    let a = ask(
        &mut conn,
        "sources fields source,continuity,gaps,drift,record_failures",
    )
    .await;
    assert_eq!(a.total, 2);
    for row in a.rows {
        assert_eq!(row["continuity"], "unknown");
        assert_eq!(row["gaps"], json!([]));
        assert_eq!(row["drift"]["status"], "unknown");
        assert_eq!(row["record_failures"], json!([]));
    }
}

#[devkit::test]
async fn fill_uses_wall_clock_days_through_a_change_of_offset(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let ctx = Context::new(
        "2026-11-01T00:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    );
    let text = "commands where when in 2026-10 group by day(when) count fill";
    let q = bind(text, &ctx).unwrap();
    let a = run(&mut conn, text, &q, &ctx, &Page::default())
        .await
        .unwrap();
    assert_eq!(a.total, 31);
    assert_eq!(a.rows[24]["day"], "2026-10-25");
    assert_eq!(a.rows[30]["day"], "2026-10-31");
    let text = "commands where when in 2026-10 group by week(when) count fill";
    let q = bind(text, &ctx).unwrap();
    let a = run(&mut conn, text, &q, &ctx, &Page::default())
        .await
        .unwrap();
    assert_eq!(a.total, 5);
}

#[devkit::test]
async fn a_live_run_establishes_zero_only_for_buckets_it_covers(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    sqlx::query("INSERT INTO registry.sources(name,driver,version,event_types,purpose) VALUES ('terminal.atuin','live-stream',1,ARRAY['command.executed'],'synthetic query test')").execute(&mut conn).await.unwrap();
    let unknown = ask(
        &mut conn,
        "sources where source = terminal.atuin fields continuity",
    )
    .await;
    assert_eq!(unknown.rows[0]["continuity"], "unknown");
    sqlx::query(
        "INSERT INTO core.runs (id,name,host,started_at,ended_at) SELECT DISTINCT run_id,'synthetic','synthetic','2026-09-13T04:00:00Z'::timestamptz,'2026-09-16T04:00:00Z'::timestamptz FROM core.events",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    let a = ask(
        &mut conn,
        "events where source = terminal.atuin and when in 2026-09 group by day(when) count fill",
    )
    .await;
    assert_eq!(a.rows[11]["coverage"], "no_data");
    assert_eq!(a.rows[12]["count"], 0);
    assert_eq!(a.rows[12]["coverage"], "observed");
    assert_eq!(a.rows[13]["count"], 5);
    assert_eq!(a.rows[14]["count"], 0);
    assert_eq!(a.rows[16]["coverage"], "no_data");
}

#[devkit::test]
async fn source_gaps_merge_overlapping_runs_and_drift_keeps_recorded_changes(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    sqlx::query("INSERT INTO registry.sources(name,driver,version,event_types,purpose) VALUES ('terminal.atuin','live-stream',1,ARRAY['command.executed'],'synthetic query test')").execute(&mut conn).await.unwrap();
    sqlx::query(
        "INSERT INTO core.runs (id,name,host,started_at,ended_at) SELECT DISTINCT run_id,'synthetic','synthetic','2026-09-14T04:00:00Z'::timestamptz,'2026-09-14T06:00:00Z'::timestamptz FROM core.events",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    admit_commands(&mut conn, &[("synthetic later", "/src/other")]).await;
    sqlx::query("INSERT INTO core.runs (id,name,host,started_at,ended_at) SELECT DISTINCT e.run_id,'synthetic','synthetic','2026-09-14T08:00:00Z'::timestamptz,'2026-09-14T09:00:00Z'::timestamptz FROM core.events e WHERE NOT EXISTS (SELECT 1 FROM core.runs r WHERE r.id=e.run_id)").execute(&mut conn).await.unwrap();
    let payload: sinex_model::event_types::health::HealthRecord = serde_json::from_value(json!({"provider":"samsung-health","category":"step_daily_trend","native_id":"synthetic-drift","start":null,"end":null,"offset_minutes":null,"values":{"count":1,"synthetic_added_field":true}})).unwrap();
    let event = Event::build(
        &payload,
        "terminal.atuin",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(3),
        [3; 32],
        Class::Open,
    )
    .unwrap();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    sqlx::query("INSERT INTO core.runs (id,name,host,started_at,ended_at) VALUES ($1,'synthetic','synthetic','2026-09-14T08:30:00Z','2026-09-14T08:45:00Z')").bind(batch.run.0).execute(&mut conn).await.unwrap();
    let a = ask(
        &mut conn,
        "sources where source = terminal.atuin fields source,continuity,gaps,drift",
    )
    .await;
    assert_eq!(a.total, 1);
    assert_eq!(a.rows[0]["continuity"], "run_lifetimes");
    assert_eq!(a.rows[0]["gaps"].as_array().unwrap().len(), 1);
    assert_eq!(a.rows[0]["drift"]["status"], "changes_recorded");
    assert!(!a.rows[0]["drift"]["events"].as_array().unwrap().is_empty());
}

#[devkit::test]
async fn text_lists_compare_members_and_return_the_declared_array(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    spanning_session(&mut conn).await;
    for (condition, total) in [
        ("payload.cwds = /src/tide", 1),
        ("payload.cwds != /src/tide", 0),
        ("payload.cwds != /src/absent", 1),
        ("payload.cwds ~ TiDe", 1),
        ("payload.cwds ^ /src/o", 1),
        (r#"payload.cwds ~ ",""#, 0),
        ("payload.cwds = [/src/absent, /src/other]", 1),
    ] {
        let answer = ask(
            &mut conn,
            &format!("events where event_type = shell.session and {condition} fields payload.cwds"),
        )
        .await;
        assert_eq!(answer.total, total, "{condition}");
        if total == 1 {
            assert_eq!(
                answer.rows[0]["payload.cwds"],
                json!(["/src/tide", "/src/other"])
            );
        }
    }
}

async fn spanning_session(conn: &mut PgConnection) {
    let parent: Uuid = sqlx::query_scalar("SELECT id FROM core.events LIMIT 1")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let event = Event::build(
        &sinex_model::event_types::ShellSession {
            host: "synthetic-list".into(),
            started_at: Timestamp::from_unix_secs(BASE - 86400).unwrap(),
            ended_at: Timestamp::from_unix_secs(BASE + 3600).unwrap(),
            day: "2026-09-13".into(),
            commands: 1,
            failed: 0,
            cwds: vec!["/src/tide".into(), "/src/other".into()],
        },
        "synthetic.sessions",
        Origin::Derived {
            parents: vec![sinex_model::EventId(parent)],
        },
        ReadingOrder(1),
        [8; 32],
        Class::Open,
    )
    .unwrap();
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = conn.begin().await.unwrap();
    admit(&mut tx, &batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

#[devkit::test]
async fn filled_recall_keeps_recorded_buckets_before_its_window_and_intersects_filters(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    world(&mut conn).await;
    spanning_session(&mut conn).await;
    let answer = ask(&mut conn, "recall sinex:window/2026-09-14T08:30:00Z/2026-09-14T09:00:00Z group by day(when) count fill").await;
    assert_eq!(answer.total, 2);
    assert_eq!(answer.rows[0]["day"], "2026-09-13");
    assert_eq!(answer.rows[0]["count"], 1);
    assert_eq!(answer.rows[0]["coverage"], "recorded");
    assert_eq!(answer.rows[1]["day"], "2026-09-14");
    assert!(answer.rows[1]["count"].is_null());
    let answer = ask(&mut conn, "recall sinex:window/2026-09-13T04:00:00Z/2026-09-16T04:00:00Z where when >= 2026-09-14T04:00:00Z and when < 2026-09-15T04:00:00Z group by day(when) count fill").await;
    assert_eq!(answer.total, 1);
    assert_eq!(answer.rows[0]["day"], "2026-09-14");
    assert_eq!(answer.rows[0]["count"], 10);
}

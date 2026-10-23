use super::*;

async fn ask_at(conn: &mut PgConnection, text: &str, ctx: &Context, page: Page) -> Result<Answer> {
    let fixed = context_for_page(ctx, &page)?;
    let bound = bind(text, &fixed).unwrap();
    run(conn, text, &bound, &fixed, &page).await
}

#[tokio::test]
async fn continuation_keeps_relative_time_at_the_original_logical_day() {
    let (_db, mut conn) = testdb::fresh().await;
    world(&mut conn).await;
    let before = Context::utc("2026-09-14T12:00:00Z".parse().unwrap());
    let after = Context::utc("2026-09-15T12:00:00Z".parse().unwrap());
    let text = "commands where when in today sort when";
    let first = ask_at(
        &mut conn,
        text,
        &before,
        Page {
            size: Some(2),
            cursor: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.total, 5);
    let later = ask_at(
        &mut conn,
        text,
        &after,
        Page {
            size: Some(3),
            cursor: first.page.cursor,
        },
    )
    .await
    .unwrap();
    assert_eq!(later.resolved.now, first.resolved.now);
    assert_eq!(later.total, 5);
    assert_eq!(later.rows.len(), 3);
    assert!(later.page.cursor.is_none());
    let fresh = ask_at(&mut conn, text, &after, Page::default())
        .await
        .unwrap();
    assert_eq!(fresh.total, 0);
}

#[tokio::test]
async fn continuation_refuses_changed_time_settings_and_a_different_bound_ast() {
    let (_db, mut conn) = testdb::fresh().await;
    world(&mut conn).await;
    let ctx = ctx();
    let text = "commands sort when";
    let first = ask_at(
        &mut conn,
        text,
        &ctx,
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await
    .unwrap();
    let page = Page {
        size: Some(1),
        cursor: first.page.cursor,
    };
    for changed in [
        Context::new(
            ctx.now,
            jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
            ctx.day_start,
        ),
        Context::new(
            ctx.now,
            jiff::tz::TimeZone::fixed(jiff::tz::offset(2)),
            ctx.day_start,
        ),
        Context::new(
            ctx.now,
            ctx.zone.clone(),
            jiff::civil::Time::constant(6, 0, 0, 0),
        ),
    ] {
        assert!(context_for_page(&changed, &page).is_err());
    }
    let mut changed = bind(text, &ctx).unwrap();
    changed.stages.push(Stage::Limit(1));
    assert!(
        run(&mut conn, text, &changed, &ctx, &page)
            .await
            .unwrap_err()
            .to_string()
            .contains("different question")
    );
}

async fn runs(conn: &mut PgConnection) -> Vec<RunId> {
    let mut ids = Vec::new();
    for name in ["cursor-a", "cursor-b"] {
        ids.push(
            crate::runs::start(
                conn,
                &crate::runs::Manifest {
                    name,
                    kind: "source",
                    version: "1",
                    build: "synthetic",
                    emits: &[],
                },
                "synthetic",
            )
            .await
            .unwrap(),
        );
    }
    ids
}

#[tokio::test]
async fn duplicate_projected_rows_cannot_cancel_the_view_change_check() {
    let (_db, mut conn) = testdb::fresh().await;
    let ids = runs(&mut conn).await;
    let text = "runs sort started fields status";
    let first = ask_at(
        &mut conn,
        text,
        &ctx(),
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.rows[0]["status"], "running");
    let mut tx = conn.begin().await.unwrap();
    for id in ids {
        crate::runs::end(&mut tx, id).await.unwrap();
    }
    tx.commit().await.unwrap();
    let next = ask_at(
        &mut conn,
        text,
        &ctx(),
        Page {
            size: Some(1),
            cursor: first.page.cursor,
        },
    )
    .await;
    assert!(matches!(next, Err(Error::StaleCursor)));
}

#[tokio::test]
async fn walk_pages_use_the_same_question_and_visible_result_checks() {
    let (_db, mut conn) = testdb::fresh().await;
    let ids = world(&mut conn).await;
    let text = format!("trace {}", ids[0].0);
    let first = ask_at(
        &mut conn,
        &text,
        &ctx(),
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await
    .unwrap();
    let next = ask_at(
        &mut conn,
        &text,
        &ctx(),
        Page {
            size: Some(1),
            cursor: first.page.cursor.clone(),
        },
    )
    .await
    .unwrap();
    assert_eq!(first.total, next.total);
    assert_ne!(first.rows, next.rows);
    assert!(next.page.cursor.is_none());
    let other = format!("trace {}", ids[1].0);
    assert!(
        ask_at(
            &mut conn,
            &other,
            &ctx(),
            Page {
                size: Some(1),
                cursor: first.page.cursor
            }
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("different question")
    );
}

#[tokio::test]
async fn cursor_sizes_and_offsets_are_checked_before_integer_conversion() {
    use base64::Engine as _;
    let (_db, mut conn) = testdb::fresh().await;
    world(&mut conn).await;
    let first = ask_at(
        &mut conn,
        "commands",
        &ctx(),
        Page {
            size: Some(1),
            cursor: None,
        },
    )
    .await
    .unwrap();
    let raw = first.page.cursor.unwrap();
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .unwrap();
    let value: Json = serde_json::from_slice(&bytes).unwrap();
    for bad in [0, u64::MAX] {
        let mut changed = value.clone();
        changed["offset"] = json!(bad);
        let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&changed).unwrap());
        assert!(
            ask_at(
                &mut conn,
                "commands",
                &ctx(),
                Page {
                    size: Some(1),
                    cursor: Some(raw)
                }
            )
            .await
            .is_err()
        );
    }
    assert!(
        ask_at(
            &mut conn,
            "commands",
            &ctx(),
            Page {
                size: Some(u64::MAX),
                cursor: None
            }
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn page_count_and_rows_keep_one_snapshot_across_a_concurrent_hide() {
    use std::time::{Duration, Instant};
    let (db, mut conn) = testdb::fresh().await;
    let ids = world(&mut conn).await;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION public.cursor_wait(v jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE AS $$
         BEGIN PERFORM pg_advisory_xact_lock(982451653); RETURN v; END $$;
         ALTER VIEW core.visible_events RENAME TO cursor_original_events;
         CREATE VIEW core.visible_events AS SELECT id,event_type,source,run_id,material_id,anchor,ordinal,parents,
         ts_orig,ts_quality,ts_coided,ts_persisted,commit_seq,public.cursor_wait(payload) AS payload,
         equivalence_key,reading_order,record_hash,record_state,class,support FROM core.cursor_original_events;"
    ).execute(&mut conn).await.unwrap();
    let mut writer = PgConnection::connect(&db.url(None)).await.unwrap();
    let mut block = writer.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(982451653)")
        .execute(&mut *block)
        .await
        .unwrap();
    let read = tokio::spawn(async move {
        ask_at(
            &mut conn,
            "events sort when",
            &ctx(),
            Page {
                size: Some(100),
                cursor: None,
            },
        )
        .await
        .unwrap()
    });
    let mut watch = PgConnection::connect(&db.url(None)).await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event='advisory')",
        )
        .bind(pid)
        .fetch_one(&mut watch)
        .await
        .unwrap();
        if waiting {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "query did not reach the gated count projection"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let hide = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![],
        archive: vec![ArchiveRequest::Hide {
            target: EventId(ids[0].0),
            operation: OperationId::new(),
        }],
    };
    admit(&mut block, &hide, &NoDerivations).await.unwrap();
    block.commit().await.unwrap();
    let answer = read.await.unwrap();
    assert_eq!(answer.total, 10);
    assert_eq!(
        answer.rows.len(),
        10,
        "count and rows observed different committed views"
    );
    assert!(
        answer
            .rows
            .iter()
            .any(|row| row["id"] == ids[0].0.to_string())
    );
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events e WHERE NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)")
        .fetch_one(&mut watch).await.unwrap();
    assert_eq!(after, 9, "the hide really committed during the page read");
}

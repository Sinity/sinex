use super::*;

async fn ask_at(conn: &mut PgConnection, text: &str, ctx: &Context, page: Page) -> Result<Answer> {
    let fixed = context_for_page(conn, ctx, &page, text).await?;
    let bound = bind(text, &fixed).unwrap();
    run(conn, text, Some(&bound), &fixed, &page).await
}

#[devkit::test]
async fn continuation_keeps_relative_time_at_the_original_logical_day(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
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

#[devkit::test]
async fn continuation_refuses_changed_time_settings_and_a_different_bound_ast(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
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
        assert!(
            context_for_page(&mut conn, &changed, &page, text)
                .await
                .is_err()
        );
    }
    let mut changed = bind(text, &ctx).unwrap();
    changed.stages.push(Stage::Limit(1));
    assert!(
        run(&mut conn, text, Some(&changed), &ctx, &page)
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

#[devkit::test]
async fn duplicate_projected_rows_cannot_cancel_the_view_change_check(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
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

#[devkit::test]
async fn walk_pages_use_the_same_question_and_visible_result_checks(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
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

#[devkit::test]
async fn cursor_authentication_and_page_sizes_are_checked_before_integer_conversion(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
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
    for index in [0, raw.len() / 2, raw.len() - 1] {
        let mut bytes = raw.as_bytes().to_vec();
        bytes[index] = if bytes[index] == b'0' { b'1' } else { b'0' };
        let token = String::from_utf8(bytes).unwrap();
        assert!(
            ask_at(
                &mut conn,
                "commands",
                &ctx(),
                Page {
                    size: Some(1),
                    cursor: Some(token)
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

#[devkit::test]
async fn page_count_and_rows_keep_one_snapshot_across_concurrent_run_completion(
    db: devkit::pg::TestDatabase,
) {
    use std::time::{Duration, Instant};
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let run = runs(&mut conn).await[0];
    // Align this synthetic run with the fixed event fixture without changing runtime clocks.
    sqlx::query("UPDATE core.runs SET started_at = '2026-09-14T08:00:00Z' WHERE id=$1")
        .bind(run.0)
        .execute(&mut conn)
        .await
        .unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE FUNCTION public.cursor_wait(v jsonb) RETURNS jsonb LANGUAGE plpgsql VOLATILE AS $$
         BEGIN PERFORM pg_advisory_xact_lock(982451653); RETURN v; END $$;
         ALTER VIEW core.visible_events RENAME TO cursor_original_events;
         CREATE VIEW core.visible_events AS SELECT id,event_type,source,run_id,material_id,anchor,output_index,parents,
         ts_orig,ts_quality,ts_coided,ts_persisted,commit_seq,public.cursor_wait(payload) AS payload,
         equivalence_key,reading_order,record_hash,record_state,class,kind_version,validation FROM core.cursor_original_events;"
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
            "events near (runs where status = running) within 1h sort when",
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
    crate::runs::end(&mut block, run).await.unwrap();
    block.commit().await.unwrap();
    let answer = read.await.unwrap();
    assert_eq!(answer.total, 10);
    assert_eq!(
        answer.rows.len(),
        10,
        "count and rows observed different committed views"
    );
    let after = ask_at(
        &mut watch,
        "events near (runs where status = running) within 1h sort when",
        &ctx(),
        Page::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        after.total, 0,
        "run completion really committed during the page read"
    );
}

#[devkit::test]
async fn provenance_walks_refuse_stages_the_reader_does_not_execute(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = world(&mut conn).await;
    let text = format!("trace {} limit 1", ids[0].0);
    let error = ask_at(&mut conn, &text, &ctx(), Page::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("do not support query stages"));
}

/// A continuation checks only the rows already handed out: a change to a row
/// not yet handed out shows on its page instead of refusing the question.
#[devkit::test]
async fn a_change_after_the_rows_handed_out_shows_on_a_later_page(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let ids = runs(&mut conn).await;
    let text = "runs sort started fields id,status";
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
    assert_eq!(first.total, 2);
    assert_eq!(first.rows[0]["id"], json!(ids[0].0.to_string()));
    let mut tx = conn.begin().await.unwrap();
    crate::runs::end(&mut tx, ids[1]).await.unwrap();
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
    .await
    .unwrap();
    assert_eq!(next.total, 2);
    assert_eq!(next.rows[0]["id"], json!(ids[1].0.to_string()));
    assert_ne!(next.rows[0]["status"], json!("running"));
    assert!(next.page.cursor.is_none());
}

#[devkit::test]
async fn cursor_secret_is_not_public_sql_and_open_paging_needs_no_unlock(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    assert!(
        read_sql::run(&mut conn, "SELECT secret FROM core.query_cursor_key")
            .await
            .is_err()
    );
    let first = ask_at(
        &mut conn,
        "commands sort exit_code desc fields command",
        &ctx(),
        Page {
            size: Some(2),
            cursor: None,
        },
    )
    .await
    .unwrap();
    let next = ask_at(
        &mut conn,
        "commands sort exit_code desc fields command",
        &ctx(),
        Page {
            size: Some(3),
            cursor: first.page.cursor,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.rows.len() + next.rows.len(), 5);
    assert_eq!(first.total, next.total);
}
#[devkit::test]
async fn canonical_equivalent_questions_keep_one_keyset(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    let a = "commands where exit_code >= 0 and cwd exists sort when fields command";
    let b = "commands where cwd exists and exit_code >= 0 sort when fields command";
    let first = ask_at(
        &mut conn,
        a,
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
        b,
        &ctx(),
        Page {
            size: Some(50),
            cursor: first.page.cursor,
        },
    )
    .await
    .unwrap();
    assert_eq!(first.total, next.total);
    assert_eq!(first.rows.len() + next.rows.len(), first.total as usize);
}

#[devkit::test]
async fn sql_matches_shared_unknown_and_wildcard_predicates(db: devkit::pg::TestDatabase) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    world(&mut conn).await;
    for (text, total) in [
        ("commands where exit_code:unknown", 1),
        ("commands where exit_code:*", 5),
        ("commands where exit_code = unknown", 0),
        ("commands where not exit_code = unknown", 0),
        ("commands where exit_code = [unknown,0]", 2),
        ("commands where exit_code != [unknown,0]", 0),
    ] {
        let answer = ask_at(&mut conn, text, &ctx(), Page::default())
            .await
            .unwrap();
        assert_eq!(answer.total, total, "{text}");
    }
}

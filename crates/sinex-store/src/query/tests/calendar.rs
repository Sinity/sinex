use super::*;

async fn point(conn: &mut PgConnection, name: &str, at: &str) {
    sqlx::query("INSERT INTO core.runs(id,name,host,started_at) VALUES(gen_random_uuid(),$1,'synthetic',$2::text::timestamptz)")
        .bind(name).bind(at).execute(conn).await.unwrap();
}
fn clock(start: jiff::civil::Time, zone: &str) -> Context {
    Context::new(
        "2026-11-15T12:00:00Z".parse().unwrap(),
        jiff::tz::TimeZone::get(zone).unwrap(),
        start,
    )
}
async fn at(conn: &mut PgConnection, text: &str, ctx: &Context) -> Answer {
    run(conn, text, None, ctx, &Page::default()).await.unwrap()
}

#[devkit::test]
async fn sql_logical_days_use_first_fold_boundary_and_preserve_nested_grouping(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    for (name, time) in [
        ("calendar-fold-before", "2026-10-25T00:10:00Z"),
        ("calendar-fold-first", "2026-10-25T00:20:00Z"),
        ("calendar-fold-second", "2026-10-25T01:10:00Z"),
        ("calendar-fold-after", "2026-10-25T01:20:00Z"),
    ] {
        point(&mut conn, name, time).await;
    }
    let ctx = clock(jiff::civil::Time::constant(2, 15, 0, 0), "Europe/Warsaw");
    let one=at(&mut conn,"runs where producer = calendar-fold-second and day(started) = 2026-10-25 and weekday(started) = sun and month(started) = 2026-10 and month_day(started) = 10-25",&ctx).await;
    assert_eq!(
        one.total, 1,
        "second 02:10 occurs after the first 02:15 boundary"
    );
    let grouped = at(
        &mut conn,
        "runs where producer ^ calendar-fold group by day(started) count sort day",
        &ctx,
    )
    .await;
    assert_eq!(
        grouped.rows,
        json!([{"day":"2026-10-24","count":1},{"day":"2026-10-25","count":3}])
            .as_array()
            .unwrap()
            .clone()
    );
    let nested=at(&mut conn,"runs where producer ^ calendar-fold group by producer min(started) group by day(min_started) count sort day",&ctx).await;
    assert_eq!(nested.rows, grouped.rows);
    let filled=at(&mut conn,"runs where producer ^ calendar-fold and started in 2026-10-24 group by day(started) count fill",&ctx).await;
    assert_eq!(
        filled.rows,
        json!([{"day":"2026-10-24","count":1,"coverage":"recorded"}])
            .as_array()
            .unwrap()
            .clone()
    );
}

#[devkit::test]
async fn sql_gap_and_second_precision_boundaries_match_shared_days_and_weeks(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    point(&mut conn, "calendar-gap-before", "2026-03-29T00:59:59Z").await;
    point(&mut conn, "calendar-gap-after", "2026-03-29T01:00:00Z").await;
    let gap = clock(jiff::civil::Time::constant(2, 15, 0, 0), "Europe/Warsaw");
    assert_eq!(
        at(
            &mut conn,
            "runs where producer = calendar-gap-before and day(started) = 2026-03-28",
            &gap
        )
        .await
        .total,
        1
    );
    assert_eq!(
        at(
            &mut conn,
            "runs where producer = calendar-gap-after and day(started) = 2026-03-29",
            &gap
        )
        .await
        .total,
        1
    );
    point(&mut conn, "calendar-second-before", "2026-09-14T00:00:10Z").await;
    point(&mut conn, "calendar-second-after", "2026-09-14T00:00:15Z").await;
    point(&mut conn, "calendar-month-before", "2026-11-01T00:00:10Z").await;
    point(&mut conn, "calendar-month-after", "2026-11-01T00:00:15Z").await;
    let seconds = clock(jiff::civil::Time::constant(0, 0, 15, 0), "UTC");
    for text in [
        "runs where producer = calendar-second-before and day(started) = 2026-09-13 and week(started) = 2026-09-07 and weekday(started) = sun and month_day(started) = 09-13",
        "runs where producer = calendar-second-after and day(started) = 2026-09-14 and week(started) = 2026-09-14 and weekday(started) = mon",
        "runs where producer = calendar-month-before and month(started) = 2026-10",
        "runs where producer = calendar-month-after and month(started) = 2026-11",
    ] {
        assert_eq!(at(&mut conn, text, &seconds).await.total, 1, "{text}");
    }
}

#[devkit::test]
async fn calendar_preparation_streams_more_than_two_batches_without_a_date_cap(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    sqlx::query("INSERT INTO core.runs(id,name,host,started_at) SELECT gen_random_uuid(),'calendar-batch','synthetic','2000-01-01T12:00:00Z'::timestamptz+i*interval '1 day' FROM generate_series(0,4112)i").execute(&mut conn).await.unwrap();
    initialize_calendar(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    read_one_snapshot(&mut tx).await.unwrap();
    sqlx::query("SET LOCAL work_mem='64kB'")
        .execute(&mut *tx)
        .await
        .unwrap();
    let ctx = clock(jiff::civil::Time::constant(4, 0, 0, 0), "UTC");
    let query = bind(
        "runs where producer = calendar-batch group by day(started) count",
        &ctx,
    )
    .unwrap();
    let ((sql, params), stats) = {
        let (compiled, stats) = compile::calendar::compile_with_stats(&mut tx, &ctx, 0, &query)
            .await
            .unwrap();
        (compiled.count.render(), stats)
    };
    let count: i64 = super::super::sql::bind_scalar(sqlx::query_scalar(&sql), params)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(count, 4113);
    assert_eq!(stats.input_rows, 4113);
    assert_eq!(stats.max_input_batch, 2048);
    assert!(stats.max_boundary_batch <= 2048);
    let boundaries: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_temp.sinex_query_days")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(boundaries, 4113);
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn grouped_prefix_limit_keeps_its_selection_when_preparing_a_hidden_time(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    point(&mut conn, "calendar-prefix-a", "2030-01-01T12:00:00Z").await;
    point(&mut conn, "calendar-prefix-z", "2000-01-01T12:00:00Z").await;
    let ctx = clock(jiff::civil::Time::constant(4, 0, 0, 0), "UTC");
    let query = "runs where producer ^ calendar-prefix group by producer min(started) limit 1 group by day(min_started) count";
    let answer = at(&mut conn, query, &ctx).await;
    assert_eq!(
        answer.rows,
        json!([{"day":"2030-01-01","count":1}])
            .as_array()
            .unwrap()
            .clone()
    );
    let projected=at(&mut conn,"runs where producer ^ calendar-prefix group by producer min(started) limit 1 fields min_started",&ctx).await;
    assert!(
        projected.rows[0]["min_started"]
            .as_str()
            .unwrap()
            .starts_with("2030-01-01")
    );
}

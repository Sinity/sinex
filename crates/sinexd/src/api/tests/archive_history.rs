use super::*;

async fn totals(w: &World, question: &str) -> u64 {
    let (status, body) = call(w, Some(READ), json!({"method":"query","query":question})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["result"]["total"].as_u64().unwrap()
}

#[tokio::test]
async fn explicit_historical_api_views_survive_repeated_hide_and_unhide() {
    let w = world().await;
    admit(&w, &["first", "second", "third"], 0).await;
    let before = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(before, 3);
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"events sort when"}),
    )
    .await;
    let reference = body["result"]["rows"][0]["id"].as_str().unwrap().to_owned();
    let mut snapshots = vec![(format!("events as of seq {before}"), 3u64)];
    let stamp: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&w.reads)
        .await
        .unwrap();
    let stamp = jiff::Timestamp::from_nanosecond(stamp.unix_timestamp_nanos()).unwrap();
    let time_before = format!("events as of {stamp}");
    assert_eq!(totals(&w, &time_before).await, 3);
    for (method, total) in [("hide", 2u64), ("unhide", 3), ("hide", 2)] {
        let (status, body) = call(
            &w,
            Some(AGENT),
            json!({"method":method,"reference":reference}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let position = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
            .await
            .unwrap();
        assert_eq!(position, before + snapshots.len() as i64);
        assert_eq!(totals(&w, "events").await, total);
        for (question, expected) in &snapshots {
            assert_eq!(totals(&w, question).await, *expected, "{question}");
        }
        snapshots.push((format!("events as of seq {position}"), total));
        assert_eq!(
            totals(&w, &time_before).await,
            3,
            "time before hide stays complete"
        );
        let current_time: time::OffsetDateTime = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&w.reads)
            .await
            .unwrap();
        let current_time =
            jiff::Timestamp::from_nanosecond(current_time.unix_timestamp_nanos()).unwrap();
        assert_eq!(
            totals(&w, &format!("events as of {current_time}")).await,
            total
        );
        let archived = format!("archive as of seq {position}");
        assert_eq!(
            totals(&w, &archived).await,
            if method == "hide" { 1 } else { 0 }
        );
    }
    assert_eq!(totals(&w, "archive as of seq 4").await, 1);
    assert_eq!(totals(&w, "archive as of seq 5").await, 0);
    sqlx::query("DELETE FROM core.commit_log")
        .execute(&w.reads)
        .await
        .unwrap();
    assert_eq!(
        totals(&w, &time_before).await,
        3,
        "hot-log pruning must not erase time truth"
    );
    assert_eq!(totals(&w, "events as of seq 4").await, 2);
}

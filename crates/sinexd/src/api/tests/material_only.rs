use super::*;
use sinex_model::event_types::machine::LakeObservation;

fn flow(seq: u64, src: &str, dst: &str) -> Event {
    Event::build(
        &LakeObservation {
            observed_at: Timestamp::from_unix_secs(1_760_000_000).unwrap(),
            lane: "netflow".into(),
            host: "fixture-host".into(),
            seq,
            payload: json!({"src":src,"dst":dst,"proto":"tcp","bytes":10,"packets":1}),
            raw_ref: None,
        },
        "network.netflow",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(&seq.to_be_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

#[devkit::test]
async fn material_only_tombstone_removes_old_local_flow_readings(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let local: Vec<_> = (0..32)
        .map(|seq| flow(seq, "127.0.0.2", "127.0.0.1"))
        .chain([flow(32, "192.0.2.1", "::ffff:192.0.2.1")])
        .collect();
    let remote = flow(33, "192.0.2.1", "192.0.2.2");
    let remote_id = remote.id.0;
    let local_ids: Vec<_> = local.iter().map(|event| event.id.0).collect();
    let all_ids: Vec<_> = local_ids.iter().copied().chain([remote_id]).collect();
    admit_events(&w, local.into_iter().chain([remote]).collect()).await;
    let (_, archived) = surface::rows_as(
        &w,
        OPERATOR,
        surface::lifecycle_request(local_ids[0].to_string(), "archive"),
    )
    .await;
    assert_eq!(archived["outcome"], "ok", "{archived}");

    let mut conn = w.reads.acquire().await.unwrap();
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    // Flows have no search declaration. Seed search entries to exercise the
    // cascade against every table that can hold data for a reading.
    sqlx::query(
        "INSERT INTO core.search_text(event_id, text)
         SELECT id, 'fixture text' FROM unnest($1::uuid[]) AS ids(id)",
    )
    .bind(&all_ids)
    .execute(&mut *conn)
    .await
    .unwrap();

    let request = json!({"method":"tombstone","source":"network.netflow","material-only":true});
    let mut preview = request.clone();
    preview["dry-run"] = true.into();
    let (_, before) = call_unbounded(&w, Some(OPERATOR), preview.clone()).await;
    assert_eq!(before["outcome"], "ok", "{before}");
    assert_eq!(before["result"]["matching_readings"], local_ids.len());
    let remote_before: (Value, Value, Value) = sqlx::query_as(
        "SELECT to_jsonb(e), to_jsonb(s), to_jsonb(l) FROM core.events e
         JOIN core.search_text s ON s.event_id = e.id
         JOIN core.live_locations l ON l.event_id = e.id WHERE e.id = $1",
    )
    .bind(remote_id)
    .fetch_one(&mut *conn)
    .await
    .unwrap();

    let (_, removed) = call_unbounded(&w, Some(OPERATOR), request).await;
    assert_eq!(removed["outcome"], "ok", "{removed}");
    assert_eq!(
        removed["result"]["cascade"]["archived"]["lake.observed"],
        32
    );
    assert_eq!(
        removed["result"]["cascade"]["tombstoned"]["lake.observed"],
        33
    );
    assert_eq!(removed["result"]["cascade"]["materials"], 0);
    assert!(removed["result"]["batches"].as_u64().unwrap() > 1);
    for (table, id) in [
        ("core.events", "id"),
        ("core.search_text", "event_id"),
        ("core.live_locations", "event_id"),
    ] {
        let n: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM {table} WHERE {id} = ANY($1)"
        ))
        .bind(&local_ids)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        assert_eq!(n, 0, "{table}");
    }
    let remote_after = sqlx::query_as(
        "SELECT to_jsonb(e), to_jsonb(s), to_jsonb(l) FROM core.events e
         JOIN core.search_text s ON s.event_id = e.id
         JOIN core.live_locations l ON l.event_id = e.id WHERE e.id = $1",
    )
    .bind(remote_id)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    assert_eq!(remote_before, remote_after);
    let (_, after) = call_unbounded(&w, Some(OPERATOR), preview).await;
    assert_eq!(after["outcome"], "ok", "{after}");
    assert_eq!(after["result"]["matching_readings"], 0);
}

use super::*;
use crate::{AdmissionContext, NoDerivations};
use sinex_model::{
    Batch, BatchKind, EventPayload, RecordOutcome, RunId, event_types::operator::LogEntry,
};

async fn fixture(conn: &mut AdmissionContext) -> Vec<EventId> {
    let mut ids = Vec::new();
    for index in 0..6_u64 {
        let mut event = Event::build(
            &LogEntry {
                written_at: (index != 5)
                    .then(|| Timestamp::from_unix_secs(1_800_000_000 + index as i64).unwrap()),
                label: Some(if index % 2 == 0 { "selected" } else { "other" }.into()),
                text: format!("payload-{index}"),
            },
            "fixture.page-fetch",
            Origin::Record {
                location: RecordLocation {
                    material: MaterialId::new(),
                    record_offset: index,
                },
                output_index: 0,
            },
            ReadingOrder(1),
            [index as u8; 32],
            Class::Open,
        )
        .unwrap();
        let millis = 1_800_000_000_000_u128 + u128::from(index / 2) * 86_400_000;
        event.id = EventId(Uuid::from_u128(
            (millis << 80) | (7 << 76) | (2 << 62) | u128::from(index + 1),
        ));
        ids.push(event.id);
        let mut tx = conn.begin().await.unwrap();
        tx.admit(
            &Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "fixture".into(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events: vec![event],
                }],
                archive: vec![],
            },
            &NoDerivations,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    crate::lifecycle::move_events(
        conn,
        sinex_model::lifecycle::Act::Archive,
        vec![ids[2]],
        &NoDerivations,
    )
    .await;
    ids
}
fn window() -> Window {
    Window {
        range: None,
        unknown_times: false,
        intervals: vec![],
        kinds: vec![Kind {
            event_type: LogEntry::EVENT_TYPE.into(),
            contains: serde_json::json!({"label":"selected"}),
        }],
        start: Some(OffsetDateTime::UNIX_EPOCH),
        end: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::days(100_000)),
        before: vec![],
        after: vec![],
    }
}
fn identities(rows: &[Committed]) -> Vec<(EventId, Value)> {
    rows.iter()
        .map(|row| (row.event.id, row.event.payload.clone()))
        .collect()
}

#[devkit::test]
async fn bounded_payload_pages_equal_window_and_keep_exact_membership(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let ids = fixture(&mut conn).await;
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    // Dense and sparse pages agree with the existing whole-window query,
    // including when several selected IDs share a compressed chunk.
    for contains in [
        serde_json::json!({}),
        serde_json::json!({"label":"selected"}),
    ] {
        let mut w = window();
        w.kinds[0].contains = contains;
        let expected = read_window(&mut conn, &w).await.unwrap();
        let mut actual = Vec::new();
        let mut after = None;
        loop {
            let (rows, _) = window_page(&mut conn, &w, after, 3, i64::MAX)
                .await
                .unwrap();
            if rows.is_empty() {
                break;
            }
            let last = rows.last().unwrap();
            after = Some((last.event.ts_orig.at.unwrap().0, last.commit_seq));
            actual.extend(rows);
        }
        assert_eq!(identities(&actual), identities(&expected));
    }
    let mut w = window();
    // Overlapping predicates are membership tests, not duplicate-producing joins.
    w.kinds.push(w.kinds[0].clone());
    let expected = read_window(&mut conn, &w).await.unwrap();
    assert_eq!(
        expected.iter().map(|r| r.event.id).collect::<Vec<_>>(),
        vec![ids[0], ids[4]]
    );
    let mut actual = Vec::new();
    let mut after = None;
    loop {
        let (rows, _) = window_page(&mut conn, &w, after, 2, 1).await.unwrap();
        if rows.is_empty() {
            break;
        }
        assert_eq!(rows.len(), 1, "the oversized first payload travels alone");
        let last = rows.last().unwrap();
        after = Some((last.event.ts_orig.at.unwrap().0, last.commit_seq));
        actual.extend(rows);
    }
    assert_eq!(identities(&actual), identities(&expected));
    let mut grouped = Windows::new(vec![w], 2, 1);
    let result = grouped.next(&mut conn).await.unwrap().unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(identities(&result[0].1), identities(&expected));
    assert!(grouped.next(&mut conn).await.unwrap().is_none());
    let mut listed =
        ListedInputs::new(vec![ids[4], ids[0], ids[0], ids[2], EventId::new()], 2, 1).unwrap();
    let mut listed_rows = Vec::new();
    while let Some(rows) = listed.next(&mut conn).await.unwrap() {
        listed_rows.extend(rows);
    }
    assert_eq!(identities(&listed_rows), identities(&expected));
    let feed = after_seq(&mut conn, &[LogEntry::EVENT_TYPE.into()], 0, 10, i64::MAX)
        .await
        .unwrap();
    assert_eq!(
        feed.iter().map(|r| r.event.id).collect::<Vec<_>>(),
        vec![ids[0], ids[1], ids[3], ids[4], ids[5]]
    );
    let page = crate::commit_log::after(&mut conn, 0, 100, i64::MAX)
        .await
        .unwrap();
    assert_eq!(
        page.events
            .iter()
            .map(|r| EventId(r.id))
            .collect::<Vec<_>>(),
        ids
    );
    let marker = crate::commit_log::after(&mut conn, 6, 1, i64::MAX)
        .await
        .unwrap();
    assert!(marker.events.is_empty());
    assert_eq!(
        marker.up_to,
        Some(7),
        "an all-NULL event-ID page still advances"
    );
}

/// Selected rows share a compressed chunk with many later rows of the same
/// kind; the bounded fetches still return exactly the selected rows.
#[devkit::test]
async fn bounded_payload_fetches_return_selected_rows_beside_many_compressed_rows(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let ids = fixture(&mut conn).await;
    sqlx::query(
        "INSERT INTO core.events (id,event_type,source,run_id,material_id,record_offset,ts_orig,ts_quality,commit_seq,payload,reading_order,record_hash,kind_version,validation)
         SELECT overlay(e.id::text placing lpad(to_hex(1000+n),12,'0') from 25 for 12)::uuid,
           e.event_type,e.source,e.run_id,uuidv7(),0,e.ts_orig,e.ts_quality,1000+n,e.payload,1,e.record_hash,e.kind_version,e.validation
         FROM core.events e CROSS JOIN generate_series(1,12000) n WHERE e.id=$1",
    )
    .bind(ids[5].0)
    .execute(&mut *conn)
    .await
    .unwrap();
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    let feed = after_seq(&mut conn, &[LogEntry::EVENT_TYPE.into()], 4, 2, i64::MAX)
        .await
        .unwrap();
    assert_eq!(
        feed.iter().map(|r| r.event.id).collect::<Vec<_>>(),
        vec![ids[4], ids[5]]
    );
    let mut listed = ListedInputs::new(vec![ids[4], ids[5]], 2, i64::MAX).unwrap();
    let mut listed_rows = Vec::new();
    while let Some(rows) = listed.next(&mut conn).await.unwrap() {
        listed_rows.extend(rows);
    }
    assert_eq!(
        listed_rows.iter().map(|r| r.event.id).collect::<Vec<_>>(),
        vec![ids[4], ids[5]]
    );
    let page = crate::commit_log::after(&mut conn, 4, 2, i64::MAX)
        .await
        .unwrap();
    assert_eq!(
        page.events
            .iter()
            .map(|r| EventId(r.id))
            .collect::<Vec<_>>(),
        vec![ids[4], ids[5]]
    );
}

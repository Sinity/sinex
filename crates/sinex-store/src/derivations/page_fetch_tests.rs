use super::*;
use crate::{AdmissionContext, NoDerivations};
use sinex_model::{Batch, BatchKind, RecordOutcome, RunId, event_types::operator::LogEntry};

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
                    anchor: index,
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
            event_type: "operator_log.entry".into(),
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
    let mut exact =
        ExactInputs::new(vec![ids[4], ids[0], ids[0], ids[2], EventId::new()], 2, 1).unwrap();
    let mut exact_rows = Vec::new();
    while let Some(rows) = exact.next(&mut conn).await.unwrap() {
        exact_rows.extend(rows);
    }
    assert_eq!(identities(&exact_rows), identities(&expected));
    let feed = after_seq(&mut conn, &["operator_log.entry".into()], 0, 10, i64::MAX)
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
}

fn payload_lookup_is_bounded(plan: &Value) {
    fn walk(node: &Value, excluding: bool, hashed: bool, payloads: &mut usize) {
        let kind = node["Node Type"].as_str().unwrap_or("");
        // InitPlans are independent executions attached to the outer node.
        // Its bounded metadata join cannot make the payload CTE unbounded.
        let independent = node["Parent Relationship"].as_str() == Some("InitPlan")
            || node["Subplan Name"].as_str() == Some("CTE payload");
        let excluding = (!independent && excluding)
            || node["Runtime Exclusion"].as_bool() == Some(true)
            || node["Startup Exclusion"].as_bool() == Some(true);
        let hashed = (!independent && hashed) || kind == "Hash Join" || kind == "Merge Join";
        let alias = node["Alias"].as_str().unwrap_or("");
        if alias.starts_with("payload_event") {
            *payloads += 1;
            assert!(
                excluding && !hashed,
                "payload scan must be an excluding selected-ID fetch: {node}"
            );
            let condition = ["Filter", "Index Cond", "Recheck Cond", "Vectorized Filter"]
                .iter()
                .map(|key| node[*key].to_string())
                .collect::<Vec<_>>()
                .join(" ");
            assert!(
                condition.contains("ANY"),
                "payload scan lost its selected IDs: {node}"
            );
        }
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                walk(child, excluding, hashed, payloads);
            }
        }
    }
    let mut payloads = 0;
    walk(&plan[0]["Plan"], false, false, &mut payloads);
    assert!(
        payloads > 0,
        "plan must exercise the payload relation: {plan}"
    );
}

#[devkit::test]
async fn bounded_payload_queries_keep_excluding_batch_plans_on_compressed_history(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let ids = fixture(&mut conn).await;
    sqlx::query("SELECT compress_chunk(c) FROM show_chunks('core.events') c")
        .execute(&mut *conn)
        .await
        .unwrap();
    // Prefer hash joins wherever legal. Joining the bounded materialized
    // payload is valid; the payload scan itself must retain selected-ID pruning.
    sqlx::query("SET enable_nestloop=off")
        .execute(&mut *conn)
        .await
        .unwrap();
    macro_rules! explain {
        ($file:literal $(, $arg:expr)* $(,)?) => {{
            let statement = format!("EXPLAIN (FORMAT JSON) {}", include_str!($file));
            sqlx::query_scalar::<_, Value>(&statement)$(.bind($arg))*
                .fetch_one(&mut *conn).await.unwrap()
        }};
    }
    let types = vec!["operator_log.entry".to_owned()];
    let contains = vec![serde_json::json!({"label":"selected"})];
    let w = window();
    let raw_ids: Vec<_> = ids.iter().map(|id| id.0).collect();
    let plans = [
        explain!(
            "sql/window_page.sql",
            &types,
            w.start,
            w.end,
            &contains,
            None::<OffsetDateTime>,
            None::<i64>,
            2_i64,
            1_i64
        ),
        explain!(
            "sql/value_page.sql",
            vec!["selected".to_owned()],
            "label",
            &types,
            w.start.unwrap(),
            w.end.unwrap(),
            None::<String>,
            None::<OffsetDateTime>,
            None::<i64>,
            2_i64,
            1_i64
        ),
        explain!("sql/after_seq.sql", &types, 0_i64, 2_i64, 1_i64),
        explain!(
            "sql/exact_page.sql",
            &raw_ids,
            0_i64,
            2_i64,
            1_i64,
            None::<Uuid>
        ),
        explain!("../commit_log/page.sql", 0_i64, 2_i64, 1_i64),
    ];
    for plan in plans {
        payload_lookup_is_bounded(&plan);
    }
}

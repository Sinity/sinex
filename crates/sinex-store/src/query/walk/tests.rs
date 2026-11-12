use super::*;
use crate::query::{Answer, Page};
use crate::{AdmissionContext, NoDerivations};
use sinex_model::event_types::{CommandFused, CommandImported};
use sinex_model::lifecycle::Act;
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};
use std::collections::BTreeSet;

fn source(class: Class) -> Event {
    Event::build(
        &CommandImported {
            command: "synthetic lineage root".into(),
            hostname: "fixture".into(),
            started_at: None,
            header_elapsed: None,
            paths: vec![],
            n: 0,
        },
        "fixture",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [1; 32],
        class,
    )
    .unwrap()
}

fn derived(parents: &[EventId], name: &str) -> Event {
    Event::build(
        &CommandFused {
            record: name.into(),
            command: "synthetic derived reading".into(),
            host: "fixture".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            execution: None,
            members: vec![sinex_model::event_types::fused::Member {
                record: format!("fixture:{}", "fixture"),
                source: "fixture".into(),
                event_type: "command.executed".into(),
                joined: sinex_model::event_types::fused::Joined::Lead,
            }],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        },
        "fixture.derived",
        Origin::Derived {
            parents: parents.to_vec(),
        },
        ReadingOrder(1),
        *blake3::hash(name.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "test".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

async fn insert(conn: &mut AdmissionContext, event: Event) {
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        tx.admit(&batch(vec![event]), &NoDerivations)
            .await
            .unwrap()
            .admitted,
        1
    );
    tx.commit().await.unwrap();
}

async fn page(conn: &mut PgConnection, text: &str, size: u64, cursor: Option<String>) -> Answer {
    let context = sinex_model::query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let operation = sinex_model::query::native::parse(text, &context).unwrap();
    crate::query::run_operation(
        conn,
        text,
        &operation,
        &context,
        &Page {
            size: Some(size),
            cursor,
        },
    )
    .await
    .unwrap()
}

async fn all_pages(conn: &mut PgConnection, text: &str) -> Vec<Json> {
    let mut rows = vec![];
    let mut cursor = None;
    loop {
        let answer = page(conn, text, 19, cursor).await;
        rows.extend(answer.rows);
        cursor = answer.page.cursor;
        if cursor.is_none() {
            assert_eq!(i64::try_from(rows.len()).unwrap(), answer.total);
            break;
        }
    }
    rows
}

fn depths(rows: &[Json]) -> BTreeMap<Uuid, u64> {
    let result: BTreeMap<_, _> = rows
        .iter()
        .map(|row| {
            (
                row["id"].as_str().unwrap().parse().unwrap(),
                row["depth"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        result.len(),
        rows.len(),
        "every reachable event appears once"
    );
    result
}

#[devkit::test]
async fn no_depth_caps_complete_deep_branches_through_continuation(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let root = source(Class::Open);
    let mut chain = vec![root.id];
    insert(&mut conn, root).await;
    for index in 1..=128 {
        let event = derived(&[*chain.last().unwrap()], &format!("chain-{index}"));
        chain.push(event.id);
        insert(&mut conn, event).await;
    }
    let branch = derived(&[chain[0]], "branch");
    let branch_id = branch.id;
    insert(&mut conn, branch).await;
    let join = derived(&[*chain.last().unwrap(), branch_id], "join");
    let join_id = join.id;
    insert(&mut conn, join).await;
    let up = all_pages(&mut conn, &format!("trace {}", join_id.0)).await;
    let actual = depths(&up);
    assert_eq!(actual.len(), 131);
    assert_eq!(actual[&join_id.0], 0);
    assert_eq!(actual[&branch_id.0], 1);
    assert_eq!(actual[&chain[0].0], 2);
    for (index, id) in chain.iter().enumerate().skip(1) {
        assert_eq!(actual[&id.0], u64::try_from(129 - index).unwrap());
    }
    assert_eq!(actual.values().copied().max(), Some(128));
    let down = all_pages(&mut conn, &format!("descendants {}", chain[0].0)).await;
    let actual = depths(&down);
    assert_eq!(actual.len(), 130);
    assert!(!actual.contains_key(&chain[0].0));
    assert_eq!(actual[&branch_id.0], 1);
    assert_eq!(actual[&join_id.0], 2);
    for (index, id) in chain.iter().enumerate().skip(1) {
        assert_eq!(actual[&id.0], u64::try_from(index).unwrap());
    }
    let parents = walk(&mut conn, Walk::Parents, &Reference::Event(join_id.0))
        .await
        .unwrap();
    assert_eq!(
        depths(&parents).keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([chain[128].0, branch_id.0])
    );
}

#[devkit::test]
async fn retained_edge_cycles_terminate_with_shortest_depths(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut ids = vec![];
    for _ in 0..4 {
        let event = source(Class::Open);
        ids.push(event.id.0);
        insert(&mut conn, event).await;
    }
    // Retained historical edges can be malformed. Walking them must terminate
    // without inventing a depth limit or repeating a vertex for every path.
    for (parent, child) in [
        (ids[0], ids[1]),
        (ids[1], ids[2]),
        (ids[2], ids[0]),
        (ids[0], ids[3]),
        (ids[2], ids[3]),
    ] {
        sqlx::query("INSERT INTO core.event_children(parent,child) VALUES($1,$2)")
            .bind(parent)
            .bind(child)
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let self_loop = sqlx::query("INSERT INTO core.event_children(parent,child) VALUES($1,$1)")
        .bind(ids[3])
        .execute(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(
        self_loop
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23514")
    );
    let up = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        walk(&mut conn, Walk::Trace, &Reference::Event(ids[3])),
    )
    .await
    .expect("a finite cyclic graph must terminate")
    .unwrap();
    assert_eq!(
        depths(&up),
        BTreeMap::from([(ids[3], 0), (ids[0], 1), (ids[2], 1), (ids[1], 2)])
    );
    let down = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        walk(&mut conn, Walk::Descendants, &Reference::Event(ids[0])),
    )
    .await
    .expect("a finite cyclic graph must terminate")
    .unwrap();
    assert_eq!(
        depths(&down),
        BTreeMap::from([(ids[1], 1), (ids[2], 2), (ids[3], 1)])
    );
    assert_eq!(
        walk(&mut conn, Walk::Trace, &Reference::Event(ids[3]))
            .await
            .unwrap(),
        up
    );
}

#[devkit::test]
async fn traces_name_masked_archived_missing_and_tombstoned_links(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let root = source(Class::Guarded);
    let root_id = root.id;
    insert(&mut conn, root).await;
    let child = derived(&[root_id], "guarded child");
    let child_id = child.id;
    insert(&mut conn, child).await;
    let leaf = derived(&[child_id], "guarded leaf");
    let leaf_id = leaf.id;
    insert(&mut conn, leaf).await;
    let reference = Reference::Event(leaf_id.0);
    let masked = walk(&mut conn, Walk::Trace, &reference).await.unwrap();
    assert_eq!(masked.len(), 3);
    assert!(
        masked
            .iter()
            .all(|row| row["payload"] == json!({"withheld":"guarded"})
                && row["withheld"] == "guarded")
    );
    assert_eq!(masked[2]["bytes"], "unavailable: material record missing");
    sqlx::query("SELECT set_config('sinex.grants','guarded',false)")
        .execute(&mut conn)
        .await
        .unwrap();
    let allowed = walk(&mut conn, Walk::Trace, &reference).await.unwrap();
    assert!(
        allowed
            .iter()
            .all(|row| row["payload"]["command"].is_string() && row["withheld"].is_null())
    );
    sqlx::query("SELECT set_config('sinex.grants','',false)")
        .execute(&mut conn)
        .await
        .unwrap();
    crate::lifecycle::move_events(&mut conn, Act::Archive, vec![child_id], &NoDerivations).await;
    let archived = walk(&mut conn, Walk::Trace, &reference).await.unwrap();
    assert_eq!(archived[0]["state"], "archived");
    assert_eq!(archived[1]["state"], "archived");
    assert_eq!(archived[2]["state"], "live");
    // The retained skeleton and edges outlive tombstoned readings.
    let tombstoned =
        crate::lifecycle::move_events(&mut conn, Act::Tombstone, vec![child_id], &NoDerivations)
            .await;
    assert!(
        matches!(tombstoned, crate::lifecycle::Ended::Ok { .. }),
        "{tombstoned:?}"
    );
    let missing = Uuid::now_v7();
    sqlx::query("INSERT INTO core.event_children(parent,child) VALUES($1,$2)")
        .bind(missing)
        .bind(child_id.0)
        .execute(&mut conn)
        .await
        .unwrap();
    let rows = walk(&mut conn, Walk::Trace, &reference).await.unwrap();
    let by_id: BTreeMap<_, _> = rows
        .iter()
        .map(|row| (row["id"].as_str().unwrap().parse::<Uuid>().unwrap(), row))
        .collect();
    assert_eq!(by_id.len(), 4);
    assert_eq!(by_id[&missing]["state"], "unavailable");
    assert_eq!(by_id[&missing]["unavailable"], "event record missing");
    for id in [child_id.0, leaf_id.0] {
        assert_eq!(by_id[&id]["state"], "tombstoned");
        assert!(by_id[&id]["payload"].is_null());
        assert!(by_id[&id]["tombstone_operation"].is_string());
    }
    assert_eq!(by_id[&root_id.0]["payload"], json!({"withheld":"guarded"}));
    let down = walk(&mut conn, Walk::Descendants, &Reference::Event(root_id.0))
        .await
        .unwrap();
    assert_eq!(down.len(), 2);
    assert!(down.iter().all(|row| row["state"] == "tombstoned"));
    assert_eq!(
        show(&mut conn, &reference).await.unwrap()["state"],
        "tombstoned"
    );
}

#[devkit::test]
async fn trace_reads_selected_events_in_unrelated_history(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let root = source(Class::Open);
    let root_id = root.id;
    insert(&mut conn, root).await;
    let child = derived(&[root_id], "selected child");
    let child_id = child.id;
    insert(&mut conn, child).await;
    let leaf = derived(&[child_id], "selected leaf");
    let leaf_id = leaf.id;
    insert(&mut conn, leaf).await;
    let unrelated_root = source(Class::Open);
    let unrelated_id = unrelated_root.id;
    insert(&mut conn, unrelated_root).await;
    let unrelated: Vec<_> = (0..8192)
        .map(|index| derived(&[unrelated_id], &format!("unrelated-{index}")))
        .collect();
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        tx.admit(&batch(unrelated), &NoDerivations)
            .await
            .unwrap()
            .admitted,
        8192
    );
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        8196
    );
    sqlx::raw_sql("ANALYZE core.events; ANALYZE core.event_children")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    // Keep all tuple counters in this backend, and exercise broad joins where
    // they are legal even when the fixture is smaller than a captured history.
    sqlx::raw_sql("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_nestloop=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN (SELECT show_chunks('core.events'))";
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let rows = walk(&mut tx, Walk::Trace, &Reference::Event(leaf_id.0))
        .await
        .unwrap();
    assert_eq!(
        depths(&rows),
        BTreeMap::from([(leaf_id.0, 0), (child_id.0, 1), (root_id.0, 2)])
    );
    assert!(rows.iter().all(|row| row["payload"]["command"].is_string()));
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before <= 4 * rows.len() as i64,
        "three selected readings visited {} event tuples",
        after - before
    );

    // The original join must demonstrate the unrelated-history work that the
    // production query avoids; answer equality alone cannot catch that cost.
    let selected = json!([
        {"id": leaf_id.0, "depth": 0},
        {"id": child_id.0, "depth": 1},
        {"id": root_id.0, "depth": 2}
    ]);
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let original: Vec<Uuid> = sqlx::query_scalar(
        "WITH selected AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS r(id uuid, depth numeric)) SELECT e.id FROM selected f LEFT JOIN core.visible_events e ON e.id=f.id"
    ).bind(selected).fetch_all(&mut *tx).await.unwrap();
    assert_eq!(
        original.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([leaf_id.0, child_id.0, root_id.0])
    );
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before >= 8192,
        "original selected join visited only {} event tuples",
        after - before
    );
    tx.rollback().await.unwrap();
}

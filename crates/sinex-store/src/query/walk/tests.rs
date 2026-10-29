use super::*;
use crate::query::{Answer, Page};
use crate::{AdmissionContext, NoDerivations};
use sinex_model::event_types::{CommandFused, CommandImported};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, EventId, MaterialId, OperationId, Origin,
    ReadingOrder, RecordLocation, RecordOutcome, RunId, Timestamp,
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
            observed_by: vec!["fixture".into()],
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
    crate::query::run(
        conn,
        text,
        None,
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
    let hide = Batch {
        archive: vec![ArchiveRequest::Hide {
            target: child_id,
            operation: OperationId::new(),
        }],
        ..batch(vec![])
    };
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&hide, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    let archived = walk(&mut conn, Walk::Trace, &reference).await.unwrap();
    assert_eq!(archived[0]["state"], "archived");
    assert_eq!(archived[1]["state"], "archived");
    assert_eq!(archived[2]["state"], "live");
    // The retained skeleton and edges outlive cleared readings. This fixture
    // isolates the reader from material deletion and writer custody mechanics.
    let gone = vec![child_id.0, leaf_id.0];
    let mut tx = conn.begin().await.unwrap();
    let position = crate::commit_log::allocate_position(&mut tx).await.unwrap();
    let operation = OperationId::new();
    sqlx::query("INSERT INTO core.event_tombstones(event_id,source,event_type,ts_orig,operation_id,commit_seq,original_commit_seq,class) SELECT id,source,event_type,ts_orig,$2,$3,commit_seq,class FROM core.events WHERE id=ANY($1)")
        .bind(&gone).bind(operation.0).bind(position).execute(&mut *tx).await.unwrap();
    sqlx::query("SELECT set_config('sinex.excise_operation',$1,true)")
        .bind(operation.0.to_string())
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM core.events WHERE id=ANY($1)")
        .bind(&gone)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
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

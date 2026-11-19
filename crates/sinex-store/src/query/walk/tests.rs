use super::*;
use crate::query::Page;
use crate::{AdmissionContext, NoDerivations};
use sinex_model::event_types::{CommandFused, CommandImported};
use sinex_model::lifecycle::Act;
use sinex_model::query_reply::QueryPage;
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};
use std::collections::BTreeSet;

async fn current_walk(
    conn: &mut PgConnection,
    direction: Walk,
    reference: &Reference,
) -> Result<Vec<Json>> {
    let position = crate::query::current_seq(conn).await?;
    walk_at(conn, direction, reference, position, position).await
}

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
                record_offset: 0,
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

async fn page(conn: &mut PgConnection, text: &str, size: u64, cursor: Option<String>) -> QueryPage {
    let context = sinex_query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let operation = sinex_query::template::parse(text, &context).unwrap();
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
    let parents = current_walk(&mut conn, Walk::Parents, &Reference::Event(join_id.0))
        .await
        .unwrap();
    assert_eq!(
        depths(&parents).keys().copied().collect::<BTreeSet<_>>(),
        BTreeSet::from([chain[128].0, branch_id.0])
    );
}

#[devkit::test]
async fn stored_edge_cycles_terminate_with_shortest_depths(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let mut ids = vec![];
    for _ in 0..4 {
        let event = source(Class::Open);
        ids.push(event.id.0);
        insert(&mut conn, event).await;
    }
    // Stored historical edges can be malformed. Walking them must terminate
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
        current_walk(&mut conn, Walk::Trace, &Reference::Event(ids[3])),
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
        current_walk(&mut conn, Walk::Descendants, &Reference::Event(ids[0])),
    )
    .await
    .expect("a finite cyclic graph must terminate")
    .unwrap();
    assert_eq!(
        depths(&down),
        BTreeMap::from([(ids[1], 1), (ids[2], 2), (ids[3], 1)])
    );
    assert_eq!(
        current_walk(&mut conn, Walk::Trace, &Reference::Event(ids[3]))
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
    let masked = current_walk(&mut conn, Walk::Trace, &reference)
        .await
        .unwrap();
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
    let allowed = current_walk(&mut conn, Walk::Trace, &reference)
        .await
        .unwrap();
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
    let archived = current_walk(&mut conn, Walk::Trace, &reference)
        .await
        .unwrap();
    assert_eq!(archived[0]["state"], "archived");
    assert_eq!(archived[1]["state"], "archived");
    assert_eq!(archived[2]["state"], "live");
    // The kept skeleton and edges outlive tombstoned readings.
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
    let rows = current_walk(&mut conn, Walk::Trace, &reference)
        .await
        .unwrap();
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
    let down = current_walk(&mut conn, Walk::Descendants, &Reference::Event(root_id.0))
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
async fn trace_and_show_read_selected_events_in_compressed_history(db: devkit::pg::TestDatabase) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let root = source(Class::Open);
    let root_id = root.id;
    let Origin::Record { location, .. } = &root.origin else {
        unreachable!("source fixture has a material")
    };
    let material = location.material;
    insert(&mut conn, root).await;
    let child = derived(&[root_id], "selected child");
    let child_id = child.id;
    let child_lookup = (
        child.source.clone(),
        child.event_type.clone(),
        child.payload["record"].clone(),
        child.payload.clone(),
    );
    insert(&mut conn, child).await;
    let leaf = derived(&[child_id], "selected leaf");
    let leaf_id = leaf.id;
    insert(&mut conn, leaf).await;
    let unrelated_root = source(Class::Open);
    let unrelated_id = unrelated_root.id;
    insert(&mut conn, unrelated_root).await;
    let (source, kind, key, payload) = &child_lookup;
    sqlx::query("INSERT INTO derivation.outputs (derivation, key, event_id) VALUES ($1, $2, $3)")
        .bind(source)
        .bind(key)
        .bind(child_id.0)
        .execute(&mut *conn)
        .await
        .unwrap();
    assert_eq!(
        crate::payloads::live_derived(&mut conn, source, key, kind)
            .await
            .unwrap(),
        [payload.clone()]
    );
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
        sqlx::query(
            "INSERT INTO derivation.outputs (derivation, key, event_id)
             SELECT e.source, e.payload->'record', e.id FROM core.events e
             WHERE e.source=$1 AND e.event_type=$2 AND e.id<>$3",
        )
        .bind(source)
        .bind(kind)
        .bind(child_id.0)
        .execute(&mut *conn)
        .await
        .unwrap()
        .rows_affected(),
        8193
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.events")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        8196
    );
    let blob = format!("blake3:{}", "01".repeat(32));
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES ($1,0,'present')")
        .bind(&blob)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version)
         SELECT $1::uuid,$2,0,'{}'::jsonb,'fixture-parser',1
         UNION ALL
         SELECT uuidv7(),$2,0,'{}'::jsonb,'fixture-parser',1 FROM generate_series(1,4096)",
    )
    .bind(material.0)
    .bind(&blob)
    .execute(&mut *conn)
    .await
    .unwrap();
    let compressed: i64 =
        sqlx::query_scalar("SELECT count(compress_chunk(c)) FROM show_chunks('core.events') AS c")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(compressed > 0);
    sqlx::raw_sql("ANALYZE core.events; ANALYZE raw.materials; ANALYZE derivation.outputs")
        .execute(&mut *conn)
        .await
        .unwrap();

    let rows = current_walk(&mut conn, Walk::Trace, &Reference::Event(leaf_id.0))
        .await
        .unwrap();
    assert_eq!(
        depths(&rows),
        BTreeMap::from([(leaf_id.0, 0), (child_id.0, 1), (root_id.0, 2)])
    );
    assert!(rows.iter().all(|row| row["payload"]["command"].is_string()));

    let shown = show(&mut conn, &Reference::Event(root_id.0)).await.unwrap();
    assert_eq!(shown["id"], json!(root_id));
    assert_eq!(shown["state"], "live");
    assert_eq!(shown["material"], json!(material));
    assert_eq!(shown["blob"], blob);
    assert_eq!(shown["parser"], "fixture-parser");
    assert_eq!(shown["parser_version"], 1);
    let without_material = show(&mut conn, &Reference::Event(unrelated_id.0))
        .await
        .unwrap();
    assert_eq!(without_material["id"], json!(unrelated_id));
    assert!(without_material["material"].is_string());
    assert!(without_material["parser"].is_null());
    assert!(without_material["blob"].is_null());
    assert!(matches!(
        show(&mut conn, &Reference::Event(EventId::new().0)).await,
        Err(Error::NotFound(_))
    ));

    // Use the production SQL and count real plan work on this compressed fixture.
    let sql = include_str!("../read_events.sql");
    let explain: Json =
        sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {sql}"))
            .bind(&[root_id.0][..])
            .bind(None::<i64>)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    fn collect_nodes<'a>(node: &'a Json, nodes: &mut Vec<&'a Json>) {
        nodes.push(node);
        if let Some(children) = node["Plans"].as_array() {
            for child in children {
                collect_nodes(child, nodes);
            }
        }
    }
    let mut nodes = Vec::new();
    collect_nodes(&explain[0]["Plan"], &mut nodes);
    assert!(nodes.iter().any(|node| node["Node Type"] == "Limit"
        && node["Actual Rows"].as_f64() == Some(1.0)
        && node["Actual Loops"].as_f64() == Some(1.0)));
    let material_scan = nodes
        .into_iter()
        .find(|node| node["Relation Name"] == "materials")
        .expect("the record has material metadata");
    assert_eq!(material_scan["Index Name"], "materials_pkey");
    assert_eq!(material_scan["Actual Rows"].as_f64(), Some(1.0));
    assert_eq!(material_scan["Actual Loops"].as_f64(), Some(1.0));

    // All 8,192 new readings share the selected source and kind. Their keys
    // must not add payload work to one exact-key read in compressed history.
    assert_eq!(
        crate::payloads::live_derived(&mut conn, source, key, kind)
            .await
            .unwrap(),
        [payload.clone()]
    );
    assert!(
        crate::payloads::live_derived(&mut conn, source, &json!("absent"), kind)
            .await
            .unwrap()
            .is_empty()
    );
    let sql = include_str!("../../payloads/live_derived.sql");
    let explain: Json =
        sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) {sql}"))
            .bind(source)
            .bind(key)
            .bind(kind)
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    let mut nodes = Vec::new();
    collect_nodes(&explain[0]["Plan"], &mut nodes);
    let lookup = nodes
        .iter()
        .find(|node| node["Index Name"] == "outputs_pkey")
        .expect("the lookup uses the complete derivation key");
    assert_eq!(lookup["Actual Rows"].as_f64(), Some(1.0));
    assert_eq!(lookup["Actual Loops"].as_f64(), Some(1.0));
    assert!(nodes.iter().any(|node| node["Node Type"] == "Limit"
        && node["Actual Rows"].as_f64() == Some(1.0)
        && node["Actual Loops"].as_f64() == Some(1.0)));
    assert!(
        nodes
            .iter()
            .any(|node| node["Index Name"] == "live_equivalence_event"
                && node["Actual Rows"].as_f64() == Some(1.0)
                && node["Actual Loops"].as_f64() == Some(1.0))
    );
}

#[devkit::test]
async fn historical_walk_excludes_a_later_child_and_keeps_prior_archive_state(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let root = source(Class::Open);
    let root_id = root.id;
    insert(&mut conn, root).await;
    let early = derived(&[root_id], "before-selection");
    let early_id = early.id;
    insert(&mut conn, early).await;
    let cut = crate::query::current_seq(&mut conn).await.unwrap();
    let late = derived(&[root_id], "after-selection");
    let late_id = late.id;
    insert(&mut conn, late).await;
    let moved =
        crate::lifecycle::move_events(&mut conn, Act::Archive, vec![early_id], &NoDerivations)
            .await;
    assert!(
        matches!(moved, crate::lifecycle::Ended::Ok { .. }),
        "{moved:?}"
    );
    let context = sinex_query::Context::utc("2026-09-15T00:00:00Z".parse().unwrap());
    let text = format!("descendants sinex:ev/{}", root_id.0);
    let past = crate::query::read_at(&mut conn, &text, &context, &Page::default(), cut)
        .await
        .unwrap();
    assert_eq!(past.resolved.commit_seq, cut);
    assert_eq!(
        past.rows
            .iter()
            .map(|row| row["id"].as_str().unwrap().to_owned())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([early_id.0.to_string()])
    );
    assert!(past.rows.iter().all(|row| row["state"] == "live"));
    let full = read_events(&mut conn, &[early_id.0, root_id.0, early_id.0], Some(cut))
        .await
        .unwrap();
    assert_eq!(
        full.iter()
            .map(|row| row["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            early_id.0.to_string(),
            root_id.0.to_string(),
            early_id.0.to_string()
        ]
    );
    assert!(full.iter().all(|row| row["state"] == "live"));
    assert!(
        read_events(&mut conn, &[late_id.0], Some(cut))
            .await
            .is_err()
    );
    let current = current_walk(&mut conn, Walk::Descendants, &Reference::Event(root_id.0))
        .await
        .unwrap();
    assert!(current.iter().any(|row| row["id"] == late_id.0.to_string()));
    assert!(
        current
            .iter()
            .any(|row| row["id"] == early_id.0.to_string() && row["state"] == "archived")
    );
}

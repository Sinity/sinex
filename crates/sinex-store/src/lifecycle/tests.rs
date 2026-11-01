//! The lifecycle's laws against a real database: whole chains move, no
//! event's state is earlier than its parents', restore refuses while another
//! reading of the record is live, refusal is per batch and resuming reaches
//! the uninterrupted result, tombstones are reached only from the archive and
//! stop no new readings, undo swaps forever, and material status cascades.
use super::*;
use crate::AdmissionContext;
use crate::admission::{KeyInput, NoDerivations};
use serde_json::{Value, json};
use sinex_model::event_types::CommandExecuted;
use sinex_model::lifecycle::{Act, MaterialStatus, Scope};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder,
    RecordLocation, RecordOutcome, ReplayScope, RunId, Timestamp,
};

fn reading(source: &str, key: u8, content: u8, material: MaterialId, anchor: u64) -> Event {
    let command = format!("cmd-{content}");
    Event::build(
        &CommandExecuted {
            native_id: format!("k{key}"),
            command: command.clone(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000 + i64::from(key)).unwrap(),
            duration_ns: Some(1),
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        source,
        Origin::Record {
            location: RecordLocation { material, anchor },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(command.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn derived(source: &str, key: u8, parents: &[&Event]) -> Event {
    let mut event = reading(source, key, key, MaterialId::new(), 0);
    event.origin = Origin::Derived {
        parents: parents.iter().map(|p| p.id).collect(),
    };
    event
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

async fn context(db: &devkit::pg::TestDatabase) -> AdmissionContext {
    AdmissionContext::connect(&db.url(None)).await.unwrap()
}

async fn admit(conn: &mut AdmissionContext, b: &Batch) {
    let mut tx = conn.begin().await.unwrap();
    tx.admit(b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

/// A stored material holding `blob`, with no readings yet.
async fn material(conn: &mut PgConnection, blob: &str) -> MaterialId {
    let id = MaterialId::new();
    sqlx::query("INSERT INTO core.blobs (hash, size, status) VALUES ($1, 3, 'present') ON CONFLICT DO NOTHING")
        .bind(blob)
        .execute(&mut *conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO raw.materials (id, blob, length, manifest, parser, parser_version) VALUES ($1, $2, 3, '{}', 'test', 1)")
        .bind(id.0)
        .bind(blob)
        .execute(&mut *conn)
        .await
        .unwrap();
    id
}

fn blob(n: u8) -> String {
    format!("blake3:{}", format!("{n:02x}").repeat(32))
}

async fn operate(conn: &mut PgConnection, act: Act, scope: Scope, roots: i64) -> Ended {
    let op = begin(conn, act, "agent:test", scope).await.unwrap();
    run_on(conn, &op, Progress::default(), roots, &NoDerivations)
        .await
        .unwrap()
}

fn events(list: &[&Event]) -> Scope {
    Scope::Events {
        events: list.iter().map(|e| e.id).collect(),
        query: None,
    }
}

fn operation(ended: &Ended) -> OperationId {
    match ended {
        Ended::Ok { operation, .. } | Ended::StoppedEarly { operation, .. } => *operation,
        Ended::Refused(refusals) => panic!("refused: {refusals:?}"),
    }
}

async fn state(conn: &mut PgConnection, event: &Event) -> &'static str {
    let row: (bool, bool, bool) = sqlx::query_as(
        "SELECT EXISTS (SELECT 1 FROM core.live_equivalence WHERE event_id = $1)
                OR EXISTS (SELECT 1 FROM core.live_locations WHERE event_id = $1),
              EXISTS (SELECT 1 FROM audit.archived WHERE event_id = $1),
              EXISTS (SELECT 1 FROM core.event_tombstones WHERE event_id = $1)",
    )
    .bind(event.id.0)
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    match row {
        (true, false, false) => "live",
        (false, true, false) => "archived",
        (false, false, true) => "tombstoned",
        other => panic!("event {} is in no single state: {other:?}", event.id),
    }
}

/// The state order law, checked over the whole store: every event's state
/// is at least each parent's (live < archived < tombstoned).
async fn assert_state_order(conn: &mut PgConnection) {
    let violations: Vec<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
        r#"WITH state AS (
             SELECT e.id, CASE
               WHEN EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id) THEN 1
               ELSE 0 END AS rank FROM core.events e
             UNION ALL SELECT t.event_id, 2 FROM core.event_tombstones t)
           SELECT c.child, c.parent FROM core.event_children c
           JOIN state p ON p.id = c.parent JOIN state s ON s.id = c.child
           WHERE s.rank < p.rank"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    assert!(violations.is_empty(), "state order broken: {violations:?}");
}

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(&mut *conn).await.unwrap()
}

#[devkit::test]
async fn archive_moves_whole_chains(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = MaterialId::new();
    let root = reading("chain", 1, 1, m, 0);
    let other = reading("chain", 2, 2, m, 1);
    admit(&mut conn, &batch(vec![root.clone(), other.clone()])).await;
    let child = derived("chain.derived", 3, &[&root]);
    admit(&mut conn, &batch(vec![child.clone()])).await;
    let grandchild = derived("chain.derived", 4, &[&child, &other]);
    admit(&mut conn, &batch(vec![grandchild.clone()])).await;

    let before = (
        count(&mut conn, "SELECT count(*) FROM audit.archive_changes").await,
        count(&mut conn, "SELECT count(*) FROM core.operations").await,
        count(
            &mut conn,
            "SELECT value FROM core.counters WHERE name = 'commit_seq'",
        )
        .await,
    );
    let op = begin(&mut conn, Act::Archive, "agent:test", events(&[&root]))
        .await
        .unwrap();
    let compared = compare(&mut conn, &op, 16).await.unwrap();
    assert_eq!(
        before,
        (
            count(&mut conn, "SELECT count(*) FROM audit.archive_changes").await,
            count(&mut conn, "SELECT count(*) FROM core.operations").await,
            count(
                &mut conn,
                "SELECT value FROM core.counters WHERE name = 'commit_seq'"
            )
            .await,
        ),
        "a comparison writes nothing"
    );
    let Ended::Ok { counts, operation } =
        run_on(&mut conn, &op, Progress::default(), 16, &NoDerivations)
            .await
            .unwrap()
    else {
        panic!("archive refused");
    };
    assert_eq!(
        counts, compared.counts,
        "the cascade equals the comparison's"
    );
    assert_eq!(
        counts.archived,
        [("command.executed".to_owned(), 3)].into(),
        "the root, its child and the grandchild the child shares with a live parent"
    );
    for (event, expected) in [
        (&root, "archived"),
        (&child, "archived"),
        (&grandchild, "archived"),
        (&other, "live"),
    ] {
        assert_eq!(state(&mut conn, event).await, expected);
    }
    let reasons: Vec<(String, Option<uuid::Uuid>)> =
        sqlx::query_as("SELECT reason, operation_id FROM audit.archived WHERE event_id = ANY($1)")
            .bind(vec![root.id.0, child.id.0, grandchild.id.0])
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert!(
        reasons
            .iter()
            .all(|r| *r == ("archive".to_owned(), Some(operation.0))),
        "cascade members carry the move's reason and operation: {reasons:?}"
    );
    assert_state_order(&mut conn).await;
}

#[devkit::test]
async fn operations_recorded_with_actor(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let e = reading("ops", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![e.clone()])).await;
    let ended = operate(&mut conn, Act::Archive, events(&[&e]), 16).await;
    let row: (String, String, Value, Option<i64>, Option<i64>, Option<String>, Value) =
        sqlx::query_as(
            "SELECT kind, actor, scope, start_seq, end_seq, result, counts FROM core.operations WHERE id = $1",
        )
        .bind(operation(&ended).0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.0, "archive");
    assert_eq!(row.1, "agent:test");
    assert_eq!(row.2, json!({"by": "events", "events": [e.id]}));
    assert!(row.3.is_some() && row.4 >= row.3, "{row:?}");
    assert_eq!(row.5.as_deref(), Some("ok"));
    assert_eq!(row.6["archived"], json!({"command.executed": 1}));
}

#[devkit::test]
async fn restore_refused_while_other_reading_live(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let first = reading("records", 7, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![first.clone()])).await;
    operate(&mut conn, Act::Archive, events(&[&first]), 16).await;
    // Archive is not hiding: the record read again is admitted.
    let second = reading("records", 7, 2, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![second.clone()])).await;
    assert_eq!(state(&mut conn, &second).await, "live");

    let refused = operate(&mut conn, Act::Restore, events(&[&first]), 16).await;
    let Ended::Refused(refusals) = refused else {
        panic!("restore over a live reading: {refused:?}");
    };
    assert_eq!(refusals[0].member, first.id.0);
    assert!(refusals[0].reason.contains(&second.id.0.to_string()));
    assert_eq!(state(&mut conn, &first).await, "archived");

    operate(&mut conn, Act::Archive, events(&[&second]), 16).await;
    operation(&operate(&mut conn, Act::Restore, events(&[&first]), 16).await);
    assert_eq!(state(&mut conn, &first).await, "live");
    assert_eq!(state(&mut conn, &second).await, "archived");
    assert_state_order(&mut conn).await;
}

#[devkit::test]
async fn restore_brings_back_the_chain_that_left_together(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let root = reading("chain", 1, 1, MaterialId::new(), 0);
    let other = reading("chain", 2, 2, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![root.clone(), other.clone()])).await;
    let child = derived("chain", 3, &[&root, &other]);
    admit(&mut conn, &batch(vec![child.clone()])).await;
    let grandchild = derived("chain", 4, &[&child]);
    admit(&mut conn, &batch(vec![grandchild.clone()])).await;

    // The chain leaves with the root; the other parent leaves on its own.
    operate(&mut conn, Act::Archive, events(&[&root]), 16).await;
    operate(&mut conn, Act::Archive, events(&[&other]), 16).await;
    let refused = operate(&mut conn, Act::Restore, events(&[&root]), 16).await;
    assert!(
        matches!(&refused, Ended::Refused(r) if r.iter().any(|r| r.member == child.id.0 && r.reason.contains("parent"))),
        "{refused:?}"
    );
    for event in [&root, &child, &grandchild] {
        assert_eq!(state(&mut conn, event).await, "archived");
    }
    operate(&mut conn, Act::Restore, events(&[&other]), 16).await;
    operate(&mut conn, Act::Restore, events(&[&root]), 16).await;
    for event in [&root, &other, &child, &grandchild] {
        assert_eq!(state(&mut conn, event).await, "live");
    }
    assert_eq!(
        count(
            &mut conn,
            "SELECT events FROM core.source_counts WHERE source = 'chain'"
        )
        .await,
        4
    );
    assert_state_order(&mut conn).await;
}

#[devkit::test]
async fn refusal_per_batch(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let readings: Vec<Event> = (1..=3)
        .map(|k| reading("batches", k, k, MaterialId::new(), 0))
        .collect();
    for r in &readings {
        admit(&mut conn, &batch(vec![r.clone()])).await;
    }
    let archived = operation(
        &operate(
            &mut conn,
            Act::Archive,
            Scope::Source {
                source: "batches".into(),
                after: None,
                before: None,
            },
            1,
        )
        .await,
    );
    let by_operation = Scope::Operation {
        operation: archived,
    };

    // A refusal in the first batch changes and records nothing.
    let blocker = reading("batches", 1, 9, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![blocker.clone()])).await;
    let operations = count(&mut conn, "SELECT count(*) FROM core.operations").await;
    assert!(matches!(
        operate(&mut conn, Act::Restore, by_operation.clone(), 1).await,
        Ended::Refused(_)
    ));
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.operations").await,
        operations
    );
    operate(&mut conn, Act::Archive, events(&[&blocker]), 1).await;

    // A refusal in the second batch keeps the first and stops early.
    let blocker = reading("batches", 2, 9, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![blocker.clone()])).await;
    let stopped = operate(&mut conn, Act::Restore, by_operation.clone(), 1).await;
    let Ended::StoppedEarly {
        operation: restore,
        counts,
        refusals,
    } = stopped
    else {
        panic!("expected a stop after one batch: {stopped:?}");
    };
    assert_eq!(counts.restored, [("command.executed".to_owned(), 1)].into());
    assert_eq!(refusals[0].member, readings[1].id.0);
    let result: Option<String> =
        sqlx::query_scalar("SELECT result FROM core.operations WHERE id = $1")
            .bind(restore.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(result.as_deref(), Some("stopped_early"));
    assert_eq!(state(&mut conn, &readings[0]).await, "live");
    assert_eq!(state(&mut conn, &readings[1]).await, "archived");
    assert_state_order(&mut conn).await;

    // Once the cause is cleared, resuming reaches the uninterrupted result.
    operate(&mut conn, Act::Archive, events(&[&blocker]), 1).await;
    let (op, progress) = stored(&mut conn, restore).await.unwrap();
    assert_eq!(progress.batches, 1);
    let ended = run_on(&mut conn, &op, progress, 1, &NoDerivations)
        .await
        .unwrap();
    assert!(matches!(ended, Ended::Ok { .. }), "{ended:?}");
    for r in &readings {
        assert_eq!(state(&mut conn, r).await, "live");
    }
    let total: Value = sqlx::query_scalar("SELECT counts FROM core.operations WHERE id = $1")
        .bind(restore.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(total["restored"], json!({"command.executed": 3}));
}

#[devkit::test]
async fn tombstone_only_archived(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = MaterialId::new();
    let e = reading("graves", 1, 1, m, 4);
    admit(&mut conn, &batch(vec![e.clone()])).await;
    let child = derived("graves", 2, &[&e]);
    admit(&mut conn, &batch(vec![child.clone()])).await;
    assert!(matches!(
        operate(&mut conn, Act::Tombstone, events(&[&e]), 16).await,
        Ended::Refused(_)
    ));
    assert_eq!(state(&mut conn, &e).await, "live");

    operate(&mut conn, Act::Archive, events(&[&e]), 16).await;
    let tombstone = operation(&operate(&mut conn, Act::Tombstone, events(&[&e]), 16).await);
    for event in [&e, &child] {
        assert_eq!(state(&mut conn, event).await, "tombstoned");
    }
    let skeleton: (String, String, bool, String, Option<uuid::Uuid>, Option<i64>, Option<i64>, Option<String>) =
        sqlx::query_as(
            "SELECT source, event_type, ts_quality IS NOT NULL AND key_hash IS NOT NULL, operation_id::text,
               material_id, anchor, output_index, reason
             FROM core.event_tombstones WHERE event_id = $1",
        )
        .bind(e.id.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        skeleton,
        (
            "graves".into(),
            "command.executed".into(),
            true,
            tombstone.0.to_string(),
            Some(m.0),
            Some(4),
            Some(0),
            Some("archive".into()),
        )
    );
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.events WHERE source = 'graves'"
        )
        .await,
        0,
        "no payload is kept"
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.search_text").await,
        0
    );
    let refused = operate(&mut conn, Act::Restore, events(&[&e]), 16).await;
    assert!(
        matches!(&refused, Ended::Refused(r) if r[0].reason.contains("tombstoned")),
        "{refused:?}"
    );
    assert!(matches!(
        operate(
            &mut conn,
            Act::Undo,
            Scope::Operation {
                operation: tombstone
            },
            16
        )
        .await,
        Ended::Refused(_)
    ));
    assert_state_order(&mut conn).await;
}

#[devkit::test]
async fn tombstone_allows_new_readings(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let e = reading("again", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![e.clone()])).await;
    operate(&mut conn, Act::Archive, events(&[&e]), 16).await;
    operate(&mut conn, Act::Tombstone, events(&[&e]), 16).await;
    // The same record read again, as a replay or a re-stage would.
    let again = reading("again", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![again.clone()])).await;
    assert_eq!(state(&mut conn, &again).await, "live");
}

#[devkit::test]
async fn no_dead_parents(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let parent = reading("dead", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![parent.clone()])).await;
    operate(&mut conn, Act::Archive, events(&[&parent]), 16).await;
    operate(&mut conn, Act::Tombstone, events(&[&parent]), 16).await;
    let child = derived("dead", 2, &[&parent]);
    let mut tx = conn.begin().await.unwrap();
    assert!(
        tx.admit(&batch(vec![child]), &NoDerivations).await.is_err(),
        "no admitted event names a tombstoned parent"
    );
    tx.rollback().await.unwrap();
}

#[devkit::test]
async fn archive_material_archives_readings(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = material(&mut conn, &blob(1)).await;
    let a = reading("mat", 1, 1, m, 0);
    let b = reading("mat", 2, 2, m, 1);
    admit(&mut conn, &batch(vec![a.clone(), b.clone()])).await;
    let child = derived("mat", 3, &[&a]);
    admit(&mut conn, &batch(vec![child.clone()])).await;

    let scope = Scope::Materials { materials: vec![m] };
    let ended = operate(&mut conn, Act::Archive, scope.clone(), 1).await;
    let Ended::Ok { counts, operation } = ended else {
        panic!("{ended:?}");
    };
    assert_eq!(counts.materials, 1);
    for event in [&a, &b, &child] {
        assert_eq!(state(&mut conn, event).await, "archived");
    }
    let status: (String, uuid::Uuid) =
        sqlx::query_as("SELECT status, status_operation FROM raw.materials WHERE id = $1")
            .bind(m.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        status,
        (MaterialStatus::Archived.as_db().into(), operation.0)
    );

    // A new stage of the same bytes is a new material, admitted live.
    let again = material(&mut conn, &blob(1)).await;
    let fresh = reading("mat", 1, 1, again, 0);
    admit(&mut conn, &batch(vec![fresh.clone()])).await;
    assert_eq!(state(&mut conn, &fresh).await, "live");

    // Undo of the archive brings the material back with its readings, but
    // not the record that is live again under the new material.
    let refused = operate(&mut conn, Act::Undo, Scope::Operation { operation }, 16).await;
    assert!(matches!(refused, Ended::Refused(_)), "{refused:?}");
    operate(&mut conn, Act::Archive, events(&[&fresh]), 16).await;
    operation_ok(operate(&mut conn, Act::Undo, Scope::Operation { operation }, 16).await);
    for event in [&a, &b, &child] {
        assert_eq!(state(&mut conn, event).await, "live");
    }
    let status: String = sqlx::query_scalar("SELECT status FROM raw.materials WHERE id = $1")
        .bind(m.0)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(status, "live");
    assert_state_order(&mut conn).await;
}

fn operation_ok(ended: Ended) -> OperationId {
    match ended {
        Ended::Ok { operation, .. } => operation,
        other => panic!("expected ok: {other:?}"),
    }
}

#[devkit::test]
async fn material_tombstone_spares_shared_bytes(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let shared = blob(2);
    let first = material(&mut conn, &shared).await;
    let second = material(&mut conn, &shared).await;
    let unrelated = material(&mut conn, &blob(3)).await;
    for (m, key) in [(first, 1), (second, 2), (unrelated, 3)] {
        admit(&mut conn, &batch(vec![reading("bytes", key, key, m, 0)])).await;
    }
    let before: Vec<(uuid::Uuid, String, i64)> =
        sqlx::query_as("SELECT id, blob, length FROM raw.materials WHERE id = $1")
            .bind(unrelated.0)
            .fetch_all(&mut conn)
            .await
            .unwrap();

    let first_scope = Scope::Materials {
        materials: vec![first],
    };
    assert!(matches!(
        operate(&mut conn, Act::Tombstone, first_scope.clone(), 16).await,
        Ended::Refused(_)
    ));
    operate(&mut conn, Act::Archive, first_scope.clone(), 16).await;
    let Ended::Ok { counts, .. } = operate(&mut conn, Act::Tombstone, first_scope, 16).await else {
        panic!("tombstone refused");
    };
    assert_eq!(counts.bytes_removed, 0);
    assert_eq!(counts.bytes_kept, vec![first.0]);
    let status: String = sqlx::query_scalar("SELECT status FROM core.blobs WHERE hash = $1")
        .bind(&shared)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(status, "present", "another material still holds the bytes");

    let second_scope = Scope::Materials {
        materials: vec![second],
    };
    operate(&mut conn, Act::Archive, second_scope.clone(), 16).await;
    let Ended::Ok { counts, .. } = operate(&mut conn, Act::Tombstone, second_scope, 16).await
    else {
        panic!("tombstone refused");
    };
    assert_eq!(counts.bytes_removed, 1);
    let status: String = sqlx::query_scalar("SELECT status FROM core.blobs WHERE hash = $1")
        .bind(&shared)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(status, "deleting");
    let after: Vec<(uuid::Uuid, String, i64)> =
        sqlx::query_as("SELECT id, blob, length FROM raw.materials WHERE id = $1")
            .bind(unrelated.0)
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(before, after, "unrelated materials are untouched");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM core.blobs WHERE hash = $1")
            .bind(blob(3))
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        "present"
    );
}

/// A replay of `material` whose new reading of the record replaces `old`.
async fn replay(
    conn: &mut AdmissionContext,
    material: MaterialId,
    content: u8,
) -> (OperationId, Event) {
    let operation = OperationId::new();
    let mut tx = conn.begin().await.unwrap();
    crate::replay::begin(
        &mut tx,
        operation,
        "agent:test",
        &json!({"selected_materials": [material]}),
    )
    .await
    .unwrap();
    let new = reading("replayed", 1, content, material, 0);
    let b = Batch {
        run: RunId(operation.0),
        kind: BatchKind::Replay {
            operation,
            source: None,
            host: None,
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![new.clone()],
        }],
        archive: vec![ArchiveRequest::Replay {
            scope: ReplayScope {
                materials: vec![material],
                ..ReplayScope::default()
            },
            operation,
        }],
    };
    tx.admit(&b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    (operation, new)
}

#[devkit::test]
async fn undo_swaps_forever(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = material(&mut conn, &blob(4)).await;
    let old = reading("replayed", 1, 1, m, 0);
    admit(&mut conn, &batch(vec![old.clone()])).await;
    let child = derived("replayed.derived", 2, &[&old]);
    admit(&mut conn, &batch(vec![child.clone()])).await;

    let (replayed, new) = replay(&mut conn, m, 2).await;
    assert_eq!(state(&mut conn, &old).await, "archived");
    assert_eq!(state(&mut conn, &child).await, "archived");
    assert_eq!(state(&mut conn, &new).await, "live");
    let reason: (String, uuid::Uuid) =
        sqlx::query_as("SELECT reason, operation_id FROM audit.archived WHERE event_id = $1")
            .bind(child.id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(reason, ("replay".into(), replayed.0));

    let mut target = replayed;
    for round in 0..4 {
        target = operation_ok(
            operate(
                &mut conn,
                Act::Undo,
                Scope::Operation { operation: target },
                1,
            )
            .await,
        );
        let (live, archived) = if round % 2 == 0 {
            (&old, &new)
        } else {
            (&new, &old)
        };
        assert_eq!(state(&mut conn, live).await, "live", "round {round}");
        assert_eq!(
            state(&mut conn, archived).await,
            "archived",
            "round {round}"
        );
        assert_eq!(
            state(&mut conn, &child).await,
            if round % 2 == 0 { "live" } else { "archived" },
            "round {round}"
        );
        assert_state_order(&mut conn).await;
    }
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.events WHERE source = 'replayed'"
        )
        .await,
        2,
        "the archive holds every reading"
    );
}

/// Keys of one keyed derivation, one per event type.
struct OneKey;

impl crate::admission::DirtyKeys for OneKey {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(String, Value)> {
        vec![("by.type".into(), json!({"type": event.event_type}))]
    }
}

#[devkit::test]
async fn dirty_keys_wait_for_operation(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let a = reading("held", 1, 1, MaterialId::new(), 0);
    let b = reading("held", 2, 2, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![a.clone(), b.clone()])).await;
    sqlx::query("DELETE FROM derivation.dirty")
        .execute(&mut *conn)
        .await
        .unwrap();
    let op = begin(
        &mut conn,
        Act::Archive,
        "agent:test",
        Scope::Source {
            source: "held".into(),
            after: None,
            before: None,
        },
    )
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    let Step::Moved { progress, done, .. } = step(&mut tx, &op, Progress::default(), 1, &OneKey)
        .await
        .unwrap()
    else {
        panic!("refused");
    };
    tx.commit().await.unwrap();
    assert!(!done);
    let held: Vec<Option<uuid::Uuid>> = sqlx::query_scalar("SELECT held_by FROM derivation.dirty")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(held, vec![Some(op.id.0)], "held between batches");
    assert!(
        crate::derivations::dirty(&mut conn, &["by.type".into()], 10)
            .await
            .unwrap()
            .is_empty(),
        "no derivation runs over a held key"
    );
    run_on(&mut conn, &op, progress, 1, &OneKey).await.unwrap();
    let held: Vec<Option<uuid::Uuid>> = sqlx::query_scalar("SELECT held_by FROM derivation.dirty")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(held, vec![None], "released at the operation's end");
    assert_eq!(
        crate::derivations::dirty(&mut conn, &["by.type".into()], 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[devkit::test]
async fn archive_by_run(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let first = batch(vec![reading("runs", 1, 1, MaterialId::new(), 0)]);
    let run = first.run;
    let kept = reading("runs", 2, 2, MaterialId::new(), 0);
    admit(&mut conn, &first).await;
    admit(&mut conn, &batch(vec![kept.clone()])).await;
    let Ended::Ok { counts, .. } = operate(&mut conn, Act::Archive, Scope::Run { run }, 16).await
    else {
        panic!("refused");
    };
    assert_eq!(counts.archived, [("command.executed".to_owned(), 1)].into());
    assert_eq!(state(&mut conn, &kept).await, "live");
}

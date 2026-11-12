//! The lifecycle against a real database: whole chains move, no
//! event's state is earlier than its parents', restore refuses while another
//! reading of the record is live and derives keyed aggregates again, refusal
//! is per batch and resuming reaches the uninterrupted result, replay archives
//! exactly what it replaces, tombstones are reached only from the archive,
//! clear every copy and stop no new readings, undo swaps forever, material
//! status cascades, and as-of reads return the live set of each position.
use super::*;
use crate::admission::{KeyInput, NoDerivations};
use crate::{AdmissionContext, ComputedKey, DerivedScope, Outcome, ReadMark};
use serde_json::{Value, json};
use sinex_model::event_types::CommandExecuted;
use sinex_model::lifecycle::{Act, MaterialStatus, Scope};
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, InputRef, MaterialId, MaterialRef,
    ObservationFacts, Origin, ReadingOrder, RecordLocation, RecordOutcome, ReplayScope,
    RevisionChange, RunId, SourceProgress, Timestamp,
};
use std::collections::{BTreeMap, BTreeSet};

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

async fn admit(conn: &mut AdmissionContext, b: &Batch) -> Outcome {
    let mut tx = conn.begin().await.unwrap();
    let outcome = tx.admit(b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    outcome
}

/// A source batch that stages `blob` as the new material `material` and
/// admits `event`, read from it.
fn stage(material: MaterialId, blob: &str, event: Event) -> Batch {
    let mut staged = batch(vec![event]);
    staged.kind = BatchKind::Source(Box::new(SourceProgress {
        source: None,
        host: None,
        binding: "staged".into(),
        data_keys: vec![],
        input: InputRef {
            kind: "file".into(),
            locator: "/staged".into(),
            host: "h".into(),
        },
        observation: ObservationFacts {
            changed: true,
            ..Default::default()
        },
        revision: RevisionChange::Snapshot,
        material: Some(MaterialRef {
            id: material,
            input: None,
            blob: blob.into(),
            length: 3,
            part_index: 0,
            previous_part: None,
            parser: "test".into(),
            parser_version: 1,
            manifest: Default::default(),
        }),
        cursor: Value::Null,
        carry: Value::Null,
        memory: None,
    }));
    staged
}

/// A keyed derivation's complete output for one key, computed after the
/// key's dirty mark at `dirty_seq`.
fn aggregate(derivation: &str, key: Value, dirty_seq: i64, output: Event) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Derived {
            derivation: derivation.into(),
            scope: DerivedScope::Keys {
                keys: vec![ComputedKey {
                    key,
                    dirty_seq,
                    waiting: false,
                    refresh_at: None,
                }],
                read: ReadMark::default(),
            }
            .to_value(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![output],
        }],
        archive: vec![],
    }
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

/// The state order, checked over the whole store: every event's state
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
async fn lifecycle_member_reads_follow_chain_keys_in_unrelated_history(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = context(&db).await;
    let unrelated_root = reading("unrelated.root", 0, 0, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![unrelated_root.clone()])).await;
    let unrelated: Vec<_> = (0..8192)
        .map(|index| {
            let mut payload: CommandExecuted = serde_json::from_value(
                reading("unrelated", 0, 0, MaterialId::new(), index).payload,
            )
            .unwrap();
            payload.native_id = format!("unrelated-{index}");
            payload.command = format!("synthetic-{index}");
            Event::build(
                &payload,
                "unrelated",
                Origin::Derived {
                    parents: vec![unrelated_root.id],
                },
                ReadingOrder(1),
                *blake3::hash(&index.to_le_bytes()).as_bytes(),
                Class::Open,
            )
            .unwrap()
        })
        .collect();
    admit(&mut conn, &batch(unrelated)).await;
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.events WHERE source='unrelated'"
        )
        .await,
        8192
    );
    assert_eq!(
        count(&mut conn, "SELECT count(*) FROM core.event_children").await,
        8192
    );
    let mut roots: Vec<_> = (0..=255)
        .map(|key| reading("roots", key, key, MaterialId::new(), 0))
        .collect();
    // Exercise location membership as well as field-key membership, with a
    // different type so the returned lifecycle metadata is checked too.
    roots[0] = Event::build(
        &sinex_model::event_types::operator::LogEntry {
            written_at: None,
            label: None,
            text: "synthetic location reading".into(),
        },
        "roots.location",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        [42; 32],
        Class::Open,
    )
    .unwrap();
    admit(&mut conn, &batch(roots.clone())).await;
    let children: Vec<_> = roots
        .iter()
        .enumerate()
        .map(|(key, root)| derived("children", key as u8, &[root]))
        .collect();
    admit(&mut conn, &batch(children.clone())).await;
    let grandchildren: Vec<_> = children
        .iter()
        .enumerate()
        .map(|(key, child)| derived("grandchildren", key as u8, &[child]))
        .collect();
    admit(&mut conn, &batch(grandchildren.clone())).await;
    let expected: BTreeSet<_> = roots
        .iter()
        .chain(&children)
        .chain(&grandchildren)
        .map(|event| event.id.0)
        .collect();
    let ids: Vec<_> = roots.iter().map(|event| event.id.0).collect();
    let expected_types: BTreeMap<_, _> = roots
        .iter()
        .chain(&children)
        .chain(&grandchildren)
        .map(|event| (event.id.0, event.event_type.as_str()))
        .collect();
    sqlx::raw_sql("ANALYZE core.events; ANALYZE core.event_children")
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    // Count this backend's tuple work, including every event chunk. Parallel
    // workers would have separate counters and hide a global history scan.
    sqlx::query("SET LOCAL max_parallel_workers_per_gather=0")
        .execute(&mut *tx)
        .await
        .unwrap();
    // Prefer whole-relation joins where legal, so the small fixture exercises
    // the same cardinality risk as a long history. Keyed lateral lookups must
    // retain their bound under that planner preference too.
    sqlx::query("SET LOCAL enable_nestloop=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let work = "SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN (SELECT show_chunks('core.events'))";
    let edge_work = "SELECT seq_tup_read+idx_tup_fetch FROM pg_stat_xact_user_tables WHERE relid='core.event_children'::regclass";
    for phase in [Phase::Archive, Phase::ArchiveMadeLive, Phase::Tombstone] {
        let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
        let edges_before: i64 = sqlx::query_scalar(edge_work)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        let planned = plan(&mut tx, phase, &ids, &HashSet::new()).await.unwrap();
        let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
        let edges_after: i64 = sqlx::query_scalar(edge_work)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(
            planned
                .members
                .iter()
                .map(|member| member.id)
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert_eq!(
            planned
                .members
                .iter()
                .map(|member| (member.id, member.event_type.as_str()))
                .collect::<BTreeMap<_, _>>(),
            expected_types
        );
        if phase == Phase::Tombstone {
            assert_eq!(
                planned.refusals.len(),
                expected.len(),
                "live members refuse tombstoning"
            );
        } else {
            assert!(planned.refusals.is_empty());
            assert_eq!(after, before, "archive membership needs no event reads");
        }
        assert!(
            after - before <= expected.len() as i64 * 4,
            "chain of {} members visited {} event tuples",
            expected.len(),
            after - before
        );
        assert!(
            edges_after - edges_before <= expected.len() as i64 * 4,
            "chain of {} members visited {} edge tuples",
            expected.len(),
            edges_after - edges_before
        );
    }
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let original: Vec<uuid::Uuid> = sqlx::query_scalar(
        "WITH RECURSIVE chain(id) AS (
           SELECT unnest($1::uuid[]) UNION
           SELECT c.child FROM chain CROSS JOIN LATERAL core.event_children_of(chain.id)c)
         SELECT e.id FROM chain JOIN core.events e ON e.id=chain.id
         WHERE EXISTS(SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id)
            OR EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    assert_eq!(original.into_iter().collect::<BTreeSet<_>>(), expected);
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    assert!(
        after - before >= 8192,
        "original SQL must read unrelated history for this work oracle"
    );
    tx.rollback().await.unwrap();
    let archived = operate(
        &mut conn,
        Act::Archive,
        events(&roots.iter().collect::<Vec<_>>()),
        256,
    )
    .await;
    assert!(matches!(archived, Ended::Ok { .. }));
    let mut tx = conn.begin().await.unwrap();
    sqlx::raw_sql("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL enable_nestloop=off")
        .execute(&mut *tx)
        .await
        .unwrap();
    let before: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let edges_before: i64 = sqlx::query_scalar(edge_work)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let restored = plan(&mut tx, Phase::Restore, &ids, &HashSet::new())
        .await
        .unwrap();
    let after: i64 = sqlx::query_scalar(work).fetch_one(&mut *tx).await.unwrap();
    let edges_after: i64 = sqlx::query_scalar(edge_work)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        restored
            .members
            .iter()
            .map(|member| member.id)
            .collect::<BTreeSet<_>>(),
        expected
    );
    assert!(restored.refusals.is_empty());
    assert_eq!(
        restored
            .members
            .iter()
            .map(|member| (member.id, member.event_type.as_str()))
            .collect::<BTreeMap<_, _>>(),
        expected_types
    );
    assert!(restored.derived_again.is_empty());
    assert!(
        after - before <= expected.len() as i64 * 4,
        "restore visited {} event tuples",
        after - before
    );
    assert!(
        edges_after - edges_before <= expected.len() as i64 * 4,
        "restore visited {} edge tuples",
        edges_after - edges_before
    );
    tx.rollback().await.unwrap();
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
    // One record read from a material that stays live, to replay it.
    let kept = material(&mut conn, &blob(5)).await;
    let replayed = reading("again", 1, 1, kept, 0);
    admit(&mut conn, &batch(vec![replayed.clone()])).await;
    // Another staged from bytes, to stage the same bytes again.
    let bytes = blob(6);
    let first_stage = MaterialId::new();
    let staged = reading("again", 2, 2, first_stage, 0);
    admit(&mut conn, &stage(first_stage, &bytes, staged.clone())).await;
    operation(&operate(&mut conn, Act::Archive, events(&[&replayed, &staged]), 16).await);
    operation(&operate(&mut conn, Act::Tombstone, events(&[&replayed, &staged]), 16).await);
    for event in [&replayed, &staged] {
        assert_eq!(state(&mut conn, event).await, "tombstoned");
    }

    // A replay of the live material reads the record again.
    let read_again = reading("again", 1, 1, kept, 0);
    let (_, replay_outcome) = replay(&mut conn, kept, &[read_again.clone()]).await;
    assert_eq!(
        (replay_outcome.admitted, replay_outcome.replay_archived),
        (1, 0),
        "the tombstoned reading is gone; the new one is admitted"
    );
    assert_ne!(read_again.id, replayed.id);
    assert_eq!(state(&mut conn, &read_again).await, "live");

    // A new stage of identical bytes is a new material, and its reading of
    // the record is admitted.
    let second_stage = MaterialId::new();
    let staged_again = reading("again", 2, 2, second_stage, 0);
    let stage_outcome = admit(
        &mut conn,
        &stage(second_stage, &bytes, staged_again.clone()),
    )
    .await;
    assert_eq!(stage_outcome.admitted, 1, "{stage_outcome:?}");
    assert_ne!(staged_again.id, staged.id);
    assert_eq!(state(&mut conn, &staged_again).await, "live");
    let materials: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT id FROM raw.materials WHERE blob = $1 ORDER BY id")
            .bind(&bytes)
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let mut expected = vec![first_stage.0, second_stage.0];
    expected.sort();
    assert_eq!(
        materials, expected,
        "identical bytes are staged as a second material"
    );
    assert_state_order(&mut conn).await;
}

/// Why admission refuses an output of `parent`.
async fn child_refusal(conn: &mut AdmissionContext, parent: &Event) -> String {
    let child = derived("dead.derived", 2, &[parent]);
    let mut tx = conn.begin().await.unwrap();
    let result = tx.admit(&batch(vec![child]), &NoDerivations).await;
    tx.rollback().await.unwrap();
    match result {
        Err(crate::Error::Invalid(reason)) => reason,
        other => panic!("an output of a parent that is not live: {other:?}"),
    }
}

#[devkit::test]
async fn no_dead_parents(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let parent = reading("dead", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![parent.clone()])).await;
    operate(&mut conn, Act::Archive, events(&[&parent]), 16).await;
    assert_eq!(
        child_refusal(&mut conn, &parent).await,
        format!("derived parent {} is not live", parent.id.0)
    );
    operate(&mut conn, Act::Tombstone, events(&[&parent]), 16).await;
    assert_eq!(
        child_refusal(&mut conn, &parent).await,
        format!("derived parent {} is not a committed reading", parent.id.0),
        "no admitted event names a tombstoned parent"
    );

    // An output whose input was archived after the derivation read it is
    // dropped and counted, not refused; the other output is admitted.
    let input = reading("dead", 3, 3, MaterialId::new(), 0);
    let other = reading("dead", 4, 4, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![input.clone(), other.clone()])).await;
    let position = crate::query::current_seq(&mut conn).await.unwrap();
    let read = ReadMark::now(&mut conn).await.unwrap();
    operate(&mut conn, Act::Archive, events(&[&input]), 16).await;
    let dropped = derived("dead.derived", 5, &[&input]);
    let kept = derived("dead.derived", 6, &[&other]);
    let outcome = admit(
        &mut conn,
        &Batch {
            run: RunId::new(),
            kind: BatchKind::Derived {
                derivation: "dead.derived".into(),
                scope: DerivedScope::Position {
                    inputs: vec![input.id.0, other.id.0],
                    position,
                    read,
                }
                .to_value(),
            },
            records: vec![RecordOutcome::Events {
                location: None,
                events: vec![dropped.clone(), kept.clone()],
            }],
            archive: vec![],
        },
    )
    .await;
    assert_eq!(
        (outcome.admitted, outcome.superseded_input),
        (1, 1),
        "{outcome:?}"
    );
    assert_eq!(state(&mut conn, &kept).await, "live");
    assert_eq!(
        count(
            &mut conn,
            &format!(
                "SELECT count(*) FROM core.events WHERE id = '{}'",
                dropped.id.0
            )
        )
        .await,
        0
    );
    assert_state_order(&mut conn).await;
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

/// A replay of `material` whose new readings are `readings`.
async fn replay(
    conn: &mut AdmissionContext,
    material: MaterialId,
    readings: &[Event],
) -> (OperationId, Outcome) {
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
    let b = Batch {
        run: RunId(operation.0),
        kind: BatchKind::Replay {
            operation,
            source: None,
            host: None,
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: readings.to_vec(),
        }],
        archive: vec![ArchiveRequest::Replay {
            scope: ReplayScope {
                materials: vec![material],
                ..ReplayScope::default()
            },
            operation,
        }],
    };
    let outcome = tx.admit(&b, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    (operation, outcome)
}

#[devkit::test]
async fn undo_swaps_forever(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = material(&mut conn, &blob(4)).await;
    let old = reading("replayed", 1, 1, m, 0);
    admit(&mut conn, &batch(vec![old.clone()])).await;
    let child = derived("replayed.derived", 2, &[&old]);
    admit(&mut conn, &batch(vec![child.clone()])).await;

    let new = reading("replayed", 1, 2, m, 0);
    let (replayed, _) = replay(&mut conn, m, &[new.clone()]).await;
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

/// An undone archive never brings an old aggregate back beside the one
/// recomputed from what is live (docs/lifecycle.md: "Keyed aggregates are
/// not restored as rows").
#[devkit::test]
async fn an_undone_archive_leaves_keyed_aggregates_archived_and_dirties_their_keys(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = context(&db).await;
    let root = reading("removed.fusion", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![root.clone()])).await;
    let figure = derived("metric.hourly", 2, &[&root]);
    admit(&mut conn, &batch(vec![figure.clone()])).await;
    sqlx::query(
        "INSERT INTO derivation.outputs (derivation, key, event_id) VALUES ('metric.hourly', '{\"hour\": 1}', $1)",
    )
    .bind(figure.id.0)
    .execute(&mut *conn)
    .await
    .unwrap();
    let archived = operation_ok(
        operate(
            &mut conn,
            Act::Archive,
            Scope::Source {
                source: "removed.fusion".into(),
                after: None,
                before: None,
            },
            16,
        )
        .await,
    );
    assert_eq!(state(&mut conn, &figure).await, "archived");
    sqlx::query("DELETE FROM derivation.dirty")
        .execute(&mut *conn)
        .await
        .unwrap();
    operate(
        &mut conn,
        Act::Undo,
        Scope::Operation {
            operation: archived,
        },
        16,
    )
    .await;
    assert_eq!(state(&mut conn, &root).await, "live");
    assert_eq!(state(&mut conn, &figure).await, "archived");
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM derivation.dirty WHERE derivation = 'metric.hourly' AND key = '{\"hour\": 1}'"
        )
        .await,
        1,
        "the aggregate is derived again from what is live"
    );
    assert_state_order(&mut conn).await;
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

#[devkit::test]
async fn restore_rederives_aggregates(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let a = reading("inputs", 1, 1, MaterialId::new(), 0);
    let b = reading("inputs", 2, 2, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![a.clone(), b.clone()])).await;
    let key = json!({"day": "2023-11-14"});
    let old = derived("test.sum", 3, &[&a, &b]);
    admit(
        &mut conn,
        &aggregate("test.sum", key.clone(), 0, old.clone()),
    )
    .await;

    operation(&operate(&mut conn, Act::Archive, events(&[&a]), 16).await);
    assert_eq!(
        state(&mut conn, &old).await,
        "archived",
        "archiving one input archives the aggregate"
    );
    let Ended::Ok {
        counts,
        operation: restore,
    } = operate(&mut conn, Act::Restore, events(&[&a]), 16).await
    else {
        panic!("restore refused");
    };
    assert_eq!(counts.restored, [("command.executed".to_owned(), 1)].into());
    assert_eq!(counts.derived_again, 1);
    for (event, expected) in [(&a, "live"), (&b, "live"), (&old, "archived")] {
        assert_eq!(state(&mut conn, event).await, expected);
    }
    let (dirty_seq, end_seq): (i64, Option<i64>) = sqlx::query_as(
        "SELECT d.last_dirty_seq, o.end_seq FROM derivation.dirty d, core.operations o
         WHERE d.derivation = 'test.sum' AND d.key = $1 AND o.id = $2",
    )
    .bind(&key)
    .bind(restore.0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        Some(dirty_seq),
        end_seq,
        "the restore marks the aggregate's key dirty at its own position"
    );

    // The derivation computes the key again from what is live: a new reading,
    // while the old aggregate row stays archived.
    let new = derived("test.sum", 3, &[&a, &b]);
    let outcome = admit(
        &mut conn,
        &aggregate("test.sum", key, dirty_seq, new.clone()),
    )
    .await;
    assert_eq!(outcome.admitted, 1, "{outcome:?}");
    assert_ne!(new.id, old.id);
    assert_eq!(state(&mut conn, &new).await, "live");
    assert_eq!(state(&mut conn, &old).await, "archived");
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM derivation.dirty WHERE derivation = 'test.sum'"
        )
        .await,
        0
    );
    assert_state_order(&mut conn).await;

    // A restore of an operation that moved in several batches brings back
    // exactly the chains that operation moved.
    let moved: Vec<Event> = (4..=6)
        .map(|k| reading("several", k, k, MaterialId::new(), 0))
        .collect();
    let apart = reading("several", 7, 7, MaterialId::new(), 0);
    for event in moved.iter().chain([&apart]) {
        admit(&mut conn, &batch(vec![event.clone()])).await;
    }
    operation(&operate(&mut conn, Act::Archive, events(&[&apart]), 16).await);
    let archived = operation(
        &operate(
            &mut conn,
            Act::Archive,
            Scope::Source {
                source: "several".into(),
                after: None,
                before: None,
            },
            1,
        )
        .await,
    );
    let batches: i64 = sqlx::query_scalar(
        "SELECT (progress->>'batches')::bigint FROM core.operations WHERE id = $1",
    )
    .bind(archived.0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(batches, 3);
    let Ended::Ok { counts, .. } = operate(
        &mut conn,
        Act::Restore,
        Scope::Operation {
            operation: archived,
        },
        1,
    )
    .await
    else {
        panic!("restore refused");
    };
    assert_eq!(counts.restored, [("command.executed".to_owned(), 3)].into());
    for event in &moved {
        assert_eq!(state(&mut conn, event).await, "live");
    }
    assert_eq!(state(&mut conn, &apart).await, "archived");
}

/// The readings archived by `operation`, each with its reason.
async fn archived_by(
    conn: &mut PgConnection,
    operation: OperationId,
) -> BTreeSet<(uuid::Uuid, String)> {
    sqlx::query_as("SELECT event_id, reason FROM audit.archived WHERE operation_id = $1")
        .bind(operation.0)
        .fetch_all(&mut *conn)
        .await
        .unwrap()
        .into_iter()
        .collect()
}

#[devkit::test]
async fn replay_archives_replaced_readings_of_a_material(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = material(&mut conn, &blob(7)).await;
    let n = material(&mut conn, &blob(8)).await;
    let first = reading("replayed", 1, 1, m, 0);
    let second = reading("replayed", 2, 2, m, 1);
    let elsewhere = reading("replayed", 3, 3, n, 0);
    admit(
        &mut conn,
        &batch(vec![first.clone(), second.clone(), elsewhere.clone()]),
    )
    .await;
    let child = derived("replayed.derived", 4, &[&first]);
    admit(&mut conn, &batch(vec![child.clone()])).await;

    let once = [
        reading("replayed", 1, 1, m, 0),
        reading("replayed", 2, 5, m, 1),
    ];
    let (replayed, outcome) = replay(&mut conn, m, &once).await;
    assert_eq!(
        (outcome.replay_archived, outcome.admitted),
        (3, 2),
        "{outcome:?}"
    );
    let reason = "replay".to_owned();
    assert_eq!(
        archived_by(&mut conn, replayed).await,
        [
            (first.id.0, reason.clone()),
            (second.id.0, reason.clone()),
            (child.id.0, reason.clone()),
        ]
        .into(),
        "exactly the replaced readings and their chains, reason replay"
    );
    for event in &once {
        assert!(![first.id, second.id].contains(&event.id));
        assert_eq!(state(&mut conn, event).await, "live");
    }
    assert_eq!(state(&mut conn, &elsewhere).await, "live");

    let twice = [
        reading("replayed", 1, 1, m, 0),
        reading("replayed", 2, 5, m, 1),
    ];
    let (replayed_again, outcome) = replay(&mut conn, m, &twice).await;
    assert_eq!(
        (outcome.replay_archived, outcome.admitted),
        (2, 2),
        "{outcome:?}"
    );
    assert_eq!(
        archived_by(&mut conn, replayed_again).await,
        [(once[0].id.0, reason.clone()), (once[1].id.0, reason)].into()
    );
    // Each record: the original and the first replay's reading archived, the
    // second replay's live.
    for (original, first_replay, second_replay) in [
        (&first, &once[0], &twice[0]),
        (&second, &once[1], &twice[1]),
    ] {
        assert_eq!(state(&mut conn, original).await, "archived");
        assert_eq!(state(&mut conn, first_replay).await, "archived");
        assert_eq!(state(&mut conn, second_replay).await, "live");
    }
    assert_eq!(state(&mut conn, &elsewhere).await, "live");
    assert_state_order(&mut conn).await;
}

/// How many search entries, embeddings and keyed output rows hold `events`.
async fn copies(conn: &mut PgConnection, events: &[uuid::Uuid]) -> (i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM core.search_text WHERE event_id = ANY($1)),
                (SELECT count(*) FROM core.embedding_vectors WHERE target_id = ANY($1)),
                (SELECT count(*) FROM derivation.outputs WHERE event_id = ANY($1))",
    )
    .bind(events)
    .fetch_one(&mut *conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn tombstone_clears_copies(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let e = reading("copies", 1, 1, MaterialId::new(), 0);
    admit(&mut conn, &batch(vec![e.clone()])).await;
    let summary = derived("copies.sum", 2, &[&e]);
    admit(
        &mut conn,
        &aggregate("copies.sum", json!({"all": true}), 0, summary.clone()),
    )
    .await;
    crate::read_models::tests::catch_up_search_text(&mut conn)
        .await
        .unwrap();
    let ids = [e.id.0, summary.id.0];
    for id in ids {
        sqlx::query(
            "INSERT INTO core.embedding_vectors (embedding_id, target_id, coverage) VALUES ($1, $2, 'unavailable')",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(id)
        .execute(&mut *conn)
        .await
        .unwrap();
    }
    assert_eq!(copies(&mut conn, &ids).await, (2, 2, 1));

    operation(&operate(&mut conn, Act::Archive, events(&[&e]), 16).await);
    operation(&operate(&mut conn, Act::Tombstone, events(&[&e]), 16).await);
    for event in [&e, &summary] {
        assert_eq!(state(&mut conn, event).await, "tombstoned");
    }
    assert_eq!(copies(&mut conn, &ids).await, (0, 0, 0));
}

#[devkit::test]
async fn as_of_reads_exact(db: devkit::pg::TestDatabase) {
    let mut conn = context(&db).await;
    let m = material(&mut conn, &blob(9)).await;
    let a = reading("history", 1, 1, MaterialId::new(), 0);
    let b = reading("history", 2, 2, m, 0);
    let b_again = reading("history", 2, 3, m, 0);
    let start = crate::query::current_seq(&mut conn).await.unwrap();
    // The live set at each commit position from `start` on, as the script
    // makes it; every step but the replay takes one position.
    let mut expected: Vec<BTreeSet<String>> = vec![BTreeSet::new()];
    let live = |events: &[&Event]| -> BTreeSet<String> {
        events.iter().map(|e| e.id.0.to_string()).collect()
    };
    admit(&mut conn, &batch(vec![a.clone()])).await;
    expected.push(live(&[&a]));
    admit(&mut conn, &batch(vec![b.clone()])).await;
    expected.push(live(&[&a, &b]));
    operation(&operate(&mut conn, Act::Archive, events(&[&a]), 16).await);
    expected.push(live(&[&b]));
    // A replay archives at one position and admits at the next.
    replay(&mut conn, m, &[b_again.clone()]).await;
    expected.push(live(&[]));
    expected.push(live(&[&b_again]));
    operation(&operate(&mut conn, Act::Restore, events(&[&a]), 16).await);
    expected.push(live(&[&a, &b_again]));
    operation(&operate(&mut conn, Act::Archive, events(&[&b_again]), 16).await);
    expected.push(live(&[&a]));
    let end = crate::query::current_seq(&mut conn).await.unwrap();
    assert_eq!(
        end - start,
        i64::try_from(expected.len()).unwrap() - 1,
        "commit positions are dense"
    );

    let at = sinex_model::query::Context::utc("2023-11-15T00:00:00Z".parse().unwrap());
    for (position, live) in (start..=end).zip(&expected) {
        let answer = crate::query::run(
            &mut conn,
            &format!("events where source = history | fields id | as of seq {position}"),
            None,
            &at,
            &crate::query::Page::default(),
        )
        .await
        .unwrap();
        let read: BTreeSet<String> = answer
            .rows
            .iter()
            .map(|row| row["id"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(&read, live, "as of seq {position}");
    }
}

#[devkit::test]
async fn deletion_marker_is_atomic_with_catalog_transition_and_material_readmission(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = context(&db).await;
    let dir = tempfile::tempdir().unwrap();
    let blobs = crate::Blobs::open(dir.path()).unwrap();
    let name = blobs.put(b"abc").unwrap();
    material(&mut conn, &name).await;
    crate::blobs::commit_pending_extents(&mut conn, &blobs)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &name).await.unwrap();
    let marked: bool = sqlx::query_scalar("SELECT deleting FROM core.blob_extents WHERE hash=$1")
        .bind(&name)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(marked, "marking does not wait for another extent commit");
    tx.rollback().await.unwrap();
    let marked: bool = sqlx::query_scalar("SELECT deleting FROM core.blob_extents WHERE hash=$1")
        .bind(&name)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(!marked);
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &name).await.unwrap();
    tx.commit().await.unwrap();
    let (removed, left) = remove_unreferenced(&mut conn, &blobs).await.unwrap();
    assert!(
        removed.is_empty() && left.is_empty(),
        "a material still holds these bytes"
    );
    let marked: bool = sqlx::query_scalar("SELECT deleting FROM core.blob_extents WHERE hash=$1")
        .bind(&name)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(!marked, "cancelling deletion clears its marker immediately");
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &name).await.unwrap();
    tx.commit().await.unwrap();
    // Admission restores an existing descriptor even when no new bytes were prepared.
    let id = MaterialId::new();
    admit(
        &mut conn,
        &stage(id, &name, reading("staged", 42, 42, id, 0)),
    )
    .await;
    let marked: bool = sqlx::query_scalar("SELECT deleting FROM core.blob_extents WHERE hash=$1")
        .bind(&name)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(!marked);
    drop(blobs);
    let reopened = crate::Blobs::open(dir.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    assert_eq!(reopened.read(&name).unwrap(), b"abc");
}

#[devkit::test]
async fn deleting_a_direct_blob_preserves_its_bytes_as_a_held_chunk_then_reclaims_them(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = context(&db).await;
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let blobs = crate::Blobs::open(dir.path()).unwrap();
    let direct = blobs.put(b"abc").unwrap();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,3,'present')")
        .bind(&direct)
        .execute(&mut *conn)
        .await
        .unwrap();
    crate::blobs::commit_pending_extents(&mut conn, &blobs)
        .await
        .unwrap();
    // First mark the direct owner. Later admission of the chunked owner
    // must clear the marker without appending those bytes again.
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &direct).await.unwrap();
    tx.commit().await.unwrap();
    let parent = format!("blake3:{}", blake3::hash(b"abcxyz").to_hex());
    let other = hashed_blobs::Preparation::new(scratch.path())
        .unwrap()
        .plain(b"xyz".as_slice(), 3)
        .unwrap();
    blobs
        .put_chunks(
            &parent,
            6,
            vec![
                (direct.clone(), None),
                (other.hash().to_string(), Some(other)),
            ],
        )
        .unwrap();
    let (removed, left) = remove_unreferenced(&mut conn, &blobs).await.unwrap();
    assert_eq!(removed, [direct.clone()]);
    assert!(left.is_empty());
    assert_eq!(
        blobs.read(&parent).unwrap(),
        b"abcxyz",
        "pending chunk references survive direct removal"
    );
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,6,'present')")
        .bind(&parent)
        .execute(&mut *conn)
        .await
        .unwrap();
    crate::blobs::commit_pending_extents(&mut conn, &blobs)
        .await
        .unwrap();
    let marked: bool = sqlx::query_scalar("SELECT deleting FROM core.blob_extents WHERE hash=$1")
        .bind(&direct)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert!(!marked);
    // Repeating the lifecycle transition with an existing shared owner also
    // preserves its physical read authority.
    sqlx::query("UPDATE core.blobs SET status='present' WHERE hash=$1")
        .bind(&direct)
        .execute(&mut *conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &direct).await.unwrap();
    tx.commit().await.unwrap();
    drop(blobs);
    let blobs = crate::Blobs::open(dir.path()).unwrap();
    blobs.recover(&mut conn).await.unwrap();
    assert_eq!(blobs.read(&parent).unwrap(), b"abcxyz");
    let (removed, left) = remove_unreferenced(&mut conn, &blobs).await.unwrap();
    assert_eq!(removed, [direct.clone()]);
    assert!(left.is_empty());
    let status: String = sqlx::query_scalar("SELECT status FROM core.blobs WHERE hash=$1")
        .bind(&direct)
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(status, "removed");
    drop(blobs);
    let blobs = crate::Blobs::open(dir.path()).unwrap();
    blobs.recover(&mut conn).await.unwrap();
    assert_eq!(blobs.read(&parent).unwrap(), b"abcxyz");
    let mut tx = conn.begin().await.unwrap();
    bytes::mark_removing(&mut tx, &parent).await.unwrap();
    tx.commit().await.unwrap();
    let (removed, left) = remove_unreferenced(&mut conn, &blobs).await.unwrap();
    assert_eq!(removed, [parent]);
    assert!(left.is_empty());
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM core.blob_extents")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(remaining, 0, "the final owner removal reclaims every chunk");
}

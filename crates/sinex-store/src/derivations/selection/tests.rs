//! A comparison owns one initial snapshot, then validates only its newer
//! input-kind ranges. All fixtures and source bytes are synthetic.
use super::*;
use crate::{AdmissionContext, NoDerivations};
use serde_json::{Value, json};
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};
use sqlx::{Connection, Executor};

const INITIAL: &str = include_str!("input_state.sql");
const OLD: &str = "SELECT (SELECT coalesce(max(commit_seq),0) FROM core.events WHERE event_type=ANY($1)),(SELECT coalesce(max(a.change_seq),0) FROM audit.archive_changes a JOIN core.events e ON e.id=a.event_id WHERE e.event_type=ANY($1)),(SELECT value FROM core.counters WHERE name='tombstoned')";
fn kinds() -> Vec<String> {
    vec!["command.imported".into()]
}
fn event(kind: &str, n: u32) -> Event {
    let at = Timestamp::from_unix_secs(1_600_000_000 + i64::from(n)).unwrap();
    let payload = if kind == "command.imported" {
        json!({"command":format!("synthetic-{n}"),"hostname":"synthetic","started_at":at,"header_elapsed":0,"paths":[],"n":n})
    } else {
        json!({"native_id":format!("synthetic-{n}"),"command":format!("synthetic-{n}"),"cwd":"/synthetic","session":"synthetic","hostname":"synthetic","started_at":at,"duration_ns":null,"exit_code":null,"deleted_at":null,"author":null,"intent":null,"shell":null})
    };
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(i64::from(n)),
        [n as u8; 32],
        Class::Open,
    )
    .unwrap()
}
async fn admit(owner: &mut AdmissionContext, event: Event) -> (EventId, i64) {
    let id = event.id;
    let batch = Batch {
        run: RunId::new(),
        kind: BatchKind::Api {
            actor: "synthetic".into(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![event],
        }],
        archive: vec![],
    };
    let mut tx = owner.begin().await.unwrap();
    let out = tx.admit(&batch, &NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
    (id, out.commit_seq.unwrap().0)
}
async fn old(conn: &mut PgConnection, inputs: &[String]) -> (i64, i64, i64) {
    sqlx::query_as(OLD)
        .bind(inputs)
        .fetch_one(conn)
        .await
        .unwrap()
}
fn fingerprint(state: &InputState) -> (i64, i64, i64) {
    (state.commit, state.changes, state.tombstoned)
}
async fn move_event(owner: &mut AdmissionContext, act: sinex_model::lifecycle::Act, id: EventId) {
    let result = crate::lifecycle::move_events(owner, act, vec![id], &NoDerivations).await;
    assert!(
        matches!(result, crate::lifecycle::Ended::Ok { .. }),
        "{result:?}"
    );
}
#[devkit::test]
async fn input_state_scopes_new_commits_lifecycle_and_tombstones(db: devkit::pg::TestDatabase) {
    use sinex_model::lifecycle::Act;
    let mut owner = AdmissionContext::connect(&db.url(None)).await.unwrap();
    let inputs = kinds();
    let empty = input_state(&mut owner, &inputs).await.unwrap();
    assert_eq!(fingerprint(&empty), (0, 0, 0));
    let (related, seq) = admit(&mut owner, event("command.imported", 1)).await;
    assert!(!empty.is_current(&mut owner, &inputs).await.unwrap());
    let state = input_state(&mut owner, &inputs).await.unwrap();
    assert_eq!(state.commit, seq);
    assert_eq!(fingerprint(&state), old(&mut owner, &inputs).await);
    let (other, _) = admit(&mut owner, event("command.executed", 2)).await;
    assert!(
        state.is_current(&mut owner, &inputs).await.unwrap(),
        "unrelated captures do not invalidate"
    );
    let mixed = vec![
        "command.imported".into(),
        "command.executed".into(),
        "synthetic.absent".into(),
        "command.imported".into(),
    ];
    let mix = input_state(&mut owner, &mixed).await.unwrap();
    assert_eq!(fingerprint(&mix), old(&mut owner, &mixed).await);
    let absent = vec!["synthetic.absent".into()];
    let none = input_state(&mut owner, &absent).await.unwrap();
    assert_eq!((none.commit, none.changes), (0, 0));
    move_event(&mut owner, Act::Archive, other).await;
    assert!(state.is_current(&mut owner, &inputs).await.unwrap());
    assert!(!mix.is_current(&mut owner, &mixed).await.unwrap());
    assert!(none.is_current(&mut owner, &absent).await.unwrap());
    move_event(&mut owner, Act::Restore, other).await;
    assert!(state.is_current(&mut owner, &inputs).await.unwrap());
    move_event(&mut owner, Act::Archive, related).await;
    assert!(!state.is_current(&mut owner, &inputs).await.unwrap());
    let archived = input_state(&mut owner, &inputs).await.unwrap();
    assert_eq!(fingerprint(&archived), old(&mut owner, &inputs).await);
    assert_eq!(
        archived.commit, seq,
        "archived input remains part of the fingerprint"
    );
    move_event(&mut owner, Act::Restore, related).await;
    assert!(!archived.is_current(&mut owner, &inputs).await.unwrap());
    let mut late = event("command.imported", 3);
    late.id = EventId(uuid::Uuid::parse_str("01700000-0000-7000-8000-000000000001").unwrap());
    let restored = input_state(&mut owner, &inputs).await.unwrap();
    let (late, _) = admit(&mut owner, late).await;
    assert!(
        !restored.is_current(&mut owner, &inputs).await.unwrap(),
        "old UUID/time still has a new commit position"
    );
    move_event(&mut owner, Act::Archive, late).await;
    let before_tombstone = input_state(&mut owner, &inputs).await.unwrap();
    let absent_before = input_state(&mut owner, &absent).await.unwrap();
    move_event(&mut owner, Act::Tombstone, late).await;
    assert!(
        !before_tombstone
            .is_current(&mut owner, &inputs)
            .await
            .unwrap()
    );
    assert!(
        !absent_before.is_current(&mut owner, &absent).await.unwrap(),
        "preserve the existing global tombstone semantics"
    );
}

#[devkit::test]
async fn one_statement_boundary_cannot_hide_a_commit_during_capture(db: devkit::pg::TestDatabase) {
    let url = db.url(None);
    let mut owner = AdmissionContext::connect(&url).await.unwrap();
    admit(&mut owner, event("command.imported", 1)).await;
    let mut gate = PgConnection::connect(&url).await.unwrap();
    let key = 810257_i64;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(key)
        .execute(&mut gate)
        .await
        .unwrap();
    let mut capture = PgConnection::connect(&url).await.unwrap();
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut capture)
        .await
        .unwrap();
    // The exact production statement runs inside a gate in the SAME outer
    // statement snapshot. The gate permits a commit before it returns.
    let sql = format!(
        "WITH gate AS MATERIALIZED (SELECT pg_advisory_xact_lock({key})) SELECT state.* FROM gate CROSS JOIN LATERAL ({INITIAL}) state"
    );
    let captured = tokio::spawn(async move {
        sqlx::query_as::<_, InputState>(&sql)
            .bind(kinds())
            .persistent(false)
            .fetch_one(&mut capture)
            .await
            .unwrap()
    });
    tokio::time::timeout(std::time::Duration::from_secs(5),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND NOT granted)").bind(pid).fetch_one(&mut gate).await.unwrap();
            if waiting {break;}tokio::task::yield_now().await;
        }
    }).await.unwrap();
    let (_, new_seq) = admit(&mut owner, event("command.imported", 2)).await;
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .execute(&mut gate)
        .await
        .unwrap();
    let state = captured.await.unwrap();
    assert!(state.commit < new_seq);
    assert!(state.through_commit < new_seq);
    assert!(!state.is_current(&mut owner, &kinds()).await.unwrap());
    // The flawed two-statement capture loses precisely this relevant commit.
    let mut split = state.clone();
    split.through_commit =
        sqlx::query_scalar("SELECT value FROM core.counters WHERE name='commit_seq'")
            .fetch_one(&mut *owner)
            .await
            .unwrap();
    assert!(
        split.is_current(&mut owner, &kinds()).await.unwrap(),
        "positive control: a later boundary hides the intervening input"
    );
}

#[devkit::test]
async fn comparison_snapshot_survives_retry_and_keeps_exact_build_contract(
    db: devkit::pg::TestDatabase,
) {
    let url = db.url(None);
    let mut owner = AdmissionContext::connect(&url).await.unwrap();
    admit(&mut owner, event("command.imported", 1)).await;
    let state = input_state(&mut owner, &kinds()).await.unwrap();
    let saved = serde_json::to_vec(&state).unwrap();
    let build = "synthetic-build";
    let settings = "synthetic-day";
    let comparison=crate::derivations::store_comparison(&mut owner,&crate::derivations::StoredComparison {id:0,derivation:"synthetic".into(),version:3,range:None,made_at:Timestamp::now(),keys:1,unchanged:0,added:1,changed:0,removed:0,
        context:json!({"build":build,"inputs":state,"input_types":kinds(),"output_types":["document.text"],"kind":"per_event","history":sinex_model::DerivationHistory::Recompute,"settings":settings}),examples:json!([])}).await.unwrap();
    let mut selection = Selection {
        comparison,
        operation: uuid::Uuid::now_v7(),
        actor: "synthetic".into(),
        history: sinex_model::DerivationHistory::Recompute,
        version: 3,
        build: build.into(),
        settings: settings.into(),
        inputs: kinds(),
        outputs: vec!["document.text".into()],
        kind: "per_event".into(),
    };
    let mut tx = owner.begin().await.unwrap();
    validate(&mut tx, "synthetic", &selection).await.unwrap();
    tx.rollback().await.unwrap();
    let restored: InputState = serde_json::from_slice(&saved).unwrap();
    assert_eq!(restored, state);
    let mut reopened = PgConnection::connect(&url).await.unwrap();
    validate(&mut reopened, "synthetic", &selection)
        .await
        .unwrap();
    selection.build = "another-build".into();
    assert!(
        validate(&mut reopened, "synthetic", &selection)
            .await
            .unwrap_err()
            .to_string()
            .contains("version and build")
    );
    selection.build = build.into();
    admit(&mut owner, event("command.imported", 2)).await;
    assert!(
        validate(&mut reopened, "synthetic", &selection)
            .await
            .unwrap_err()
            .to_string()
            .contains("inputs changed")
    );
}

const INITIAL_OLD: &str = include_str!("predecessor_initial.sql");
async fn table_work(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar("SELECT coalesce(sum(seq_tup_read+idx_tup_fetch),0)::bigint FROM pg_stat_xact_user_tables WHERE relid='core.events'::regclass OR relid IN(SELECT show_chunks('core.events')) OR relid='audit.archive_changes'::regclass OR relid IN(SELECT compress_relid FROM _timescaledb_catalog.compression_settings WHERE relid IN(SELECT show_chunks('core.events')))")
        .fetch_one(conn).await.unwrap()
}
async fn state_sql(conn: &mut PgConnection, sql: &str, inputs: &[String]) -> InputState {
    sqlx::query_as(sql)
        .bind(inputs)
        .persistent(false)
        .fetch_one(conn)
        .await
        .unwrap()
}
fn has(plan: &Value, key: &str, text: &str) -> bool {
    match plan {
        Value::Object(v) => {
            v.get(key)
                .is_some_and(|v| v.as_str().is_some_and(|v| v.contains(text)))
                || v.values().any(|v| has(v, key, text))
        }
        Value::Array(v) => v.iter().any(|v| has(v, key, text)),
        _ => false,
    }
}
#[devkit::test]
async fn comparison_initial_kind_first_and_suffix_work_avoid_unrelated_history(
    db: devkit::pg::TestDatabase,
) {
    let mut checked = PgConnection::connect(&db.url(None)).await.unwrap();
    sqlx::query_file!("src/derivations/selection/input_state.sql", &kinds())
        .fetch_one(&mut checked)
        .await
        .unwrap();
    sqlx::query_file!(
        "src/derivations/selection/is_current.sql",
        &kinds(),
        0_i64,
        0_i64,
        0_i64
    )
    .fetch_one(&mut checked)
    .await
    .unwrap();
    checked.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    // Old matching records precede a long unrelated hot tail. The planner's
    // average selectivity cannot describe where that kind ends in commit
    // order; the global-feed minmax path must pass the unrelated tail.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT ('01700000-0000-7000-8000-'||lpad(to_hex(g),12,'0'))::uuid,CASE WHEN g<=16 THEN 'command.imported' ELSE 'command.executed' END,'synthetic',uuidv7(),uuidv7(),0,'unknown',g,jsonb_build_object('synthetic',repeat(md5(g::text),32)),1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8208)g;
      INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT uuidv7(),CASE WHEN g<=256 THEN 'command.imported' ELSE 'command.executed' END,'synthetic',uuidv7(),uuidv7(),0,'unknown',8208+g,jsonb_build_object('synthetic',repeat(md5(g::text),32)),1,decode(repeat('01',32),'hex'),'open' FROM generate_series(1,8448)g;
      UPDATE core.counters SET value=16656 WHERE name='commit_seq'; ANALYZE core.events").await.unwrap();
    // Frequent and mixed kinds keep exactly the same initial fingerprint.
    for inputs in [
        kinds(),
        vec!["command.executed".into()],
        vec![
            "command.imported".into(),
            "command.executed".into(),
            "command.imported".into(),
        ],
        vec!["synthetic.absent".into()],
    ] {
        let mut measured = conn.begin().await.unwrap();
        measured
            .execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
            .await
            .unwrap();
        let before = table_work(&mut measured).await;
        let a = state_sql(&mut measured, INITIAL_OLD, &inputs).await;
        let old_work = table_work(&mut measured).await - before;
        let before = table_work(&mut measured).await;
        let b = state_sql(&mut measured, INITIAL, &inputs).await;
        let kind_work = table_work(&mut measured).await - before;
        eprintln!("initial scope {inputs:?}: old={old_work}, kind-first={kind_work}");
        assert_eq!(a, b);
        measured.rollback().await.unwrap();
    }
    let old_chunk:String=sqlx::query_scalar("SELECT tableoid::regclass::text FROM core.events WHERE id='01700000-0000-7000-8000-000000000001'::uuid").fetch_one(&mut conn).await.unwrap();
    sqlx::query("SELECT compress_chunk($1::regclass)")
        .bind(old_chunk)
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
        .await
        .unwrap();
    let old_plan: Value = sqlx::query_scalar("EXPLAIN(FORMAT JSON) SELECT coalesce(max(commit_seq),0) FROM core.events WHERE event_type=ANY($1)")
        .bind(kinds())
        .persistent(false)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        has(&old_plan, "Index Name", "events_feed"),
        "old minmax must exercise the observed global feed path: {old_plan}"
    );
    let new_plan: Value = sqlx::query_scalar("EXPLAIN(FORMAT JSON) SELECT coalesce(max(commit_seq),0) FROM (SELECT commit_seq FROM core.events WHERE event_type=ANY($1) OFFSET 0) selected")
        .bind(kinds())
        .persistent(false)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert!(
        !has(&new_plan, "Node Type", "Sort")
            && !has(&new_plan, "Node Type", "Materialize")
            && !has(&new_plan, "Node Type", "CTE Scan"),
        "kind-first aggregate must stream metadata: {new_plan}"
    );
    let before = table_work(&mut tx).await;
    let baseline = state_sql(&mut tx, INITIAL_OLD, &kinds()).await;
    let old_work = table_work(&mut tx).await - before;
    let before = table_work(&mut tx).await;
    let current = state_sql(&mut tx, INITIAL, &kinds()).await;
    let new_work = table_work(&mut tx).await - before;
    eprintln!("initial fingerprint tuple visits: global-feed={old_work},kind-first={new_work}");
    assert_eq!(baseline, current);
    assert_eq!(current.commit, 8464);
    assert!(
        old_work >= 8192,
        "old must visit unrelated hot history: {old_work}"
    );
    assert!(new_work < old_work / 4, "old={old_work} new={new_work}");
    for inputs in [
        kinds(),
        vec!["command.executed".into()],
        vec!["command.imported".into(), "command.executed".into()],
        vec!["synthetic.absent".into()],
    ] {
        assert_eq!(
            state_sql(&mut tx, INITIAL_OLD, &inputs).await,
            state_sql(&mut tx, INITIAL, &inputs).await
        );
    }
    tx.commit().await.unwrap();
    let baseline = input_state(&mut conn, &kinds()).await.unwrap();
    // New unrelated rows and changes exercise both filtered suffixes. They
    // must neither invalidate the comparison nor reopen compressed history.
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class)
      SELECT uuidv7(),'command.executed','synthetic',uuidv7(),uuidv7(),0,'unknown',16656+g,'{}',1,decode(repeat('02',32),'hex'),'open' FROM generate_series(1,128)g;
      INSERT INTO audit.archive_changes(change_seq,event_id,commit_seq,archived,reason)
      SELECT row_number() OVER(),id,16785,true,'archive' FROM core.events WHERE commit_seq>16656;
      UPDATE core.counters SET value=16785 WHERE name='commit_seq'").await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    tx.execute("SET LOCAL max_parallel_workers_per_gather=0; SET LOCAL jit=off")
        .await
        .unwrap();
    let before = table_work(&mut tx).await;
    for _ in 0..8 {
        assert_eq!(fingerprint(&baseline), old(&mut tx, &kinds()).await);
    }
    let repeated = table_work(&mut tx).await - before;
    let before = table_work(&mut tx).await;
    for _ in 0..8 {
        assert!(baseline.is_current(&mut tx, &kinds()).await.unwrap());
    }
    let suffix = table_work(&mut tx).await - before;
    eprintln!("validation tuple visits over8: repeated={repeated},filtered-suffix={suffix}");
    assert!(
        repeated >= 8192 * 8,
        "old must re-read unrelated hot history: {repeated}"
    );
    assert!(
        suffix <= 128 * 8 * 4,
        "suffix query must stay within newer rows/point lookups: {suffix}"
    );
    assert!(repeated > suffix * 4, "old={repeated} new={suffix}");
    tx.rollback().await.unwrap();
}

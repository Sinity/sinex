//! Unit runs keyed by unit scope: one unit event recomputes one key, a boot
//! wakes the open runs of its host, and the day keys they left compute to
//! nothing.
use super::*;
use sinex_derivations::state_interval::StateIntervals;
use sinex_derivations::unit_interval::UnitIntervals;

fn unit_keys() -> Vec<Box<dyn AnyKeyed>> {
    vec![Box::new(UnitIntervals)]
}

fn journal(kind: &str, seconds: u64, mut payload: Value) -> Event {
    payload["cursor"] = json!(format!("{kind}/{seconds}"));
    payload["logged_at"] = json!(at(seconds as i64));
    payload["started_at"] = json!(at(seconds as i64));
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "machine.journald",
        location(seconds),
        ReadingOrder(0),
        *blake3::hash(payload.to_string().as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

fn unit(kind: &str, seconds: u64, host: Option<&str>, unit: &str, invocation: &str) -> Event {
    journal(
        kind,
        seconds,
        json!({"host": host, "boot_id": "boot-a", "unit": unit, "manager": "system", "uid": 0,
               "invocation_id": invocation, "outcome": "stopped"}),
    )
}

fn boot(seconds: u64, host: &str, boot: &str) -> Event {
    journal(
        "boot.started",
        seconds,
        json!({"host": host, "boot_id": boot}),
    )
}

/// Each live unit interval's reading and payload, by unit name.
async fn runs(w: &World) -> BTreeMap<String, (uuid::Uuid, StateInterval)> {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let rows: Vec<(uuid::Uuid, Value)> = sqlx::query_as(
        "SELECT id, payload FROM core.events e WHERE event_type='state.interval'
           AND source='unit.intervals'
           AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    rows.into_iter()
        .map(|(id, payload)| {
            let interval: StateInterval = serde_json::from_value(payload).unwrap();
            (interval.label.clone().unwrap(), (id, interval))
        })
        .collect()
}

async fn keys(w: &World, table: &str) -> Vec<Value> {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    sqlx::query_scalar(&format!(
        "SELECT key FROM derivation.{table} WHERE derivation='unit.intervals' ORDER BY key"
    ))
    .fetch_all(&mut conn)
    .await
    .unwrap()
}

#[devkit::test]
async fn a_unit_event_recomputes_only_its_own_scope(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, unit_keys()).await;
    let mut runner = w.driver_with(unit_keys(), vec![]).await;
    w.admit(vec![
        unit("unit.started", 1, Some("box"), "a.service", "run-a"),
        unit("unit.started", 2, Some("box"), "b.service", "run-b"),
        unit("unit.started", 3, None, "c.service", "run-c"),
    ])
    .await;
    runner.catch_up().await.unwrap();
    let before = runs(&w).await;
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        ["a.service", "b.service"],
        "the record that names no host has no key and opens no interval"
    );
    assert_eq!(keys(&w, "waiting").await.len(), 2, "the two open runs wait");
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.inputs WHERE derivation='unit.intervals'")
            .await,
        2
    );
    assert_eq!(
        w.count("SELECT count(*) FROM core.live_intervals WHERE ended_at IS NULL")
            .await,
        2
    );

    // A stop a day later names one scope, whatever day its run started on.
    let stop = unit("unit.stopped", 86_400, Some("box"), "a.service", "run-a");
    w.admit(vec![stop.clone()]).await;
    let dirty = keys(&w, "dirty").await;
    assert_eq!(dirty.len(), 1, "{dirty:?}");
    assert_eq!(dirty[0]["unit"], "a.service");
    runner.catch_up().await.unwrap();
    let after = runs(&w).await;
    assert_eq!(after["a.service"].1.ended_at, Some(at(86_400)));
    assert_eq!(
        after["a.service"]
            .1
            .ended_at_evidence
            .as_ref()
            .unwrap()
            .parent,
        stop.id
    );
    assert_eq!(after["b.service"], before["b.service"], "the same reading");
    assert_eq!(
        keys(&w, "waiting").await.len(),
        1,
        "a closed run waits for nothing"
    );

    // Archiving the stop reopens that run alone.
    w.moved(Act::Archive, vec![stop.id]).await;
    runner.catch_up().await.unwrap();
    let reopened = runs(&w).await;
    assert_eq!(reopened["a.service"].1, before["a.service"].1);
    assert_eq!(reopened["b.service"], before["b.service"]);
    runner.end_runs().await.unwrap();
}

#[devkit::test]
async fn a_boot_fences_the_open_runs_of_its_host_once(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, unit_keys()).await;
    let mut runner = w.driver_with(unit_keys(), vec![]).await;
    w.admit(vec![
        boot(0, "box", "boot-a"),
        unit("unit.started", 1, Some("box"), "a.service", "run-a"),
        unit("unit.started", 2, Some("box"), "b.service", "run-b"),
        unit("unit.started", 3, Some("other"), "c.service", "run-c"),
    ])
    .await;
    runner.catch_up().await.unwrap();
    assert_eq!(keys(&w, "waiting").await.len(), 3);

    w.admit(vec![boot(3600, "box", "boot-b")]).await;
    runner.catch_up().await.unwrap();
    let fenced = runs(&w).await;
    for unit in ["a.service", "b.service"] {
        assert_eq!(fenced[unit].1.fenced_at, Some(at(3600)), "{unit}");
        assert_eq!(fenced[unit].1.ended_at, None, "{unit}");
    }
    // A fenced run reaches no further than its fence, so the one run still
    // open is the only one a later window is handed.
    assert_eq!(
        w.count("SELECT count(*) FROM core.live_intervals WHERE ended_at IS NULL")
            .await,
        1
    );
    assert_eq!(
        w.count(&format!(
            "SELECT count(*) FROM core.live_intervals WHERE ended_at = to_timestamp({})",
            at(3600).0.unix_timestamp()
        ))
        .await,
        2
    );
    assert_eq!(fenced["c.service"].1.fenced_at, None, "another host's boot");
    let waiting = keys(&w, "waiting").await;
    assert_eq!(waiting.len(), 1, "{waiting:?}");
    assert_eq!(waiting[0]["host"], "other");
    assert!(keys(&w, "dirty").await.is_empty());

    // A fenced run is no longer waiting: a later boot leaves it alone.
    w.admit(vec![boot(7200, "box", "boot-c")]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(runs(&w).await, fenced, "the same readings");
    assert!(keys(&w, "dirty").await.is_empty());
    runner.end_runs().await.unwrap();
}

#[devkit::test]
async fn a_recompute_leaves_nothing_under_the_day_keys_unit_runs_had(db: devkit::pg::TestDatabase) {
    let state_keys = || -> Vec<Box<dyn AnyKeyed>> { vec![Box::new(StateIntervals)] };
    let w = World::with_keys(db, state_keys()).await;
    let mut runner = w.driver_with(state_keys(), vec![]).await;
    // An output, a waiting row and a dirty mark under a day key of the
    // earlier shape, as the earlier version left them.
    let start = unit("unit.started", 1, Some("box"), "a.service", "run-a");
    w.admit(vec![start.clone()]).await;
    let old_key =
        json!({"state":"machine.unit","day":"2024-06-15","host":"box","unknown_record":null});
    let old_output = sinex_derivations::derived_event::derived(
        &StateInterval {
            interval_id: "machine.unit:fixture".into(),
            state_kind: "machine.unit".into(),
            subject_id: None,
            label: Some("a.service".into()),
            started_at: at(1),
            ended_at: None,
            started_at_evidence: None,
            ended_at_evidence: None,
            fenced_at: None,
            start_event_type: "unit.started".into(),
            end_event_type: None,
            attributes: Default::default(),
        },
        StateIntervals::NAME,
        [&start],
    )
    .unwrap();
    let batch = Batch {
        run: runner.run_of(StateIntervals::NAME).unwrap(),
        kind: BatchKind::Derived {
            derivation: StateIntervals::NAME.into(),
            scope: DerivedScope::Keys {
                keys: vec![ComputedKey {
                    key: old_key.clone(),
                    dirty_seq: 0,
                    waiting: true,
                    refresh_at: None,
                    woken_by: None,
                }],
                read: Default::default(),
            }
            .to_value(),
        },
        records: vec![RecordOutcome::Events {
            location: None,
            events: vec![old_output],
        }],
        archive: vec![],
    };
    w.handle.submit(Priority::Derived, batch).await.unwrap();
    let old = "SELECT count(*) FROM core.events e WHERE source='state.intervals'
        AND payload->>'state_kind'='machine.unit'
        AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)";
    assert_eq!(w.count(old).await, 1);
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.waiting WHERE derivation='state.intervals'")
            .await,
        1
    );

    assert_eq!(
        runner
            .recompute_all(StateIntervals::NAME, None)
            .await
            .unwrap(),
        1,
        "the stored day key is the one key: unit events name none in this derivation"
    );
    runner.catch_up().await.unwrap();
    assert_eq!(w.count(old).await, 0, "its output is archived");
    for table in ["dirty", "waiting"] {
        assert_eq!(
            w.count(&format!(
                "SELECT count(*) FROM derivation.{table} WHERE derivation='state.intervals'"
            ))
            .await,
            0,
            "{table}"
        );
    }
    runner.end_runs().await.unwrap();
}

#[devkit::test]
async fn a_later_boot_does_not_revisit_days_whose_boot_is_already_fenced(
    db: devkit::pg::TestDatabase,
) {
    let state_keys = || -> Vec<Box<dyn AnyKeyed>> { vec![Box::new(StateIntervals)] };
    let w = World::with_keys(db, state_keys()).await;
    let mut runner = w.driver_with(state_keys(), vec![]).await;
    runner.clock = Some(at(10 * 86_400));
    w.admit(vec![boot(0, "box", "boot-a")]).await;
    runner.catch_up().await.unwrap();
    w.admit(vec![boot(86_400, "box", "boot-b")]).await;
    runner.catch_up().await.unwrap();
    let boots: Vec<StateInterval> = w.live().await;
    assert_eq!(boots[0].fenced_at, Some(at(86_400)), "{boots:?}");

    // The third boot fences the second. The first day's boot is already
    // fenced, so its key has nothing to learn.
    w.admit(vec![boot(2 * 86_400, "box", "boot-c")]).await;
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let dirty: Vec<Value> = sqlx::query_scalar(
        "SELECT key FROM derivation.dirty WHERE derivation='state.intervals'
           AND key->>'state'='machine.boot' ORDER BY key->>'day'",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    let days: Vec<_> = dirty.iter().map(|key| key["day"].clone()).collect();
    assert_eq!(
        days,
        [json!("2024-06-16"), json!("2024-06-17")],
        "{dirty:?}"
    );
    runner.end_runs().await.unwrap();
}

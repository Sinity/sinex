//! Refused work is visible and stable until its inputs or explicit replay
//! changes. Other keys still finish, and previous products remain readable.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Fault modes: 0 produces valid output, 1 injects invalid JSONB after the
/// native builder (the writer's final boundary), 2 exercises the real fallible
/// native builder. The input population, keys and producer version stay fixed.
struct Refusing<const V: u32, const KEEP: bool> {
    mode: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl<const V: u32, const KEEP: bool> Keyed for Refusing<V, KEEP> {
    type Key = Value;
    const NAME: &'static str = "test.refusing";
    const VERSION: u32 = V;
    const HISTORY: sinex_model::DerivationHistory = if KEEP {
        sinex_model::DerivationHistory::KeepHistory
    } else {
        sinex_model::DerivationHistory::Recompute
    };
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];
    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Value> {
        vec![input.payload["command"].clone()]
    }
    fn inputs(&self, key: &Value, _: &Env) -> Option<InputQuery> {
        Some(InputQuery::window(Self::INPUTS, at(-86_400), at(86_400)).only("command", key.clone()))
    }
    fn compute(&self, key: &Value, inputs: &[Event], _: &Env) -> Computed {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if inputs.is_empty() {
            return Computed::default();
        }
        let bad = key == "bad";
        let mode = self.mode.load(Ordering::SeqCst);
        let payload = CommandFused {
            record: key.as_str().unwrap().into(),
            command: if bad && mode == 2 {
                "private\0value".into()
            } else {
                format!("{}={}", key.as_str().unwrap(), inputs.len())
            },
            host: "fixture".into(),
            started_at: at(0),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            execution: None,
            members: vec![],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        };
        let output = sinex_derivations::derived_event::derived(&payload, Self::NAME, inputs).map(
            |mut event| {
                if bad && mode == 1 {
                    event.payload["command"] = json!("private\0value");
                }
                event
            },
        );
        Computed::from_outputs([output], false)
    }
}

fn producer<const V: u32, const KEEP: bool>(
    mode: &Arc<AtomicUsize>,
    calls: &Arc<AtomicUsize>,
) -> Box<dyn AnyKeyed> {
    Box::new(Refusing::<V, KEEP> {
        mode: mode.clone(),
        calls: calls.clone(),
    })
}
async fn products(world: &World) -> Vec<String> {
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    sqlx::query_scalar("SELECT payload->>'command' FROM core.events e WHERE source='test.refusing' AND EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id) ORDER BY payload->>'record'")
        .fetch_all(&mut conn).await.unwrap()
}
async fn diagnosis(world: &World) -> Vec<sinex_store::diagnostics::DerivationRefusal> {
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    sinex_store::diagnostics::derivation_refusals(&mut conn)
        .await
        .unwrap()
}

#[devkit::test]
async fn data_refusal_marks_key_failed_and_runner_continues(db: devkit::pg::TestDatabase) {
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![producer::<1, false>(&mode, &calls)]).await;
    world
        .admit(vec![zsh("bad", 0, 0), zsh("good", 1, 10)])
        .await;
    let mut runner = world
        .driver_with(vec![producer::<1, false>(&mode, &calls)], vec![])
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(products(&world).await, ["bad=1", "good=1"]);
    mode.store(1, Ordering::SeqCst);
    world
        .admit(vec![zsh("bad", 2, 20), zsh("good", 3, 30)])
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        products(&world).await,
        ["bad=1", "good=2"],
        "failed key retains its exact previous output; good key finished"
    );
    let failed = diagnosis(&world).await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].derivation, "test.refusing");
    assert_eq!(failed[0].failed, 1);
    assert_eq!(
        failed[0].latest_reason,
        "database refused invalid data (22P05)"
    );
    assert!(!failed[0].latest_reason.contains("private"));
    assert!(
        !world._writer.is_finished(),
        "the admission writer remains running"
    );
    let attempted = calls.load(Ordering::SeqCst);
    assert_eq!(runner.pass().await.unwrap(), 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        attempted,
        "unchanged bad input is not recomputed"
    );
    drop(runner);
    let mut runner = world
        .driver_with(vec![producer::<1, false>(&mode, &calls)], vec![])
        .await;
    assert_eq!(runner.pass().await.unwrap(), 0);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        attempted,
        "restart preserves terminal refusal"
    );
    mode.store(0, Ordering::SeqCst);
    let head = world
        .count("SELECT value FROM core.counters WHERE name='commit_seq'")
        .await;
    assert_eq!(
        runner.recompute_all("test.refusing", None).await.unwrap(),
        2
    );
    assert_eq!(
        world
            .count("SELECT value FROM core.counters WHERE name='commit_seq'")
            .await,
        head,
        "explicit retry works at the same head"
    );
    runner.catch_up().await.unwrap();
    assert_eq!(products(&world).await, ["bad=2", "good=2"]);
    assert!(diagnosis(&world).await.is_empty());
    // A fresh relevant input, without a replay command, also re-enables work.
    mode.store(1, Ordering::SeqCst);
    world.admit(vec![zsh("bad", 4, 40)]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(diagnosis(&world).await[0].failed, 1);
    mode.store(0, Ordering::SeqCst);
    world.admit(vec![zsh("bad", 5, 50)]).await;
    runner.catch_up().await.unwrap();
    assert_eq!(products(&world).await, ["bad=4", "good=2"]);
    assert!(diagnosis(&world).await.is_empty());
    assert_eq!(
        world.count("SELECT count(*) FROM raw.failures").await,
        0,
        "no fabricated source failure coordinates"
    );
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE event_type='command.imported'")
            .await,
        6,
        "original input readings stay retained"
    );
}

#[devkit::test]
async fn native_build_refusal_records_the_field_and_finishes_other_keys(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![producer::<1, false>(&mode, &calls)]).await;
    world
        .admit(vec![zsh("bad", 0, 0), zsh("good", 1, 10)])
        .await;
    let mut runner = world
        .driver_with(vec![producer::<1, false>(&mode, &calls)], vec![])
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(products(&world).await, ["good=1"]);
    let failed = diagnosis(&world).await;
    assert_eq!(failed.len(), 1);
    assert_eq!(
        failed[0].latest_reason,
        "unstorable_character: NUL in payload at \"/command\""
    );
    assert!(!failed[0].latest_reason.contains("private"));
    assert_eq!(runner.pass().await.unwrap(), 0);
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    let runs_before = world
        .count("SELECT count(*) FROM core.runs WHERE name='test.refusing'")
        .await;
    let attempted = calls.load(Ordering::SeqCst);
    let direct = producer::<1, false>(&mode, &calls);
    let failure = compute_now(
        &mut conn,
        &world.handle,
        direct.as_ref(),
        vec![json!("bad")],
        "fixture",
        TimeSettings::new("Europe/Warsaw", 4, 0).unwrap(),
    )
    .await
    .unwrap_err()
    .failure();
    assert_eq!(failure.kind, command_surface::FailureKind::Refused);
    assert!(failure.message.contains("unstorable_character"));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        attempted,
        "automatic product reads must not recompute unchanged refused keys"
    );
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.runs WHERE name='test.refusing'")
            .await,
        runs_before
    );
    let readings = sinex_store::telemetry::read(&mut conn, None).await.unwrap();
    assert_eq!(
        readings
            .iter()
            .find(|r| r.component == "derivation" && r.name == "test.refusing")
            .unwrap()
            .measures["refused_work"],
        1.0
    );
}

struct PerInputRefusing {
    mode: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl sinex_derivations::PerEvent for PerInputRefusing {
    const NAME: &'static str = "test.refusing.event";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["command.imported"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];
    fn derive(
        &self,
        input: sinex_derivations::Input<'_>,
        _: &Env,
    ) -> std::result::Result<Vec<Event>, sinex_derivations::ComputationError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let command = input.event.payload["command"].as_str().unwrap();
        let bad = command == "bad";
        let mode = self.mode.load(Ordering::SeqCst);
        if bad && mode == 3 {
            return Err(sinex_derivations::ComputationError::Unavailable(
                "synthetic reader unavailable".into(),
            ));
        }
        let payload = CommandFused {
            record: sinex_model::record::identity(input.event),
            command: if bad && mode == 2 {
                "private\0value".into()
            } else {
                command.to_uppercase()
            },
            host: "fixture".into(),
            started_at: at(0),
            cwd: None,
            session: None,
            duration_ns: None,
            exit_code: None,
            execution: None,
            members: vec![],
            basis: sinex_model::event_types::fused::Basis::Rule,
            ambiguous: 1,
            competitors: vec![],
            rule_version: 1,
        };
        let mut output =
            sinex_derivations::derived_event::derived(&payload, Self::NAME, [input.event])?;
        if bad && mode == 1 {
            output.payload["command"] = json!("private\0value");
        }
        Ok(vec![output])
    }
}
fn per_input(mode: &Arc<AtomicUsize>, calls: &Arc<AtomicUsize>) -> Box<dyn AnyPerEvent> {
    Box::new(PerInputRefusing {
        mode: mode.clone(),
        calls: calls.clone(),
    })
}

#[devkit::test]
async fn per_event_data_refusal_records_input_before_advancing_and_replays_normally(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![]).await;
    let bad = zsh("bad", 0, 0);
    let bad_id = bad.id;
    world.admit(vec![bad, zsh("good", 1, 10)]).await;
    let input_head = world
        .count("SELECT max(commit_seq) FROM core.events WHERE event_type='command.imported'")
        .await;
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        world
            .count(
                "SELECT commit_seq FROM derivation.positions WHERE derivation='test.refusing.event'"
            )
            .await,
        input_head
    );
    assert_eq!(world.count("SELECT count(*) FROM core.events e WHERE source='test.refusing.event' AND EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)").await,1);
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    let (input, reason): (Value, String) = sqlx::query_as(
        "SELECT input,reason FROM derivation.refusals WHERE derivation='test.refusing.event'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(input, json!({"input":bad_id}));
    assert_eq!(reason, "database refused invalid data (22P05)");
    assert!(!reason.contains("private"));
    let attempts = calls.load(Ordering::SeqCst);
    assert_eq!(runner.pass().await.unwrap(), 0);
    drop(runner);
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    assert_eq!(runner.pass().await.unwrap(), 0);
    assert_eq!(calls.load(Ordering::SeqCst), attempts);
    // A genuine explicit recompute switch rewinds this population and clears
    // its failed inputs. Comparison alone is read-only and preserves failures.
    mode.store(0, Ordering::SeqCst);
    let compared = runner
        .compare("test.refusing.event", None, 3)
        .await
        .unwrap();
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        1
    );
    runner
        .switch("test.refusing.event", compared.id.unwrap(), "fixture")
        .await
        .unwrap();
    runner.catch_up().await.unwrap();
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        0
    );
    assert_eq!(world.count("SELECT count(*) FROM core.events e WHERE source='test.refusing.event' AND EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)").await,2);
}

#[devkit::test]
async fn temporary_input_failure_keeps_progress_and_creates_no_terminal_refusal(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(3));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![]).await;
    world
        .admit(vec![zsh("good", 0, 0), zsh("bad", 1, 10)])
        .await;
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    let failure = runner.pass().await.unwrap_err().failure();
    assert!(failure.retryable);
    assert_eq!(
        failure.code.as_deref(),
        Some("derivation_input_unavailable")
    );
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        0
    );
    assert_eq!(world.count("SELECT coalesce(max(commit_seq),0) FROM derivation.positions WHERE derivation='test.refusing.event'").await,0);
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE source='test.refusing.event'")
            .await,
        0,
        "an uncommitted good prefix is not falsely reported finished"
    );
    mode.store(0, Ordering::SeqCst);
    runner.catch_up().await.unwrap();
    assert_eq!(world.count("SELECT count(*) FROM core.events e WHERE source='test.refusing.event' AND EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)").await,2);
}

/// Inject a real PostgreSQL write error at the transaction boundary. The
/// fixture owns its database; capture inputs are already committed.
async fn refuse_writes(world: &World, table: &str) {
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    let filter = if table == "core.events" {
        "WHEN (NEW.source='test.refusing.event')"
    } else {
        ""
    };
    let sql = format!(
        "CREATE FUNCTION fixture_refuse_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING ERRCODE='40001', MESSAGE='private synthetic database detail'; END $$; CREATE TRIGGER fixture_refuse_write BEFORE INSERT ON {table} FOR EACH ROW {filter} EXECUTE FUNCTION fixture_refuse_write()"
    );
    sqlx::raw_sql(&sql).execute(&mut conn).await.unwrap();
}
async fn allow_writes(world: &World, table: &str) {
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    sqlx::raw_sql(&format!(
        "DROP TRIGGER fixture_refuse_write ON {table}; DROP FUNCTION fixture_refuse_write()"
    ))
    .execute(&mut conn)
    .await
    .unwrap();
}

#[devkit::test]
async fn temporary_output_write_does_not_refuse_inputs_or_advance_progress(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![]).await;
    world
        .admit(vec![zsh("good", 0, 0), zsh("bad", 1, 10)])
        .await;
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    refuse_writes(&world, "core.events").await;
    let failure = runner.pass().await.unwrap_err().failure();
    assert!(failure.retryable);
    assert_eq!(failure.code.as_deref(), Some("database_unavailable"));
    assert!(!failure.message.contains("private synthetic"));
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        0
    );
    assert_eq!(world.count("SELECT coalesce(max(commit_seq),0) FROM derivation.positions WHERE derivation='test.refusing.event'").await,0);
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE source='test.refusing.event'")
            .await,
        0
    );
    allow_writes(&world, "core.events").await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE source='test.refusing.event'")
            .await,
        2
    );
}

#[devkit::test]
async fn failed_diagnosis_write_leaves_input_unhandled_until_retry(db: devkit::pg::TestDatabase) {
    let mode = Arc::new(AtomicUsize::new(1));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![]).await;
    world
        .admit(vec![zsh("bad", 0, 0), zsh("good", 1, 10)])
        .await;
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    refuse_writes(&world, "derivation.refusals").await;
    let failure = runner.pass().await.unwrap_err().failure();
    assert!(failure.retryable);
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        0
    );
    assert_eq!(world.count("SELECT coalesce(max(commit_seq),0) FROM derivation.positions WHERE derivation='test.refusing.event'").await,0);
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE source='test.refusing.event'")
            .await,
        0
    );
    allow_writes(&world, "derivation.refusals").await;
    runner.catch_up().await.unwrap();
    assert_eq!(
        world
            .count("SELECT count(*) FROM derivation.refusals")
            .await,
        1
    );
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE source='test.refusing.event'")
            .await,
        1
    );
}

#[devkit::test]
async fn archived_input_loses_its_current_refusal_and_explicit_replay_retries(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![]).await;
    let input = zsh("bad", 0, 0);
    let id = input.id;
    world.admit(vec![input]).await;
    let mut runner = world
        .driver_with(vec![], vec![per_input(&mode, &calls)])
        .await;
    runner.catch_up().await.unwrap();
    assert_eq!(diagnosis(&world).await[0].failed, 1);
    world.moved(Act::Archive, vec![id]).await;
    assert!(diagnosis(&world).await.is_empty());
    assert_eq!(
        world
            .count("SELECT count(*) FROM core.events WHERE event_type='command.imported'")
            .await,
        1,
        "archiving retains the original reading"
    );
    mode.store(0, Ordering::SeqCst);
    world.moved(Act::Restore, vec![id]).await;
    let compared = runner
        .compare("test.refusing.event", None, 3)
        .await
        .unwrap();
    runner
        .switch("test.refusing.event", compared.id.unwrap(), "fixture")
        .await
        .unwrap();
    runner.catch_up().await.unwrap();
    assert_eq!(world.count("SELECT count(*) FROM core.events e WHERE source='test.refusing.event' AND EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)").await,1);
}

#[devkit::test]
async fn keep_history_switch_preserves_refusals_until_explicit_recompute(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(2));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![producer::<1, true>(&mode, &calls)]).await;
    world.admit(vec![zsh("bad", 0, 0)]).await;
    let mut old = world
        .driver_with(vec![producer::<1, true>(&mode, &calls)], vec![])
        .await;
    old.catch_up().await.unwrap();
    assert_eq!(diagnosis(&world).await[0].failed, 1);
    drop(old);
    mode.store(0, Ordering::SeqCst);
    let mut next = world
        .driver_with(vec![producer::<2, true>(&mode, &calls)], vec![])
        .await;
    let comparison = next.compare("test.refusing", None, 3).await.unwrap();
    assert_eq!(
        diagnosis(&world).await[0].failed,
        1,
        "comparison is read-only"
    );
    next.switch("test.refusing", comparison.id.unwrap(), "fixture")
        .await
        .unwrap();
    assert_eq!(
        next.pass().await.unwrap(),
        0,
        "keep-history does not replay a failed old population"
    );
    assert_eq!(diagnosis(&world).await[0].failed, 1);
    assert_eq!(next.recompute_all("test.refusing", None).await.unwrap(), 1);
    next.catch_up().await.unwrap();
    assert!(diagnosis(&world).await.is_empty());
    assert_eq!(products(&world).await, ["bad=1"]);
}

#[devkit::test]
async fn queued_computation_rechecks_refusal_after_acquiring_the_lock(
    db: devkit::pg::TestDatabase,
) {
    let mode = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let world = World::with_keys(db, vec![producer::<1, false>(&mode, &calls)]).await;
    world
        .admit(vec![zsh("bad", 0, 0), zsh("good", 1, 10)])
        .await;
    let runner = world
        .driver_with(vec![producer::<1, false>(&mode, &calls)], vec![])
        .await;
    let run = runner.run_of("test.refusing").unwrap();
    let derivation = producer::<1, false>(&mode, &calls);
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    let read = ReadMark::now(&mut conn).await.unwrap();
    let head = derivations::head(&mut conn).await.unwrap();
    let mut counts = Counts::default();
    let mut computation = KeyComputation {
        read: &mut conn,
        writer: &world.handle,
        derivation: derivation.as_ref(),
        time_settings: runner.time_settings.clone(),
        run,
        counts: &mut counts,
    };
    let lock = world.handle.lock_derivations().await;
    let env = runner.env();
    let mut queued =
        Box::pin(computation.compute(vec![(json!("bad"), head), (json!("good"), head)], &env));
    assert!(futures::poll!(&mut queued).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    // This is the preceding computation's actual admitted diagnosis, not a
    // changed input or a later command requesting a retry.
    admit_refusals(
        &world.handle,
        "test.refusing",
        run,
        vec![RefusedWork {
            input: RefusedInput::Key(json!("bad")),
            input_seq: head,
            reason: "synthetic invalid key".into(),
        }],
        None,
        read,
    )
    .await
    .unwrap();
    drop(lock);
    queued.await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the still-eligible good key computes"
    );
    assert_eq!(counts.failed, 1);
    assert_eq!(products(&world).await, ["good=1"]);
    assert_eq!(diagnosis(&world).await[0].failed, 1);
}

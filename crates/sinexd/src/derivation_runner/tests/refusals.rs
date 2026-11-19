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
    assert_eq!(failed[0].reason, "database refused invalid data (22P05)");
    assert!(!failed[0].reason.contains("private"));
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
        failed[0].reason,
        "unstorable_character: NUL in payload at \"/command\""
    );
    assert!(!failed[0].reason.contains("private"));
    assert_eq!(runner.pass().await.unwrap(), 0);
    let mut conn = PgConnection::connect(&world.url).await.unwrap();
    let readings = sinex_store::telemetry::read(&mut conn).await.unwrap();
    assert_eq!(
        readings
            .iter()
            .find(|r| r.component == "derivation" && r.name == "test.refusing")
            .unwrap()
            .measures["refused_work"],
        1.0
    );
}

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct ObservedInputs(Arc<AtomicBool>);

impl Keyed for ObservedInputs {
    const NAME: &'static str = "library.items";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["library.copy", "library.preview"];
    const OUTPUTS: &'static [&'static str] = &["library.item", "library.rendition"];

    fn keys_for(&self, input: &KeyView<'_>, env: &Env) -> Vec<Value> {
        Keyed::keys_for(&sinex_derivations::library::LibraryItems, input, env)
    }

    fn inputs(&self, key: &Value, env: &Env) -> Option<InputQuery> {
        self.0.store(true, Ordering::SeqCst);
        Keyed::inputs(&sinex_derivations::library::LibraryItems, key, env)
    }

    fn compute(&self, key: &Value, inputs: &[Event], env: &Env) -> Computed {
        Keyed::compute(&sinex_derivations::library::LibraryItems, key, inputs, env)
    }

    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}

#[devkit::test]
async fn keyed_computations_wait_before_reading_inputs(db: devkit::pg::TestDatabase) {
    let w = World::new(db).await;
    let mut read = PgConnection::connect(&w.url).await.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let derivation = ObservedInputs(called.clone());
    let mut counts = Counts::default();
    let guard = w.handle.lock_derivations().await;
    let mut work = KeyComputation {
        read: &mut read,
        writer: &w.handle,
        derivation: &derivation,
        run: RunId::new(),
        counts: &mut counts,
    };
    let env = Env::at(at(0));
    let mut computation = Box::pin(work.compute(vec![(json!("item"), 0, false)], &env));
    assert!(futures::poll!(&mut computation).is_pending());
    assert!(!called.load(Ordering::SeqCst));
    drop(guard);
    computation.await.unwrap();
    assert!(called.load(Ordering::SeqCst));
    assert_eq!(work.counts.keys, 1);
}

#[devkit::test]
async fn a_failed_command_computation_ends_its_run(db: devkit::pg::TestDatabase) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    let (writer, queues) = writer::channels();
    drop(queues);
    let result = compute_now(
        &mut conn,
        &writer,
        &sinex_derivations::library::LibraryItems,
        vec![json!("item")],
        "h",
    )
    .await;
    assert!(result.is_err());
    let counts: (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE ended_at IS NULL) FROM core.runs WHERE name = 'library.items'"
    ).fetch_one(&mut conn).await.unwrap();
    assert_eq!(counts, (1, 0));
}

//! Keys read by their stored inputs, and waiting keys that name what wakes
//! them.
use super::*;
use sinex_model::event_types::operator::LogEntry;
use std::sync::atomic::{AtomicUsize, Ordering};

fn entry(label: &str, text: &str, record_offset: u64) -> Event {
    entry_at(label, text, record_offset, None)
}

fn entry_at(label: &str, text: &str, record_offset: u64, written_at: Option<Timestamp>) -> Event {
    Event::build(
        &LogEntry {
            written_at,
            label: Some(label.into()),
            text: text.into(),
        },
        "fixture.entries",
        location(record_offset),
        ReadingOrder(1),
        *blake3::hash(format!("{label}{text}").as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

/// One key per entry label. The key has no query: every input it computes
/// from was read through its stored list.
struct ByLabel;

impl Keyed for ByLabel {
    type Key = Value;
    const NAME: &'static str = "fixture.by_label";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["operator_log.entry"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Self::Key> {
        vec![json!({"label": input.payload["label"]})]
    }
    fn stores_inputs(&self, _: &Self::Key) -> bool {
        true
    }
    fn inputs(&self, _: &Self::Key, _: &Env) -> Option<InputQuery> {
        None
    }
    fn compute(&self, key: &Self::Key, inputs: &[Event], _: &Env) -> Computed {
        if inputs.is_empty() {
            return Computed::default();
        }
        let mut texts: Vec<_> = inputs
            .iter()
            .map(|event| event.payload["text"].as_str().unwrap())
            .collect();
        texts.sort_unstable();
        Computed {
            outputs: vec![fused_output(
                Self::NAME,
                key["label"].as_str().unwrap(),
                texts.join("/"),
                inputs,
            )],
            ..Computed::default()
        }
    }
}

/// Each label's fused output as `label=texts`.
async fn by_label(w: &World) -> Vec<String> {
    let mut fused: Vec<_> = w
        .live::<CommandFused>()
        .await
        .into_iter()
        .map(|fused| format!("{}={}", fused.record, fused.command))
        .collect();
    fused.sort();
    fused
}

#[devkit::test]
async fn a_key_reads_the_events_admission_stored_for_it(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(ByLabel)]).await;
    let mut d = w.driver_with(vec![Box::new(ByLabel)], vec![]).await;

    w.admit(vec![entry("x", "a", 1)]).await;
    d.catch_up().await.unwrap();
    assert_eq!(by_label(&w).await, ["x=a"]);

    // A later batch adds to the same key and opens another.
    let b = entry("x", "b", 2);
    w.admit(vec![b.clone(), entry("y", "c", 3)]).await;
    d.catch_up().await.unwrap();
    assert_eq!(by_label(&w).await, ["x=a/b", "y=c"]);
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.inputs WHERE derivation='fixture.by_label'")
            .await,
        3
    );

    // An archived input leaves the key's inputs and a restored one returns,
    // through the same stored row.
    w.moved(Act::Archive, vec![b.id]).await;
    d.catch_up().await.unwrap();
    assert_eq!(by_label(&w).await, ["x=a", "y=c"]);
    w.moved(Act::Restore, vec![b.id]).await;
    d.catch_up().await.unwrap();
    assert_eq!(by_label(&w).await, ["x=a/b", "y=c"]);
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.inputs WHERE derivation='fixture.by_label'")
            .await,
        3,
        "archive and restore add no row"
    );
}

#[devkit::test]
async fn a_recompute_fills_the_lists_of_events_admitted_before_the_declaration(
    db: devkit::pg::TestDatabase,
) {
    // The writer knows no derivation: these events get no stored row.
    let w = World::with_keys(db, vec![]).await;
    w.admit(vec![
        entry("x", "a", 1),
        entry("x", "b", 2),
        entry("y", "c", 3),
    ])
    .await;
    assert_eq!(w.count("SELECT count(*) FROM derivation.inputs").await, 0);

    let mut d = w.driver_with(vec![Box::new(ByLabel)], vec![]).await;
    let comparison = d.compare(ByLabel::NAME, None, 3).await.unwrap();
    assert_eq!(
        (comparison.keys, comparison.added),
        (2, 2),
        "{comparison:?}"
    );
    assert!(by_label(&w).await.is_empty(), "a comparison admits nothing");

    assert_eq!(
        d.recompute_all(ByLabel::NAME, None)
            .await
            .unwrap()
            .keys_marked,
        2
    );
    d.catch_up().await.unwrap();
    assert_eq!(by_label(&w).await, ["x=a/b", "y=c"]);
    assert_eq!(w.count("SELECT count(*) FROM derivation.inputs").await, 3);
}

#[devkit::test]
async fn ranged_recompute_marks_one_key_and_uses_its_older_inputs(db: devkit::pg::TestDatabase) {
    let w = World::with_keys(db, vec![Box::new(ByLabel)]).await;
    let mut d = w.driver_with(vec![Box::new(ByLabel)], vec![]).await;
    w.admit(vec![
        entry_at("x", "old", 1, Some(at(-9 * 86_400))),
        entry_at("y", "other", 2, Some(at(-2 * 86_400))),
        entry("untimed", "untimed", 3),
        entry_at("x", "in window", 4, Some(at(0))),
        entry_at("z", "upper boundary", 5, Some(at(3_600))),
    ])
    .await;
    d.catch_up().await.unwrap();
    assert_eq!(
        by_label(&w).await,
        [
            "untimed=untimed",
            "x=in window/old",
            "y=other",
            "z=upper boundary"
        ]
    );

    let result = d
        .recompute_all(ByLabel::NAME, Some((at(0), at(3_600))))
        .await
        .unwrap();
    assert_eq!(result.inputs_read, 1);
    assert_eq!(result.keys_marked, 1);
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let dirty = derivations::dirty(&mut conn, &[ByLabel::NAME.to_owned()], 10)
        .await
        .unwrap();
    assert_eq!(dirty.len(), 1);
    assert_eq!(dirty[0].key, json!({"label":"x"}));

    d.catch_up().await.unwrap();
    assert_eq!(
        by_label(&w).await,
        [
            "untimed=untimed",
            "x=in window/old",
            "y=other",
            "z=upper boundary"
        ]
    );
}

/// Item keys wait and name the `wake` key; `other` is an unrelated key of the
/// same derivation.
struct Woken {
    item_computations: Arc<AtomicUsize>,
}

impl Keyed for Woken {
    type Key = Value;
    const NAME: &'static str = "fixture.woken";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["operator_log.entry"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];

    fn keys_for(&self, input: &KeyView<'_>, _: &Env) -> Vec<Self::Key> {
        match input.payload["label"].as_str() {
            Some("wake") => vec![json!({"wake": true})],
            Some("other") => vec![json!({"other": true})],
            _ => vec![json!({"item": input.payload["label"]})],
        }
    }
    fn stores_inputs(&self, key: &Self::Key) -> bool {
        key.get("item").is_some()
    }
    fn inputs(&self, _: &Self::Key, _: &Env) -> Option<InputQuery> {
        None
    }
    fn compute(&self, key: &Self::Key, inputs: &[Event], _: &Env) -> Computed {
        self.item_computations.fetch_add(1, Ordering::SeqCst);
        Computed {
            outputs: vec![fused_output(
                Self::NAME,
                key["item"].as_str().unwrap(),
                "open".into(),
                inputs,
            )],
            waiting: true,
            woken_by: Some(json!({"wake": true})),
            error: None,
        }
    }
    fn key_state(&self, _: &Self::Key, _: &Env) -> KeyState {
        KeyState::Open
    }
}

#[devkit::test]
async fn a_waiting_key_recomputes_when_the_key_it_names_is_dirty(db: devkit::pg::TestDatabase) {
    let computations = Arc::new(AtomicUsize::new(0));
    let woken = || -> Vec<Box<dyn AnyKeyed>> {
        vec![Box::new(Woken {
            item_computations: computations.clone(),
        })]
    };
    let w = World::with_keys(db, woken()).await;
    let mut d = w.driver_with(woken(), vec![]).await;

    w.admit(vec![entry("first", "a", 1), entry("second", "b", 2)])
        .await;
    d.catch_up().await.unwrap();
    assert_eq!(computations.load(Ordering::SeqCst), 2);
    assert_eq!(
        w.count("SELECT count(*) FROM derivation.waiting WHERE woken_by = '{\"wake\": true}'")
            .await,
        2
    );

    // Another key of the derivation is dirty: the waiting keys stay asleep.
    w.admit(vec![entry("other", "c", 3)]).await;
    d.catch_up().await.unwrap();
    assert_eq!(computations.load(Ordering::SeqCst), 2);

    // The named key is dirty: each waiting key recomputes once, and the
    // named key's mark clears after them.
    w.admit(vec![entry("wake", "d", 4)]).await;
    d.catch_up().await.unwrap();
    assert_eq!(computations.load(Ordering::SeqCst), 4);
    assert_eq!(w.count("SELECT count(*) FROM derivation.dirty").await, 0);
    assert_eq!(w.count("SELECT count(*) FROM derivation.waiting").await, 2);
}

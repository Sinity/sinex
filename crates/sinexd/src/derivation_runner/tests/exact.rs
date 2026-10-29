use super::*;
struct ExactTargets {
    ids: Vec<sinex_model::EventId>,
}
impl Keyed for ExactTargets {
    const NAME: &'static str = "fixture.exact_targets";
    const VERSION: u32 = 1;
    const INPUTS: &'static [&'static str] = &["operator_log.entry"];
    const OUTPUTS: &'static [&'static str] = &["command.fused"];
    fn keys_for(&self, _: &KeyView<'_>, _: &Env) -> Vec<Value> {
        vec![json!("all")]
    }
    fn exact_inputs(&self, _: &Value, _: &Env) -> Option<Vec<sinex_model::EventId>> {
        Some(self.ids.clone())
    }
    fn inputs(&self, _: &Value, _: &Env) -> Option<InputQuery> {
        None
    }
    fn compute(&self, _: &Value, inputs: &[Event], _: &Env) -> Computed {
        assert_eq!(
            inputs.len(),
            2,
            "a missing target is not replaced with an unrelated row"
        );
        assert!(inputs.iter().all(|event| event.ts_orig.at.is_none()));
        let mut labels: Vec<_> = inputs
            .iter()
            .map(|event| event.payload["text"].as_str().unwrap())
            .collect();
        labels.sort();
        Computed {
            error: None,
            waiting: false,
            outputs: vec![sinex_derivations::derived_event::derived(
                &CommandFused {
                    record: "exact targets".into(),
                    command: labels.join("/"),
                    host: "fixture".into(),
                    started_at: at(0),
                    cwd: None,
                    session: None,
                    duration_ns: None,
                    exit_code: None,
                    observed_by: vec![],
                },
                Self::NAME,
                inputs,
            )],
        }
    }
    fn key_state(&self, _: &Value, _: &Env) -> KeyState {
        KeyState::Closed
    }
}
#[devkit::test]
async fn exact_input_hook_runs_without_a_temporal_window(db: devkit::pg::TestDatabase) {
    use sinex_model::event_types::operator::LogEntry;
    let make = |text: &str, anchor| {
        Event::build(
            &LogEntry {
                written_at: None,
                label: None,
                text: text.into(),
            },
            "fixture.exact",
            location(anchor),
            ReadingOrder(1),
            *blake3::hash(text.as_bytes()).as_bytes(),
            Class::Open,
        )
        .unwrap()
    };
    let inputs = [make("a", 1), make("b", 2), make("unrelated", 3)];
    let ids = vec![inputs[0].id, inputs[1].id, sinex_model::EventId::new()];
    let w = World::with_keys(db, vec![Box::new(ExactTargets { ids: ids.clone() })]).await;
    w.admit(inputs.to_vec()).await;
    let mut runner = w
        .driver_with(vec![Box::new(ExactTargets { ids })], vec![])
        .await;
    runner.catch_up().await.unwrap();
    let outputs = w.live::<CommandFused>().await;
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].command, "a/b");
    assert_eq!(
        w.count(
            "SELECT cardinality(parents)::bigint FROM core.events WHERE source='fixture.exact_targets'"
        )
        .await,
        2
    );
}

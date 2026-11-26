use super::*;
fn journal_event(kind: &str, seconds: i64, mut payload: Value) -> Event {
    payload["host"] = json!("box");
    payload["cursor"] = json!(format!("{kind}/{seconds}"));
    payload["logged_at"] = json!(at(seconds));
    payload["started_at"] = json!(at(seconds));
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic.journal",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                record_offset: seconds.max(0) as u64,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}
fn machine(kind: &str, events: &[Event]) -> Vec<StateInterval> {
    let env = Env::at(
        at(10 * 86400),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let day = env.time_settings.day(at(0)).unwrap();
    StateIntervals
        .compute(
            &json!({"state":kind,"day":day.to_string(),"host":"box"}),
            events,
            &env,
        )
        .outputs
        .iter()
        .map(|event| event.payload_as::<StateInterval>().unwrap())
        .collect()
}
#[devkit::test]
fn a_new_boot_fences_the_previous_one_and_leaves_its_end_unknown() {
    let first = journal_event("boot.started", 0, json!({"boot_id":"boot-a"}));
    let second = journal_event("boot.started", 3600, json!({"boot_id":"boot-b"}));
    let result = machine("machine.boot", &[first, second]);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].ended_at, None);
    assert_eq!(result[0].fenced_at, Some(at(3600)));
    assert_eq!(result[1].fenced_at, None);
}
#[devkit::test]
fn a_day_key_of_unit_runs_computes_to_nothing() {
    // Unit runs moved to their own derivation: a stored day key of the old
    // shape has no query, so a recompute archives what it held.
    let env = Env::at(
        at(0),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let key = json!({"state":"machine.unit","day":"2024-06-15","host":"box","unknown_record":null});
    assert!(Keyed::inputs(&StateIntervals, &key, &env).is_none());
}

#[devkit::test]
fn a_boot_or_sleep_that_names_no_host_has_no_key_and_opens_no_interval() {
    let env = Env::at(
        at(10 * 86400),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    for kind in ["boot.started", "sleep.started"] {
        let mut record = journal_event(kind, 0, json!({"boot_id":"boot-a"}));
        record.payload["host"] = Value::Null;
        let keys = Keyed::keys_for(
            &StateIntervals,
            &KeyView {
                event_type: &record.event_type,
                ts_orig: record.ts_orig.at,
                payload: &record.payload,
            },
            &env,
        );
        assert_eq!(keys, Vec::<Value>::new(), "{kind}");
        // A stored key of the earlier form has no query and computes to
        // nothing, so the open interval it held is archived and it stops
        // waiting.
        for state in ["machine.boot", "machine.suspend"] {
            let stored = json!({"state":state,"day":"2024-06-15","host":null,
                                "unknown_record":format!("{kind}/0")});
            assert!(Keyed::inputs(&StateIntervals, &stored, &env).is_none());
            let computed = StateIntervals.compute(&stored, std::slice::from_ref(&record), &env);
            assert!(computed.outputs.is_empty(), "{kind} {state}");
            assert!(!computed.waiting);
        }
    }
}

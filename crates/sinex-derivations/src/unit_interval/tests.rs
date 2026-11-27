//! Unit runs of one scope: pairing by invocation, restarts, the boot fence.
use super::*;
use crate::AnyKeyed;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};

const T: i64 = 1_718_438_400; // 2024-06-15 10:00 CEST

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_secs(T + secs).unwrap()
}

fn journal_event(kind: &str, seconds: i64, mut payload: Value) -> Event {
    if payload.get("host").is_none() {
        payload["host"] = json!("box");
    }
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

fn unit(kind: &str, seconds: i64, boot: &str, invocation: &str) -> Event {
    journal_event(
        kind,
        seconds,
        json!({"boot_id":boot,
    "unit":"example.service","manager":"system","uid":0,"invocation_id":invocation,"outcome":"stopped"}),
    )
}

fn env() -> Env {
    Env::at(
        at(10 * 86400),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    )
}

fn key_of(event: &Event) -> Value {
    let keys = AnyKeyed::keys_for(
        &UnitIntervals,
        &KeyView {
            event_type: &event.event_type,
            ts_orig: event.ts_orig.at,
            payload: &event.payload,
        },
        &env(),
    );
    assert_eq!(keys.len(), 1, "{keys:?}");
    keys.into_iter().next().unwrap()
}

/// The first event's key, computed from all of `events`.
fn computed(events: &[Event]) -> Computed {
    AnyKeyed::compute(&UnitIntervals, &key_of(&events[0]), events, &env())
}

fn runs_of(events: &[Event]) -> Vec<StateInterval> {
    computed(events)
        .outputs
        .iter()
        .map(|event| event.payload_as::<StateInterval>().unwrap())
        .collect()
}

#[devkit::test]
fn unit_pairing_keeps_boot_and_invocation_identity() {
    let start = unit("unit.started", 0, "boot-a", "run-a");
    let wrong = unit("unit.stopped", 5, "boot-a", "run-b");
    let other_boot = unit("unit.stopped", 6, "boot-b", "run-a");
    let stop = unit("unit.stopped", 10, "boot-a", "run-a");
    assert_ne!(key_of(&start), key_of(&other_boot), "a boot is a scope");
    let result = runs_of(&[start.clone(), wrong, other_boot]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].ended_at, None);
    let result = runs_of(&[start, stop]);
    assert_eq!(result[0].ended_at, Some(at(10)));
}

#[devkit::test]
fn boot_fences_unknown_unit_end_without_inventing_continuous_uptime() {
    let start = unit("unit.started", 0, "boot-a", "run-a");
    let boot = journal_event("boot.started", 3 * 86400, json!({"boot_id":"boot-b"}));
    let open = computed(std::slice::from_ref(&start));
    assert!(open.waiting);
    assert_eq!(
        open.woken_by,
        Some(key_of(&boot)),
        "an open run names the key its host's boots dirty"
    );
    let fenced = computed(&[start, boot]);
    assert!(!fenced.waiting, "a fenced run waits for nothing");
    assert_eq!(fenced.woken_by, None);
    let result: Vec<StateInterval> = fenced
        .outputs
        .iter()
        .map(|event| event.payload_as().unwrap())
        .collect();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].ended_at, None);
    assert_eq!(result[0].fenced_at, Some(at(3 * 86400)));
    assert_eq!(result[0].ended_at_evidence, None);
    assert_eq!(result[0].end_event_type.as_deref(), Some("boot.started"));
}

#[devkit::test]
fn the_boot_a_run_started_in_does_not_fence_it() {
    let boot = journal_event("boot.started", 0, json!({"boot_id":"boot-a"}));
    let start = unit("unit.started", 5, "boot-a", "run-a");
    let again = journal_event("boot.started", 9, json!({"boot_id":"boot-a"}));
    let result = runs_of(&[start, boot, again]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].fenced_at, None);
}

#[devkit::test]
fn a_late_start_job_completion_does_not_reopen_an_already_stopped_oneshot() {
    let result = runs_of(&[
        unit("unit.stopped", 0, "boot-a", "run-a"),
        unit("unit.started", 1, "boot-a", "run-a"),
    ]);
    assert!(result.is_empty());
}

#[devkit::test]
fn start_after_start_closes_previous() {
    let output = runs_of(&[
        unit("unit.started", 0, "boot-a", "run-a"),
        unit("unit.started", 30, "boot-a", "run-b"),
    ]);
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].started_at, at(0));
    assert_eq!(output[0].ended_at, Some(at(30)));
    assert_eq!(output[1].started_at, at(30));
    assert_eq!(output[1].ended_at, None);
}

#[devkit::test]
fn a_start_reported_again_for_the_open_run_opens_nothing() {
    let output = runs_of(&[
        unit("unit.started", 0, "boot-a", "run-a"),
        unit("unit.started", 1800, "boot-a", "run-a"),
    ]);
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].started_at, at(0));
}

#[devkit::test]
fn each_unit_host_manager_and_user_is_its_own_key() {
    let start = unit("unit.started", 0, "boot-a", "run-a");
    for (field, value) in [
        ("host", json!("other-box")),
        ("unit", json!("other.service")),
        ("manager", json!("user")),
        ("uid", json!(1000)),
    ] {
        let mut other = unit("unit.stopped", 10, "boot-a", "run-a");
        other.payload[field] = value;
        assert_ne!(key_of(&start), key_of(&other), "{field}");
        // Even handed the other scope's stop, the run stays open.
        assert_eq!(runs_of(&[start.clone(), other])[0].ended_at, None);
    }
}

#[devkit::test]
fn a_unit_record_without_a_host_has_no_key_and_opens_no_interval() {
    let mut start = unit("unit.started", 0, "boot-a", "run-a");
    start.payload["host"] = Value::Null;
    let keys = AnyKeyed::keys_for(
        &UnitIntervals,
        &KeyView {
            event_type: &start.event_type,
            ts_orig: start.ts_orig.at,
            payload: &start.payload,
        },
        &env(),
    );
    assert_eq!(keys, Vec::<Value>::new());
    // Read beside a named scope's events, it adds no run to that scope.
    let named = unit("unit.started", 5, "boot-a", "run-b");
    let runs = runs_of(&[named, start]);
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].started_at, at(5));
}

#[devkit::test]
fn only_scope_keys_read_stored_inputs() {
    let start = unit("unit.started", 0, "boot-a", "run-a");
    let boot = journal_event("boot.started", 9, json!({"boot_id":"boot-b"}));
    assert!(AnyKeyed::stores_inputs(&UnitIntervals, &key_of(&start)));
    assert!(!AnyKeyed::stores_inputs(&UnitIntervals, &key_of(&boot)));
    assert_eq!(key_of(&boot), json!({"boots_of": "box"}));
}

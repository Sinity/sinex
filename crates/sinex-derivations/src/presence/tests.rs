use super::*;
use crate::activity::ActivitySpans;
use sinex_model::{Class, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(1_718_438_400 + seconds).unwrap()
}
fn record(kind: &str, payload: Value, anchor: u64) -> Event {
    (sinex_model::event_types::find(kind).unwrap().build)(
        &payload,
        "synthetic.presence",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap()
}
fn physical(seconds: i64) -> Event {
    record(
        "pointer.moved",
        json!({"session":"input","logged_at":at(seconds),"axis":"REL_X","value":1,"absolute":false,"n":0,
        "extra":{"layer":"evdev","virtual":false,"scope":"seat"}}),
        seconds as u64,
    )
}
fn intervals(inputs: &[Event]) -> Vec<PresenceInterval> {
    let env = Env::at(
        at(86400),
        sinex_model::time_settings::TimeSettings::new("Europe/Warsaw", 4, 0)
            .expect("fixture day setting"),
    );
    let key = json!({"hour":at(0)});
    let spans = Keyed::compute(&ActivitySpans, &key, inputs, &env).outputs;
    Keyed::compute(&Presence, &key, &spans, &env)
        .outputs
        .iter()
        .map(|event| event.payload_as().unwrap())
        .collect()
}

#[devkit::test]
fn physical_bouts_override_afk_and_keep_the_contradiction() {
    let mut claim = record(
        "presence.span",
        json!({"bucket":"test","row":1,"started_at":at(0),"ended_at":at(60),"status":"afk"}),
        100,
    );
    claim.payload["scope"] = json!("seat");
    let output = intervals(&[physical(0), physical(20), claim]);
    let present: Vec<_> = output
        .iter()
        .filter(|interval| interval.state == "present")
        .collect();
    assert!(!present.is_empty());
    assert!(
        present
            .iter()
            .all(|interval| interval.method == "physical" && interval.contradictions == ["afk"])
    );
    assert!(present.iter().all(|interval| !interval.support.is_empty()));
    assert_eq!(output.last().unwrap().state, "absent");
}

#[devkit::test]
fn a_not_afk_claim_without_operator_input_is_unknown() {
    let claim = record(
        "presence.span",
        json!({"bucket":"test","row":1,"started_at":at(0),"ended_at":at(7200),"status":"not-afk"}),
        100,
    );
    let output = intervals(&[claim]);
    assert!(!output.is_empty());
    assert!(
        output
            .iter()
            .all(|interval| interval.state == "unknown" && interval.method == "claim_reconciled")
    );
}

#[devkit::test]
fn missing_input_coverage_is_unknown_and_silence_inside_coverage_is_absent() {
    let mut events = vec![physical(0), physical(20)];
    assert!(
        intervals(&events)
            .iter()
            .all(|interval| interval.state == "present")
    );
    events.push(record(
        "input.coverage",
        json!({"coverage_id":"input","scope":"seat","started_at":at(0),"ended_at":at(600)}),
        600,
    ));
    let output = intervals(&events);
    assert!(output.iter().any(|interval| interval.started_at == at(200)
        && interval.state == "absent"
        && interval.method == "physical"));
    assert!(
        output
            .windows(2)
            .all(|pair| pair[0].ended_at <= pair[1].started_at)
    );
}

use super::*;
use serde_json::json;

#[devkit::test]
fn recall_descriptions_keep_recorded_objectives_checks_and_source_states() {
    let episode = json!({"event_type":"work.episode","payload":{"label":"Recorded branch label","objective":{"reference":"task:example","repository":"/synthetic/repo"},"state":"suspended","trail":[{"text":"retained detail"}]}});
    assert_eq!(
        description(&episode),
        "Recorded branch label; objective task:example; repository /synthetic/repo; state suspended"
    );
    let act = json!({"event_type":"work.act","payload":{"action":"check","label":"Recorded branch label","check_name":"unit tests","check_passed":false,"revision":"abc123","failed":true}});
    assert_eq!(
        description(&act),
        "check; Recorded branch label; check unit tests: failed; revision abc123"
    );
    let coverage = json!({"event_type":"source.coverage","payload":{"observed_source":"synthetic.source","state":"recorded_unknown_coverage","reason":"input checked successfully; no new events","binding":"retained-binding"}});
    assert_eq!(
        description(&coverage),
        "synthetic.source; state recorded_unknown_coverage; input checked successfully; no new events"
    );
}

#[devkit::test]
fn recall_presentation_uses_its_selected_zone_and_keeps_raw_utc_rows() {
    let parent = EventId::new();
    let answer = json!({"resolved":{"zone":"Europe/Warsaw"},"rows":[{"id":parent,"event_type":"command.executed","source":"synthetic.shell","when":"2026-09-14T08:00:00Z","payload":{"command":"synthetic command"}}]});
    let sections = sections(&answer, &[parent]);
    let finding = &sections
        .iter()
        .find(|section| section.title == "Machine")
        .unwrap()
        .findings[0];
    let Template::Observation {
        when: Some(when), ..
    } = &finding.template
    else {
        panic!("recorded time is absent")
    };
    assert!(when.starts_with("2026-09-14T10:00:00+02:00"), "{when}");
    assert_eq!(answer["rows"][0]["when"], "2026-09-14T08:00:00Z");
}

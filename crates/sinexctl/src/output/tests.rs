use super::*;
use serde_json::json;

#[devkit::test]
fn tables_align_and_times_turn_local() {
    let local = Local::new(Some("Europe/Warsaw"));
    let rows = vec![
        json!({"command": "cargo test", "when": "2026-09-14T08:00:00+00:00"}),
        json!({"command": "ls", "when": null}),
    ];
    let t = table(&["command".into(), "when".into()], &rows, &local);
    assert_eq!(
        t,
        "command     when\ncargo test  2026-09-14 10:00:00\nls          —\n"
    );
}

#[devkit::test]
fn long_cells_end_with_an_ellipsis() {
    let local = Local::new(Some("UTC"));
    let long = "x".repeat(100);
    let c = cell(&local, &json!(long));
    assert_eq!(c.chars().count(), CELL);
    assert!(c.ends_with('…'));
}

#[devkit::test]
fn recall_prints_the_mark_and_each_sources_latest_observation() {
    let result = json!({"read_as":"synthetic context","rows":[],"total":0,"resolved":{"zone":"UTC"},"recall":{
        "mark":{"payload":{"text":"synthetic moment"}},
        "latest_by_source":[{"source":"synthetic.source","event_type":"command.executed","when":"2026-09-14T08:00:00Z","payload":{"command":"synthetic latest"}}]
    }});
    let text = answer(&result, &Local::new(Some("UTC")), None);
    assert!(text.contains("Mark: synthetic moment"));
    assert!(text.contains("Latest by source:"));
    assert!(text.contains("synthetic.source"));
    assert!(text.contains("synthetic latest"));
}

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

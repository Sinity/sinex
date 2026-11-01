use super::*;

#[devkit::test]
fn capture_accepts_daemon_sections_without_executing_them() {
    let config: Configuration = serde_json::from_value(
        serde_json::json!({"sources":{},"signals":{"interrupt_budget_per_hour":3}}),
    )
    .unwrap();
    assert_eq!(config.signals["interrupt_budget_per_hour"], 3);
    assert!(
        serde_json::from_value::<Configuration>(
            serde_json::json!({"unknown_capture_setting":true})
        )
        .is_err()
    );
}

use super::*;

#[test]
fn context_request_does_not_accept_a_claimed_identity() {
    assert!(
        serde_json::from_value::<PublicContextRequest>(serde_json::json!({"producer":"foreign"}))
            .is_err()
    );
    assert!(serde_json::from_value::<PublicContextRequest>(serde_json::json!({"coverage":{"producer":"foreign","cache":"c","completed_position":1}})).is_err());
}

#[test]
fn day_wire_does_not_resolve_a_zone_during_decode() {
    let day: query_language::time::DaySettingWire = serde_json::from_value(
        serde_json::json!({"zone":"not/an/installed-zone","start":"04:00:00"}),
    )
    .unwrap();
    assert_eq!(day.zone, "not/an/installed-zone");
}

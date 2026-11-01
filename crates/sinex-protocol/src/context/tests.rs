use super::*;

#[test]
fn context_request_does_not_accept_a_claimed_identity() {
    assert!(serde_json::from_value::<PublicContextRequest>(serde_json::json!({"producer":"foreign"})).is_err());
    assert!(serde_json::from_value::<PublicContextRequest>(serde_json::json!({"coverage":{"producer":"foreign","cache":"c","completed_position":1}})).is_err());
}

#[test]
fn day_wire_does_not_resolve_a_zone_during_decode() {
    let day:query_language::time::DaySettingWire=serde_json::from_value(serde_json::json!({"zone":"not/an/installed-zone","start":"04:00:00"})).unwrap();
    assert_eq!(day.zone,"not/an/installed-zone");
}

#[test]
fn offline_permission_binds_actual_invocation_identity_and_exclusive_lease() {
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let mut permission = OfflineEmergency {
        producer: "primary".into(), host: "host-a".into(), settings_identity: "settings-a".into(),
        rule: crate::signals::emergency_rule(), route: OfflineEmergencyRoute::Desktop,
        issued_at: at, expires_at: Timestamp::from_unix_secs(1_800_000_900).unwrap(),
    };
    assert!(permission.permits("primary", "host-a", "settings-a", at));
    assert!(!permission.permits("foreign", "host-a", "settings-a", at));
    assert!(!permission.permits("primary", "host-b", "settings-a", at));
    assert!(!permission.permits("primary", "host-a", "settings-b", at));
    assert!(!permission.permits("primary", "host-a", "settings-a", Timestamp::from_unix_secs(1_799_999_999).unwrap()));
    assert!(!permission.permits("primary", "host-a", "settings-a", permission.expires_at));
    permission.rule.fingerprint.push('0');
    assert!(!permission.permits("primary", "host-a", "settings-a", at));
}

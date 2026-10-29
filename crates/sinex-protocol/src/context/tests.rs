use super::*;

#[test]
fn coverage_requires_explicit_signed_epoch_and_typed_failure_certainty() {
    let proof=CoverageProof{stamp:"opaque-owner-stamp".into(),position:17,policy_epoch:9,scope:"scope-a".into()};
    let mut wire=serde_json::to_value(&proof).unwrap();
    assert_eq!(wire["policy_epoch"],9);
    wire.as_object_mut().unwrap().remove("policy_epoch");
    assert!(serde_json::from_value::<CoverageProof>(wire).is_err());
    for basis in [crate::ReplayBasisStatus::Unverified,crate::ReplayBasisStatus::Invalidated{policy_epoch:Some(9),scope:Some("scope-a".into())},crate::ReplayBasisStatus::Invalidated{policy_epoch:None,scope:None}] {
        let status=CoverageStatus::Incomplete{reason:"synthetic-failure".into(),basis};
        assert_eq!(serde_json::from_value::<CoverageStatus>(serde_json::to_value(&status).unwrap()).unwrap(),status);
    }
    assert!(serde_json::from_value::<CoverageStatus>(serde_json::json!({"state":"incomplete","reason":"synthetic-failure"})).is_err());
}

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

#[test]
fn offline_permission_binds_actual_invocation_identity_and_exclusive_lease() {
    let at = Timestamp::from_unix_secs(1_800_000_000).unwrap();
    let mut permission = OfflineEmergency {
        producer: "primary".into(),
        host: "host-a".into(),
        settings_identity: "settings-a".into(),
        rule: crate::signals::emergency_rule(),
        route: OfflineEmergencyRoute::Desktop,
        issued_at: at,
        expires_at: Timestamp::from_unix_secs(1_800_000_900).unwrap(),
    };
    assert!(permission.permits("primary", "host-a", "settings-a", at));
    assert!(!permission.permits("foreign", "host-a", "settings-a", at));
    assert!(!permission.permits("primary", "host-b", "settings-a", at));
    assert!(!permission.permits("primary", "host-a", "settings-b", at));
    assert!(!permission.permits(
        "primary",
        "host-a",
        "settings-a",
        Timestamp::from_unix_secs(1_799_999_999).unwrap()
    ));
    assert!(!permission.permits("primary", "host-a", "settings-a", permission.expires_at));
    permission.rule.fingerprint.push('0');
    assert!(!permission.permits("primary", "host-a", "settings-a", at));
}

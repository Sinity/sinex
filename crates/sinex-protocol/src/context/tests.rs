use super::*;

#[test]
fn coverage_requires_explicit_signed_epoch_and_typed_failure_certainty() {
    let proof = CoverageProof {
        stamp: "opaque-owner-stamp".into(),
        position: 17,
        policy_epoch: 9,
        scope: "scope-a".into(),
    };
    let mut wire = serde_json::to_value(&proof).unwrap();
    assert_eq!(wire["policy_epoch"], 9);
    wire.as_object_mut().unwrap().remove("policy_epoch");
    assert!(serde_json::from_value::<CoverageProof>(wire).is_err());
    for basis in [
        crate::ReplayBasisStatus::Unverified,
        crate::ReplayBasisStatus::Invalidated {
            policy_epoch: Some(9),
            scope: Some("scope-a".into()),
        },
        crate::ReplayBasisStatus::Invalidated {
            policy_epoch: None,
            scope: None,
        },
    ] {
        let status = CoverageStatus::Incomplete {
            reason: "synthetic-failure".into(),
            basis,
        };
        assert_eq!(
            serde_json::from_value::<CoverageStatus>(serde_json::to_value(&status).unwrap())
                .unwrap(),
            status
        );
    }
    assert!(
        serde_json::from_value::<CoverageStatus>(
            serde_json::json!({"state":"incomplete","reason":"synthetic-failure"})
        )
        .is_err()
    );
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

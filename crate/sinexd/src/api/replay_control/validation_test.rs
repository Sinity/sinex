use super::*;
use xtask::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn actor_validation_rejects_empty_actor() -> Result<()> {
    let result = validate_actor_for_action("", ReplayAction::Plan);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    Ok(())
}

#[sinex_test]
async fn actor_validation_rejects_invalid_role() -> Result<()> {
    let result = validate_actor_for_action("invalid:user", ReplayAction::Plan);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Invalid actor role"));
    Ok(())
}

#[sinex_test]
async fn actor_validation_rejects_empty_identifier() -> Result<()> {
    let result = validate_actor_for_action("user:", ReplayAction::Plan);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("identifier cannot be empty")
    );
    Ok(())
}

#[sinex_test]
async fn actor_validation_accepts_valid_actors() -> Result<()> {
    assert!(validate_actor_for_action("user:alice", ReplayAction::Plan).is_ok());
    assert!(validate_actor_for_action("admin:root", ReplayAction::Plan).is_ok());
    assert!(validate_actor_for_action("service:replay-worker", ReplayAction::Plan).is_ok());
    assert!(validate_actor_for_action("system:internal", ReplayAction::Plan).is_ok());
    assert!(validate_actor_for_action("operator:ops-team", ReplayAction::Plan).is_ok());
    assert!(validate_actor_for_action("test:unit-test", ReplayAction::Plan).is_ok());
    Ok(())
}

#[sinex_test]
async fn replay_test_actor_flag_rejects_invalid_boolean() -> Result<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_ALLOW_TEST_ACTORS", "certainly");

    let error = allow_test_actors_in_runtime(false)
        .expect_err("invalid replay actor toggle should be rejected");
    assert!(error.to_string().contains("SINEX_ALLOW_TEST_ACTORS"));
    Ok(())
}

#[sinex_test]
async fn privileged_actions_reject_user_role() -> Result<()> {
    let result = validate_actor_for_action("user:alice", ReplayAction::Execute);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("cannot perform this replay action")
    );
    Ok(())
}

#[sinex_test]
async fn replay_gates_report_tripped_defaults() -> Result<()> {
    let preview = serde_json::json!({
        "anchor_churn_pct": 6.0,
        "time_quality_flip_pct": 3.0,
        "max_observed_depth": 6,
        "schema_boundary_crossed": true,
    });

    let report = replay_gate_report(&preview);
    assert_eq!(report["tripped"].as_u64(), Some(4));
    assert!(format!("{report}").contains("--force-schema-mismatch"));
    Ok(())
}

#[sinex_test]
async fn replay_gates_report_unmeasured_metrics_as_advisory() -> Result<()> {
    let preview = serde_json::json!({
        "anchor_churn_pct": null,
        "time_quality_flip_pct": null,
        "max_observed_depth": 0,
        "schema_boundary_crossed": false,
    });

    let report = replay_gate_report(&preview);
    assert_eq!(report["tripped"].as_u64(), Some(0));
    let gates = report["gates"]
        .as_array()
        .expect("gate report should include gates");
    assert!(
        gates
            .iter()
            .filter(|gate| gate["advisory"].as_bool() == Some(true))
            .all(|gate| gate["observed"].as_str() == Some("not measured (advisory)")),
        "unmeasured metrics should be advisory, not rendered as zero: {report}"
    );
    ensure_replay_gates_pass(Uuid::now_v7(), &preview, &ReplayGateOverrides::default())?;
    Ok(())
}

#[sinex_test]
async fn replay_gates_reject_without_required_overrides() -> Result<()> {
    let cases = [
        (
            "anchor_churn_threshold_percent",
            serde_json::json!({
                "anchor_churn_pct": 5.01,
                "time_quality_flip_pct": 0.0,
                "max_observed_depth": 0,
                "schema_boundary_crossed": false,
            }),
        ),
        (
            "time_quality_flip_threshold_percent",
            serde_json::json!({
                "anchor_churn_pct": 0.0,
                "time_quality_flip_pct": 2.01,
                "max_observed_depth": 0,
                "schema_boundary_crossed": false,
            }),
        ),
        (
            "max_cascade_depth_warn",
            serde_json::json!({
                "anchor_churn_pct": 0.0,
                "time_quality_flip_pct": 0.0,
                "max_observed_depth": 6,
                "schema_boundary_crossed": false,
            }),
        ),
        (
            "require_force_on_schema_mismatch",
            serde_json::json!({
                "anchor_churn_pct": 0.0,
                "time_quality_flip_pct": 0.0,
                "max_observed_depth": 0,
                "schema_boundary_crossed": true,
            }),
        ),
    ];

    for (gate_name, preview) in cases {
        let error =
            ensure_replay_gates_pass(Uuid::now_v7(), &preview, &ReplayGateOverrides::default())
                .expect_err("tripped gate must require an explicit override");
        assert!(error.to_string().contains("TARGET_CANONICAL"));
        assert!(
            format!("{error:#}").contains(gate_name),
            "expected error for {gate_name}, got {error:#}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn replay_gates_accept_matching_overrides() -> Result<()> {
    let preview = serde_json::json!({
        "anchor_churn_pct": 6.0,
        "time_quality_flip_pct": 3.0,
        "max_observed_depth": 6,
        "schema_boundary_crossed": true,
    });
    let overrides = ReplayGateOverrides {
        allow_anchor_churn: true,
        allow_time_quality_flips: true,
        allow_deep_cascade: true,
        force_schema_mismatch: true,
    };

    ensure_replay_gates_pass(Uuid::now_v7(), &preview, &overrides)?;
    Ok(())
}

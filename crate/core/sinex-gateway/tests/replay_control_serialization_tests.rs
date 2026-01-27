use sinex_gateway::replay_control::{ReplayControlRequest, ReplayControlResponse, ReplayScope};
use xtask::sandbox::sinex_test;

#[sinex_test]
fn replay_control_request_round_trip() -> TestResult<()> {
    let scope = ReplayScope {
        processor_id: "fs-test".to_string(),
        time_window: None,
        material_filter: None,
        filters: Default::default(),
    };

    let request = ReplayControlRequest::Plan {
        actor: "tester".into(),
        scope: scope.clone(),
    };

    let data = serde_json::to_string(&request)?;
    let decoded: ReplayControlRequest = serde_json::from_str(&data)?;

    match decoded {
        ReplayControlRequest::Plan {
            actor,
            scope: decoded_scope,
        } => {
            assert_eq!(actor, "tester");
            assert_eq!(decoded_scope.processor_id, scope.processor_id);
        }
        other => panic!("expected plan request, got {other:?}"),
    }

    Ok(())
}

#[sinex_test]
fn replay_control_response_error_serializes() -> TestResult<()> {
    let response = ReplayControlResponse::error("boom");
    let json = serde_json::to_string(&response)?;
    let decoded: ReplayControlResponse = serde_json::from_str(&json)?;

    assert_eq!(decoded.status, "error");
    assert_eq!(decoded.message.as_deref(), Some("boom"));
    assert!(decoded.operation.is_none());
    assert!(decoded.preview.is_none());
    assert!(decoded.operations.is_none());

    Ok(())
}

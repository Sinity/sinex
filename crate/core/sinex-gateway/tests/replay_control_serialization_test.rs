use sinex_gateway::replay_control::{
    ReplayControlRequest, ReplayControlResponse, ReplayControlStatus, ReplayScope,
};
use std::collections::HashMap;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn replay_control_request_round_trip() -> TestResult<()> {
    let scope = ReplayScope {
        node_id: "fs-test".to_string(),
        time_window: None,
        material_filter: None,
        filters: HashMap::default(),
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
            assert_eq!(decoded_scope.node_id, scope.node_id);
        }
        other => panic!("expected plan request, got {other:?}"),
    }

    Ok(())
}

#[sinex_test]
async fn replay_control_response_error_serializes() -> TestResult<()> {
    let response = ReplayControlResponse::error("boom");
    let json = serde_json::to_string(&response)?;
    let decoded: ReplayControlResponse = serde_json::from_str(&json)?;

    assert_eq!(decoded.status, ReplayControlStatus::Error);
    assert_eq!(decoded.message.as_deref(), Some("boom"));
    assert!(decoded.operation.is_none());
    assert!(decoded.preview.is_none());
    assert!(decoded.operations.is_none());

    Ok(())
}

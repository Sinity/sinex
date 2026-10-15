use serde_json::{Value, json};
use sinex_protocol::*;

#[test]
fn null_success_is_not_missing_and_two_outcomes_are_rejected() {
    let response = RpcResponse::success(RpcId::Number(1), Value::Null);
    let wire = serde_json::to_value(response).unwrap();
    assert_eq!(wire, json!({"jsonrpc":"2.0","id":1,"result":null}));
    assert!(
        serde_json::from_value::<RpcResponse>(wire)
            .unwrap()
            .outcome()
            .is_ok()
    );
    assert!(
        serde_json::from_value::<RpcResponse>(
            json!({"jsonrpc":"2.0","id":1,"result":null,"error":RpcError::parse_error()})
        )
        .is_err()
    );
}
#[test]
fn surface_failures_have_declared_exit_and_application_codes() {
    for (kind, exit) in [
        (ErrorKind::BadQuery, 2),
        (ErrorKind::NotFound, 3),
        (ErrorKind::Refused, 4),
        (ErrorKind::Partial, 5),
        (ErrorKind::Budget, 6),
        (ErrorKind::Internal, 1),
    ] {
        let error = RpcError::new(kind, "failure", Value::Null);
        assert_eq!(error.exit_code(), exit);
        assert_eq!(error.code, -32000 - i32::from(exit));
    }
}
#[test]
fn unimplemented_surfaces_are_absent_and_real_methods_validate_params() {
    for method in ["replay", "polylogue.have", "commands"] {
        let request: RpcRequest =
            serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":method,"params":{}}))
                .unwrap();
        assert_eq!(request.validate().unwrap_err().code, -32601);
    }
    let request = RpcRequest::new(RpcId::Number(1), Method::Status, json!({"scope":"all"}));
    assert_eq!(request.validate().unwrap_err().kind, ErrorKind::BadQuery);
    assert!(
        serde_json::from_value::<RpcRequest>(
            json!({"jsonrpc":"2.0","id":1,"method":"status","params":{},"extra":1})
        )
        .is_err()
    );
}

#[test]
fn owned_writer_state_rejects_unavailable_reason() {
    let valid = json!({"state":"owned"});
    let parsed: WriterOwnership = serde_json::from_value(valid.clone()).unwrap();
    assert_eq!(serde_json::to_value(parsed).unwrap(), valid);
    assert!(
        serde_json::from_value::<WriterOwnership>(json!({"state":"owned","reason":"unavailable"}))
            .is_err()
    );
}

#[test]
fn query_deadline_is_optional_bounded_and_does_not_change_its_question() {
    for timeout in [None, Some(1), Some(10_000), Some(60_000), Some(3_600_000)] {
        let mut params = json!({"query":"events"});
        if let Some(ms) = timeout {
            params["timeout_ms"] = json!(ms);
        }
        assert_eq!(
            RpcRequest::new(RpcId::Number(1), Method::Query, params)
                .validate()
                .unwrap(),
            Method::Query
        );
    }
    for invalid in [
        json!(0),
        json!(-1),
        json!(3_600_001),
        json!("1000"),
        json!(1.5),
    ] {
        let params = json!({"query":"events", "timeout_ms":invalid});
        assert_eq!(
            RpcRequest::new(RpcId::Number(1), Method::Query, params)
                .validate()
                .unwrap_err()
                .kind,
            ErrorKind::BadQuery
        );
    }
}

#[test]
fn replay_has_explicit_scope_action_and_no_caller_authorship() {
    let start = json!({"query":"materials where id = 019a0000-0000-7000-8000-000000000001","action_id":"019a0000-0000-7000-8000-000000000002"});
    assert_eq!(
        RpcRequest::new(RpcId::Number(1), Method::ReplayStart, start.clone())
            .validate()
            .unwrap(),
        Method::ReplayStart
    );
    for invalid in [
        json!({}),
        json!({"query":"materials"}),
        json!({"action_id":"019a0000-0000-7000-8000-000000000002"}),
    ] {
        assert_eq!(
            RpcRequest::new(RpcId::Number(1), Method::ReplayStart, invalid)
                .validate()
                .unwrap_err()
                .kind,
            ErrorKind::BadQuery
        );
    }
    let mut forged = start;
    forged["principal"] = json!("operator");
    assert_eq!(
        RpcRequest::new(RpcId::Number(1), Method::ReplayStart, forged)
            .validate()
            .unwrap_err()
            .kind,
        ErrorKind::BadQuery
    );
}

use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use serde_json::Value;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn atuin_payload_builder_normalizes_negative_duration_to_zero() -> TestResult<()> {
    let payload = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        0,
        -1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "test-host",
    )?;
    let payload: Value = serde_json::to_value(&payload)?;

    assert_eq!(payload.get("duration_ns").and_then(Value::as_i64), Some(0));
    assert_eq!(payload.get("ts_start_orig"), payload.get("ts_end_orig"));
    Ok(())
}

#[sinex_test]
async fn atuin_payload_builder_rejects_out_of_range_exit_code() -> TestResult<()> {
    let error = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        i64::MAX,
        1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "test-host",
    )
    .expect_err("exit code outside i32 range should be rejected");

    assert!(error.to_string().contains("exit code"));
    Ok(())
}

#[sinex_test]
async fn atuin_payload_builder_normalizes_atuin_hostname_identity_suffix() -> TestResult<()> {
    let payload = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        0,
        1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "test-host:test-user",
    )?;
    let payload: Value = serde_json::to_value(&payload)?;

    assert_eq!(
        payload.get("hostname").and_then(Value::as_str),
        Some("test-host")
    );
    Ok(())
}

#[sinex_test]
async fn atuin_payload_builder_rejects_invalid_hostname_after_normalization() -> TestResult<()> {
    let error = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        0,
        1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "bad_host:test-user",
    )
    .expect_err("invalid hostname should still be rejected");

    assert!(error.to_string().contains("hostname"));
    Ok(())
}

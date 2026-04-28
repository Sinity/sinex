use serde_json::Value;
use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use sinex_primitives::SinexError;
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
async fn atuin_payload_builder_happy_path() -> TestResult<()> {
    let payload = AtuinCommandExecutedPayload::from_raw_history(
        "ls -la",
        RecordedPath::from("/home/user"),
        0,           // successful exit
        1_000_000,   // 1ms duration
        "hist-id-1",
        "sess-id-1",
        1_700_000_000_000_000_000,
        "my-host",
    )?;
    let payload: Value = serde_json::to_value(&payload)?;

    assert_eq!(
        payload.get("exit_code").and_then(Value::as_i64),
        Some(0),
        "exit_code should be 0 for success"
    );
    assert_eq!(
        payload.get("duration_ns").and_then(Value::as_i64),
        Some(1_000_000),
        "duration_ns should match the input"
    );
    assert_eq!(
        payload.get("hostname").and_then(Value::as_str),
        Some("my-host"),
        "hostname should be preserved"
    );
    assert_ne!(
        payload.get("ts_start_orig"),
        payload.get("ts_end_orig"),
        "ts_end_orig should differ from ts_start_orig when duration > 0"
    );
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

    assert!(
        matches!(error, SinexError::Validation(_)),
        "expected SinexError::Validation for out-of-range exit code, got: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn atuin_payload_builder_large_valid_timestamp_and_duration() -> TestResult<()> {
    // Verify that a large but in-range timestamp combined with a large positive
    // duration still produces a valid payload where ts_end_orig > ts_start_orig.
    // This exercises the ts_end computation path without relying on overflow
    // (i64 inputs are always representable in i128 for the checked_add call).
    let big_ts_ns: i64 = 1_700_000_000_000_000_000; // ~2023-11-15
    let big_duration_ns: i64 = 3_600_000_000_000; // 1 hour in ns

    let payload = AtuinCommandExecutedPayload::from_raw_history(
        "long-running-cmd",
        RecordedPath::from("/tmp"),
        0,
        big_duration_ns,
        "h1",
        "s1",
        big_ts_ns,
        "test-host",
    )?;
    let payload: Value = serde_json::to_value(&payload)?;

    let ts_start = payload
        .get("ts_start_orig")
        .expect("ts_start_orig should be present");
    let ts_end = payload
        .get("ts_end_orig")
        .expect("ts_end_orig should be present");

    assert_ne!(ts_start, ts_end, "ts_end_orig must differ from ts_start_orig for nonzero duration");
    assert_eq!(
        payload.get("duration_ns").and_then(Value::as_i64),
        Some(big_duration_ns),
        "duration_ns should be preserved exactly"
    );
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

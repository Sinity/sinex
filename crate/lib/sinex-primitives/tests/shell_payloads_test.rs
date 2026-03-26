use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn atuin_payload_builder_rejects_negative_duration() -> TestResult<()> {
    let error = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        0,
        -1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "test-host",
    )
    .expect_err("negative duration should be rejected");

    assert!(error.to_string().contains("duration"));
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
async fn atuin_payload_builder_rejects_invalid_hostname() -> TestResult<()> {
    let error = AtuinCommandExecutedPayload::from_raw_history(
        "echo hi",
        RecordedPath::from("/tmp"),
        0,
        1,
        "h1",
        "s1",
        1_700_000_000_000_000_000,
        "bad_host",
    )
    .expect_err("invalid hostname should be rejected");

    assert!(error.to_string().contains("hostname"));
    Ok(())
}

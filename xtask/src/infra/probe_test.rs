use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn nats_probe_message_includes_connect_error_for_unreachable_port() -> TestResult<()> {
    let message = nats_probe_message(
        false,
        false,
        4222,
        Some("Connection refused (os error 111)"),
    );

    assert_eq!(
        message.as_deref(),
        Some("NATS is not reachable on port 4222: Connection refused (os error 111)")
    );
    Ok(())
}

#[sinex_test]
async fn nats_probe_message_preserves_pid_drift_signal() -> TestResult<()> {
    let message = nats_probe_message(false, true, 4222, None);

    assert_eq!(
        message.as_deref(),
        Some("NATS is reachable on port 4222, but no managed nats-server PID is tracked")
    );
    Ok(())
}

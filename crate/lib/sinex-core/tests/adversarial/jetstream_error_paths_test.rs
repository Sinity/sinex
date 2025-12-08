//! Adversarial coverage for JetStream error paths (publish/connection failures).

use sinex_test_utils::{sinex_test, EphemeralNats, TestResult};

#[sinex_test]
async fn nats_connect_failure_is_surfaceable() -> TestResult<()> {
    // A failure_rate of 1.0 guarantees connection attempts fail.
    let nats = EphemeralNats::start()
        .await
        .expect("nats should start")
        .with_chaos(std::time::Duration::ZERO, 1.0);

    let err = nats
        .connect()
        .await
        .expect_err("chaos failure_rate=1 should force connect error");
    assert!(
        err.to_string()
            .to_lowercase()
            .contains("simulated connection failure"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn publish_fails_when_nats_is_stopped() -> TestResult<()> {
    let mut nats = EphemeralNats::start().await.expect("nats should start");
    let client = nats.connect().await.expect("connect should succeed");
    let js = nats.jetstream_with_client(client);

    // Kill the server process to simulate a hard partition/outage.
    if let Some(mut child) = nats.process.take() {
        let _ = child.start_kill();
    }

    let err = js
        .publish("some.subject", b"payload".to_vec().into())
        .await
        .expect_err("publish should fail when server is down");
    assert!(
        err.to_string().to_lowercase().contains("connection") // generic connection failure
            || err
                .to_string()
                .to_lowercase()
                .contains("stream or consumer was deleted"),
        "unexpected publish error after shutdown: {err}"
    );
    Ok(())
}

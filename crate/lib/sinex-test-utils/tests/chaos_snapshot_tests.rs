use std::time::Duration;

use sinex_test_utils::{sinex_test, ChaosInjestor, TestResult, TestSnapshot};

#[sinex_test]
async fn chaos_injestor_injects_failures() -> TestResult<()> {
    let chaos = ChaosInjestor::new(Duration::from_millis(5), 0.0);
    chaos
        .with_simulated_failures(|| async { Ok::<_, color_eyre::Report>(42) })
        .await?;

    let chaos_fail = ChaosInjestor::new(Duration::from_millis(0), 1.0);
    let result = chaos_fail
        .with_simulated_failures(|| async { Ok::<_, color_eyre::Report>(1) })
        .await;
    assert!(result.is_err(), "failure rate of 1.0 should always fail");
    Ok(())
}

#[sinex_test]
fn snapshot_assertions_work() -> TestResult<()> {
    let mut snapshot = TestSnapshot::new();
    snapshot.db_events = 5;
    snapshot.jetstream_msgs = 3;
    snapshot.dlq_entries = 0;

    snapshot.assert_events_persisted(5).unwrap();
    snapshot.assert_confirmations_received(2).unwrap();
    snapshot.assert_no_dlq_entries().unwrap();

    assert!(snapshot.assert_events_persisted(6).is_err());
    snapshot.dlq_entries = 1;
    assert!(snapshot.assert_no_dlq_entries().is_err());
    Ok(())
}

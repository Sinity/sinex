use super::*;
use sinex_primitives::SinexError;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_metrics_collection() -> TestResult<()> {
    let metrics = GatewayMetrics::disabled();

    metrics.record_request_start();
    metrics.record_request_success(1000); // 1ms

    let snapshot = metrics.snapshot_and_reset();
    assert_eq!(snapshot.total_requests, 1);
    assert_eq!(snapshot.successful_requests, 1);
    assert_eq!(snapshot.latency_count, 1);
    assert_eq!(snapshot.latency_sum_us, 1000);
    Ok(())
}

#[sinex_test]
async fn test_latency_min_max() -> TestResult<()> {
    let metrics = GatewayMetrics::disabled();

    metrics.record_request_start();
    metrics.record_request_success(500); // 0.5ms

    metrics.record_request_start();
    metrics.record_request_success(2000); // 2ms

    metrics.record_request_start();
    metrics.record_request_success(1000); // 1ms

    let snapshot = metrics.snapshot_and_reset();
    assert_eq!(snapshot.latency_min_us, 500);
    assert_eq!(snapshot.latency_max_us, 2000);
    assert_eq!(snapshot.latency_count, 3);
    Ok(())
}

#[sinex_test]
async fn test_restore_snapshot_preserves_failed_interval_and_new_traffic() -> TestResult<()> {
    let metrics = GatewayMetrics::disabled();

    metrics.record_request_start();
    metrics.record_request_success(500);

    let failed_snapshot = metrics.snapshot_and_reset();

    metrics.record_request_start();
    metrics.record_request_success(2000);

    metrics.restore_from_snapshot(&failed_snapshot);

    let snapshot = metrics.snapshot_and_reset();
    assert_eq!(snapshot.total_requests, 2);
    assert_eq!(snapshot.successful_requests, 2);
    assert_eq!(snapshot.latency_count, 2);
    assert_eq!(snapshot.latency_sum_us, 2500);
    assert_eq!(snapshot.latency_min_us, 500);
    assert_eq!(snapshot.latency_max_us, 2000);
    Ok(())
}

#[sinex_test]
async fn emission_task_exits_when_shutdown_sender_is_dropped() -> TestResult<()> {
    let metrics = Arc::new(GatewayMetrics::disabled());
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let handle = metrics.spawn_emission_task(cancel_rx);

    drop(cancel_tx);

    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .map_err(|_| {
            SinexError::timeout("metrics task should exit after shutdown sender drops")
        })??;
    Ok(())
}

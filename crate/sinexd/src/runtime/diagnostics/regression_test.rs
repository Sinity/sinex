use super::{BaselineTracker, EnvironmentInfo, RegressionDetector, windowed_throughput};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn throughput_uses_the_live_window_span() -> TestResult<()> {
    let t0 = Instant::now();
    let samples = VecDeque::from([
        t0,
        t0 + Duration::from_millis(25),
        t0 + Duration::from_millis(50),
        t0 + Duration::from_millis(75),
        t0 + Duration::from_millis(100),
    ]);
    let throughput = windowed_throughput(&samples, t0 + Duration::from_millis(100));
    assert!(
        throughput > 40.0 && throughput < 60.0,
        "expected about 50 operations per second, got {throughput}"
    );
    Ok(())
}

#[sinex_test]
async fn throughput_does_not_keep_stale_samples() -> TestResult<()> {
    let t0 = Instant::now();
    let samples = VecDeque::from([t0]);
    assert_eq!(
        windowed_throughput(&samples, t0 + Duration::from_secs(90)),
        0.0,
        "stale samples must not contribute to a throughput window"
    );
    Ok(())
}

#[sinex_test]
async fn baseline_and_current_performance_report_windowed_throughput() -> TestResult<()> {
    let environment = EnvironmentInfo {
        test_data_size: 1,
        concurrent_operations: 1,
        database_pool_size: 1,
        system_load: "test".to_string(),
    };
    let mut tracker = BaselineTracker::new();
    for _ in 0..10 {
        tracker.record_measurement("op", Duration::from_millis(1), true);
    }
    let baseline = tracker
        .calculate_baseline("op", environment)
        .expect("ten samples should produce a baseline");
    assert!(baseline.throughput.is_finite() && baseline.throughput > 0.0);

    let mut detector = RegressionDetector::new();
    detector.set_baseline(baseline);
    for _ in 0..10 {
        detector.record_measurement("op", Duration::from_millis(1), true);
    }
    let current = detector
        .calculate_current_performance("op")
        .expect("ten samples should produce current performance");
    assert!(current.throughput.is_finite() && current.throughput > 0.0);
    Ok(())
}

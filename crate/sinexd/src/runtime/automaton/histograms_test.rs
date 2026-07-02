use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn latency_window_percentile_on_uniform_distribution() -> TestResult<()> {
    let mut win = LatencyWindow::new(1024);
    for i in 0..1000 {
        win.record(f64::from(i));
    }
    // Nearest-rank p50 of 0..999 is at index ceil(0.5 * 1000) - 1 = 499 → 499.0.
    assert_eq!(win.percentile(0.5), Some(499.0));
    // p99 nearest-rank is index 989 → 989.0.
    assert_eq!(win.percentile(0.99), Some(989.0));
    // p100 is the max.
    assert_eq!(win.percentile(1.0), Some(999.0));
    Ok(())
}

#[sinex_test]
async fn latency_window_overwrites_oldest_when_full() -> TestResult<()> {
    let mut win = LatencyWindow::new(4);
    for i in 0..6 {
        win.record(f64::from(i));
    }
    // Reservoir should hold {2,3,4,5} (in some order).
    assert_eq!(win.len(), 4);
    let mut sorted = win.samples.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(sorted, vec![2.0, 3.0, 4.0, 5.0]);
    Ok(())
}

#[sinex_test]
async fn latency_window_drops_non_finite() -> TestResult<()> {
    let mut win = LatencyWindow::new(8);
    win.record(1.0);
    win.record(f64::NAN);
    win.record(f64::INFINITY);
    win.record(2.0);
    assert_eq!(win.len(), 2);
    assert_eq!(win.percentile(0.5), Some(1.0));
    Ok(())
}

#[sinex_test]
async fn latency_window_empty_returns_none() -> TestResult<()> {
    let win = LatencyWindow::new(8);
    assert_eq!(win.percentile(0.5), None);
    Ok(())
}

#[sinex_test]
async fn throughput_window_eps_uses_live_span_not_window_length() -> TestResult<()> {
    let mut tp = ThroughputWindow::new(Duration::from_mins(1));
    let t0 = Instant::now();
    // Record 5 events spread over 100 ms — should report ~50 eps, not
    // 5 / 60 ≈ 0.083 eps.
    tp.record(t0);
    tp.record(t0 + Duration::from_millis(25));
    tp.record(t0 + Duration::from_millis(50));
    tp.record(t0 + Duration::from_millis(75));
    tp.record(t0 + Duration::from_millis(100));
    let eps = tp.eps(t0 + Duration::from_millis(100));
    assert!(
        eps > 40.0 && eps < 60.0,
        "expected ~50 eps over 100ms, got {eps}"
    );
    Ok(())
}

#[sinex_test]
async fn throughput_window_evicts_stale_samples() -> TestResult<()> {
    let mut tp = ThroughputWindow::new(Duration::from_mins(1));
    let t0 = Instant::now();
    tp.record(t0);
    tp.record(t0 + Duration::from_secs(1));
    // 90 seconds later — both samples should evict.
    let later = t0 + Duration::from_secs(90);
    let eps = tp.eps(later);
    assert_eq!(eps, 0.0, "stale samples must evict from the window");
    Ok(())
}

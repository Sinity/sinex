use super::{SystemMetrics, TestMonitor};
use crate::sandbox::sinex_test;
use std::thread;
use std::time::Duration;

#[sinex_test]
async fn system_metrics_compute_averages() -> ::xtask::sandbox::TestResult<()> {
    let metrics = SystemMetrics {
        cpu_samples: vec![25.0, 75.0],
        mem_samples: vec![1024 * 1024, 2 * 1024 * 1024],
    };

    assert_eq!(metrics.avg_cpu(), 50.0);
    assert_eq!(metrics.max_mem_mb(), 2.0);
    Ok(())
}

#[sinex_test]
async fn test_monitor_collects_samples_before_stop() -> ::xtask::sandbox::TestResult<()> {
    let mut monitor = TestMonitor::start();
    thread::sleep(Duration::from_millis(350));

    let metrics = monitor.stop();

    assert!(
        !metrics.cpu_samples.is_empty(),
        "expected at least one CPU sample"
    );
    assert_eq!(metrics.cpu_samples.len(), metrics.mem_samples.len());
    Ok(())
}

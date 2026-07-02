use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_median() -> TestResult<()> {
    assert_eq!(median(&[1.0, 2.0, 3.0]), 2.0);
    assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), 2.5);
    assert_eq!(median(&[5.0]), 5.0);
    Ok(())
}

#[sinex_test]
async fn test_mean() -> TestResult<()> {
    assert_eq!(mean(&[1.0, 2.0, 3.0, 4.0]), 2.5);
    assert_eq!(mean(&[10.0]), 10.0);
    Ok(())
}

#[sinex_test]
async fn test_run_stats() -> TestResult<()> {
    let samples = vec![100.0, 105.0, 95.0, 110.0, 90.0];
    let stats = RunStats::from_samples(&samples);
    assert!(stats.median_ms > 90.0 && stats.median_ms < 110.0);
    assert!(stats.mean_ms == 100.0);
    assert_eq!(stats.p95_ms, 110.0);
    assert!(stats.throughput_runs_per_sec > 0.0);
    Ok(())
}

#[sinex_test]
async fn test_stddev() -> TestResult<()> {
    let data = vec![100.0, 105.0, 95.0, 110.0, 90.0];
    let mean_val = mean(&data);
    let stddev_val = stddev(&data, mean_val);
    // Sample stddev of [90, 95, 100, 105, 110] = sqrt(62.5) ≈ 7.906
    assert!((stddev_val - 7.906).abs() < 0.01);
    Ok(())
}

#[sinex_test]
async fn test_stddev_identical_values() -> TestResult<()> {
    let data = vec![42.0, 42.0, 42.0];
    let mean_val = mean(&data);
    let stddev_val = stddev(&data, mean_val);
    assert!((stddev_val - 0.0).abs() < f64::EPSILON);
    Ok(())
}

#[sinex_test]
async fn test_median_single_value() -> TestResult<()> {
    assert!((median(&[42.0]) - 42.0).abs() < f64::EPSILON);
    Ok(())
}

#[sinex_test]
async fn test_ci95_small_sample() -> TestResult<()> {
    // With 2 samples, CI should be wide: t_critical(2)=4.303, margin=30.4
    let (lower, upper) = ci95(100.0, 10.0, 2);
    assert!(
        lower < 75.0,
        "CI lower bound should be around 69.6 with n=2"
    );
    assert!(
        upper > 125.0,
        "CI upper bound should be around 130.4 with n=2"
    );
    Ok(())
}

#[sinex_test]
async fn test_ci95_large_sample() -> TestResult<()> {
    // With many samples, CI should be narrow
    let (lower, upper) = ci95(100.0, 10.0, 100);
    assert!(lower > 95.0, "CI should be narrow with n=100");
    assert!(upper < 105.0, "CI should be narrow with n=100");
    Ok(())
}

#[sinex_test]
async fn test_detect_outliers_iqr_no_outliers() -> TestResult<()> {
    let outliers = detect_outliers_iqr(&[98.0, 99.0, 100.0, 101.0, 102.0]);
    assert!(outliers.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_detect_outliers_iqr_with_outlier() -> TestResult<()> {
    let outliers = detect_outliers_iqr(&[100.0, 101.0, 102.0, 103.0, 200.0]);
    assert!(!outliers.is_empty(), "200.0 should be detected as outlier");
    assert!(outliers.contains(&200.0));
    Ok(())
}

#[sinex_test]
async fn test_detect_outliers_iqr_too_few_samples() -> TestResult<()> {
    // With fewer than 4 samples, IQR can't detect outliers reliably
    let outliers = detect_outliers_iqr(&[1.0, 1000.0]);
    // Should not panic regardless of result
    let _ = outliers;
    Ok(())
}

#[sinex_test]
async fn test_percentile_boundaries() -> TestResult<()> {
    let data = &[10.0, 20.0, 30.0, 40.0, 50.0];
    assert!((percentile(data, 0.0) - 10.0).abs() < f64::EPSILON);
    assert!((percentile(data, 100.0) - 50.0).abs() < f64::EPSILON);
    assert!((percentile(data, 50.0) - 30.0).abs() < f64::EPSILON);
    Ok(())
}

#[sinex_test]
async fn test_compare_with_baseline_no_regression() -> TestResult<()> {
    let current = RunStats::from_samples(&[100.0, 102.0, 98.0]);
    let baseline = RunStats::from_samples(&[100.0, 101.0, 99.0]);
    let result = compare_with_baseline(&current, &baseline, 10.0);
    assert!(matches!(result, Regression::None));
    Ok(())
}

#[sinex_test]
async fn test_compare_with_baseline_regression_detected() -> TestResult<()> {
    let current = RunStats::from_samples(&[150.0, 155.0, 145.0]);
    let baseline = RunStats::from_samples(&[100.0, 101.0, 99.0]);
    let result = compare_with_baseline(&current, &baseline, 10.0);
    match result {
        Regression::Detected { pct_change, .. } => {
            assert!(
                pct_change > 40.0,
                "50% slowdown should be detected, got {pct_change}%"
            );
        }
        Regression::None => panic!("Should detect regression for 50% slowdown"),
    }
    Ok(())
}

#[sinex_test]
async fn test_run_stats_from_single_sample() -> TestResult<()> {
    let stats = RunStats::from_samples(&[42.0]);
    assert!((stats.median_ms - 42.0).abs() < f64::EPSILON);
    assert!((stats.mean_ms - 42.0).abs() < f64::EPSILON);
    assert_eq!(stats.sample_count, 1);
    assert!((stats.min_ms - 42.0).abs() < f64::EPSILON);
    assert!((stats.max_ms - 42.0).abs() < f64::EPSILON);
    Ok(())
}

#[sinex_test]
async fn test_run_stats_format_summary() -> TestResult<()> {
    let stats = RunStats::from_samples(&[100.0, 105.0, 95.0]);
    let summary = stats.format_summary();
    assert!(summary.contains("ms"), "Summary should contain 'ms' unit");
    Ok(())
}

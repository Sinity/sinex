use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn parse_psi_avg10_extracts_correct_value() -> xtask::sandbox::TestResult<()> {
    let line = "some avg10=12.34 avg60=8.90 avg300=5.67 total=12345678";
    let value = PressureMonitor::parse_psi_avg10(line);
    assert_eq!(value, Some(12.34));
    Ok(())
}

#[sinex_test]
async fn parse_psi_avg10_returns_none_for_malformed_line() -> xtask::sandbox::TestResult<()> {
    assert!(PressureMonitor::parse_psi_avg10("garbage").is_none());
    assert!(PressureMonitor::parse_psi_avg10("").is_none());
    Ok(())
}

#[sinex_test]
async fn pressure_monitor_below_threshold_does_not_backoff() -> xtask::sandbox::TestResult<()> {
    let monitor = PressureMonitor::new(101.0, 101.0);
    assert!(!monitor.should_backoff());
    Ok(())
}

#[cfg(not(target_os = "linux"))]
#[sinex_test]
async fn non_linux_stub_always_returns_false() -> xtask::sandbox::TestResult<()> {
    let monitor = PressureMonitor::default_thresholds();
    assert!(!monitor.should_backoff());
    assert!(!monitor.io_pressure_high());
    assert!(!monitor.memory_pressure_high());
    Ok(())
}

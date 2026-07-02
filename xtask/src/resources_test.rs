use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_resource_capture() -> TestResult<()> {
    // Should not panic, even if /proc doesn't exist (non-Linux)
    let status = ResourceStatus::capture()?;
    // On Linux, these should be > 0
    if cfg!(target_os = "linux") {
        assert!(status.cpu_count > 0);
    }
    Ok(())
}

#[sinex_test]
async fn test_warning_low_memory() -> TestResult<()> {
    let status = ResourceStatus {
        memory_available_gb: Some(3.0),
        memory_total_gb: Some(32.0),
        load_1min: Some(1.0),
        cpu_count: 8,
    };
    let warning = status.warning(8);
    assert!(warning.is_some());
    assert!(warning.unwrap().contains("Low memory"));
    Ok(())
}

#[sinex_test]
async fn test_warning_high_load() -> TestResult<()> {
    let status = ResourceStatus {
        memory_available_gb: Some(16.0),
        memory_total_gb: Some(32.0),
        load_1min: Some(15.0),
        cpu_count: 8,
    };
    let warning = status.warning(8);
    assert!(warning.is_some());
    assert!(warning.unwrap().contains("High system load"));
    Ok(())
}

#[sinex_test]
async fn test_no_warning_when_ok() -> TestResult<()> {
    let status = ResourceStatus {
        memory_available_gb: Some(16.0),
        memory_total_gb: Some(32.0),
        load_1min: Some(2.0),
        cpu_count: 8,
    };
    assert!(status.warning(8).is_none());
    Ok(())
}

#[sinex_test]
async fn test_summary_reports_missing_memory_honestly() -> TestResult<()> {
    let status = ResourceStatus {
        memory_available_gb: None,
        memory_total_gb: None,
        load_1min: Some(2.0),
        cpu_count: 8,
    };

    assert_eq!(status.summary(), "Memory: unavailable, Load: 2.00 (8 CPUs)");
    assert!(status.warning(8).is_none());
    assert!(status.has_memory_for(8));
    Ok(())
}

#[sinex_test]
async fn test_summary_reports_missing_load_honestly() -> TestResult<()> {
    let status = ResourceStatus {
        memory_available_gb: Some(16.0),
        memory_total_gb: Some(32.0),
        load_1min: None,
        cpu_count: 8,
    };

    assert_eq!(
        status.summary(),
        "Memory: 16.0/32.0GB free, Load: unavailable (8 CPUs)"
    );
    assert!(status.warning(8).is_none());
    assert!(status.load_acceptable());
    Ok(())
}

#[sinex_test]
async fn test_parse_load_1min_rejects_invalid_first_field() -> TestResult<()> {
    let error = parse_load_1min("not-a-number 0.00 0.00 1/1 1").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to parse 1-minute load average"));
    Ok(())
}

#[sinex_test]
async fn test_parse_load_1min_rejects_missing_first_field() -> TestResult<()> {
    let error = parse_load_1min("").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("/proc/loadavg did not contain a 1-minute load value"));
    Ok(())
}

#[sinex_test]
async fn test_parse_memory_info_rejects_missing_memavailable() -> TestResult<()> {
    let error = parse_memory_info("MemTotal: 1024 kB\n").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("/proc/meminfo is missing MemAvailable"));
    Ok(())
}

#[sinex_test]
async fn test_parse_memory_info_rejects_invalid_memtotal() -> TestResult<()> {
    let error = parse_memory_info("MemAvailable: 512 kB\nMemTotal: no\n").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("failed to parse /proc/meminfo entry MemTotal"));
    Ok(())
}

#[sinex_test]
async fn test_pressure_recommendation_classifies_measured_bands() -> TestResult<()> {
    let clear = PressureRecommendation::from_snapshots(
        PressureSnapshot {
            some_avg10: Some(4.0),
            full_avg10: Some(0.0),
            ..PressureSnapshot::default()
        },
        PressureSnapshot {
            some_avg10: Some(8.0),
            full_avg10: Some(2.9),
            ..PressureSnapshot::default()
        },
        PressureSnapshot {
            some_avg10: Some(2.0),
            full_avg10: Some(4.9),
            ..PressureSnapshot::default()
        },
        Some((512.0, 15_000.0)),
    );
    assert_eq!(clear.level, PressureLevel::Clear);
    assert!(clear.warning("test").is_none());

    let elevated = PressureRecommendation::from_snapshots(
        PressureSnapshot::default(),
        PressureSnapshot {
            some_avg10: Some(16.0),
            full_avg10: Some(6.0),
            ..PressureSnapshot::default()
        },
        PressureSnapshot::default(),
        None,
    );
    assert_eq!(elevated.level, PressureLevel::Elevated);
    assert!(elevated.warning("test").is_some());

    let severe = PressureRecommendation::from_snapshots(
        PressureSnapshot::default(),
        PressureSnapshot {
            some_avg10: Some(22.0),
            full_avg10: Some(14.0),
            ..PressureSnapshot::default()
        },
        PressureSnapshot {
            some_avg10: Some(28.0),
            full_avg10: Some(24.0),
            ..PressureSnapshot::default()
        },
        None,
    );
    assert_eq!(severe.level, PressureLevel::Severe);
    assert!(severe.warning("test").is_some());
    Ok(())
}

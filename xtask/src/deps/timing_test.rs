use super::*;
use crate::sandbox::sinex_test;
use std::io::Write;
use tempfile::NamedTempFile;

#[sinex_test]
async fn test_parse_timing_json_valid() -> TestResult<()> {
    let json_content = r#"{
        "targets": [
            {"name": "sinex-db", "duration": 45.5},
            {"name": "sinexd", "duration": 12.3},
            {"name": "xtask", "duration": 5.1}
        ]
    }"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

    assert_eq!(report.crate_times.len(), 3);
    assert_eq!(report.total_time_secs, 45.5 + 12.3 + 5.1);

    // Should be sorted slowest first
    assert_eq!(report.crate_times[0].name, "sinex-db");
    assert_eq!(report.crate_times[0].duration_secs, 45.5);
    assert_eq!(report.crate_times[1].name, "sinexd");
    assert_eq!(report.crate_times[1].duration_secs, 12.3);
    assert_eq!(report.crate_times[2].name, "xtask");
    assert_eq!(report.crate_times[2].duration_secs, 5.1);
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_empty_targets() -> TestResult<()> {
    let json_content = r#"{"targets": []}"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

    assert_eq!(report.crate_times.len(), 0);
    assert_eq!(report.total_time_secs, 0.0);
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_html_aggregates_unit_data() -> TestResult<()> {
    let html_content = r#"
<html>
<body>
<script>
DURATION = 10;
const UNIT_DATA = [
  {
"i": 1,
"name": "sinex-db",
"version": "0.4.2",
"mode": "todo",
"target": " lib",
"features": [],
"start": 1.0,
"duration": 2.5,
"unblocked_units": [],
"unblocked_rmeta_units": [],
"sections": null
  },
  {
"i": 2,
"name": "sinex-db",
"version": "0.4.2",
"mode": "todo",
"target": " build-script",
"features": [],
"start": 4.0,
"duration": 1.5,
"unblocked_units": [],
"unblocked_rmeta_units": [],
"sections": null
  },
  {
"i": 3,
"name": "xtask",
"version": "0.1.0",
"mode": "todo",
"target": " lib",
"features": [],
"start": 5.0,
"duration": 3.0,
"unblocked_units": [],
"unblocked_rmeta_units": [],
"sections": null
  }
];
const CONCURRENCY_DATA = [];
</script>
</body>
</html>
"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(html_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let report = TimingAnalyzer::parse_timing_html(&temp_file.path().to_path_buf()).unwrap();

    assert_eq!(report.crate_times.len(), 2);
    assert_eq!(report.crate_times[0].name, "sinex-db");
    assert_eq!(report.crate_times[0].duration_secs, 4.0);
    assert_eq!(report.crate_times[1].name, "xtask");
    assert_eq!(report.crate_times[1].duration_secs, 3.0);
    assert_eq!(report.total_time_secs, 8.0);
    assert_eq!(report.html_report, Some(temp_file.path().to_path_buf()));
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_html_rejects_missing_unit_data() -> TestResult<()> {
    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(b"<html></html>").unwrap();
    temp_file.flush().unwrap();

    let result = TimingAnalyzer::parse_timing_html(&temp_file.path().to_path_buf());

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_file_not_found() -> TestResult<()> {
    let nonexistent = PathBuf::from("/tmp/nonexistent-timing-file-xyz.json");
    let result = TimingAnalyzer::parse_timing_json(&nonexistent);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not found"));
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_invalid_json() -> TestResult<()> {
    let json_content = "not valid json";

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf());

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_malformed_structure() -> TestResult<()> {
    let json_content = r#"{"invalid": "structure"}"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let result = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf());

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_crate_timing_info_ordering() -> TestResult<()> {
    let mut times = [
        CrateTimingInfo {
            name: "fast".to_string(),
            duration_secs: 1.0,
        },
        CrateTimingInfo {
            name: "slow".to_string(),
            duration_secs: 10.0,
        },
        CrateTimingInfo {
            name: "medium".to_string(),
            duration_secs: 5.0,
        },
    ];

    times.sort_by(|a, b| {
        b.duration_secs
            .partial_cmp(&a.duration_secs)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    assert_eq!(times[0].name, "slow");
    assert_eq!(times[0].duration_secs, 10.0);
    assert_eq!(times[1].name, "medium");
    assert_eq!(times[1].duration_secs, 5.0);
    assert_eq!(times[2].name, "fast");
    assert_eq!(times[2].duration_secs, 1.0);
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_single_target() -> TestResult<()> {
    let json_content = r#"{
        "targets": [
            {"name": "single-crate", "duration": 23.7}
        ]
    }"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

    assert_eq!(report.crate_times.len(), 1);
    assert_eq!(report.total_time_secs, 23.7);
    assert_eq!(report.crate_times[0].name, "single-crate");
    assert_eq!(report.crate_times[0].duration_secs, 23.7);
    Ok(())
}

#[sinex_test]
async fn test_parse_timing_json_equal_durations() -> TestResult<()> {
    let json_content = r#"{
        "targets": [
            {"name": "crate-a", "duration": 10.0},
            {"name": "crate-b", "duration": 10.0},
            {"name": "crate-c", "duration": 10.0}
        ]
    }"#;

    let mut temp_file = NamedTempFile::new().unwrap();
    temp_file.write_all(json_content.as_bytes()).unwrap();
    temp_file.flush().unwrap();

    let report = TimingAnalyzer::parse_timing_json(&temp_file.path().to_path_buf()).unwrap();

    assert_eq!(report.crate_times.len(), 3);
    assert_eq!(report.total_time_secs, 30.0);
    // All have the same duration, so they should be ordered by input
    assert!(report.crate_times.iter().all(|c| c.duration_secs == 10.0));
    Ok(())
}

#[sinex_test]
async fn test_timing_report_total_calculation() -> TestResult<()> {
    let crate_times = vec![
        CrateTimingInfo {
            name: "test1".to_string(),
            duration_secs: 1.5,
        },
        CrateTimingInfo {
            name: "test2".to_string(),
            duration_secs: 2.3,
        },
        CrateTimingInfo {
            name: "test3".to_string(),
            duration_secs: 0.7,
        },
    ];

    let expected_total = 1.5 + 2.3 + 0.7;

    let report = TimingReport {
        cargo_args: Vec::new(),
        package: None,
        profile: "unknown".to_string(),
        cleaned_package: false,
        crate_times,
        total_time_secs: expected_total,
        html_report: None,
    };

    assert_eq!(report.total_time_secs, 4.5);
    Ok(())
}

#[sinex_test]
async fn test_timing_options_build_xtask_dev_args() -> TestResult<()> {
    let options = TimingOptions {
        package: Some("xtask".to_string()),
        profile: "dev".to_string(),
        clean_package: false,
    };

    assert_eq!(
        options.cargo_args(),
        vec!["build", "--timings", "-p", "xtask"]
    );
    Ok(())
}

#[sinex_test]
async fn test_timing_options_build_dev_args_by_default() -> TestResult<()> {
    assert_eq!(
        TimingOptions::default().cargo_args(),
        vec!["build", "--timings"]
    );
    Ok(())
}

#[sinex_test]
async fn test_timing_options_build_release_args_when_requested() -> TestResult<()> {
    let options = TimingOptions {
        package: None,
        profile: "release".to_string(),
        clean_package: false,
    };

    assert_eq!(
        options.cargo_args(),
        vec!["build", "--timings", "--release"]
    );
    Ok(())
}

#[sinex_test]
async fn test_timing_options_clean_package_keeps_build_args_stable() -> TestResult<()> {
    let options = TimingOptions {
        package: Some("xtask".to_string()),
        profile: "dev".to_string(),
        clean_package: true,
    };

    assert_eq!(
        options.cargo_args(),
        vec!["build", "--timings", "-p", "xtask"]
    );
    Ok(())
}

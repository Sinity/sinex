use super::*;
use crate::bench::runner::RunResult;
use crate::sandbox::sinex_test;
use tempfile::TempDir;

fn test_db() -> (TempDir, HistoryDb) {
    let dir = TempDir::new().unwrap();
    let db = HistoryDb::open(&dir.path().join("bench.db")).unwrap();
    (dir, db)
}

fn test_metadata(git_sha: &str) -> BenchRunMetadata {
    BenchRunMetadata {
        mode: "sweeps".to_string(),
        profile: "fast".to_string(),
        git_sha: git_sha.to_string(),
        git_branch: "main".to_string(),
        git_dirty: false,
        rustc_version: "1.75.0".to_string(),
    }
}

fn sample_results() -> Vec<ScenarioResult> {
    vec![ScenarioResult {
        scenario: Scenario {
            threads: 12,
            package: String::new(),
            db_pool_size: None,
        },
        runs: vec![
            RunResult {
                success: true,
                elapsed_ms: 100.0,
                stdout: String::new(),
                stderr: String::new(),
            },
            RunResult {
                success: true,
                elapsed_ms: 105.0,
                stdout: String::new(),
                stderr: String::new(),
            },
            RunResult {
                success: true,
                elapsed_ms: 95.0,
                stdout: String::new(),
                stderr: String::new(),
            },
        ],
        stats: RunStats::from_samples(&[100.0, 105.0, 95.0]),
    }]
}

#[sinex_test]
async fn test_open_creates_schema() -> TestResult<()> {
    let (_dir, _db) = test_db();
    // If we get here without error, schema was created
    Ok(())
}

#[sinex_test]
async fn test_open_idempotent() -> TestResult<()> {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bench.db");
    let _db1 = HistoryDb::open(&path).unwrap();
    let _db2 = HistoryDb::open(&path).unwrap();
    Ok(())
}

#[sinex_test]
async fn test_save_run() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    let run_id = db.save_run(&test_metadata("abc123"), &results).unwrap();
    assert!(run_id > 0);
    Ok(())
}

#[sinex_test]
async fn test_get_trend_empty() -> TestResult<()> {
    let (_dir, db) = test_db();
    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let trend = db.get_trend(&scenario, 5).unwrap();
    assert!(trend.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_get_trend_after_save() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    db.save_run(&test_metadata("abc123"), &results).unwrap();

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let trend = db.get_trend(&scenario, 5).unwrap();
    assert_eq!(trend.len(), 1);
    assert!((trend[0].median_ms - 100.0).abs() < 1.0);
    assert!(trend[0].p95_ms > 0.0);
    assert!(trend[0].throughput_runs_per_sec > 0.0);
    assert_eq!(trend[0].git_sha, "abc123");
    Ok(())
}

#[sinex_test]
async fn test_get_trend_respects_limit() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    for i in 0..10 {
        db.save_run(&test_metadata(&format!("sha{i}")), &results)
            .unwrap();
    }

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let trend = db.get_trend(&scenario, 3).unwrap();
    assert_eq!(trend.len(), 3);
    Ok(())
}

#[sinex_test]
async fn test_get_baseline() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    db.save_run(&test_metadata("abc123"), &results).unwrap();

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let baseline = db.get_rolling_baseline(&scenario, None, 5).unwrap();
    assert!(baseline.is_some());
    let stats = baseline.unwrap();
    assert!((stats.median_ms - 100.0).abs() < 1.0);
    assert!(stats.p95_ms > 0.0);
    assert!(stats.throughput_runs_per_sec > 0.0);
    Ok(())
}

#[sinex_test]
async fn test_get_baseline_excludes_run_id() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    let run_id = db.save_run(&test_metadata("abc123"), &results).unwrap();

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let baseline = db.get_rolling_baseline(&scenario, Some(run_id), 5).unwrap();
    // Only one run, excluding it should give None
    assert!(baseline.is_none());
    Ok(())
}

#[sinex_test]
async fn test_summarize_scenarios() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    let run_id = db.save_run(&test_metadata("abc123"), &results).unwrap();

    let summaries = db
        .summarize_scenarios(&results, Some(run_id), 10.0, 5)
        .unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].scenario_key, "t=12");
    Ok(())
}

#[sinex_test]
async fn test_multiple_scenarios() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = vec![
        ScenarioResult {
            scenario: Scenario {
                threads: 12,
                package: String::new(),
                db_pool_size: None,
            },
            runs: vec![RunResult {
                success: true,
                elapsed_ms: 100.0,
                stdout: String::new(),
                stderr: String::new(),
            }],
            stats: RunStats::from_samples(&[100.0]),
        },
        ScenarioResult {
            scenario: Scenario {
                threads: 24,
                package: String::new(),
                db_pool_size: None,
            },
            runs: vec![RunResult {
                success: true,
                elapsed_ms: 80.0,
                stdout: String::new(),
                stderr: String::new(),
            }],
            stats: RunStats::from_samples(&[80.0]),
        },
    ];

    db.save_run(&test_metadata("abc123"), &results).unwrap();

    let trend_12 = db
        .get_trend(
            &Scenario {
                threads: 12,
                package: String::new(),
                db_pool_size: None,
            },
            5,
        )
        .unwrap();
    let trend_24 = db
        .get_trend(
            &Scenario {
                threads: 24,
                package: String::new(),
                db_pool_size: None,
            },
            5,
        )
        .unwrap();
    assert_eq!(trend_12.len(), 1);
    assert_eq!(trend_24.len(), 1);
    assert!((trend_12[0].median_ms - 100.0).abs() < 1.0);
    assert!((trend_24[0].median_ms - 80.0).abs() < 1.0);
    Ok(())
}

#[sinex_test]
async fn test_rolling_baseline_single_run_fallback() -> TestResult<()> {
    let (_dir, db) = test_db();
    let results = sample_results();
    let run_id = db.save_run(&test_metadata("abc123"), &results).unwrap();

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let baseline = db.get_rolling_baseline(&scenario, Some(run_id), 5).unwrap();
    // Only one run which is excluded — should return None
    assert!(baseline.is_none());

    // Now get without excluding — single run fallback
    let baseline = db.get_rolling_baseline(&scenario, None, 5).unwrap();
    assert!(baseline.is_some());
    assert!((baseline.unwrap().median_ms - 100.0).abs() < 1.0);
    Ok(())
}

#[sinex_test]
async fn test_rolling_baseline_five_run_median() -> TestResult<()> {
    let (_dir, db) = test_db();
    let medians = [100.0, 102.0, 98.0, 101.0, 500.0];
    for (i, median) in medians.iter().enumerate() {
        let results = vec![ScenarioResult {
            scenario: Scenario {
                threads: 12,
                package: String::new(),
                db_pool_size: None,
            },
            runs: vec![RunResult {
                success: true,
                elapsed_ms: *median,
                stdout: String::new(),
                stderr: String::new(),
            }],
            stats: RunStats::from_samples(&[*median]),
        }];
        db.save_run(&test_metadata(&format!("sha{i}")), &results)
            .unwrap();
    }

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let baseline = db
        .get_rolling_baseline(&scenario, None, 5)
        .unwrap()
        .unwrap();
    // Sorted medians: [98, 100, 101, 102, 500] → median index 2 → 101
    assert!((baseline.median_ms - 101.0).abs() < 1.0);
    Ok(())
}

#[sinex_test]
async fn test_rolling_baseline_outlier_immunity() -> TestResult<()> {
    let (_dir, db) = test_db();
    // 4 normal + 1 extreme outlier
    let medians = [100.0, 102.0, 98.0, 101.0, 500.0];
    for (i, median) in medians.iter().enumerate() {
        let results = vec![ScenarioResult {
            scenario: Scenario {
                threads: 12,
                package: String::new(),
                db_pool_size: None,
            },
            runs: vec![RunResult {
                success: true,
                elapsed_ms: *median,
                stdout: String::new(),
                stderr: String::new(),
            }],
            stats: RunStats::from_samples(&[*median]),
        }];
        db.save_run(&test_metadata(&format!("sha{i}")), &results)
            .unwrap();
    }

    let scenario = Scenario {
        threads: 12,
        package: String::new(),
        db_pool_size: None,
    };
    let baseline = db
        .get_rolling_baseline(&scenario, None, 5)
        .unwrap()
        .unwrap();
    // With rolling baseline: median of [98, 100, 101, 102, 500] = 101
    // Current run at 105ms: regression = (105-101)/101 = 3.96% — under 8% threshold
    // Old single-prior behavior would use 500ms: bogus -79% "improvement"
    assert!(
        baseline.median_ms < 110.0,
        "Rolling baseline should ignore outlier 500ms, got {}",
        baseline.median_ms
    );
    assert!(
        baseline.median_ms > 90.0,
        "Rolling baseline should be around 101ms, got {}",
        baseline.median_ms
    );
    Ok(())
}

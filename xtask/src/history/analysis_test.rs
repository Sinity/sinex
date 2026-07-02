use super::*;
use crate::history::{InvocationStatus, TestResult, TestStatus};
use crate::sandbox::prelude::*;
use tempfile::tempdir;

#[sinex_test]
async fn test_status_summary_snapshot_uses_global_test_rate_without_package_fanout()
-> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-status-summary-snapshot.db");
    let db = HistoryDb::open(&db_path)?;

    let check_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(check_id, InvocationStatus::Success, Some(0), 1.0)?;

    let test_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(test_id, InvocationStatus::Success, Some(0), 2.0)?;
    db.store_test_results(
        test_id,
        &[TestResult {
            test_name: "status_summary_smoke".into(),
            package: "sinex-status".into(),
            status: TestStatus::Pass,
            duration_secs: Some(0.2),
            attempt: 1,
            output: None,
        }],
    )?;

    let analysis = HistoryAnalysis::new(&db);
    let snapshot = analysis.status_summary_snapshot()?;

    assert!(snapshot.health.packages.is_empty());
    assert_eq!(snapshot.health.avg_test_pass_rate, Some(1.0));
    assert!(snapshot.health.score > 0);
    assert!(snapshot.recommendations.is_empty());
    Ok(())
}

//! Regression coverage for sinex-erbt: `record_system_metrics` /
//! `record_resource_metrics` run bare `UPDATE ... WHERE id = ?` statements
//! and discard the affected-row count. An UPDATE against a stale or
//! nonexistent `invocation_id` matches zero rows in SQLite without erroring,
//! so both functions return `Ok(())` even though nothing was written.

use super::*;
use tempfile::tempdir;
use xtask::sandbox::{TestResult, sinex_test};

#[sinex_test]
#[ignore = "sinex-erbt open: record_system_metrics returns Ok(()) for a nonexistent \
            invocation_id because it discards the UPDATE's affected-row count -- a stale \
            id silently drops the metrics instead of surfacing an error"]
async fn record_system_metrics_errors_on_nonexistent_invocation_id() -> TestResult<()> {
    let dir = tempdir()?;
    let db = HistoryDb::open(&dir.path().join("metrics-swallow.db"))?;

    let nonexistent_invocation_id = 999_999_i64;
    let result = db.record_system_metrics(nonexistent_invocation_id, 12.5, 256.0);

    assert!(
        result.is_err(),
        "record_system_metrics returned Ok(()) for a nonexistent invocation_id -- the \
         zero-affected-rows UPDATE was silently accepted as success"
    );
    Ok(())
}

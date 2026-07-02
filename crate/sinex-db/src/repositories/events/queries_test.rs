use super::extract_plan_rows;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn extract_plan_rows_reads_estimate() -> TestResult<()> {
    let plan = serde_json::json!([{"Plan": {"Plan Rows": 42}}]);
    assert_eq!(extract_plan_rows(&plan), 42);
    Ok(())
}

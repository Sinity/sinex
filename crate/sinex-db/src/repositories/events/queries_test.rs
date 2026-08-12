use super::extract_plan_rows;
use super::validate_keyset_limit;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn extract_plan_rows_reads_estimate() -> TestResult<()> {
    let plan = serde_json::json!([{"Plan": {"Plan Rows": 42}}]);
    assert_eq!(extract_plan_rows(&plan), 42);
    Ok(())
}

#[test]
fn keyset_limit_rejects_unbounded_or_zero_pages() {
    assert!(validate_keyset_limit(1).is_ok());
    assert!(validate_keyset_limit(0).is_err());
    assert!(validate_keyset_limit(-1).is_err());
}

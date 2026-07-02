use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_status_symbol() -> TestResult<()> {
    assert_eq!(Status::Success.symbol(), "✓");
    assert_eq!(Status::Failed.symbol(), "✗");
    Ok(())
}

#[sinex_test]
async fn test_command_result_json() -> TestResult<()> {
    let result = CommandResult::success("test", 1.5)
        .with_subcommand("fast")
        .with_error(StructuredError::new("E001", "Test failed"));

    let json = serde_json::to_string(&result)?;
    assert!(json.contains("\"command\":\"test\""));
    assert!(json.contains("\"subcommand\":\"fast\""));
    assert!(json.contains("\"status\":\"success\""));
    Ok(())
}

use sinexctl::fmt::with_spinner_result;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_with_spinner_result_success() -> TestResult<()> {
    let result: Result<i32, &str> =
        with_spinner_result("Testing...", "Success!", async { Ok(42) }).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
    Ok(())
}

#[sinex_test]
async fn test_with_spinner_result_failure() -> TestResult<()> {
    let result: Result<i32, &str> =
        with_spinner_result("Testing...", "Success!", async { Err("test error") }).await;

    assert!(result.is_err());
    Ok(())
}


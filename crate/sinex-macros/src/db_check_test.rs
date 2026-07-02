use super::pascal_to_snake_case;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn snake_case_conversion() -> TestResult<()> {
    assert_eq!(pascal_to_snake_case("Healthy"), "healthy");
    assert_eq!(pascal_to_snake_case("FailedRetryable"), "failed_retryable");
    assert_eq!(pascal_to_snake_case("Ingestor"), "ingestor");
    Ok(())
}

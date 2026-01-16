use sinex_core::db::query_helpers::{is_retryable_db_error, RetryConfig};
use sinex_core::types::error::SinexError;
use sinex_core::types::{retry, timeouts};
use sinex_test_utils::sinex_test;
use sinex_test_utils::TestResult;

#[sinex_test]
async fn retry_config_default() -> TestResult<()> {
    let config = RetryConfig::default();
    assert_eq!(config.max_attempts, retry::MAX_RETRY_ATTEMPTS);
    assert_eq!(
        config.initial_delay,
        timeouts::DEFAULT_TERMINAL_POLL_INTERVAL
    );
    assert_eq!(config.max_delay, timeouts::RETRY_MAX_DELAY);
    assert_eq!(config.exponential_base, retry::BACKOFF_MULTIPLIER as f64);
    Ok(())
}

#[sinex_test]
async fn is_retryable_db_error_recognises_non_retryable_cases() -> TestResult<()> {
    let timeout_err = SinexError::timeout("test timeout");
    assert!(!is_retryable_db_error(&timeout_err));

    let db_err = SinexError::database("test database error");
    assert!(!is_retryable_db_error(&db_err));
    Ok(())
}

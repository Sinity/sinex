use super::db_error;
use sinex_primitives::SinexError;
use xtask::sandbox::sinex_test;

// Small inline tests are justified here because they exercise the local db_error
// classification helper directly.
#[sinex_test]
async fn db_error_classifies_row_not_found() -> TestResult<()> {
    let error = db_error(sqlx::Error::RowNotFound, "lookup event");
    assert!(matches!(error, SinexError::NotFound(_)));
    assert_eq!(error.message(), "lookup event");
    assert_eq!(
        error.context_map().get("operation"),
        Some(&"database".to_string())
    );
    assert_eq!(
        error.kind(),
        sinex_primitives::error::SinexErrorKind::NotFound
    );
    assert!(!error.source_chain().is_empty());
    Ok(())
}

#[sinex_test]
async fn db_error_classifies_pool_timeout() -> TestResult<()> {
    let error = db_error(sqlx::Error::PoolTimedOut, "begin transaction");
    assert!(matches!(error, SinexError::Timeout(_)));
    assert_eq!(error.message(), "begin transaction");
    assert_eq!(
        error.context_map().get("timeout_reason"),
        Some(&"pool_exhausted".to_string())
    );
    Ok(())
}

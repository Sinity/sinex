use sinex_primitives::SinexError;

pub type DbResult<T> = Result<T, SinexError>;

/// Convert a `SQLx` error into a `SinexError` with full error chain preserved
#[must_use]
pub fn db_error(e: sqlx::Error, context_msg: impl ToString) -> SinexError {
    let context_msg = context_msg.to_string();
    let mut err = match &e {
        sqlx::Error::RowNotFound => SinexError::not_found(&context_msg),
        sqlx::Error::PoolTimedOut => {
            SinexError::timeout(&context_msg).with_context("timeout_reason", "pool_exhausted")
        }
        sqlx::Error::Database(db_err) => {
            use sqlx::error::ErrorKind;

            let mut err = match db_err.kind() {
                ErrorKind::UniqueViolation => SinexError::already_exists(&context_msg),
                ErrorKind::ForeignKeyViolation
                | ErrorKind::NotNullViolation
                | ErrorKind::CheckViolation => SinexError::validation(&context_msg),
                ErrorKind::Other | _ => SinexError::database(&context_msg),
            };

            if let Some(code) = db_err.code() {
                err = err.with_context("sqlstate", code.as_ref());
            }
            if let Some(constraint) = db_err.constraint() {
                err = err.with_context("constraint", constraint);
            }
            if let Some(table) = db_err.table() {
                err = err.with_context("table", table);
            }

            err.with_context("database_error_kind", format!("{:?}", db_err.kind()))
        }
        _ => SinexError::database(&context_msg),
    };

    err = err.with_std_error(&e).with_context("operation", "database");
    err
}

#[cfg(test)]
mod tests {
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
        assert_eq!(error.context_map().get("operation"), Some(&"database".to_string()));
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
}

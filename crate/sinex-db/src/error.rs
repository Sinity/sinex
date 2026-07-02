use sinex_primitives::SinexError;

pub type DbResult<T> = Result<T, SinexError>;

/// Convert a `SQLx` error into a typed `SinexError` with the full source chain preserved.
#[must_use]
#[allow(
    clippy::needless_pass_by_value,
    reason = "sqlx::Error ownership matches the repository call-site error boundary"
)]
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

    err = err
        .with_error_source(&e)
        .with_context("operation", "database");
    err
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;

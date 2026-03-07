use sinex_primitives::SinexError;
use sqlx::error::DatabaseError;

pub type DbResult<T> = Result<T, SinexError>;

/// Convert a `SQLx` error into a `SinexError` with full error chain preserved
#[must_use]
pub fn db_error(e: sqlx::Error, context_msg: impl ToString) -> SinexError {
    let mut err = SinexError::database(context_msg.to_string())
        .with_std_error(&e)
        .with_context("operation", "database");

    if let sqlx::Error::Database(db_err) = &e {
        if let Some(code) = db_err.code() {
            err = err.with_context("sqlstate", code.as_ref());
        }
        if let Some(constraint) = db_err.constraint() {
            err = err.with_context("constraint", constraint);
        }
    }

    err
}

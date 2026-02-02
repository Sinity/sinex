use sinex_primitives::SinexError;

pub type DbResult<T> = Result<T, SinexError>;

/// Convert a SQLx error into a SinexError with full error chain preserved
#[must_use]
pub fn db_error(e: sqlx::Error, context_msg: impl ToString) -> SinexError {
    SinexError::database(context_msg.to_string())
        .with_std_error(&e)
        .with_context("operation", "database")
}

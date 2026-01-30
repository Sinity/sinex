use sinex_primitives::SinexError;

pub type DbResult<T> = Result<T, SinexError>;

/// Convert a SQLx error into a SinexError with context
pub fn db_error(e: sqlx::Error, context_msg: impl ToString) -> SinexError {
    SinexError::database(e.to_string()).with_context("operation", context_msg.to_string())
}

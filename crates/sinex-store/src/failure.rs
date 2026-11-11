//! Store errors as the shared failures every surface reports: each error is
//! classified once here, so a caller can tell input to fix from a database to
//! wait for.
use crate::Error;
use command_surface::{Failure, FailureKind};

/// A database error's shared kind, retryability and code. The SQLSTATE is
/// kept as the code wherever a constraint name does not say more.
pub fn database_failure(error: &sqlx::Error) -> Failure {
    let message = error.to_string();
    match error {
        sqlx::Error::RowNotFound => Failure::new(FailureKind::NotFound, message),
        // A connection URL or option this build cannot use.
        sqlx::Error::Configuration(_) => Failure::new(FailureKind::BadValue, message),
        sqlx::Error::PoolTimedOut => {
            Failure::new(FailureKind::Timeout, message).with_code("read_pool_busy")
        }
        sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::WorkerCrashed => {
            Failure::new(FailureKind::NotRunning, message).with_code("database_unavailable")
        }
        sqlx::Error::Database(database) => {
            let state = database.code().map(|code| code.into_owned());
            let constraint = database.constraint().map(str::to_owned);
            let (kind, retryable, code) = match state.as_deref() {
                Some("23505") => (FailureKind::Conflict, false, constraint.or(state)),
                Some("23503" | "23502" | "23514") => {
                    (FailureKind::BadValue, false, constraint.or(state))
                }
                Some("57014") => (FailureKind::Timeout, true, Some("statement_timeout".into())),
                // The reader role may not do this.
                Some("42501") => (FailureKind::Refused, false, state),
                Some("40001" | "40P01") => (FailureKind::Conflict, true, state),
                // A connection that ended, or a server shutting down.
                Some(class) if class.starts_with("08") || class.starts_with("57P") => (
                    FailureKind::NotRunning,
                    true,
                    Some("database_unavailable".into()),
                ),
                _ => (FailureKind::Io, false, state),
            };
            let mut failure = Failure::new(kind, message);
            failure.retryable = retryable;
            failure.code = code;
            failure
        }
        _ => Failure::new(FailureKind::Io, message),
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        match error {
            // Without the query text there is no place to mark; methods that
            // hold the text convert with `QueryError::failure` instead.
            Error::Query(error) => error.failure(""),
            Error::Database(error) => database_failure(&error),
            Error::Schema(error) => schema_failure(error),
            Error::Blob(error) => blob_failure(&error),
            Error::Invalid(message) => Failure::new(FailureKind::BadValue, message),
            Error::NotFound(message) => Failure::new(FailureKind::NotFound, message),
            Error::StaleCursor => Failure::new(FailureKind::StaleCursor, error.to_string()),
            Error::StaleCheckpoint => {
                let mut failure = Failure::new(FailureKind::Conflict, error.to_string())
                    .with_code("stale_checkpoint");
                failure.retryable = true;
                failure
            }
            Error::Unsealed(_) => Failure::new(FailureKind::Refused, error.to_string()),
        }
    }
}

/// A blob store error: a missing file is not found and a sealed blob without
/// its key is refused; damaged bytes are the disk's failure.
pub fn blob_failure(error: &hashed_blobs::Error) -> Failure {
    use hashed_blobs::Error as Blob;
    let kind = match error {
        Blob::Io(error) => io_kind(error.kind()),
        Blob::Locked(_) | Blob::Refused(_) => FailureKind::Refused,
        Blob::Corrupt { .. } | Blob::Unwrap(_) => FailureKind::Io,
    };
    Failure::new(kind, error.to_string())
}

/// A file error's shared kind: a missing file is not found, a forbidden one
/// refused, a slow one timed out; anything else is the disk's failure.
pub fn io_kind(kind: std::io::ErrorKind) -> FailureKind {
    match kind {
        std::io::ErrorKind::NotFound => FailureKind::NotFound,
        std::io::ErrorKind::PermissionDenied => FailureKind::Refused,
        std::io::ErrorKind::TimedOut => FailureKind::Timeout,
        _ => FailureKind::Io,
    }
}

/// A schema error: the database's own errors keep their classes; a database
/// this build cannot use is refused with the step that fixes it.
pub fn schema_failure(error: sinex_schema::Error) -> Failure {
    use sinex_schema::Error as Schema;
    match error {
        Schema::Database(error) => database_failure(&error),
        Schema::WriterRunning => {
            let mut failure =
                Failure::new(FailureKind::Conflict, error.to_string()).with_code("writer_running");
            failure.retryable = true;
            failure
        }
        Schema::Declaration(_) => Failure::new(FailureKind::Io, error.to_string()),
        Schema::Version { .. }
        | Schema::UpgradeNeeded { .. }
        | Schema::Unversioned
        | Schema::UnsafeReadBoundary => Failure::new(FailureKind::Refused, error.to_string()),
    }
}

#[cfg(test)]
mod tests;

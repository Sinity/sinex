//! The daemon's error. Each layer's typed error converts into it with `?`,
//! keeping its type until the method boundary, where it becomes the one shared
//! failure every surface reports ([`Failure`]).
use command_surface::{Failure, FailureKind};
use sinex_store::failure::io_kind;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// A failure classified where it arose: a request to fix, a missing
    /// record, a refusal.
    #[error(transparent)]
    Failure(#[from] Failure),
    /// A refusal of query text; [`Error::in_query`] places it in the text.
    #[error(transparent)]
    Query(#[from] sinex_query::QueryError),
    #[error(transparent)]
    Store(#[from] sinex_store::Error),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Schema(#[from] sinex_schema::Error),
    #[error(transparent)]
    Write(#[from] crate::writer::WriteError),
    #[error(transparent)]
    Blob(#[from] hashed_blobs::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A file that could not be read or written, with its path.
    #[error("{}: {source}", path.display())]
    File {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Source(#[from] sinex_source_core::DriverError),
    #[error(transparent)]
    Sealing(#[from] sinex_sealing::Error),
    #[error(transparent)]
    Telemetry(#[from] crate::telemetry::Error),
    /// A task of the daemon's own that panicked or was cancelled.
    #[error(transparent)]
    Task(#[from] tokio::task::JoinError),
    /// The NATS server or a stream on it.
    #[error("NATS: {0}")]
    Nats(String),
}

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub(crate) fn new(kind: FailureKind, message: impl std::fmt::Display) -> Self {
        Self::Failure(Failure::new(kind, message.to_string()))
    }
    /// A request the caller must change: a missing or contradictory argument.
    pub(crate) fn bad_request(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::BadRequest, message)
    }
    /// An argument whose value cannot be used.
    pub(crate) fn bad_value(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::BadValue, message)
    }
    pub(crate) fn not_found(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::NotFound, message)
    }
    pub(crate) fn conflict(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::Conflict, message)
    }
    /// A request this caller or this state does not allow, with the reason.
    pub(crate) fn refused(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::Refused, message)
    }
    pub(crate) fn not_built(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::NotBuilt, message)
    }
    pub(crate) fn not_configured(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::NotConfigured, message)
    }
    pub(crate) fn no_writer() -> Self {
        Self::Failure(
            Failure::new(
                FailureKind::NotConfigured,
                "this daemon has no writer; start it with `sinexd run`",
            )
            .with_code("no_writer"),
        )
    }
    /// A failure inside the daemon that the caller cannot fix by changing the
    /// request: stored data it cannot read, a broken invariant.
    pub(crate) fn internal(message: impl std::fmt::Display) -> Self {
        Self::new(FailureKind::Internal, message)
    }
    pub(crate) fn file(path: impl AsRef<Path>, source: std::io::Error) -> Self {
        Self::File {
            path: path.as_ref().to_owned(),
            source,
        }
    }
    pub(crate) fn nats(error: impl std::fmt::Display) -> Self {
        Self::Nats(error.to_string())
    }

    /// This error as a refusal of `text`: a query error is placed in the
    /// text with its span and caret; any other error is unchanged.
    pub(crate) fn in_query(self, text: &str) -> Self {
        match self {
            Self::Query(error) | Self::Store(sinex_store::Error::Query(error)) => {
                Self::Failure(error.failure(text))
            }
            other => other,
        }
    }

    /// This error with `note` appended to its message, keeping its kind: what
    /// the failure left behind, or where it happened.
    pub(crate) fn with_note(self, note: impl std::fmt::Display) -> Self {
        let mut failure = self.failure();
        failure.message = format!("{}; {note}", failure.message);
        Self::Failure(failure)
    }

    /// This error with `context` put before its message, keeping its kind.
    pub(crate) fn within(self, context: impl std::fmt::Display) -> Self {
        let mut failure = self.failure();
        failure.message = format!("{context}: {}", failure.message);
        Self::Failure(failure)
    }

    /// The shared failure this error is reported as.
    pub(crate) fn failure(self) -> Failure {
        self.into()
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        let message = error.to_string();
        match error {
            Error::Failure(failure) => failure,
            Error::Query(error) => error.failure(""),
            Error::Store(error) => error.into(),
            Error::Database(error) => sinex_store::failure::database_failure(&error),
            Error::Schema(error) => sinex_store::failure::schema_failure(error),
            Error::Write(error) => error.into(),
            Error::Blob(error) => sinex_store::failure::blob_failure(&error),
            Error::Io(error) | Error::File { source: error, .. } => {
                Failure::new(io_kind(error.kind()), message)
            }
            Error::Json(_) => Failure::new(FailureKind::BadValue, message),
            Error::Source(sinex_source_core::DriverError::Cancelled) => {
                Failure::new(FailureKind::Cancelled, message)
            }
            Error::Source(error) => {
                Failure::new(error.io_kind().map_or(FailureKind::Io, io_kind), message)
            }
            Error::Sealing(_) => Failure::new(FailureKind::Refused, message),
            Error::Telemetry(error) => match error {
                crate::telemetry::Error::Read(error) => error.into(),
                crate::telemetry::Error::Spool(error) => {
                    Failure::new(io_kind(error.kind()), message)
                }
            },
            Error::Task(_) => Failure::new(FailureKind::Internal, message),
            Error::Nats(_) => Failure::new(FailureKind::NotRunning, message).with_code("nats"),
        }
    }
}

/// Library errors classified as one failure kind.
macro_rules! from_as {
    ($kind:ident: $($error:ty),+ $(,)?) => {$(
        impl From<$error> for Error {
            fn from(error: $error) -> Self { Self::$kind(error) }
        }
    )+};
}
from_as!(bad_value: sinex_model::time_settings::TimeSettingsError, jiff::Error,
    time::error::ComponentRange, std::num::TryFromIntError, signals::EvaluationError,
    zip::result::ZipError, sinex_analysis::methods::MethodError, ranking::RankError);
from_as!(internal: tokio::sync::AcquireError, sinex_model::BuildError);

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(_: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::internal("a daemon task stopped receiving")
    }
}

impl<K> From<async_nats::error::Error<K>> for Error
where
    K: Clone + std::fmt::Debug + std::fmt::Display + PartialEq,
{
    fn from(error: async_nats::error::Error<K>) -> Self {
        Self::nats(error)
    }
}

impl From<sinex_library::reads::Error> for Error {
    fn from(error: sinex_library::reads::Error) -> Self {
        match error {
            sinex_library::reads::Error::Store(error) => error.into(),
            other => Self::internal(other),
        }
    }
}

impl From<sinex_library::curation::Error> for Error {
    fn from(error: sinex_library::curation::Error) -> Self {
        match error {
            sinex_library::curation::Error::Store(error) => error.into(),
            other => Self::internal(other),
        }
    }
}

impl From<sinex_library::plan::PlanError> for Error {
    fn from(error: sinex_library::plan::PlanError) -> Self {
        use sinex_library::plan::PlanError;
        match error {
            PlanError::Store(error) => error.into(),
            PlanError::Items(error) => error.into(),
            error @ PlanError::Invalid { .. } => Self::bad_value(error),
            error @ (PlanError::Refused(_) | PlanError::RequestConflict(_)) => {
                Self::conflict(error)
            }
        }
    }
}

impl From<zbus::Error> for Error {
    fn from(error: zbus::Error) -> Self {
        Self::new(FailureKind::NotRunning, error).within("D-Bus")
    }
}

#[cfg(test)]
mod tests;

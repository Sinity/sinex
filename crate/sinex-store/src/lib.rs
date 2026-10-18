//! PostgreSQL storage for Sinex. One writer task owns the only write connection and
//! admits every event; readers get read-only pools.
mod admission;
mod atuin;
mod blob;
mod browser;
mod exclusion;
mod projection;
mod query;
mod read_pool;
mod recall;
mod replay;
mod schema;
mod search;
mod session;
mod shell;
mod source_coverage;
mod source_inspection;
mod synthetic;
mod test_pause;
mod writer;
mod zsh;

pub use admission::{Admission, ReplayCommand};
pub use atuin::mask as present;
pub use atuin::{
    AtuinInterpretation, AtuinStep, MAX_STEP_BYTES, MAX_STEP_ROWS, PrivacyRule,
    SOURCE as ATUIN_SOURCE,
};
pub use browser::{BrowserBegin, BrowserPending, BrowserStep};
pub use projection::Projection;
pub use query::{validate_installed_raw_query, validate_query};
pub use read_pool::{ReadPool, StoreStatus};
pub use recall::{is_recall, validate_recall};
pub use schema::{
    CAPTURED_FUTURE_SKEW_SECONDS, READER_ROLE, SchemaStatus, TIMESCALEDB_VERSION, WRITER_ROLE,
    apply_changes, bootstrap, check_schema,
};
pub use search::indexed as is_indexed_query;
pub use synthetic::{MaterialEvent, Receipt};
pub use test_pause::TestPause;
pub use writer::{RuntimeIdentity, Store, prepare_admission_statements};
pub use zsh::{PROTOCOL as ZSH_PROTOCOL, SOURCE as ZSH_SOURCE, ZshStep};

pub(crate) use blob::{BLOB_LOOKUP, BLOB_PRESENT, BLOB_REGISTER, parse_hash};
pub(crate) use writer::{COUNTER, MATERIAL, WriterPool, coordinate_hash};

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("schema incompatible: {0}")]
    Schema(String),
    #[error("invalid input: {0}")]
    Invalid(&'static str),
    #[error("indexed projection unavailable: {0}")]
    ProjectionUnavailable(&'static str),
    #[error("read execution deadline exceeded")]
    Deadline,
    #[error("primary source inspection statement cancelled")]
    SourceStatementCancelled {
        configured_statement_timeout_ms: u64,
    },
    #[error("writer already owned by another connection")]
    WriterOwned,
    #[error("admission owner has stopped")]
    Stopped,
    #[error("blob operation failed: {0}")]
    Blob(#[from] hashed_blobs::Error),
    #[error("background task failed: {0}")]
    Task(#[from] tokio::task::JoinError),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("product queries are unavailable in the admin-only build")]
    ProductUnavailable,
    #[error("replay refused: {0}")]
    ReplayRefused(&'static str),
}

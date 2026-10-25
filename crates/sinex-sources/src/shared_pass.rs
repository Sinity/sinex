//! Reading one stored file in a single pass shared by every compatible
//! binding of it. An export archive is stored once and read forward once,
//! however many sources bind it; each binding keeps its own record parts and
//! cursor.
use crate::DriverError;
use serde_json::Value;
use sinex_model::{Class, MaterialId, ObservationFacts};
use std::path::Path;

pub use crate::drivers::export_archive::group::{Input, ReadSeek, Route, Work, read_input as read};

/// A binding read in a shared pass over one file.
pub trait SharedPass: Send + Sync {
    /// The file the pass reads.
    fn path(&self) -> &Path;

    /// Where the committed `cursor` leaves this binding for the file as it is
    /// now (`facts`), read in `class`.
    fn resume(
        &self,
        cursor: &Value,
        facts: &ObservationFacts,
        class: Class,
    ) -> Result<Resume, DriverError>;

    /// This binding's part in a pass that starts from `cursor`.
    fn route(&self, cursor: Value, class: Class) -> Route;
}

/// What a binding needs from the next pass.
#[derive(Debug, PartialEq, Eq)]
pub enum Resume {
    /// The cursor already covers the whole file as it is now.
    Finished,
    /// Read the file: from the stored copy the cursor names, or from a new
    /// copy when it names none of this file.
    Read(Option<StoredCopy>),
}

/// The stored copy of the file a cursor names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredCopy {
    pub blob: String,
    pub length: u64,
    /// The material that holds it, whose key opens a sealed copy.
    pub material: Option<MaterialId>,
}

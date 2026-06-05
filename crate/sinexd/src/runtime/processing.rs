#[cfg(feature = "messaging")]
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{error::Error, fmt};

use crate::runtime::SinexError;

/// Errors returned by node processing logic before transport/runtime handling.
#[derive(Debug)]
pub enum AutomatonLogicError {
    Processing(String),

    Serialization(serde_json::Error),

    InputParsing(String),

    OutputSerialization(String),
}

impl fmt::Display for AutomatonLogicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Processing(message) => write!(f, "Processing error: {message}"),
            Self::Serialization(error) => write!(f, "Serialization error: {error}"),
            Self::InputParsing(message) => write!(f, "Input parsing error: {message}"),
            Self::OutputSerialization(message) => {
                write!(f, "Output serialization error: {message}")
            }
        }
    }
}

impl Error for AutomatonLogicError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Serialization(error) => Some(error),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for AutomatonLogicError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization(error)
    }
}

impl AutomatonLogicError {
    /// Convert a node-logic failure into the structured Sinex error class that
    /// settlement should use. This intentionally preserves the distinction
    /// between transient processing failures and data-shaped input/output
    /// failures.
    #[must_use]
    pub fn to_sinex_error(&self) -> SinexError {
        match self {
            Self::Processing(msg) => SinexError::processing(msg.clone()),
            Self::Serialization(error) => SinexError::serialization("node serialization error")
                .with_std_error(error as &(dyn std::error::Error + 'static)),
            Self::InputParsing(msg) => SinexError::validation(msg.clone()),
            Self::OutputSerialization(msg) => SinexError::serialization(msg.clone()),
        }
        .with_source(self.to_string())
    }
}

impl From<AutomatonLogicError> for SinexError {
    fn from(err: AutomatonLogicError) -> Self {
        err.to_sinex_error()
    }
}

/// Persisted state wrapper used by derived node checkpointing.
#[cfg(feature = "messaging")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedState<S> {
    /// User-defined state.
    pub state: S,
    /// Number of events processed.
    pub events_processed: u64,
    /// Last input event that was durably incorporated into this state.
    #[serde(default)]
    pub last_input_event_id: Option<uuid::Uuid>,
    /// Last checkpoint time.
    pub last_checkpoint: sinex_primitives::temporal::Timestamp,
    /// State version for future migrations.
    pub version: u32,
}

#[cfg(feature = "messaging")]
impl<S: Default + Serialize + DeserializeOwned> Default for PersistedState<S> {
    fn default() -> Self {
        Self {
            state: S::default(),
            events_processed: 0,
            last_input_event_id: None,
            last_checkpoint: sinex_primitives::temporal::Timestamp::now(),
            version: 1,
        }
    }
}

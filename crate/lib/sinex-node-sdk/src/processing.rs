use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::SinexError;

/// Errors returned by node processing logic before transport/runtime handling.
#[derive(Debug, Error)]
pub enum NodeLogicError {
    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Input parsing error: {0}")]
    InputParsing(String),

    #[error("Output serialization error: {0}")]
    OutputSerialization(String),
}

impl NodeLogicError {
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

impl From<NodeLogicError> for SinexError {
    fn from(err: NodeLogicError) -> Self {
        err.to_sinex_error()
    }
}

/// Persisted state wrapper used by derived node checkpointing.
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

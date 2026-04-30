//! Wire-level request/response/error/status enums for the replay control bus.
//!
//! These types are serialized over NATS and exposed publicly under
//! `sinex_gateway::replay_control` so client and server siblings can speak
//! the same protocol.

use serde::{Deserialize, Serialize};
use sinex_db::replay::state_machine::{ReplayOperation, ReplayState, ReplayScope};
use sinex_primitives::{SinexError, Uuid};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ReplayControlRequest {
    Plan {
        actor: String,
        scope: ReplayScope,
    },
    Preview {
        operation_id: Uuid,
    },
    Approve {
        operation_id: Uuid,
        approver: String,
    },
    Submit {
        operation_id: Uuid,
        submitter: String,
    },
    Execute {
        operation_id: Uuid,
        executor: String,
        #[serde(default)]
        dry_run: bool,
    },
    Cancel {
        operation_id: Uuid,
        canceller: String,
        reason: Option<String>,
    },
    Status {
        operation_id: Uuid,
    },
    List {
        state: Option<ReplayState>,
        node: Option<String>,
        limit: Option<i64>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplayControlResponse {
    pub status: ReplayControlStatus,
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<ReplayControlErrorKind>,
    pub operation: Option<ReplayOperation>,
    pub operations: Option<Vec<ReplayOperation>>,
    pub preview: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayControlErrorKind {
    Validation,
    NotFound,
    AlreadyExists,
    InvalidState,
    PermissionDenied,
    Parse,
    Cancelled,
    Timeout,
    Database,
    Network,
    ResourceExhausted,
    Service,
    Io,
    Configuration,
    Serialization,
    Channel,
    Processing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayControlStatus {
    Ok,
    Error,
}

impl ReplayControlResponse {
    #[must_use]
    pub fn success(
        operation: Option<ReplayOperation>,
        preview: Option<serde_json::Value>,
        operations: Option<Vec<ReplayOperation>>,
    ) -> Self {
        Self {
            status: ReplayControlStatus::Ok,
            message: None,
            error_kind: None,
            operation,
            operations,
            preview,
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: ReplayControlStatus::Error,
            message: Some(message.into()),
            error_kind: None,
            operation: None,
            operations: None,
            preview: None,
        }
    }

    pub(super) fn from_report(err: &color_eyre::Report) -> Self {
        if let Some(sinex_err) = err.downcast_ref::<SinexError>() {
            return Self {
                status: ReplayControlStatus::Error,
                message: Some(sinex_err.client_message().to_string()),
                error_kind: Some(ReplayControlErrorKind::from_sinex_error(sinex_err)),
                operation: None,
                operations: None,
                preview: None,
            };
        }

        Self::error(err.to_string())
    }
}

impl ReplayControlErrorKind {
    pub(super) fn from_sinex_error(err: &SinexError) -> Self {
        match err {
            SinexError::Validation(_) => Self::Validation,
            SinexError::NotFound(_) => Self::NotFound,
            SinexError::AlreadyExists(_) => Self::AlreadyExists,
            SinexError::InvalidState(_) => Self::InvalidState,
            SinexError::PermissionDenied(_) => Self::PermissionDenied,
            SinexError::Parse(_) => Self::Parse,
            SinexError::Cancelled(_) => Self::Cancelled,
            SinexError::Timeout(_) => Self::Timeout,
            SinexError::Database(_) | SinexError::DbPersistenceFailed(_) => Self::Database,
            SinexError::Network(_) => Self::Network,
            SinexError::ResourceExhausted(_) => Self::ResourceExhausted,
            SinexError::Service(_) => Self::Service,
            SinexError::Io(_) => Self::Io,
            SinexError::Configuration(_) => Self::Configuration,
            SinexError::Serialization(_) => Self::Serialization,
            SinexError::ChannelSend(_) | SinexError::ChannelReceive(_) => Self::Channel,
            SinexError::MaxRetriesExceeded(_)
            | SinexError::Kv(_)
            | SinexError::Automaton(_)
            | SinexError::Checkpoint(_)
            | SinexError::Lifecycle(_)
            | SinexError::Processing(_)
            | _ => Self::Processing,
        }
    }

    pub(super) fn into_sinex_error(self, message: String) -> SinexError {
        match self {
            Self::Validation => SinexError::validation(message),
            Self::NotFound => SinexError::not_found(message),
            Self::AlreadyExists => SinexError::already_exists(message),
            Self::InvalidState => SinexError::invalid_state(message),
            Self::PermissionDenied => SinexError::permission_denied(message),
            Self::Parse => SinexError::parse(message),
            Self::Cancelled => SinexError::cancelled(message),
            Self::Timeout => SinexError::timeout(message),
            Self::Database => SinexError::database(message),
            Self::Network => SinexError::network(message),
            Self::ResourceExhausted => SinexError::resource_exhausted(message),
            Self::Service => SinexError::service(message),
            Self::Io => SinexError::io(message),
            Self::Configuration => SinexError::configuration(message),
            Self::Serialization => SinexError::serialization(message),
            Self::Channel | Self::Processing => SinexError::processing(message),
        }
    }
}

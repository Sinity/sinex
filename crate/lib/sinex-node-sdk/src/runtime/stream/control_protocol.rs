//! Control-plane wire helpers shared across runner control-channel code paths.
//!
//! These items live behind the `messaging` feature because the control plane is
//! NATS-borne. The size guard and JSON encoder are pure helpers — they do not
//! perform any I/O themselves; that is the runner's responsibility.

use crate::{NodeResult, SinexError};
use serde::{Deserialize, Serialize};
use sinex_primitives::{Timestamp, Uuid};

#[cfg(feature = "messaging")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ControlCommandKind {
    Scan,
    Drain,
    Resume,
}

#[cfg(feature = "messaging")]
pub(super) fn control_command_kind(subject: &str) -> Option<ControlCommandKind> {
    if subject.ends_with(".scan") {
        Some(ControlCommandKind::Scan)
    } else if subject.ends_with(".drain") {
        Some(ControlCommandKind::Drain)
    } else if subject.ends_with(".resume") {
        Some(ControlCommandKind::Resume)
    } else {
        None
    }
}

#[cfg(feature = "messaging")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct NodeDrainComplete {
    pub(super) node_name: String,
    pub(super) timestamp: Timestamp,
    pub(super) checkpoint: Option<String>,
}

pub(super) const MAX_CONTROL_MESSAGE_BYTES: usize = 512 * 1024;

pub(super) fn ensure_control_payload_fits(
    error_message: &'static str,
    subject: &str,
    node_name: &str,
    operation_id: Option<Uuid>,
    payload_len: usize,
) -> NodeResult<()> {
    if payload_len <= MAX_CONTROL_MESSAGE_BYTES {
        return Ok(());
    }

    let mut error = SinexError::messaging(error_message.to_string())
        .with_context("node", node_name.to_string())
        .with_context("subject", subject.to_string())
        .with_context("payload_bytes", payload_len.to_string())
        .with_context("max_payload_bytes", MAX_CONTROL_MESSAGE_BYTES.to_string());
    if let Some(operation_id) = operation_id {
        error = error.with_context("operation_id", operation_id.to_string());
    }
    Err(error)
}

pub(super) fn encode_control_message<TPayload: Serialize>(
    payload_kind: &'static str,
    operation_id: Uuid,
    node_name: &str,
    payload: &TPayload,
) -> NodeResult<Vec<u8>> {
    serde_json::to_vec(payload).map_err(|error| {
        SinexError::serialization(format!(
            "Failed to serialize {payload_kind} for node '{node_name}' operation {operation_id}: {error}"
        ))
    })
}

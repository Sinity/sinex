//! Shared NATS payload-size guardrails.

use sinex_primitives::SinexError;
use tracing::error;

pub(crate) const NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES: usize = 1024 * 1024;

pub(crate) fn ensure_nats_payload_fits(
    context: &'static str,
    subject: &str,
    payload_bytes: usize,
) -> Result<(), SinexError> {
    if payload_bytes <= NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES {
        return Ok(());
    }

    error!(
        subject,
        payload_bytes,
        max_payload_bytes = NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES,
        context,
        "Refusing oversized NATS publish before server disconnect"
    );
    Err(SinexError::validation(
        "NATS payload exceeds configured hard limit",
    )
    .with_context("subject", subject.to_string())
    .with_context("payload_bytes", payload_bytes.to_string())
    .with_context(
        "max_payload_bytes",
        NATS_PUBLISH_PAYLOAD_HARD_LIMIT_BYTES.to_string(),
    )
    .with_context("publish_context", context))
}

//! Native record identity for delivered completions and owner projection cuts.
use super::coverage::Failure;
use crate::Error;
use async_nats::jetstream::{
    message::StreamMessage,
    stream::{RawMessageErrorKind, Stream},
};
use sinex_protocol::{Confirmed, DeliveredCompletion};
use sinex_store::read_coverage::NativePrefix;

pub(super) fn fingerprint(message: &StreamMessage) -> String {
    DeliveredCompletion::fingerprint(
        message.sequence,
        message.time.unix_timestamp_nanos(),
        message.subject.as_str(),
        &message.payload,
    )
}

/// Call before opening the SQL snapshot. Publication follows committed
/// admission, so the later SQL cut covers every record through this prefix.
pub(super) async fn capture(stream: &Stream) -> Result<NativePrefix, Failure> {
    let state = &stream.cached_info().state;
    if state.messages == 0 {
        return Ok(NativePrefix::Empty);
    }
    let message = stream
        .get_raw_message(state.last_sequence)
        .await
        .map_err(Error::nats)?;
    Ok(NativePrefix::Message {
        sequence: message.sequence,
        fingerprint: fingerprint(&message),
    })
}

async fn record(stream: &Stream, sequence: u64, expected: &str) -> Result<StreamMessage, Failure> {
    let message = stream.get_raw_message(sequence).await.map_err(|error| {
        if error.kind() == RawMessageErrorKind::NoMessageFound {
            Failure::Invalidated("native_resume_boundary_erased".into())
        } else {
            Failure::Unverified(error.to_string())
        }
    })?;
    if fingerprint(&message) != expected {
        return Err(Failure::Invalidated(
            "native_resume_boundary_changed".into(),
        ));
    }
    Ok(message)
}

pub(super) async fn projection_start(
    stream: &Stream,
    prefix: &NativePrefix,
) -> Result<u64, Failure> {
    match prefix {
        // Starting at one skips nothing, even if an empty stream already has
        // erased sequence numbers or gains records after the snapshot cut.
        NativePrefix::Empty => Ok(1),
        NativePrefix::Message {
            sequence,
            fingerprint,
        } => {
            record(stream, *sequence, fingerprint).await?;
            sequence
                .checked_add(1)
                .ok_or_else(|| Failure::Invalidated("native_resume_sequence_exhausted".into()))
        }
    }
}

pub(super) async fn completion(
    stream: &Stream,
    receipt: &DeliveredCompletion,
    position: i64,
    control_subject: &str,
) -> Result<(), Failure> {
    if receipt.stream_instance != crate::publisher::stream_instance(stream)
        || receipt.stream_sequence == 0
    {
        return Err(Failure::Invalidated(
            "native_resume_incarnation_changed".into(),
        ));
    }
    let message = record(stream, receipt.stream_sequence, &receipt.fingerprint).await?;
    if message.subject.as_str() != control_subject
        || !matches!(serde_json::from_slice::<Confirmed>(&message.payload), Ok(Confirmed::PositionComplete { commit_seq }) if commit_seq == position)
    {
        return Err(Failure::Invalidated(
            "native_resume_completion_mismatch".into(),
        ));
    }
    Ok(())
}

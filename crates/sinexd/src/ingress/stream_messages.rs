//! Confirmed-stream messages a consumer names: the last message before a
//! snapshot's database read, and the completion marker it applied last. Sinex
//! reads each back from the broker; nothing the consumer sends is trusted
//! beyond that lookup.
use crate::{Error, Result};
use async_nats::jetstream::{
    message::StreamMessage,
    stream::{RawMessageErrorKind, Stream},
};
use sinex_protocol::{Confirmed, DeliveredCompletion, LastStreamMessage};

/// Why a named message cannot start delivery.
pub(super) enum Refusal {
    /// The message is gone or different: the consumer needs a new snapshot.
    Changed(String),
    /// The broker could not answer; ask again.
    Unavailable(Error),
}
impl From<Error> for Refusal {
    fn from(error: Error) -> Self {
        Self::Unavailable(error)
    }
}

fn fingerprint(message: &StreamMessage) -> String {
    DeliveredCompletion::fingerprint(
        message.sequence,
        message.time.unix_timestamp_nanos(),
        message.subject.as_str(),
        &message.payload,
    )
}

/// Call before opening the snapshot's database read. Publication follows
/// commit, so the later read covers every event through this message.
pub(super) async fn last(stream: &Stream) -> Result<Option<LastStreamMessage>> {
    let state = &stream.cached_info().state;
    if state.messages == 0 {
        return Ok(None);
    }
    let message = stream
        .get_raw_message(state.last_sequence)
        .await
        .map_err(Error::nats)?;
    Ok(Some(LastStreamMessage {
        sequence: message.sequence,
        fingerprint: fingerprint(&message),
    }))
}

async fn read(
    stream: &Stream,
    sequence: u64,
    expected: &str,
) -> std::result::Result<StreamMessage, Refusal> {
    let message = stream.get_raw_message(sequence).await.map_err(|error| {
        if error.kind() == RawMessageErrorKind::NoMessageFound {
            Refusal::Changed("stream_message_erased".into())
        } else {
            Refusal::Unavailable(Error::nats(error))
        }
    })?;
    if fingerprint(&message) != expected {
        return Err(Refusal::Changed("stream_message_changed".into()));
    }
    Ok(message)
}

/// The first sequence to deliver after an applied snapshot.
pub(super) async fn after_snapshot(
    stream: &Stream,
    last: &Option<LastStreamMessage>,
) -> std::result::Result<u64, Refusal> {
    match last {
        // Starting at one skips nothing, even if an empty stream already has
        // erased sequence numbers or gained messages after the snapshot read.
        None => Ok(1),
        Some(last) => {
            read(stream, last.sequence, &last.fingerprint).await?;
            last.sequence
                .checked_add(1)
                .ok_or_else(|| Refusal::Changed("stream_sequence_exhausted".into()))
        }
    }
}

/// Check that `marker` is this stream instance's completion marker for
/// `position`; delivery resumes at the marker itself.
pub(super) async fn completion(
    stream: &Stream,
    marker: &DeliveredCompletion,
    position: i64,
    control_subject: &str,
) -> std::result::Result<u64, Refusal> {
    if marker.stream_instance != crate::publisher::stream_instance(stream)
        || marker.stream_sequence == 0
    {
        return Err(Refusal::Changed("stream_instance_changed".into()));
    }
    let message = read(stream, marker.stream_sequence, &marker.fingerprint).await?;
    if message.subject.as_str() != control_subject
        || !matches!(serde_json::from_slice::<Confirmed>(&message.payload), Ok(Confirmed::PositionComplete { commit_seq }) if commit_seq == position)
    {
        return Err(Refusal::Changed("completion_marker_mismatch".into()));
    }
    Ok(marker.stream_sequence)
}

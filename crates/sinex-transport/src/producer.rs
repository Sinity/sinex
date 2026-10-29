//! Stateless producer admission transport. Capture journals stay with the product that owns them.
use async_nats::{Client, HeaderMap, Subscriber, jetstream};
use futures::StreamExt;
use sinex_protocol::{Acknowledgment, Ingress, ReceiptHint, RecordedPage, RecordedQuery, Subjects};
use std::time::Duration;
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("producer transport: {0}")]
    Transport(String),
    #[error(transparent)]
    Reply(#[from] crate::replies::ReplyError),
    #[error("no durable receipt for frame {0}")]
    NoReceipt(String),
}
/// Retrieve every matching owned receipt through native catalog pages.
/// This eager convenience method owns the resulting DTOs; callers needing a lazy traversal may request RecordedPage directly.
pub async fn recorded(
    client: &Client,
    subjects: &Subjects,
    producer: &str,
    frames: &[String],
    numbers: &[i64],
    idle: Duration,
) -> Result<Vec<Acknowledgment>, SubmitError> {
    let mut groups = Vec::new();
    let mut query = RecordedQuery {
        after: i64::MAX,
        ..Default::default()
    };
    let empty_bytes = serde_json::to_vec(&query)
        .expect("receipt selectors serialize")
        .len();
    let mut used = empty_bytes;
    for (frame, number) in frames
        .iter()
        .map(|frame| (Some(frame), None))
        .chain(numbers.iter().map(|number| (None, Some(number))))
    {
        let length = frame.map_or_else(
            || number.expect("one selector").to_string().len(),
            |frame| {
                serde_json::to_vec(frame)
                    .expect("frame name serializes")
                    .len()
            },
        );
        let comma = usize::from(if frame.is_some() {
            !query.frames.is_empty()
        } else {
            !query.numbers.is_empty()
        });
        if used + length + comma > client.server_info().max_payload {
            if query.frames.is_empty() && query.numbers.is_empty() {
                return Err(SubmitError::Transport(
                    "a receipt selector exceeds the server request budget".into(),
                ));
            }
            groups.push(query);
            query = RecordedQuery {
                after: i64::MAX,
                ..Default::default()
            };
            used = empty_bytes;
        }
        let comma = usize::from(if frame.is_some() {
            !query.frames.is_empty()
        } else {
            !query.numbers.is_empty()
        });
        if used + length + comma > client.server_info().max_payload {
            return Err(SubmitError::Transport(
                "a receipt selector exceeds the server request budget".into(),
            ));
        }
        if let Some(frame) = frame {
            query.frames.push(frame.clone());
        }
        if let Some(number) = number {
            query.numbers.push(*number);
        }
        used += length + comma;
    }
    if !query.frames.is_empty() || !query.numbers.is_empty() {
        groups.push(query);
    }
    let mut receipts = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for mut query in groups {
        query.after = 0;
        loop {
            let page: RecordedPage = crate::replies::request_json(
                client,
                subjects.recorded(producer),
                producer,
                &query,
                idle,
            )
            .await?;
            receipts.extend(
                page.acknowledgments
                    .into_iter()
                    .filter(|receipt| seen.insert(receipt.frame.clone())),
            );
            match page.next {
                Some(next) if next > query.after => query.after = next,
                Some(_) => {
                    return Err(SubmitError::Transport(
                        "receipt page did not advance".into(),
                    ));
                }
                None => break,
            }
        }
    }
    Ok(receipts)
}
/// Submit an already prepared ingress frame. Large bodies use the existing LargeFrame byte transport.
/// Broadcast hints never establish durability: only a complete integrity-verified owned receipt is returned.
pub async fn submit_frame(
    client: &Client,
    subjects: &Subjects,
    producer: &str,
    acks: &mut Subscriber,
    frame: &Ingress,
    idle: Duration,
) -> Result<Acknowledgment, SubmitError> {
    let id = frame.frame();
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", id);
    let bytes =
        serde_json::to_vec(frame).map_err(|error| SubmitError::Transport(error.to_string()))?;
    let malformed = format!("malformed/{}", blake3::hash(&bytes).to_hex());
    jetstream::new(client.clone())
        .publish_with_headers(subjects.ingress(producer), headers, bytes.into())
        .await
        .map_err(|error| SubmitError::Transport(error.to_string()))?
        .await
        .map_err(|error| SubmitError::Transport(error.to_string()))?;
    let expected = blake3::hash(id.as_bytes()).to_hex().to_string();
    let bad = blake3::hash(malformed.as_bytes()).to_hex().to_string();
    let hint = tokio::time::timeout(idle, async {
        while let Some(message) = acks.next().await {
            if let Ok(hint) = serde_json::from_slice::<ReceiptHint>(&message.payload)
                && (hint.frame_hash == expected || hint.frame_hash == bad)
            {
                return Some(hint);
            }
        }
        None
    })
    .await
    .ok()
    .flatten();
    let receipts = if let Some(hint) = hint {
        recorded(client, subjects, producer, &[], &[hint.number], idle).await?
    } else {
        recorded(
            client,
            subjects,
            producer,
            &[id.into(), malformed.clone()],
            &[],
            idle,
        )
        .await?
    };
    receipts
        .into_iter()
        .find(|receipt| {
            receipt.frame == id
                || (receipt.frame == malformed
                    && receipt.refusal_kind() == Some(sinex_model::FrameRefusal::Malformed))
        })
        .ok_or_else(|| SubmitError::NoReceipt(id.into()))
}

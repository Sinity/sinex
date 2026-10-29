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
    #[error("Sinex refused encoded custody frame {frame}: {reason}")]
    Refused {
        frame: String,
        reason: String,
        kind: sinex_model::FrameRefusal,
    },
    /// The authenticated owner recorded a different logical body under this
    /// producer's exact frame name. This evidence is not an ACK for the offer.
    #[error("Sinex recorded different bytes for frame {frame}")]
    ReceiptConflict {
        frame: String,
        offered_hash: String,
        receipt: Box<Acknowledgment>,
    },
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
/// The frozen logical JSON byte identity survives a LargeFrame envelope.
pub fn content_hash(frame: &Ingress) -> Result<String, SubmitError> {
    if let Ingress::LargeFrame { blob, .. } = frame {
        return Ok(blob.clone());
    }
    let bytes =
        serde_json::to_vec(frame).map_err(|error| SubmitError::Transport(error.to_string()))?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}
/// Resolve an owned receipt fetched on this authenticated producer's RPC.
/// A conflicting prior receipt is evidence only, never admission of the offer.
pub fn check_receipt(
    frame: &Ingress,
    receipt: Acknowledgment,
) -> Result<Acknowledgment, SubmitError> {
    let expected = content_hash(frame)?;
    if receipt.frame != frame.frame() {
        return Err(SubmitError::Refused {
            frame: frame.frame().into(),
            reason: "receipt_frame_identity_mismatch".into(),
            kind: sinex_model::FrameRefusal::Conflict,
        });
    }
    if receipt.content_hash().is_none()
        && matches!(
            receipt.refusal_kind(),
            Some(sinex_model::FrameRefusal::Excised | sinex_model::FrameRefusal::PartiallyExcised)
        )
    {
        return Ok(receipt);
    }
    if receipt.content_hash() != Some(expected.as_str()) {
        if receipt.number.is_none_or(|number| number <= 0) {
            return Err(SubmitError::NoReceipt(frame.frame().into()));
        }
        return Err(SubmitError::ReceiptConflict {
            frame: frame.frame().into(),
            offered_hash: expected,
            receipt: Box::new(receipt),
        });
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests;

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
    let receipt = receipts
        .into_iter()
        .find(|receipt| {
            receipt.frame == id
                || (receipt.frame == malformed
                    && receipt.refusal_kind() == Some(sinex_model::FrameRefusal::Malformed))
        })
        .ok_or_else(|| SubmitError::NoReceipt(id.into()))?;
    check_receipt(frame, receipt)
}

/// Transfer encoded custody under the original authenticated frame namespace.
/// The root is local sender custody, never a path accepted by the server.
#[allow(clippy::too_many_arguments)]
pub async fn transfer_frame(
    client: &Client,
    subjects: &Subjects,
    producer: &str,
    acks: &mut Subscriber,
    root: &std::path::Path,
    frame: &Ingress,
    idle: Duration,
) -> Result<(), SubmitError> {
    use base64::Engine;
    use std::io::Read;
    let directory = crate::frame_bytes::directory(root, producer, frame.frame());
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(SubmitError::Transport(error.to_string())),
    };
    for entry in entries {
        let entry = entry.map_err(|error| SubmitError::Transport(error.to_string()))?;
        let name = entry.file_name();
        let Some(name) = name
            .to_str()
            .filter(|name| blake3::Hash::from_hex(name).is_ok())
        else {
            continue;
        };
        let blob = format!("blake3:{name}");
        let (descriptor, mut reader) =
            crate::frame_bytes::encoded(root, producer, frame.frame(), &blob)
                .map_err(|error| SubmitError::Transport(error.to_string()))?;
        let attempt = uuid::Uuid::now_v7();
        let mut offset = 0u64;
        let widest = format!("bytes/{attempt}/{name}/{}", u64::MAX);
        let envelope = Ingress::Chunk {
            frame: widest.clone(),
            for_frame: frame.frame().into(),
            blob: blob.clone(),
            descriptor: descriptor.encode(),
            offset: u64::MAX,
            total: descriptor.stored_len,
            data: String::new(),
        };
        let framing = serde_json::to_vec(&envelope)
            .expect("chunk envelope serializes")
            .len()
            + format!("NATS/1.0\r\nNats-Msg-Id: {widest}\r\n\r\n").len();
        let unit =
            (client.server_info().max_payload.saturating_sub(framing) / 4 * 3).min(64 * 1024);
        if unit == 0 {
            return Err(SubmitError::Transport(
                "server budget cannot hold encoded custody framing".into(),
            ));
        }
        let mut buffer = vec![0; unit];
        while offset < descriptor.stored_len {
            let n = (descriptor.stored_len - offset).min(unit as u64) as usize;
            reader
                .read_exact(&mut buffer[..n])
                .map_err(|error| SubmitError::Transport(error.to_string()))?;
            let chunk = Ingress::Chunk {
                frame: format!("bytes/{attempt}/{name}/{offset}"),
                for_frame: frame.frame().into(),
                blob: blob.clone(),
                descriptor: descriptor.encode(),
                offset,
                total: descriptor.stored_len,
                data: base64::engine::general_purpose::STANDARD.encode(&buffer[..n]),
            };
            let receipt = submit_frame(client, subjects, producer, acks, &chunk, idle).await?;
            let kind = receipt
                .refusal_kind()
                .unwrap_or(sinex_model::FrameRefusal::Bytes);
            if let Some(reason) = receipt.refusal {
                return Err(SubmitError::Refused {
                    frame: receipt.frame,
                    reason,
                    kind,
                });
            }
            offset += n as u64;
        }
    }
    Ok(())
}

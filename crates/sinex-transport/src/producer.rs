//! Stateless producer admission transport. Capture journals stay with the product that owns them.
use async_nats::{Client, HeaderMap, Subscriber, jetstream};
use futures::StreamExt;
use sinex_protocol::{AckHint, Acknowledgment, Ingress, RecordedPage, RecordedQuery, Subjects};
use std::time::Duration;
#[derive(Debug, thiserror::Error)]
pub enum SubmitError {
    #[error("producer transport: {0}")]
    Transport(String),
    #[error(transparent)]
    Reply(#[from] crate::replies::ReplyError),
    #[error("no acknowledgment for frame {0}")]
    NoAck(String),
    #[error("Sinex refused encoded custody frame {frame}: {reason}")]
    Refused {
        frame: String,
        reason: String,
        kind: sinex_model::FrameRefusal,
    },
    /// Sinex recorded a different frame body under this producer's frame
    /// name. The recorded acknowledgment is not an acknowledgment of the offer.
    #[error("Sinex recorded different bytes for frame {frame}")]
    AckConflict {
        frame: String,
        offered_hash: String,
        ack: Box<Acknowledgment>,
    },
}
/// Retrieve every matching recorded acknowledgment, page by page.
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
        .expect("acknowledgment selectors serialize")
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
                    "an acknowledgment selector exceeds the server request budget".into(),
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
                "an acknowledgment selector exceeds the server request budget".into(),
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
    let mut acks = Vec::new();
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
            acks.extend(
                page.acknowledgments
                    .into_iter()
                    .filter(|ack| seen.insert(ack.frame.clone())),
            );
            match page.next {
                Some(next) if next > query.after => query.after = next,
                Some(_) => {
                    return Err(SubmitError::Transport(
                        "acknowledgment page did not advance".into(),
                    ));
                }
                None => break,
            }
        }
    }
    Ok(acks)
}
/// The hash of the frame's JSON bytes; a LargeFrame carries it as its blob.
pub fn content_hash(frame: &Ingress) -> Result<String, SubmitError> {
    if let Ingress::LargeFrame { blob, .. } = frame {
        return Ok(blob.clone());
    }
    let bytes =
        serde_json::to_vec(frame).map_err(|error| SubmitError::Transport(error.to_string()))?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}
/// Check an acknowledgment fetched on this authenticated producer's request
/// subject. A conflicting earlier acknowledgment is reported, never taken as
/// admission of the offer.
pub fn check_ack(frame: &Ingress, ack: Acknowledgment) -> Result<Acknowledgment, SubmitError> {
    check_ack_for(frame, ack, content_hash(frame)?, None)
}

// Only the submitter knows the digest of the envelope it sent. Recovered
// acknowledgments without that digest must name the offered frame.
fn check_ack_for(
    frame: &Ingress,
    ack: Acknowledgment,
    expected: String,
    transmitted_malformed: Option<&str>,
) -> Result<Acknowledgment, SubmitError> {
    let invalid_name = frame.frame().is_empty() || frame.frame().contains('\0');
    let correlated_malformed = invalid_name
        && transmitted_malformed == Some(ack.frame.as_str())
        && ack
            .refusal()
            .is_some_and(|(kind, _)| kind == sinex_model::FrameRefusal::Malformed)
        && ack.number > 0
        && ack.content_hash.as_deref() == Some(expected.as_str());
    if ack.frame != frame.frame() && !correlated_malformed {
        return Err(SubmitError::Refused {
            frame: frame.frame().into(),
            reason: "ack_frame_identity_mismatch".into(),
            kind: sinex_model::FrameRefusal::Conflict,
        });
    }
    if ack.content_hash.as_deref() != Some(expected.as_str()) {
        if ack.number <= 0 {
            return Err(SubmitError::NoAck(frame.frame().into()));
        }
        return Err(SubmitError::AckConflict {
            frame: frame.frame().into(),
            offered_hash: expected,
            ack: Box::new(ack),
        });
    }
    Ok(ack)
}

#[cfg(test)]
mod tests;

/// Submit an already prepared ingress frame. Large bodies use the existing LargeFrame byte transport.
/// A broadcast hint never shows that a frame was saved: only a complete,
/// integrity-checked acknowledgment recorded by Sinex is returned.
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
    let body_hash = match frame {
        Ingress::LargeFrame { blob, .. } => blob.clone(),
        _ => format!("blake3:{}", blake3::hash(&bytes).to_hex()),
    };
    let published = jetstream::new(client.clone())
        .publish_with_headers(subjects.ingress(producer), headers, bytes.into())
        .await
        .map_err(|error| SubmitError::Transport(error.to_string()))?
        .await
        .map_err(|error| SubmitError::Transport(error.to_string()))?;
    let correlated = |ack: &Acknowledgment| {
        ack.frame == id
            || (ack.frame == malformed
                && ack
                    .refusal()
                    .is_some_and(|(kind, _)| kind == sinex_model::FrameRefusal::Malformed))
    };
    if published.duplicate {
        // JetStream can acknowledge an already consumed frame without
        // delivering it again, so no new broadcast hint is required. Only
        // the acknowledgment Sinex recorded for this frame shows admission.
        if let Some(ack) = recorded(
            client,
            subjects,
            producer,
            &[id.into(), malformed.clone()],
            &[],
            idle,
        )
        .await?
        .into_iter()
        .find(correlated)
        {
            return check_ack_for(frame, ack, body_hash, Some(&malformed));
        }
        // A duplicate may still be queued or awaiting admission. With no
        // recorded acknowledgment, wait for the hint and look up again.
    }
    let expected = blake3::hash(id.as_bytes()).to_hex().to_string();
    let bad = blake3::hash(malformed.as_bytes()).to_hex().to_string();
    let hint = tokio::time::timeout(idle, async {
        while let Some(message) = acks.next().await {
            if let Ok(hint) = serde_json::from_slice::<AckHint>(&message.payload)
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
    let found = if let Some(hint) = hint {
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
    let ack = found
        .into_iter()
        .find(correlated)
        .ok_or_else(|| SubmitError::NoAck(id.into()))?;
    check_ack_for(frame, ack, body_hash, Some(&malformed))
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
            let ack = submit_frame(client, subjects, producer, acks, &chunk, idle).await?;
            if let sinex_protocol::FrameResult::Refused { kind, reason } = ack.result {
                return Err(SubmitError::Refused {
                    frame: ack.frame,
                    reason,
                    kind,
                });
            }
            offset += n as u64;
        }
    }
    Ok(())
}

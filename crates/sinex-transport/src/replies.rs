//! One bounded serialization on an authenticated reply inbox. Retries belong to the original request.
use async_nats::{Client, Subscriber};
use base64::Engine;
use futures::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use sinex_protocol::replies::{FlowAck, ReplyFrame};
use std::{
    io::{Read, Write},
    time::Duration,
};
use tokio::sync::mpsc;

const CHUNK_BYTES: usize = 64 * 1024;
const QUEUED_CHUNKS: usize = 2;
#[derive(Debug, thiserror::Error)]
pub enum ReplyError {
    #[error("reply transport: {0}")]
    Transport(String),
    #[error("reply framing: {0}")]
    Protocol(String),
    #[error("reply made no progress before its idle deadline")]
    Idle,
    #[error("reply ended before its integrity marker")]
    Closed,
    #[error("reply was cancelled")]
    Cancelled,
    #[error("reply JSON: {0}")]
    Json(String),
}
fn transport(error: impl std::fmt::Display) -> ReplyError {
    ReplyError::Transport(error.to_string())
}
fn protocol(reason: impl Into<String>) -> ReplyError {
    ReplyError::Protocol(reason.into())
}
fn stream_id(inbox: &str) -> String {
    blake3::hash(inbox.as_bytes()).to_hex().to_string()
}
fn frame_bytes(frame: &ReplyFrame) -> Result<Vec<u8>, ReplyError> {
    serde_json::to_vec(frame).map_err(|error| ReplyError::Json(error.to_string()))
}
fn budget(client: &Client, stream: &str) -> Result<usize, ReplyError> {
    let overhead = frame_bytes(&ReplyFrame::Data {
        stream: stream.into(),
        sequence: u64::MAX,
        offset: u64::MAX,
        data: String::new(),
    })?
    .len();
    let minimum = frame_bytes(&ReplyFrame::End {
        stream: stream.into(),
        sequence: u64::MAX,
        total: u64::MAX,
        hash: "0".repeat(64),
    })?
    .len();
    let available = client.server_info().max_payload;
    if available < minimum {
        return Err(protocol(
            "server message budget cannot hold the reply integrity marker",
        ));
    }
    let bytes = available.saturating_sub(overhead) / 4 * 3;
    if bytes == 0 {
        return Err(protocol(
            "server message budget cannot hold a reply transfer unit",
        ));
    }
    Ok(bytes.min(CHUNK_BYTES))
}
enum Serialized {
    Data(Vec<u8>),
    End { total: u64, hash: String },
    Failed,
}
struct ChunkWriter {
    send: mpsc::Sender<Serialized>,
    bytes: Vec<u8>,
    limit: usize,
    hash: blake3::Hasher,
    total: u64,
}
impl ChunkWriter {
    fn emit(&mut self) -> std::io::Result<()> {
        if self.bytes.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::replace(&mut self.bytes, Vec::with_capacity(self.limit));
        self.send
            .blocking_send(Serialized::Data(bytes))
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reply receiver closed")
            })
    }
    fn finish(mut self) -> std::io::Result<()> {
        self.emit()?;
        self.send
            .blocking_send(Serialized::End {
                total: self.total,
                hash: self.hash.finalize().to_hex().to_string(),
            })
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "reply receiver closed")
            })
    }
}
impl Write for ChunkWriter {
    fn write(&mut self, mut bytes: &[u8]) -> std::io::Result<usize> {
        let length = bytes.len();
        while !bytes.is_empty() {
            let take = bytes.len().min(self.limit - self.bytes.len());
            self.hash.update(&bytes[..take]);
            self.total = self
                .total
                .checked_add(take as u64)
                .ok_or_else(|| std::io::Error::other("reply length overflow"))?;
            self.bytes.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if self.bytes.len() == self.limit {
                self.emit()?;
            }
        }
        Ok(length)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.emit()
    }
}
async fn flow(
    acks: &mut Subscriber,
    stream: &str,
    sequence: u64,
    idle: Duration,
) -> Result<(), ReplyError> {
    tokio::time::timeout(idle, async {
        while let Some(message) = acks.next().await {
            match serde_json::from_slice::<FlowAck>(&message.payload) {
                Ok(FlowAck::Accepted {
                    stream: found,
                    sequence: found_sequence,
                }) if found == stream && found_sequence == sequence => return Ok(()),
                Ok(FlowAck::Cancel { stream: found }) if found == stream => {
                    return Err(ReplyError::Cancelled);
                }
                _ => {}
            }
        }
        Err(ReplyError::Closed)
    })
    .await
    .map_err(|_| ReplyError::Idle)?
}
/// `inbox` must already have been checked against the authenticated caller's inbox prefix.
/// Cancellation drops the bounded channel, releasing the serializer and its DTO.
pub async fn publish_json<T: Serialize + Send + 'static>(
    client: &Client,
    inbox: String,
    value: T,
    idle: Duration,
) -> Result<(), ReplyError> {
    let stream = stream_id(&inbox);
    let flow_subject = format!("{inbox}.flow");
    let acks = client.subscribe(flow_subject).await.map_err(transport)?;
    client.flush().await.map_err(transport)?;
    publish_with_acks(client, inbox, stream, acks, value, idle).await
}
/// Start cancellation observation before loading the response DTO, then serialize it once.
pub async fn respond_json<T, F>(
    client: &Client,
    inbox: String,
    build: F,
    idle: Duration,
) -> Result<(), ReplyError>
where
    T: Serialize + Send + 'static,
    F: std::future::Future<Output = Option<T>> + Send,
{
    let stream = stream_id(&inbox);
    let mut acks = client
        .subscribe(format!("{inbox}.flow"))
        .await
        .map_err(transport)?;
    client.flush().await.map_err(transport)?;
    tokio::pin!(build);
    let value = tokio::select! {
        value=&mut build=>value,
        cancelled=tokio::time::timeout(idle,async {
            while let Some(message)=acks.next().await {
                if matches!(serde_json::from_slice::<FlowAck>(&message.payload),Ok(FlowAck::Cancel{stream:found}) if found==stream){return ReplyError::Cancelled;}
            }
            ReplyError::Closed
        })=>return Err(cancelled.unwrap_or(ReplyError::Idle)),
    };
    match value {
        Some(value) => publish_with_acks(client, inbox, stream, acks, value, idle).await,
        None => Ok(()),
    }
}
async fn publish_with_acks<T: Serialize + Send + 'static>(
    client: &Client,
    inbox: String,
    stream: String,
    mut acks: Subscriber,
    value: T,
    idle: Duration,
) -> Result<(), ReplyError> {
    let limit = budget(client, &stream)?;
    let flow_subject = format!("{inbox}.flow");
    let (send, mut receive) = mpsc::channel(QUEUED_CHUNKS);
    let serializer = tokio::task::spawn_blocking(move || {
        let mut writer = ChunkWriter {
            send: send.clone(),
            bytes: Vec::with_capacity(limit),
            limit,
            hash: blake3::Hasher::new(),
            total: 0,
        };
        if serde_json::to_writer(&mut writer, &value).is_err() || writer.finish().is_err() {
            let _ = send.blocking_send(Serialized::Failed);
        }
    });
    let result = async {
        let (mut sequence, mut offset) = (0u64, 0u64);
        loop {
            let item = tokio::time::timeout(idle, receive.recv())
                .await
                .map_err(|_| ReplyError::Idle)?
                .ok_or(ReplyError::Closed)?;
            let end = matches!(item, Serialized::End { .. });
            let frame = match item {
                Serialized::Data(data) => {
                    let frame = ReplyFrame::Data {
                        stream: stream.clone(),
                        sequence,
                        offset,
                        data: base64::engine::general_purpose::STANDARD.encode(&data),
                    };
                    offset = offset
                        .checked_add(data.len() as u64)
                        .ok_or_else(|| protocol("reply offset overflow"))?;
                    frame
                }
                Serialized::End { total, hash } => ReplyFrame::End {
                    stream: stream.clone(),
                    sequence,
                    total,
                    hash,
                },
                Serialized::Failed => {
                    let _ = client
                        .publish_with_reply(
                            inbox.clone(),
                            flow_subject.clone(),
                            frame_bytes(&ReplyFrame::Abort {
                                stream: stream.clone(),
                                code: "serialization_failed".into(),
                            })?
                            .into(),
                        )
                        .await;
                    return Err(ReplyError::Json("serialization failed".into()));
                }
            };
            let encoded = frame_bytes(&frame)?;
            if encoded.len() > client.server_info().max_payload {
                return Err(protocol("reply frame exceeds current server budget"));
            }
            client
                .publish_with_reply(inbox.clone(), flow_subject.clone(), encoded.into())
                .await
                .map_err(transport)?;
            flow(&mut acks, &stream, sequence, idle).await?;
            if end {
                return Ok(());
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| protocol("reply sequence overflow"))?;
        }
    }
    .await;
    drop(receive);
    let _ = serializer.await;
    result
}
struct ChunkReader {
    receive: mpsc::Receiver<Vec<u8>>,
    bytes: Vec<u8>,
    at: usize,
}
impl Read for ChunkReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        while self.at == self.bytes.len() {
            match self.receive.blocking_recv() {
                Some(bytes) => {
                    self.bytes = bytes;
                    self.at = 0;
                }
                None => return Ok(0),
            }
        }
        let take = out.len().min(self.bytes.len() - self.at);
        out[..take].copy_from_slice(&self.bytes[self.at..self.at + take]);
        self.at += take;
        Ok(take)
    }
}
/// Request JSON once; verify framing and the final hash before returning the decoded DTO.
/// Only idle waits time out: a progressing reply has no total-size or total-duration cap.
pub async fn request_json<T: DeserializeOwned + Send + 'static>(
    client: &Client,
    subject: impl Into<String> + Send,
    producer: &str,
    request: &(impl Serialize + Sync),
    idle: Duration,
) -> Result<T, ReplyError> {
    let bytes = serde_json::to_vec(request).map_err(|error| ReplyError::Json(error.to_string()))?;
    request_bytes(client, subject, producer, bytes, idle).await
}
/// The same verified response path for callers that already serialized their request.
pub async fn request_bytes<T: DeserializeOwned + Send + 'static>(
    client: &Client,
    subject: impl Into<String> + Send,
    producer: &str,
    body: Vec<u8>,
    idle: Duration,
) -> Result<T, ReplyError> {
    if !sinex_protocol::nats::valid_producer(producer) {
        return Err(protocol("invalid authenticated producer identity"));
    }
    let inbox = format!("_INBOX.{producer}.{}", uuid::Uuid::now_v7().simple());
    let stream = stream_id(&inbox);
    let flow_subject = format!("{inbox}.flow");
    let mut messages = client.subscribe(inbox.clone()).await.map_err(transport)?;
    client.flush().await.map_err(transport)?;
    client
        .publish_with_reply(subject.into(), inbox, body.into())
        .await
        .map_err(transport)?;
    let (send, receive) = mpsc::channel(QUEUED_CHUNKS);
    let decoder = tokio::task::spawn_blocking(move || {
        serde_json::from_reader::<_, T>(std::io::BufReader::new(ChunkReader {
            receive,
            bytes: vec![],
            at: 0,
        }))
    });
    let result = async {
        let (mut sequence, mut offset, mut hash) = (0u64, 0u64, blake3::Hasher::new());
        loop {
            let message = tokio::time::timeout(idle, messages.next())
                .await
                .map_err(|_| ReplyError::Idle)?
                .ok_or(ReplyError::Closed)?;
            if message.reply.as_deref() != Some(flow_subject.as_str()) {
                return Err(protocol("reply flow inbox differs from this request"));
            }
            let frame: ReplyFrame = serde_json::from_slice(&message.payload)
                .map_err(|_| protocol("not a framed reply"))?;
            match frame {
                ReplyFrame::Data {
                    stream: found,
                    sequence: found_sequence,
                    offset: found_offset,
                    data,
                } => {
                    if found != stream || found_sequence != sequence || found_offset != offset {
                        return Err(protocol("reply stream, sequence or offset differs"));
                    }
                    let data = base64::engine::general_purpose::STANDARD
                        .decode(data)
                        .map_err(|_| protocol("invalid reply data encoding"))?;
                    if data.is_empty() {
                        return Err(protocol("empty reply data frame"));
                    }
                    offset = offset
                        .checked_add(data.len() as u64)
                        .ok_or_else(|| protocol("reply length overflow"))?;
                    hash.update(&data);
                    send.send(data)
                        .await
                        .map_err(|_| protocol("reply JSON decoder stopped"))?;
                }
                ReplyFrame::End {
                    stream: found,
                    sequence: found_sequence,
                    total,
                    hash: expected,
                } => {
                    if found != stream
                        || found_sequence != sequence
                        || total != offset
                        || expected != hash.finalize().to_hex().as_str()
                    {
                        return Err(protocol("reply final length or hash differs"));
                    }
                    return Ok(sequence);
                }
                ReplyFrame::Abort { .. } => return Err(protocol("server aborted reply")),
            }
            client
                .publish(
                    flow_subject.clone(),
                    serde_json::to_vec(&FlowAck::Accepted {
                        stream: stream.clone(),
                        sequence,
                    })
                    .map_err(|error| ReplyError::Json(error.to_string()))?
                    .into(),
                )
                .await
                .map_err(transport)?;
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| protocol("reply sequence overflow"))?;
        }
    }
    .await;
    drop(send);
    let decoded = decoder.await.map_err(transport)?.map_err(|error| {
        ReplyError::Json(format!(
            "invalid reply JSON at {}:{}",
            error.line(),
            error.column()
        ))
    });
    match (result, decoded) {
        (Ok(sequence), Ok(value)) => {
            client
                .publish(
                    flow_subject,
                    serde_json::to_vec(&FlowAck::Accepted { stream, sequence })
                        .map_err(|error| ReplyError::Json(error.to_string()))?
                        .into(),
                )
                .await
                .map_err(transport)?;
            // A caller-owned current-thread runtime may stop driving NATS as
            // soon as this future returns. Finish the flow handshake first.
            client.flush().await.map_err(transport)?;
            Ok(value)
        }
        (result, decoded) => {
            let _ = client
                .publish(
                    flow_subject,
                    serde_json::to_vec(&FlowAck::Cancel { stream })
                        .expect("flow cancel serializes")
                        .into(),
                )
                .await;
            let _ = client.flush().await;
            Err(result
                .err()
                .or_else(|| decoded.err())
                .unwrap_or_else(|| protocol("reply did not complete")))
        }
    }
}

#[cfg(test)]
mod tests;

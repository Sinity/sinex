//! Producing to `sinexd` over the NATS ingress (architecture §4.3), for
//! processes that run sources outside the daemon: `sinex-session` and capture
//! nodes.
//!
//! A poll goes through the same driver and drain `sinexd` uses. Then:
//!
//! 1. the material's bytes are placed in the content store this host shares
//!    with `sinexd`, and a material frame registers them; the producer waits
//!    for its acknowledgment before sending anything that cites it;
//! 2. the events go in frames that each fit one NATS message;
//! 3. only when every frame of a batch is acknowledged does the binding's
//!    cursor (and parser carry) advance, in a state file next to the spool.
//!
//! Before a batch's first frame is sent, the frames and the position after
//! them are written to a pending file. A producer that stops anywhere before
//! the position is saved sends those same frames again on its next start
//! ([`Producer::position`]), and Sinex answers frames it already recorded
//! from the record, so nothing is read into a second material or admitted
//! twice.
//!
//! A producer on another host, which does not share `sinexd`'s content store,
//! sends a material's bytes in [`Ingress::Chunk`] frames first
//! ([`Producer::upload_bytes`]). A frame too large for one message goes into
//! the content store too, and an [`Ingress::Indirect`] frame names it.
use crate::drivers::{self, Acquired, DriverError, MaterialSink};
use crate::{Limits, Source};
use async_nats::HeaderMap;
use async_nats::jetstream;
use base64::Engine;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{BatchKind, Origin, RecordOutcome, RunId};
use sinex_protocol::{
    Acknowledgment, Have, Ingress, MaterialFrame, ProducerEvent, ProducerFailure, Subjects,
};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Event frames stay well under NATS's default 1 MiB message limit.
const FRAME_BYTES: usize = 512 * 1024;
/// Bytes per chunk frame: base64 grows them by a third, and the frame must fit
/// the default 1 MiB message limit.
const CHUNK_BYTES: usize = 512 * 1024;
/// How long to wait for an acknowledgment before asking what Sinex has.
const ACK_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("driver: {0}")]
    Driver(#[from] DriverError),
    #[error("NATS: {0}")]
    Nats(String),
    #[error("state file {0}: {1}")]
    State(PathBuf, String),
    #[error("Sinex refused frame {frame}: {reason}")]
    Refused { frame: String, reason: String },
    #[error("no acknowledgment for frame {0}")]
    NoAck(String),
}

fn nats(e: impl std::fmt::Display) -> Error {
    Error::Nats(e.to_string())
}

/// The content store shared with `sinexd` on this host.
pub struct Store(hashed_blobs::Store);

impl Store {
    pub fn open(root: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(root)?;
        hashed_blobs::Store::open(root, vec![], 0o600)
            .map(Self)
            .map_err(std::io::Error::other)
    }
}

impl Store {
    /// Verify before sending any chunk and retain that representation for the
    /// complete attempt. Producer installs are plain, including indirect frames.
    fn open_upload(&self, name: &str) -> Result<hashed_blobs::PlainReader, String> {
        let invalid = || format!("invalid blob name {name}");
        let hex = name.strip_prefix("blake3:").ok_or_else(invalid)?;
        let hash = blake3::Hash::from_hex(hex).map_err(|_| invalid())?;
        let hash = hashed_blobs::Hash(*hash.as_bytes());
        self.0
            .read_plain(&hash, None, || Ok(()))
            .map_err(|e| e.to_string())
    }
}

impl MaterialSink for Store {
    fn put_file(&self, path: &Path) -> Result<String, String> {
        let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
        self.0
            .put(std::io::BufReader::new(file), 3)
            .map(|hash| format!("blake3:{}", hash.hex()))
            .map_err(|e| e.to_string())
    }

    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.0
            .put(bytes, 3)
            .map(|hash| format!("blake3:{}", hash.hex()))
            .map_err(|e| e.to_string())
    }
}

/// A binding's committed position, kept by the producer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Position {
    pub cursor: Value,
    pub carry: Value,
}

impl Position {
    fn path(dir: &Path, binding: &str) -> PathBuf {
        let safe: String = binding
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        dir.join(format!("{safe}.json"))
    }

    pub fn load(dir: &Path, binding: &str) -> Result<Self, Error> {
        let path = Self::path(dir, binding);
        match std::fs::read(&path) {
            Ok(bytes) => {
                serde_json::from_slice(&bytes).map_err(|e| Error::State(path, e.to_string()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(Error::State(path, e.to_string())),
        }
    }

    /// Write durably: a temporary file, synced, then renamed over the old one.
    pub fn save(&self, dir: &Path, binding: &str) -> Result<(), Error> {
        write_durably(
            &Self::path(dir, binding),
            &serde_json::to_vec(self).expect("position serializes"),
        )
    }
}

/// A batch being sent: its frames and the position after them. Written
/// durably before the first frame goes out and removed once the position is
/// saved.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Pending {
    frames: Vec<Ingress>,
    position: Position,
}

impl Pending {
    fn path(dir: &Path, binding: &str) -> PathBuf {
        Position::path(dir, binding).with_extension("pending.json")
    }

    fn load(dir: &Path, binding: &str) -> Result<Option<Self>, Error> {
        let path = Self::path(dir, binding);
        match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|e| Error::State(path, e.to_string())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::State(path, e.to_string())),
        }
    }

    fn save(&self, dir: &Path, binding: &str) -> Result<(), Error> {
        let path = Self::path(dir, binding);
        write_durably(
            &path,
            &serde_json::to_vec(self).expect("pending serializes"),
        )
    }

    fn clear(dir: &Path, binding: &str) -> Result<(), Error> {
        let path = Self::path(dir, binding);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::State(path, e.to_string())),
        }
    }
}

fn write_durably(path: &Path, bytes: &[u8]) -> Result<(), Error> {
    let err = |e: std::io::Error| Error::State(path.to_owned(), e.to_string());
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(err)?;
    }
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp).map_err(err)?;
    std::io::Write::write_all(&mut file, bytes).map_err(err)?;
    file.sync_all().map_err(err)?;
    std::fs::rename(&tmp, path).map_err(err)?;
    Ok(())
}

/// What one poll sent.
#[derive(Clone, Debug, Default)]
pub struct Sent {
    pub materials: usize,
    pub frames: usize,
    pub events: usize,
    pub admitted: usize,
    /// Records the parser could not read, sent to Sinex as failures.
    pub unread: usize,
    /// Frames sent again from a pending batch after a restart.
    pub resent: usize,
}

pub struct Producer {
    name: String,
    subjects: Subjects,
    client: async_nats::Client,
    js: jetstream::Context,
    acks: async_nats::Subscriber,
    /// The local content store, when `sinexd` does not share it: material
    /// bytes and oversized frames are then sent in chunks.
    upload: Option<Store>,
    /// The largest message the server accepts.
    max_message: usize,
}

impl Producer {
    /// Connect as `name` (a valid producer name: lowercase, digits, `-`, `_`).
    pub async fn connect(url: &str, subjects: Subjects, name: &str) -> Result<Self, Error> {
        if !sinex_protocol::nats::valid_producer(name) {
            return Err(Error::Nats(format!("invalid producer name {name:?}")));
        }
        let client = crate::nats::connect(url).await.map_err(Error::Nats)?;
        let acks = client.subscribe(subjects.ack(name)).await.map_err(nats)?;
        let max_message = client.server_info().max_payload;
        Ok(Self {
            name: name.to_owned(),
            js: jetstream::new(client.clone()),
            subjects,
            client,
            acks,
            upload: None,
            max_message,
        })
    }

    /// Send material bytes from `store` in chunks, for a producer whose host
    /// does not share `sinexd`'s content store (a capture node).
    pub fn upload_bytes(mut self, store: Store) -> Self {
        self.upload = Some(store);
        self
    }

    /// The binding's committed position. A batch left pending by a stop
    /// before its position was saved is sent again first (Sinex answers its
    /// recorded frames from the record), and its position is saved.
    pub async fn position(&mut self, binding: &str, state: &Path) -> Result<Position, Error> {
        if let Some(pending) = Pending::load(state, binding)? {
            for frame in pending.frames {
                self.deliver(frame).await?;
            }
            pending.position.save(state, binding)?;
            Pending::clear(state, binding)?;
        }
        Position::load(state, binding)
    }

    /// A frame as it will travel: one too large for a message is stored (in
    /// the uploading store, or the shared one) and named by an indirect frame.
    fn prepare(&self, frame: Ingress, shared: &dyn MaterialSink) -> Result<Ingress, Error> {
        let body = serde_json::to_vec(&frame).expect("frame serializes");
        // Room for headers and the subject.
        if body.len() + 4096 <= self.max_message {
            return Ok(frame);
        }
        let store: &dyn MaterialSink = match &self.upload {
            Some(store) => store,
            None => shared,
        };
        let blob = store.put(&body).map_err(Error::Nats)?;
        Ok(Ingress::Indirect {
            frame: format!("{}/indirect", frame.frame()),
            blob,
        })
    }

    /// Send one prepared frame, first sending the bytes it names when
    /// `sinexd` does not share the store.
    async fn deliver(&mut self, frame: Ingress) -> Result<Acknowledgment, Error> {
        if self.upload.is_some() {
            match &frame {
                Ingress::Material { material, .. } => {
                    let blob = material.blob.clone();
                    self.upload_blob(&blob).await?;
                }
                Ingress::Indirect { blob, .. } => {
                    let blob = blob.clone();
                    self.upload_blob(&blob).await?;
                }
                Ingress::Events { .. } | Ingress::Chunk { .. } => {}
            }
        }
        self.send(frame).await
    }

    /// Send the bytes named `blob` from the uploading store, in order, as
    /// chunk frames. Each attempt names its frames afresh: Sinex keeps the
    /// bytes it already has by offset, so a repeated upload costs only the
    /// sending, and a refused attempt never blocks the next one.
    async fn upload_blob(&mut self, blob: &str) -> Result<(), Error> {
        let store = self.upload.as_ref().expect("uploading");
        let mut reader = store.open_upload(blob).map_err(Error::Nats)?;
        // The initial whole-name verification accounts for every decoded byte.
        let total = reader.stats().decoded_bytes;
        let attempt = uuid::Uuid::now_v7();
        let mut offset = 0u64;
        let mut bytes = vec![0; total.min(CHUNK_BYTES as u64) as usize];
        loop {
            let count = (total - offset).min(CHUNK_BYTES as u64) as usize;
            reader.read_exact(&mut bytes[..count]).map_err(nats)?;
            self.send(Ingress::Chunk {
                frame: format!("{blob}/{attempt}/{offset}"),
                blob: blob.to_owned(),
                offset,
                total,
                data: base64::engine::general_purpose::STANDARD.encode(&bytes[..count]),
            })
            .await?;
            offset += count as u64;
            if offset >= total {
                return Ok(());
            }
        }
    }

    /// Send one frame and wait for its acknowledgment.
    async fn send(&mut self, frame: Ingress) -> Result<Acknowledgment, Error> {
        let id = frame.frame().to_owned();
        let mut headers = HeaderMap::new();
        // JetStream drops a duplicate within its window; the ingress also
        // answers a resent frame from its record.
        headers.insert("Nats-Msg-Id", id.as_str());
        let body = serde_json::to_vec(&frame).expect("frame serializes");
        self.js
            .publish_with_headers(self.subjects.ingress(&self.name), headers, body.into())
            .await
            .map_err(nats)?
            .await
            .map_err(nats)?;
        let wait = async {
            while let Some(message) = self.acks.next().await {
                if let Ok(ack) = serde_json::from_slice::<Acknowledgment>(&message.payload)
                    && ack.frame == id
                {
                    return Some(ack);
                }
            }
            None
        };
        let ack = match tokio::time::timeout(ACK_WAIT, wait).await {
            Ok(Some(ack)) => ack,
            // Missed it: ask what Sinex has recorded for this frame.
            _ => self
                .have(std::slice::from_ref(&id))
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| Error::NoAck(id.clone()))?,
        };
        if let Some(reason) = &ack.refusal {
            return Err(Error::Refused {
                frame: id,
                reason: reason.clone(),
            });
        }
        Ok(ack)
    }

    /// Which of these frames Sinex has recorded.
    pub async fn have(&self, frames: &[String]) -> Result<Vec<Acknowledgment>, Error> {
        let request = serde_json::to_vec(&Have {
            frames: frames.to_vec(),
        })
        .expect("request serializes");
        let reply = self
            .client
            .request(self.subjects.have(&self.name), request.into())
            .await
            .map_err(nats)?;
        serde_json::from_slice(&reply.payload).map_err(nats)
    }

    /// Send what one poll acquired, then advance the binding's position. The
    /// poll must start at [`Producer::position`].
    pub async fn produce(
        &mut self,
        source: &dyn Source,
        binding: &str,
        acquired: Acquired,
        sink: &dyn MaterialSink,
        state: &Path,
    ) -> Result<Sent, Error> {
        let position = Position::load(state, binding)?;
        let batches = drivers::batches(
            source,
            binding,
            RunId::new(),
            acquired,
            position.carry,
            sink,
            Limits::default(),
        )?;
        let mut sent = Sent::default();
        for (index, batch) in batches.into_iter().enumerate() {
            let BatchKind::Source(progress) = &batch.kind else {
                unreachable!("drivers build source batches")
            };
            let mut frames = Vec::new();
            if let Some(m) = &progress.material {
                frames.push(Ingress::Material {
                    frame: format!("{binding}/material/{}", m.id),
                    material: Box::new(MaterialFrame {
                        id: m.id,
                        input: progress.input.clone(),
                        blob: m.blob.clone(),
                        length: m.length,
                        parser: m.parser.clone(),
                        parser_version: m.parser_version,
                        part_index: m.part_index,
                        previous_part: m.previous_part,
                        manifest: m.manifest.clone(),
                    }),
                });
                sent.materials += 1;
            }
            let mut events = Vec::new();
            let mut failures = Vec::new();
            for outcome in &batch.records {
                match outcome {
                    RecordOutcome::Events { events: built, .. } => {
                        for e in built {
                            let (location, ordinal) = match &e.origin {
                                Origin::Record { location, ordinal } => (Some(*location), *ordinal),
                                Origin::Derived { .. } => (None, 0),
                            };
                            events.push(ProducerEvent {
                                event_type: e.event_type.to_owned(),
                                payload: e.payload.clone(),
                                location,
                                ordinal,
                                parents: vec![],
                                reading_order: e.reading_order.0,
                                class: e.class,
                            });
                        }
                    }
                    RecordOutcome::Failure { location, reason } => {
                        failures.push(ProducerFailure {
                            location: *location,
                            reason: reason.clone(),
                        });
                    }
                }
            }
            sent.unread += failures.len();
            let material = progress
                .material
                .as_ref()
                .map(|m| m.id.to_string())
                .or_else(|| progress.cursor.get("material").map(|m| m.to_string()))
                .unwrap_or_default();
            let mut chunks = split(events);
            if chunks.is_empty() && !failures.is_empty() {
                chunks.push(Vec::new());
            }
            for (chunk, frame_events) in chunks.into_iter().enumerate() {
                sent.events += frame_events.len();
                frames.push(Ingress::Events {
                    frame: format!("{binding}/events/{material}/{index}/{chunk}"),
                    events: frame_events,
                    // The batch's failures travel with its first events frame.
                    failures: if chunk == 0 {
                        std::mem::take(&mut failures)
                    } else {
                        Vec::new()
                    },
                });
            }
            let after = Position {
                cursor: progress.cursor.clone(),
                carry: progress.carry.clone(),
            };
            if frames.is_empty() {
                after.save(state, binding)?;
                continue;
            }
            let frames = frames
                .into_iter()
                .map(|f| self.prepare(f, sink))
                .collect::<Result<Vec<_>, _>>()?;
            Pending {
                frames: frames.clone(),
                position: after.clone(),
            }
            .save(state, binding)?;
            for frame in frames {
                let is_events = matches!(frame, Ingress::Events { .. } | Ingress::Indirect { .. });
                let ack = self.deliver(frame).await?;
                if is_events {
                    sent.admitted += ack.admitted.len();
                    sent.frames += 1;
                }
            }
            after.save(state, binding)?;
            Pending::clear(state, binding)?;
        }
        Ok(sent)
    }
}

/// Group events into frames that fit one message.
fn split(events: Vec<ProducerEvent>) -> Vec<Vec<ProducerEvent>> {
    let mut frames = Vec::new();
    let mut current = Vec::new();
    let mut bytes = 0;
    for event in events {
        let size = serde_json::to_vec(&event).map_or(0, |v| v.len());
        if !current.is_empty() && bytes + size > FRAME_BYTES {
            frames.push(std::mem::take(&mut current));
            bytes = 0;
        }
        bytes += size;
        current.push(event);
    }
    if !current.is_empty() {
        frames.push(current);
    }
    frames
}

#[cfg(test)]
mod tests;

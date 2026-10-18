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
//! 3. only when every frame of the poll is acknowledged does the binding's
//!    cursor (and parser carry) advance, in a state file next to the spool.
//!
//! Frame names are derived from the material and batch, so a producer that
//! restarts mid-poll and sends a frame again gets the recorded
//! acknowledgment back instead of a second admission.
use crate::drivers::{self, Acquired, DriverError, MaterialSink};
use crate::{Limits, Source};
use async_nats::HeaderMap;
use async_nats::jetstream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinex_model::{BatchKind, Origin, RecordOutcome, RunId};
use sinex_protocol::{Acknowledgment, Have, Ingress, MaterialFrame, ProducerEvent, Subjects};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Event frames stay well under NATS's default 1 MiB message limit.
const FRAME_BYTES: usize = 512 * 1024;
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

impl MaterialSink for Store {
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
        let path = Self::path(dir, binding);
        let err = |e: std::io::Error| Error::State(path.clone(), e.to_string());
        std::fs::create_dir_all(dir).map_err(err)?;
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(self).expect("position serializes");
        let mut file = std::fs::File::create(&tmp).map_err(err)?;
        std::io::Write::write_all(&mut file, &bytes).map_err(err)?;
        file.sync_all().map_err(err)?;
        std::fs::rename(&tmp, &path).map_err(err)?;
        Ok(())
    }
}

/// What one poll sent.
#[derive(Clone, Debug, Default)]
pub struct Sent {
    pub materials: usize,
    pub frames: usize,
    pub events: usize,
    pub admitted: usize,
    /// Records the parser could not read. The ingress has no failure frame
    /// yet, so these stay in the producer's report and the stored material.
    pub unread: usize,
}

pub struct Producer {
    name: String,
    subjects: Subjects,
    client: async_nats::Client,
    js: jetstream::Context,
    acks: async_nats::Subscriber,
}

impl Producer {
    /// Connect as `name` (a valid producer name: lowercase, digits, `-`, `_`).
    pub async fn connect(url: &str, subjects: Subjects, name: &str) -> Result<Self, Error> {
        if !sinex_protocol::nats::valid_producer(name) {
            return Err(Error::Nats(format!("invalid producer name {name:?}")));
        }
        let client = async_nats::connect(url).await.map_err(nats)?;
        let acks = client.subscribe(subjects.ack(name)).await.map_err(nats)?;
        Ok(Self {
            name: name.to_owned(),
            js: jetstream::new(client.clone()),
            subjects,
            client,
            acks,
        })
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

    /// Send what one poll acquired, then advance the binding's position.
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
            if let Some(m) = &progress.material {
                let frame = Ingress::Material {
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
                };
                self.send(frame).await?;
                sent.materials += 1;
            }
            let mut events = Vec::new();
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
                    RecordOutcome::Failure { .. } => sent.unread += 1,
                }
            }
            let material = progress
                .material
                .as_ref()
                .map(|m| m.id.to_string())
                .or_else(|| progress.cursor.get("material").map(|m| m.to_string()))
                .unwrap_or_default();
            for (chunk, frame_events) in split(events).into_iter().enumerate() {
                sent.events += frame_events.len();
                let ack = self
                    .send(Ingress::Events {
                        frame: format!("{binding}/events/{material}/{index}/{chunk}"),
                        events: frame_events,
                    })
                    .await?;
                sent.admitted += ack.admitted.len();
                sent.frames += 1;
            }
            // After each acknowledged batch, so a restart repeats at most one
            // batch. (A crash between the acknowledgment and this save still
            // re-reads that batch into a new material; the ingress cannot yet
            // store the producer's cursor with its events.)
            Position {
                cursor: progress.cursor.clone(),
                carry: progress.carry.clone(),
            }
            .save(state, binding)?;
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
mod tests {
    use super::*;
    use serde_json::json;

    fn event(n: usize) -> ProducerEvent {
        ProducerEvent {
            event_type: "window.focused".into(),
            payload: json!({"title": "x".repeat(n)}),
            location: None,
            ordinal: 0,
            parents: vec![],
            reading_order: 0,
            class: Default::default(),
        }
    }

    #[test]
    fn frames_fit_one_message_and_a_large_event_travels_alone() {
        let frames = split(vec![
            event(300_000),
            event(300_000),
            event(10),
            event(2_000_000),
        ]);
        assert_eq!(
            frames.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![1, 2, 1]
        );
    }

    /// Produce a Hyprland spool against a stand-in for `sinexd`'s ingress
    /// that records frames and acknowledges each one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_spool_is_produced_once_material_first() {
        use crate::drivers::live_stream::{Rotation, SpoolWriter, capture_lines};
        use crate::sources::hyprland;
        use async_nats::jetstream::{consumer, stream};
        use std::sync::{Arc, Mutex};

        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let client = async_nats::connect(nats.url()).await.unwrap();
        let js = jetstream::new(client.clone());
        let stream = js
            .get_or_create_stream(stream::Config {
                name: subjects.ingress_stream(),
                subjects: vec![subjects.ingress_all()],
                ..Default::default()
            })
            .await
            .unwrap();
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                durable_name: Some("standin".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        let received: Arc<Mutex<Vec<Ingress>>> = Arc::default();
        let (seen, acker, ack_subject) = (
            received.clone(),
            client.clone(),
            subjects.ack("session-test"),
        );
        tokio::spawn(async move {
            let mut messages = consumer.messages().await.unwrap();
            while let Some(Ok(message)) = messages.next().await {
                let frame: Ingress = serde_json::from_slice(&message.payload).unwrap();
                let (material, admitted) = match &frame {
                    Ingress::Material { material, .. } => (Some(material.id), vec![]),
                    Ingress::Events { events, .. } => (
                        None,
                        events.iter().map(|_| sinex_model::EventId::new()).collect(),
                    ),
                };
                let ack = Acknowledgment {
                    frame: frame.frame().to_owned(),
                    material,
                    admitted,
                    outcome: json!({}),
                    refusal: None,
                    commit_seq: Some(1),
                };
                seen.lock().unwrap().push(frame);
                acker
                    .publish(
                        ack_subject.clone(),
                        serde_json::to_vec(&ack).unwrap().into(),
                    )
                    .await
                    .unwrap();
                message.ack().await.unwrap();
            }
        });

        let dir = std::env::temp_dir().join(format!("sinex-prod-{}", uuid::Uuid::now_v7()));
        let spool = dir.join("spool");
        let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
        capture_lines(
            &b"activewindow>>kitty,t\nactivewindowv2>>55aa\nclosewindow>>55aa\n"[..],
            &mut writer,
            Duration::ZERO,
        )
        .unwrap();
        let store = Store::open(&dir.join("store")).unwrap();
        let positions = dir.join("positions");
        let driver = hyprland::driver(spool, "h".into());
        let source = hyprland::source();
        let mut producer = Producer::connect(nats.url(), subjects, "session-test")
            .await
            .unwrap();

        let poll = |producer_positions: &Path| {
            let p = Position::load(producer_positions, "hyprland@h").unwrap();
            driver.poll(&p.cursor).unwrap()
        };
        let sent = producer
            .produce(&source, "hyprland@h", poll(&positions), &store, &positions)
            .await
            .unwrap();
        assert_eq!(
            (sent.materials, sent.events, sent.admitted, sent.unread),
            (1, 2, 2, 0)
        );
        let again = producer
            .produce(&source, "hyprland@h", poll(&positions), &store, &positions)
            .await
            .unwrap();
        assert_eq!(
            again.events, 0,
            "the position advanced; nothing is sent twice"
        );

        let frames = received.lock().unwrap().clone();
        assert!(
            matches!(frames.first(), Some(Ingress::Material { .. })),
            "material first"
        );
        let Some(Ingress::Events { events, .. }) = frames.get(1) else {
            panic!("events after the material: {frames:?}")
        };
        let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
        assert_eq!(types, vec!["window.focused", "window.closed"]);
        assert!(
            events.iter().all(|e| e.location.is_some()),
            "events cite their material"
        );
    }

    #[test]
    fn position_survives_a_restart() {
        let dir = std::env::temp_dir().join(format!("sinex-pos-{}", uuid::Uuid::now_v7()));
        assert!(Position::load(&dir, "hyprland@h").unwrap().cursor.is_null());
        Position {
            cursor: json!({"segment": 3}),
            carry: json!(null),
        }
        .save(&dir, "hyprland@h")
        .unwrap();
        assert_eq!(
            Position::load(&dir, "hyprland@h").unwrap().cursor,
            json!({"segment": 3})
        );
    }
}

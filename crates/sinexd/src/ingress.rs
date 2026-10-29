//! The NATS ingress: outside producers (`sinex-session`, capture nodes,
//! Polylogue, the browser extension, programs in other languages) send frames
//! to `<root>.ingress.<producer>`, which a JetStream work-queue stream holds
//! until `sinexd` has handled them.
//!
//! For each frame the consumer:
//!
//! 1. answers from the recorded acknowledgment if the frame was handled
//!    before (a redelivery or a resend);
//! 2. checks it: a material frame's bytes must be in the content store or in
//!    the frame's directory, from where they are stored first; every
//!    event must be of a registered type, match that type's JSON Schema,
//!    decode into its Rust type, and cite a registered material or parent
//!    events;
//! 3. hands it to the writer as one `Ingress` batch, so its events and its
//!    acknowledgment commit together; a frame that fails a check is recorded
//!    as refused, with no events;
//! 4. keeps malformed frame bytes or removes the frame's directory, publishes the acknowledgment on
//!    `<root>.ack.<producer>` and acknowledges the JetStream message.
//!
//! The producer is the one its subject names; per-producer credentials limit
//! each producer to its own subjects. `<root>.recorded.<producer>` answers which
//! frames Sinex recorded, `<root>.materials.<producer>` pages through the
//! materials the producer registered, and `<root>.excise.<producer>` excises
//! whole materials the producer registered.
mod bytes;
pub(crate) mod fetch;
mod confirmed_consumer;
pub(crate) mod replay;

use crate::writer::{Priority, WriteError, WriterHandle};
use async_nats::jetstream::{self, AckKind, consumer, stream};
use base64::Engine;
use futures::StreamExt;
use serde_json::Value;
use sinex_model::{
    Batch, BatchKind, EventId, FrameRefusal, IngressFrame, MaterialRef, ObservationFacts, Origin,
    ReadingOrder, RecordOutcome, RevisionChange, RunId, SourceProgress, Timestamp,
};
use sinex_protocol::{
    Acknowledgment, Excise, ExciseReply, Ingress, MaterialFrame, Materials, ProducerEvent,
    ProducerFailure, ProducerHello, ProducerMaterial, ProducerWelcome, RecordedQuery, Subjects,
};
use sinex_store::commit_log::{self, StoredFrame};
use sinex_store::{Blobs, runs};
use sqlx::PgConnection;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

/// The durable consumer `sinexd` reads the ingress stream with.
const DURABLE: &str = "sinexd";

/// Create the ingress stream and its consumer, or bring an existing stream
/// to this configuration. Nothing a producer sent expires before admission:
/// the stream has no age limit, a full stream refuses a publish (the
/// producer keeps the frame and sends it again) instead of dropping an
/// older frame, and the consumer redelivers without a cap.
pub(crate) async fn ensure_stream(
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<consumer::PullConsumer, String> {
    js.create_or_update_stream(stream::Config {
        name: subjects.ingress_stream(),
        subjects: vec![subjects.ingress_all()],
        storage: stream::StorageType::File,
        // A frame stays until sinexd acknowledges it, then goes: its content
        // is now in Postgres and the content store.
        retention: stream::RetentionPolicy::WorkQueue,
        discard: stream::DiscardPolicy::New,
        max_age: Duration::ZERO,
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    })
    .await
    .map_err(|e| format!("ingress stream: {e}"))?;
    let stream = js
        .get_stream(subjects.ingress_stream())
        .await
        .map_err(|e| format!("ingress stream: {e}"))?;
    stream
        .create_consumer(consumer::pull::Config {
            durable_name: Some(DURABLE.into()),
            ack_policy: consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(60),
            max_deliver: -1,
            ..Default::default()
        })
        .await
        .map_err(|e| format!("ingress consumer: {e}"))
}

/// What the ingress needs from the daemon.
pub(crate) struct Ingest {
    pub subjects: Subjects,
    pub writer: WriterHandle,
    /// The connection frames are checked and answered from.
    pub conn: PgConnection,
    pub blobs: Blobs,
    pub host: String,
    runs: HashMap<String, RunId>,
    privacy_versions: HashMap<String, (i64, Option<sinex_model::Class>)>,
}

impl Ingest {
    pub(crate) fn new(
        subjects: Subjects,
        writer: WriterHandle,
        conn: PgConnection,
        blobs: Blobs,
        host: String,
    ) -> Result<Self, String> {
        Ok(Self {
            subjects,
            writer,
            conn,
            blobs,
            host,
            runs: HashMap::new(),
            privacy_versions: HashMap::new(),
        })
    }

    /// Handle one frame from `producer` and return its acknowledgment. `Err`
    /// means the frame could not be handled now (the database or writer is
    /// unavailable) and should be delivered again.
    pub(crate) async fn handle(
        &mut self,
        producer: &str,
        body: &[u8],
    ) -> Result<Acknowledgment, String> {
        let decoded = serde_json::from_slice::<Ingress>(body);
        let name = decoded
            .as_ref()
            .ok()
            .map(|f| f.frame())
            .filter(|name| !name.is_empty() && !name.contains('\0'))
            .map(str::to_owned)
            .unwrap_or_else(|| format!("malformed/{}", blake3::hash(body).to_hex()));
        if let Some(stored) = self.stored(producer, &name).await? {
            self.discard_recorded(producer, &stored, decoded.as_ref().ok());
            return Ok(acknowledgment(stored));
        }
        let run = self.run(producer).await?;
        let mut declarations = Vec::new();
        let configuration = self.configuration(producer).await?;
        let offered = decoded.as_ref().ok().cloned();
        let decoded = match decoded {
            Ok(frame) if frame.frame().contains('\0') => {
                Err(Check::Malformed("a frame name contains NUL".into()))
            }
            Ok(frame) if !frame.frame().is_empty() => {
                self.unwrap_large_frame(producer, frame).await
            }
            Ok(_) => Err(Check::Malformed("a frame needs a name".into())),
            Err(error) => Err(Check::Malformed(format!("not a frame: {error}"))),
        };
        let details = decoded.as_ref().ok().or(offered.as_ref());
        let checked = if let Some(reason) = configuration {
            Err(Check::Configuration(reason))
        } else {
            match &decoded {
                Err(check) => Err(check.clone()),
                Ok(Ingress::RegisterKinds {
                    declarations: offered,
                    ..
                }) => {
                    declarations = offered.clone();
                    Ok((None, vec![]))
                }
                Ok(Ingress::ReplaySequence { sequence, .. }) => {
                    return self
                        .replay_sequence(producer, &name, run, *sequence.clone())
                        .await;
                }
                Ok(Ingress::Material { material, .. }) => {
                    match self.store_material(producer, &name, material).await {
                        Ok(()) => self.material(producer, material),
                        Err(check) => Err(check),
                    }
                }
                Ok(Ingress::Events {
                    events, failures, ..
                }) => self
                    .events(producer, events, failures)
                    .await
                    .map(|records| (None, records)),
                Ok(chunk @ Ingress::Chunk { .. }) => self
                    .receive_chunk(producer, chunk)
                    .await
                    .map(|()| (None, vec![])),
                Ok(Ingress::LargeFrame { .. }) => unreachable!("large frame was decoded"),
            }
        };
        let batch = match checked {
            Ok((progress, records)) => Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    assign_orders:assigned_orders(details,&records),
                    producer: producer.to_owned(),
                    frame: name.clone(),
                    material: progress,
                    refusal: None,
                    declarations,
                    refusal_kind: None,
                    source: frame_source(details),
                    event_types: frame_event_types(details),
                })),
                records,
                archive: vec![],
            },
            Err(Check::Unavailable(error)) => return Err(error),
            Err(check) => self.refusal(run, producer, &name, details, check).await?,
        };
        match self.writer.submit(Priority::Live, batch).await {
            Ok(_) => {}
            Err(WriteError::Unavailable(error)) => return Err(error),
            Err(WriteError::Stopped) => return Err("the writer has stopped".into()),
            Err(WriteError::Refused(reason)) => {
                // Handled meanwhile by a redelivery: answer from the record.
                if let Some(stored) = self.stored(producer, &name).await? {
                    self.discard_recorded(producer, &stored, decoded.as_ref().ok());
                    return Ok(acknowledgment(stored));
                }
                let refused = self
                    .refusal(
                        run,
                        producer,
                        &name,
                        details,
                        Check::Refused(format!("admission refused it: {reason}")),
                    )
                    .await?;
                self.writer
                    .submit(Priority::Live, refused)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        let stored = self
            .stored(producer, &name)
            .await?
            .ok_or_else(|| format!("frame {name} from {producer} was not recorded"))?;
        self.discard_recorded(producer, &stored, decoded.as_ref().ok());
        Ok(acknowledgment(stored))
    }

    fn discard_recorded(&self, producer: &str, stored: &StoredFrame, frame: Option<&Ingress>) {
        let kind =
            serde_json::from_value::<FrameRefusal>(stored.outcome["refusal_kind"].clone()).ok();
        if stored.refusal.is_none()
            || matches!(
                kind,
                Some(FrameRefusal::Content | FrameRefusal::Bytes | FrameRefusal::Configuration)
            )
        {
            self.discard(producer, &stored.frame);
        }
        if matches!(
            kind,
            Some(FrameRefusal::Bytes | FrameRefusal::Configuration)
        ) && let Some(Ingress::Chunk { for_frame, .. }) = frame
        {
            self.discard(producer, for_frame);
        }
    }

    async fn refusal(
        &mut self,
        run: RunId,
        producer: &str,
        name: &str,
        frame: Option<&Ingress>,
        check: Check,
    ) -> Result<Batch, String> {
        let (kind, reason) = match check {
            Check::Refused(reason) => (FrameRefusal::Content, reason),
            Check::Bytes(reason) => (FrameRefusal::Bytes, reason),
            Check::Malformed(reason) => (FrameRefusal::Malformed, reason),
            Check::Configuration(reason) => (FrameRefusal::Configuration, reason),
            Check::Unavailable(reason) => return Err(reason),
        };
        let mut batch = refusal(run, producer, name, kind, reason.clone());
        if let BatchKind::Ingress(details) = &mut batch.kind {
            details.source = frame_source(frame);
            details.event_types = frame_event_types(frame);
        }
        if kind == FrameRefusal::Content
            && let Some(Ingress::Events {
                events, failures, ..
            }) = frame
        {
            let mut locations: Vec<_> = events
                .iter()
                .filter_map(|e| e.location)
                .chain(failures.iter().map(|f| f.location))
                .collect();
            let ids: Vec<_> = locations.iter().map(|l| l.material.0).collect();
            let known = sinex_store::material_bytes::registered(&mut self.conn, &ids)
                .await
                .map_err(|e| e.to_string())?;
            let named = frame
                .and_then(|frame| frame.frame().rsplit_once("/events/"))
                .and_then(|(_, suffix)| suffix.split('/').next())
                .and_then(|name| Uuid::parse_str(name).ok());
            if let Some(material) = named {
                let owned =
                    commit_log::producer_material_registered(&mut self.conn, producer, material)
                        .await
                        .map_err(|e| e.to_string())?;
                if owned {
                    for location in &mut locations {
                        location.material = sinex_model::MaterialId(material);
                    }
                } else {
                    locations.clear();
                }
            } else {
                locations.retain(|location| known.contains(&location.material.0));
            }
            locations.sort_by_key(|l| (l.material.0, l.anchor));
            locations.dedup();
            batch.records = locations
                .into_iter()
                .map(|location| RecordOutcome::Failure {
                    location,
                    reason: reason.replace('\0', "\\0"),
                })
                .collect();
            if batch.records.is_empty()
                && let BatchKind::Ingress(frame) = &mut batch.kind
            {
                frame.refusal_kind = Some(FrameRefusal::Malformed);
            }
        }
        Ok(batch)
    }

    async fn configuration(&mut self, producer: &str) -> Result<Option<String>, String> {
        let privacy = sinex_store::privacy::load(&mut self.conn, std::iter::once(producer))
            .await
            .map_err(|e| e.to_string())?;
        let version = (privacy.seq, privacy.floors.get(producer).copied());
        let changed = self
            .privacy_versions
            .insert(producer.into(), version)
            .is_some_and(|previous| previous != version);
        Ok(if sealed(&privacy) {
            Some(
                "producer bytes cannot be stored sealed; reconnect to check the configuration"
                    .into(),
            )
        } else if changed {
            Some("privacy configuration changed; reconnect to check the configuration".into())
        } else {
            None
        })
    }

    async fn stored(&mut self, producer: &str, frame: &str) -> Result<Option<StoredFrame>, String> {
        Ok(
            commit_log::frames(&mut self.conn, producer, &[frame.to_owned()])
                .await
                .map_err(|e| e.to_string())?
                .pop(),
        )
    }

    /// One run per producer per daemon start: every event names its run.
    async fn run(&mut self, producer: &str) -> Result<RunId, String> {
        if let Some(run) = self.runs.get(producer) {
            return Ok(*run);
        }
        let run = runs::start(
            &mut self.conn,
            &runs::Manifest {
                name: producer,
                kind: "producer",
                version: "1",
                build: env!("CARGO_PKG_VERSION"),
                emits: &[],
            },
            &self.host,
        )
        .await
        .map_err(|e| e.to_string())?;
        self.runs.insert(producer.to_owned(), run);
        Ok(run)
    }

    /// An indirect frame stands for the frame stored as its bytes; every other
    /// frame stands for itself.
    fn material(
        &self,
        producer: &str,
        m: &MaterialFrame,
    ) -> Result<(Option<SourceProgress>, Vec<RecordOutcome>), Check> {
        let present = self
            .blobs
            .contains(&m.blob)
            .map_err(|e| Check::Unavailable(e.to_string()))?;
        if !present {
            return Err(Check::Unavailable(format!(
                "material {} names bytes {} that are not in the content store; place them first",
                m.id, m.blob
            )));
        }
        Ok((
            Some(SourceProgress {
                data_keys: Vec::new(),
                binding: format!("ingress:{producer}"),
                input: m.input.clone(),
                observation: ObservationFacts {
                    observed_at: Some(Timestamp::now()),
                    changed: true,
                    ..Default::default()
                },
                revision: RevisionChange::Appended,
                material: Some(MaterialRef {
                    id: m.id,
                    input: None,
                    blob: m.blob.clone(),
                    length: m.length,
                    part_index: m.part_index,
                    previous_part: m.previous_part,
                    parser: m.parser.clone(),
                    parser_version: m.parser_version,
                    manifest: m.manifest.clone(),
                }),
                cursor: Value::Null,
                carry: Value::Null,
                memory: None,
            }),
            vec![],
        ))
    }

    async fn events(
        &mut self,
        producer: &str,
        events: &[ProducerEvent],
        failures: &[ProducerFailure],
    ) -> Result<Vec<RecordOutcome>, Check> {
        // Every cited material must be registered first.
        let mut cited: Vec<Uuid> = events
            .iter()
            .filter_map(|e| e.location.map(|l| l.material.0))
            .chain(failures.iter().map(|f| f.location.material.0))
            .collect();
        cited.sort_unstable();
        cited.dedup();
        if !cited.is_empty() {
            let known = sinex_store::material_bytes::registered(&mut self.conn, &cited)
                .await
                .map_err(|e| Check::Unavailable(e.to_string()))?;
            if let Some(missing) = cited.iter().find(|m| !known.contains(m)) {
                return Err(Check::Refused(format!(
                    "events cite material {missing}, which is not registered; send its material frame first"
                )));
            }
        }
        let sizes = sinex_store::material_bytes::lengths(&mut self.conn, &cited)
            .await
            .map_err(|e| Check::Unavailable(e.to_string()))?;
        for location in events
            .iter()
            .filter_map(|e| e.location)
            .chain(failures.iter().map(|f| f.location))
        {
            if sizes.get(&location.material.0).is_none_or(|length| {
                i64::try_from(location.anchor).map_or(true, |anchor| anchor > *length)
            }) {
                return Err(Check::Refused(
                    "record location is outside its material".into(),
                ));
            }
        }
        let mut records = Vec::with_capacity(events.len());
        for (i, e) in events.iter().enumerate() {
            let at =
                |reason: String| Check::Refused(format!("event {i} ({}): {reason}", e.kind.name));
            e.kind.database_version().map_err(at)?;
            let origin = match (e.location, e.parents.is_empty()) {
                (Some(_), false) => return Err(at("choose a record location or parents".into())),
                (Some(location), true) => Origin::Record {
                    location,
                    output_index: e.output_index,
                },
                (None, false) => Origin::Derived {
                    parents: e.parents.clone(),
                },
                (None, true) => {
                    return Err(at("needs a record location or parent events".into()));
                }
            };
            let event = sinex_model::Event::submitted(
                e.kind.clone(),
                e.payload.clone(),
                producer.into(),
                origin,
                ReadingOrder(e.reading_order.unwrap_or(0)),
                record_hash(&e.kind.name, &e.payload),
                e.class,
            );
            records.push(RecordOutcome::Events {
                location: e.location,
                events: vec![event],
            });
        }
        records.extend(failures.iter().map(|f| RecordOutcome::Failure {
            location: f.location,
            reason: f.reason.clone(),
        }));
        Ok(records)
    }
}

fn assigned_orders(frame:Option<&Ingress>,records:&[RecordOutcome])->Vec<EventId> {
    let Some(Ingress::Events{events,..})=frame else {return Vec::new();};
    let stored=records.iter().flat_map(|record|match record {RecordOutcome::Events{events,..}=>events.as_slice(),RecordOutcome::Failure{..}=>&[]});
    events.iter().zip(stored).filter_map(|(offered,stored)|offered.reading_order.is_none().then_some(stored.id)).collect()
}

fn sealed(privacy: &sinex_store::privacy::RuleSet) -> bool {
    privacy
        .floors
        .values()
        .any(|class| *class == sinex_model::Class::Sealed)
        || privacy
            .rules
            .iter()
            .any(|rule| rule.class == sinex_model::Class::Sealed)
}

fn frame_source(frame: Option<&Ingress>) -> Option<String> {
    let frame = frame?;
    let name = match frame {
        Ingress::Chunk { for_frame, .. } => for_frame,
        _ => frame.frame(),
    };
    name.rsplit_once("/events/")
        .or_else(|| name.rsplit_once("/material/"))
        .map(|(binding, _)| {
            binding
                .split_once(':')
                .map_or(binding, |(source, _)| source)
                .replace('\0', "\\0")
        })
        .filter(|source| !source.is_empty())
}

fn frame_event_types(frame: Option<&Ingress>) -> Vec<String> {
    let mut names = match frame {
        Some(Ingress::Events { events, .. }) => events
            .iter()
            // PostgreSQL JSONB cannot represent NUL; diagnostic names use a visible escape.
            .map(|event| event.kind.name.replace('\0', "\\0"))
            .collect::<Vec<_>>(),
        _ => vec![],
    };
    names.sort();
    names.dedup();
    names
}

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum Check {
    /// The frame itself is wrong; it is recorded as refused.
    #[error("{0}")]
    Refused(String),
    #[error("{0}")]
    Bytes(String),
    #[error("{0}")]
    Malformed(String),
    #[error("{0}")]
    Configuration(String),
    /// Sinex could not check it now; it is delivered again.
    #[error("{0}")]
    Unavailable(String),
}

impl From<String> for Check {
    fn from(reason: String) -> Self {
        Self::Refused(reason)
    }
}
impl From<&str> for Check {
    fn from(reason: &str) -> Self {
        Self::Refused(reason.into())
    }
}
impl Check {
    pub(crate) fn write_error(self) -> WriteError {
        match self {
            Self::Refused(reason)
            | Self::Bytes(reason)
            | Self::Malformed(reason)
            | Self::Configuration(reason) => WriteError::Refused(reason),
            Self::Unavailable(error) => WriteError::Unavailable(error),
        }
    }
}

/// The record hash of a producer's event: BLAKE3 over its type and payload as
/// sent, so the same reading sent again is a repeat.
fn record_hash(event_type: &str, payload: &Value) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(event_type.as_bytes());
    hasher.update(&[0]);
    hasher.update(payload.to_string().as_bytes());
    *hasher.finalize().as_bytes()
}

fn refusal(run: RunId, producer: &str, frame: &str, kind: FrameRefusal, reason: String) -> Batch {
    Batch {
        run,
        kind: BatchKind::Ingress(Box::new(IngressFrame {
            assign_orders:vec![],
            producer: producer.to_owned(),
            frame: frame.to_owned(),
            material: None,
            refusal: Some(reason.replace('\0', "\\0")),
            refusal_kind: Some(kind),
            source: None,
            event_types: vec![],
            declarations: vec![],
        })),
        records: vec![],
        archive: vec![],
    }
}

#[cfg(test)]
fn refused_unrecorded(reason: &str) -> Acknowledgment {
    Acknowledgment {
        frame: String::new(),
        material: None,
        material_extents: vec![],
        admitted: vec![],
        outcome: Value::Null,
        refusal: Some(reason.to_owned()),
        commit_seq: None,
    }
}

fn acknowledgment(s: StoredFrame) -> Acknowledgment {
    Acknowledgment {
        frame: s.frame,
        material: s.material_id.map(sinex_model::MaterialId),
        material_extents: s.material_extent.into_iter().collect(),
        admitted: s.admitted.into_iter().map(EventId).collect(),
        outcome: s.outcome,
        refusal: s.refusal,
        commit_seq: s.commit_seq,
    }
}

/// Producer requests answered at once, each in its own task.
const REQUESTS_AT_ONCE: usize = 4;

/// What answers producer requests (recorded queries, material pages,
/// excisions) beside the frame consumer, so a slow request never keeps the
/// consumer from reading its stream.
#[derive(Clone)]
pub(crate) struct Requests {
    pub client:async_nats::Client,
    pub consumer_admin:std::sync::Arc<tokio::sync::Mutex<()>>,
    pub read_grants:std::sync::Arc<fetch::ProducerGrants>,
    pub max_payload:u64,
    pub subjects: Subjects,
    pub writer: WriterHandle,
    pub reads: sqlx::PgPool,
    pub blobs: Blobs,
}

impl Requests {
    async fn connect(
        &self,
        producer: &str,
        hello: &ProducerHello,
    ) -> Result<ProducerWelcome, String> {
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        let privacy = sinex_store::privacy::load(&mut conn, std::iter::once(producer))
            .await
            .map_err(|e| e.to_string())?;
        let refusal = if hello.version != sinex_protocol::PRODUCER_VERSION {
            Some(format!(
                "producer protocol version {} is unsupported; expected {}",
                hello.version,
                sinex_protocol::PRODUCER_VERSION
            ))
        } else if hello.class == sinex_model::Class::Sealed || sealed(&privacy) {
            Some("producer bytes cannot be stored sealed".into())
        } else {
            let declarations = sinex_schema::kinds::all(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
            hello
                .event_types
                .iter()
                .find(|name| {
                    !declarations.values().any(|declaration| {
                        declaration.kind.name == **name
                            && (declaration.declaring_producer == "sinex"
                                || declaration.declaring_producer == producer)
                    })
                })
                .map(|name| format!("producer event type {name} is not registered for {producer}"))
        };
        Ok(ProducerWelcome { refusal,..Default::default() })
    }

    /// Answer a recorded query: the recorded acknowledgments of these frames.
    async fn recorded(
        &self,
        producer: &str,
        request: &RecordedQuery,
    ) -> Result<Vec<Acknowledgment>, String> {
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        Ok(commit_log::frames(&mut conn, producer, &request.frames)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .map(acknowledgment)
            .collect())
    }

    /// Answer a materials request: one page of the materials `producer`
    /// registered and Sinex still holds.
    async fn materials(
        &self,
        producer: &str,
        request: &Materials,
    ) -> Result<Vec<ProducerMaterial>, String> {
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        let rows = commit_log::producer_materials(
            &mut conn,
            producer,
            request.after,
            i64::from(request.limit),
        )
        .await
        .map_err(|e| e.to_string())?;
        rows.into_iter()
            .map(|row| {
                Ok(ProducerMaterial {
                    extents: vec![row.extent],
                    number: row.number,
                    frame: row.frame,
                    material: MaterialFrame {
                        id: sinex_model::MaterialId(row.material_id),
                        input: sinex_model::InputRef {
                            kind: row.kind,
                            locator: row.locator,
                            host: row.host,
                        },
                        blob: row.blob,
                        length: u64::try_from(row.length).map_err(|e| e.to_string())?,
                        parser: row.parser,
                        parser_version: u32::try_from(row.parser_version)
                            .map_err(|e| e.to_string())?,
                        part_index: u32::try_from(row.part_index).map_err(|e| e.to_string())?,
                        previous_part: row.previous_part.map(sinex_model::MaterialId),
                        manifest: serde_json::from_value(row.manifest)
                            .map_err(|e| e.to_string())?,
                    },
                })
            })
            .collect()
    }

    /// Answer an excise request: excise whole materials `producer`
    /// registered, or say why nothing was excised.
    async fn excise(&self, producer: &str, request: &Excise) -> Result<ExciseReply, String> {
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        Ok(crate::excision::excise_for_producer(
            &mut conn,
            &self.blobs,
            &self.writer,
            producer,
            &request.materials,
        )
        .await
        .unwrap_or_else(|refusal| ExciseReply {
            refusal: Some(refusal),
            ..ExciseReply::default()
        }))
    }

    /// The reply to one request on `subject`, or `None` when it names no
    /// producer.
    async fn answer(&self, subject: &str, payload: &[u8]) -> Option<Vec<u8>> {
        let producer = self.subjects.producer_of(subject)?.to_owned();
        let body = if subject == self.subjects.connect(&producer) {
            let welcome = match serde_json::from_slice::<ProducerHello>(payload) {
                Ok(hello) => match self.connect(&producer, &hello).await {
                    Ok(welcome) => welcome,
                    Err(_) => return None,
                },
                Err(error) => ProducerWelcome {
                    refusal: Some(format!("not a producer declaration: {error}")),
                    ..Default::default()
                },
            };
            serde_json::json!(welcome)
        } else if subject == self.subjects.confirmed_consumer(&producer) {
            match serde_json::from_slice::<sinex_protocol::ConfirmedConsumer>(payload) {
                Ok(request)=>serde_json::json!(self.confirmed_consumer(&producer,&request).await),
                Err(error)=>serde_json::json!(sinex_protocol::ConfirmedConsumerReply{refusal:Some(error.to_string()),..Default::default()}),
            }
        } else if subject == self.subjects.fetch_event(&producer) {
            match serde_json::from_slice::<sinex_protocol::FetchEvent>(payload) {
                Ok(request)=>serde_json::json!(self.fetch_event(&producer,&request).await),
                Err(error)=>serde_json::json!(sinex_protocol::FetchEventReply{refusal:Some(error.to_string()),refusal_kind:Some(sinex_protocol::FetchRefusalKind::Invalid),..Default::default()}),
            }
        } else if subject == self.subjects.fetch_payload(&producer) {
            match serde_json::from_slice::<sinex_protocol::FetchPayload>(payload) {
                Ok(request)=>serde_json::json!(self.fetch_payload(&producer,&request).await),
                Err(error)=>serde_json::json!(sinex_protocol::FetchPayloadReply{refusal:Some(error.to_string()),refusal_kind:Some(sinex_protocol::FetchRefusalKind::Invalid),..Default::default()}),
            }
        } else if subject == self.subjects.fetch_material(&producer) {
            match serde_json::from_slice::<sinex_protocol::FetchMaterial>(payload) {
                Ok(request)=>serde_json::json!(self.fetch_material(&producer,&request).await),
                Err(error)=>serde_json::json!(sinex_protocol::FetchMaterialReply{refusal:Some(error.to_string()),refusal_kind:Some(sinex_protocol::FetchRefusalKind::Invalid),..Default::default()}),
            }
        } else if subject == self.subjects.recorded(&producer) {
            match serde_json::from_slice::<RecordedQuery>(payload) {
                Ok(query) => match self.recorded(&producer, &query).await {
                    Ok(acks) => serde_json::json!(acks),
                    Err(error) => serde_json::json!({ "refusal": error.to_string() }),
                },
                Err(error) => {
                    serde_json::json!({ "refusal": format!("not a recorded query: {error}") })
                }
            }
        } else if subject == self.subjects.materials(&producer) {
            match serde_json::from_slice::<Materials>(payload) {
                Ok(page) => match self.materials(&producer, &page).await {
                    Ok(materials) => serde_json::json!(materials),
                    Err(error) => serde_json::json!({ "refusal": error.to_string() }),
                },
                Err(error) => {
                    serde_json::json!({ "refusal": format!("not a materials request: {error}") })
                }
            }
        } else if subject == self.subjects.replay_scope(&producer) {
            match serde_json::from_slice::<sinex_protocol::ReplayScopeQuery>(payload) {
                Ok(query) => {
                    let mut conn = match self.reads.acquire().await {
                        Ok(conn) => conn,
                        Err(error) => {
                            return Some(
                                serde_json::json!({"refusal": error.to_string()})
                                    .to_string()
                                    .into_bytes(),
                            );
                        }
                    };
                    match replay::select(&mut conn, &producer, &query).await {
                        Ok(sequences) => serde_json::json!(sequences),
                        Err(error) => serde_json::json!({"refusal": error.to_string()}),
                    }
                }
                Err(_) => serde_json::json!({"refusal": "invalid replay scope"}),
            }
        } else {
            let reply = match serde_json::from_slice::<Excise>(payload) {
                Ok(excise) => self
                    .excise(&producer, &excise)
                    .await
                    .unwrap_or_else(|error| ExciseReply {
                        refusal: Some(error),
                        ..ExciseReply::default()
                    }),
                Err(error) => ExciseReply {
                    refusal: Some(format!("not an excise request: {error}")),
                    ..ExciseReply::default()
                },
            };
            serde_json::json!(reply)
        };
        Some(body.to_string().into_bytes())
    }
}

/// Consume the ingress stream, and answer producer requests beside it, until
/// `stop` is set.
pub(crate) async fn run(
    client: async_nats::Client,
    ingest: Ingest,
    requests: Requests,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let stopped = async move {
        while stop.changed().await.is_ok() {
            if *stop.borrow() {
                return;
            }
        }
    };
    tokio::select! {
        () = stopped => Ok(()),
        result = consume(client.clone(), ingest) => result,
        result = serve(client, requests) => result,
    }
}

async fn consume(client: async_nats::Client, mut ingest: Ingest) -> Result<(), String> {
    let js = crate::publisher::context(client.clone());
    let consumer = ensure_stream(&js, &ingest.subjects).await?;
    let mut frames = consumer
        .messages()
        .await
        .map_err(|e| format!("ingress messages: {e}"))?;
    loop {
        let Some(message) = frames.next().await else {
            return Err("the ingress stream ended".into());
        };
        let message = match message {
            Ok(message) => message,
            // A frame that waited on a busy writer kept the stream unread past
            // its heartbeat; read it again.
            Err(error) if error.kind() == consumer::pull::MessagesErrorKind::MissingHeartbeat => {
                frames = consumer
                    .messages()
                    .await
                    .map_err(|e| format!("ingress messages: {e}"))?;
                continue;
            }
            Err(error) => return Err(format!("ingress: {error}")),
        };
        let Some(producer) = ingest
            .subjects
            .producer_of(&message.subject)
            .map(str::to_owned)
        else {
            // Not a subject a producer can send on: drop it for good.
            let _ = message.ack_with(AckKind::Term).await;
            continue;
        };
        match ingest.handle(&producer, &message.payload).await {
            Ok(ack) => {
                let body = serde_json::to_vec(&ack).map_err(|e| e.to_string())?;
                let _ = client
                    .publish(ingest.subjects.ack(&producer), body.into())
                    .await;
                message.ack().await.map_err(|e| format!("ack: {e}"))?;
            }
            Err(error) => {
                eprintln!("sinexd: ingress frame from {producer} delayed: {error}");
                let _ = message
                    .ack_with(AckKind::Nak(Some(Duration::from_secs(5))))
                    .await;
            }
        }
    }
}

/// Answer producer requests, at most [`REQUESTS_AT_ONCE`] at a time.
async fn serve(client: async_nats::Client, requests: Requests) -> Result<(), String> {
    let subscribe = |subject: String| {
        let client = client.clone();
        async move {
            client
                .subscribe(subject.clone())
                .await
                .map_err(|e| format!("{subject} subscription: {e}"))
        }
    };
    let mut incoming = futures::stream::select_all([
        subscribe(requests.subjects.connect_all()).await?,
        subscribe(requests.subjects.confirmed_consumer_all()).await?,
        subscribe(requests.subjects.recorded_all()).await?,
        subscribe(requests.subjects.materials_all()).await?,
        subscribe(requests.subjects.excise_all()).await?,
        subscribe(requests.subjects.replay_scope_all()).await?,
        subscribe(requests.subjects.fetch_event_all()).await?,
        subscribe(requests.subjects.fetch_payload_all()).await?,
        subscribe(requests.subjects.fetch_material_all()).await?,
    ]);
    client.flush().await.map_err(|e| e.to_string())?;
    let slots = std::sync::Arc::new(tokio::sync::Semaphore::new(REQUESTS_AT_ONCE));
    let mut running = tokio::task::JoinSet::new();
    loop {
        let request = tokio::select! {
            request = incoming.next() => request,
            Some(_) = running.join_next() => continue,
        };
        let Some(request) = request else {
            return Err("a producer request subscription ended".into());
        };
        let Some(reply) = request.reply.clone() else {
            continue;
        };
        let slot = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;
        let (client, requests) = (client.clone(), requests.clone());
        running.spawn(async move {
            if let Some(body) = requests.answer(&request.subject, &request.payload).await {
                let _ = client.publish(reply, body.into()).await;
            }
            drop(slot);
        });
    }
}

#[cfg(test)]
mod tests;

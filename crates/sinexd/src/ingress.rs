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

use crate::writer::{Priority, WriteError, WriterHandle};
use async_nats::jetstream::{self, AckKind, consumer, stream};
use base64::Engine;
use futures::StreamExt;
use serde_json::Value;
use sinex_model::{
    Batch, BatchKind, EventId, FrameRefusal, IngressFrame, MaterialRef, ObservationFacts, Origin,
    ReadingOrder, RecordOutcome, RevisionChange, RunId, SourceProgress, Timestamp, event_types,
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

/// Create the ingress stream and its consumer if they do not exist.
pub(crate) async fn ensure_stream(
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<consumer::PullConsumer, String> {
    let stream = js
        .get_or_create_stream(stream::Config {
            name: subjects.ingress_stream(),
            subjects: vec![subjects.ingress_all()],
            storage: stream::StorageType::File,
            // A frame stays until sinexd acknowledges it, then goes: its content
            // is now in Postgres and the content store.
            retention: stream::RetentionPolicy::WorkQueue,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .map_err(|e| format!("ingress stream: {e}"))?;
    stream
        .get_or_create_consumer(
            DURABLE,
            consumer::pull::Config {
                durable_name: Some(DURABLE.into()),
                ack_policy: consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(60),
                ..Default::default()
            },
        )
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
    validators: HashMap<&'static str, jsonschema::Validator>,
    runs: HashMap<String, RunId>,
}

impl Ingest {
    pub(crate) fn new(
        subjects: Subjects,
        writer: WriterHandle,
        conn: PgConnection,
        blobs: Blobs,
        host: String,
    ) -> Result<Self, String> {
        let mut validators = HashMap::new();
        for kind in event_types::ALL {
            let schema = (kind.json_schema)();
            let validator = jsonschema::validator_for(&schema)
                .map_err(|e| format!("schema of {}: {e}", kind.event_type))?;
            validators.insert(kind.event_type, validator);
        }
        Ok(Self {
            subjects,
            writer,
            conn,
            blobs,
            host,
            validators,
            runs: HashMap::new(),
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
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("malformed/{}", blake3::hash(body).to_hex()));
        if let Some(stored) = self.stored(producer, &name).await? {
            self.discard_recorded(producer, &stored, decoded.as_ref().ok());
            return Ok(acknowledgment(stored));
        }
        let run = self.run(producer).await?;
        let decoded = match decoded {
            Ok(frame) if !frame.frame().is_empty() => {
                self.unwrap_large_frame(producer, frame).await
            }
            Ok(_) => Err(Check::Malformed("a frame needs a name".into())),
            Err(error) => Err(Check::Malformed(format!("not a frame: {error}"))),
        };
        let checked = match &decoded {
            Err(check) => Err(check.clone()),
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
        };
        let batch = match checked {
            Ok((progress, records)) => Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    producer: producer.to_owned(),
                    frame: name.clone(),
                    material: progress,
                    refusal: None,
                    refusal_kind: None,
                })),
                records,
                archive: vec![],
            },
            Err(Check::Unavailable(error)) => return Err(error),
            Err(check) => {
                self.refusal(run, producer, &name, decoded.as_ref().ok(), check)
                    .await?
            }
        };
        match self.writer.submit(Priority::Live, batch).await {
            Ok(_) => {}
            Err(WriteError::Stopped) => return Err("the writer has stopped".into()),
            Err(WriteError::Unavailable(reason)) => return Err(reason),
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
                        decoded.as_ref().ok(),
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
            || matches!(kind, Some(FrameRefusal::Content | FrameRefusal::Bytes))
        {
            self.discard(producer, &stored.frame);
        }
        if kind == Some(FrameRefusal::Bytes)
            && let Some(Ingress::Chunk { for_frame, .. }) = frame
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
            Check::Unavailable(reason) => return Err(reason),
        };
        let mut batch = refusal(run, producer, name, kind, reason.clone());
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
            if locations.is_empty() || locations.iter().any(|l| !known.contains(&l.material.0)) {
                // A producer can send a wrong location after registering its material.
                // Keep the failure on that producer's last registered material.
                let material = commit_log::last_producer_material(&mut self.conn, producer)
                    .await
                    .map_err(|e| e.to_string())?;
                if let Some(material) = material {
                    for location in &mut locations {
                        if !known.contains(&location.material.0) {
                            location.material = sinex_model::MaterialId(material);
                        }
                    }
                    if locations.is_empty() {
                        locations.push(sinex_model::RecordLocation {
                            material: sinex_model::MaterialId(material),
                            anchor: 0,
                        });
                    }
                }
            }
            locations.retain(|l| known.contains(&l.material.0) || !ids.contains(&l.material.0));
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
        let mut records = Vec::with_capacity(events.len());
        for (i, e) in events.iter().enumerate() {
            let at =
                |reason: String| Check::Refused(format!("event {i} ({}): {reason}", e.event_type));
            let kind = event_types::find(&e.event_type)
                .ok_or_else(|| at("not a registered event type".into()))?;
            if let Some(validator) = self.validators.get(kind.event_type) {
                let errors: Vec<String> = validator
                    .iter_errors(&e.payload)
                    .map(|err| {
                        let path = err.instance_path().to_string();
                        if path.is_empty() {
                            err.to_string()
                        } else {
                            format!("{err} at {path}")
                        }
                    })
                    .collect();
                if !errors.is_empty() {
                    return Err(at(errors.join("; ")));
                }
            }
            let origin = match (e.location, e.parents.is_empty()) {
                (Some(location), _) => Origin::Record {
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
            let event = (kind.build)(
                &e.payload,
                producer,
                origin,
                ReadingOrder(e.reading_order),
                record_hash(kind.event_type, &e.payload),
                e.class,
            )
            .map_err(|err| at(err.to_string()))?;
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

#[derive(Clone)]
enum Check {
    /// The frame itself is wrong; it is recorded as refused.
    Refused(String),
    Bytes(String),
    Malformed(String),
    /// Sinex could not check it now; it is delivered again.
    Unavailable(String),
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
            producer: producer.to_owned(),
            frame: frame.to_owned(),
            material: None,
            refusal: Some(reason.replace('\0', "\\0")),
            refusal_kind: Some(kind),
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
        } else if hello.class == sinex_model::Class::Sealed
            || privacy.floors.get(producer) == Some(&sinex_model::Class::Sealed)
            || privacy
                .rules
                .iter()
                .any(|r| r.class == sinex_model::Class::Sealed)
        {
            Some("producer bytes cannot be stored sealed".into())
        } else {
            // Producer-owned kind declarations are registered at this seam.
            hello
                .event_types
                .iter()
                .find(|name| event_types::find(name).is_none())
                .map(|name| format!("producer event type {name} is not registered"))
        };
        Ok(ProducerWelcome { refusal })
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
                },
            };
            serde_json::json!(welcome)
        } else if subject == self.subjects.recorded(&producer) {
            match serde_json::from_slice::<RecordedQuery>(payload) {
                Ok(query) => match self.recorded(&producer, &query).await {
                    Ok(acks) => serde_json::json!(acks),
                    Err(error) => serde_json::json!({ "refusal": error }),
                },
                Err(error) => {
                    serde_json::json!({ "refusal": format!("not a recorded query: {error}") })
                }
            }
        } else if subject == self.subjects.materials(&producer) {
            match serde_json::from_slice::<Materials>(payload) {
                Ok(page) => match self.materials(&producer, &page).await {
                    Ok(materials) => serde_json::json!(materials),
                    Err(error) => serde_json::json!({ "refusal": error }),
                },
                Err(error) => {
                    serde_json::json!({ "refusal": format!("not a materials request: {error}") })
                }
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
        subscribe(requests.subjects.recorded_all()).await?,
        subscribe(requests.subjects.materials_all()).await?,
        subscribe(requests.subjects.excise_all()).await?,
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

//! The NATS ingress (architecture §4.3): outside producers (`sinex-session`,
//! capture nodes, Polylogue, the browser extension, programs in other
//! languages) send frames to `<root>.ingress.<producer>`, which a JetStream
//! work-queue stream holds until `sinexd` has handled them.
//!
//! For each frame the consumer:
//!
//! 1. answers from the recorded acknowledgment if the frame was handled
//!    before (a redelivery or a resend);
//! 2. checks it: a material frame's bytes must already be in the content
//!    store; every event must be of a registered type, match that type's JSON
//!    Schema, decode into its Rust type, and cite a registered material or
//!    parent events;
//! 3. hands it to the writer as one `Ingress` batch, so its events and its
//!    acknowledgment commit together; a frame that fails a check is recorded
//!    as refused, with no events;
//! 4. publishes the acknowledgment on `<root>.ack.<producer>` and acknowledges
//!    the JetStream message.
//!
//! The producer is the one its subject names; per-producer credentials limit
//! each producer to its own subjects. `<root>.have.<producer>` answers which
//! frames Sinex recorded (`mn8u`).
mod bytes;

use crate::writer::{Priority, WriteError, WriterHandle};
use async_nats::jetstream::{self, AckKind, consumer, stream};
use base64::Engine;
use futures::StreamExt;
use serde_json::Value;
use sinex_model::{
    Batch, BatchKind, EventId, IngressFrame, MaterialRef, ObservationFacts, Origin, ReadingOrder,
    RecordOutcome, RevisionChange, RunId, SourceProgress, Timestamp, event_types,
};
use sinex_protocol::{
    Acknowledgment, Have, Ingress, MaterialFrame, ProducerEvent, ProducerFailure, Subjects,
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
pub async fn ensure_stream(
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
pub struct Ingest {
    pub subjects: Subjects,
    pub writer: WriterHandle,
    /// A read connection for recorded frames and registered materials.
    pub conn: PgConnection,
    reads: sqlx::PgPool,
    pub blobs: Blobs,
    pub host: String,
    validators: HashMap<&'static str, jsonschema::Validator>,
    runs: HashMap<String, RunId>,
    #[cfg(test)]
    byte_hook: Option<std::sync::Arc<dyn Fn(bytes::Point) + Send + Sync>>,
}

impl Ingest {
    pub fn new(
        subjects: Subjects,
        writer: WriterHandle,
        conn: PgConnection,
        reads: sqlx::PgPool,
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
            reads,
            blobs,
            host,
            validators,
            runs: HashMap::new(),
            #[cfg(test)]
            byte_hook: None,
        })
    }

    /// Handle one frame from `producer` and return its acknowledgment. `Err`
    /// means the frame could not be handled now (the database or writer is
    /// unavailable) and should be delivered again.
    pub async fn handle(&mut self, producer: &str, body: &[u8]) -> Result<Acknowledgment, String> {
        let mut guard =
            Some(crate::source_excision::AcquisitionGuard::from_pool(&self.reads, producer).await?);
        let result = self.handle_guarded(producer, body, &mut guard).await;
        if let Some(guard) = guard {
            guard.release().await?;
        }
        if let Err(error) = self.cleanup_bytes().await {
            eprintln!("sinexd: producer private-spool cleanup delayed: {error}");
        }
        result
    }

    async fn handle_guarded(
        &mut self,
        producer: &str,
        body: &[u8],
        guard: &mut Option<crate::source_excision::AcquisitionGuard>,
    ) -> Result<Acknowledgment, String> {
        let frame: Ingress = match serde_json::from_slice(body) {
            Ok(frame) => frame,
            Err(error) => {
                // No frame name to record it under; the refusal goes back on
                // the producer's acknowledgment subject only.
                return Ok(refused_unrecorded(&format!("not a frame: {error}")));
            }
        };
        let name = frame.frame().to_owned();
        let publication_frame = match &frame {
            Ingress::Reserve { publication, .. } => publication.frame.as_str(),
            Ingress::Chunk { publication, .. } => publication.as_str(),
            _ => name.as_str(),
        };
        sinex_store::producer_bytes::lock(
            guard.as_mut().expect("guard before queue").connection(),
            producer,
            publication_frame,
            true,
        )
        .await
        .map_err(|e| e.to_string())?;

        if name.is_empty() {
            return Ok(refused_unrecorded("a frame needs a name"));
        }
        if !matches!(frame, Ingress::Reserve { .. })
            && let Some(stored) = self.stored(producer, &name).await?
        {
            return Ok(acknowledgment(stored));
        }
        let basis = sinex_store::excision::operations_basis(&mut self.conn, producer)
            .await
            .map_err(|e| e.to_string())?;
        if basis.count != 0 {
            return Ok(refused_unrecorded(
                "producer namespace has permanent exclusions; scoped custody filtering is unavailable",
            ));
        }
        let run = self.run(producer).await?;
        if let Ingress::Reserve { publication, .. } = frame {
            return match self.reserve_bytes(producer, &name, publication).await {
                Ok(ack) => Ok(ack),
                Err(reason) => Ok(Acknowledgment {
                    frame: name,
                    refusal: Some(reason),
                    ..refused_unrecorded("")
                }),
            };
        }
        // Capacity and source ownership precede public installation. Queued
        // admission keeps the guard if the request future disappears.
        let permit = self
            .writer
            .reserve_native(Priority::Live)
            .await
            .map_err(|e| e.to_string())?;
        let publication = if matches!(frame, Ingress::Material { .. } | Ingress::Indirect { .. }) {
            sinex_store::producer_bytes::load(&mut self.conn, producer, &name)
                .await
                .map_err(|e| e.to_string())?
                .map(|(p, _)| p)
        } else {
            None
        };
        let checked = match self.unwrap_bytes(producer, frame, guard).await {
            Err(check) => Err(check),
            Ok(Ingress::Material { material, .. }) => self.material(producer, &material),
            Ok(Ingress::Events {
                events, failures, ..
            }) => self
                .events(producer, &events, &failures)
                .await
                .map(|records| (None, records)),
            Ok(chunk @ Ingress::Chunk { .. }) => self
                .chunk_bytes(producer, &chunk, guard)
                .await
                .map(|()| (None, vec![])),
            Ok(Ingress::Indirect { .. } | Ingress::Reserve { .. }) => {
                unreachable!("unwrap replaces indirect frames")
            }
        };
        let batch = match checked {
            Ok((progress, records)) => Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    publication: publication.clone(),
                    producer: producer.to_owned(),
                    frame: name.clone(),
                    material: progress,
                    refusal: None,
                })),
                records,
                archive: vec![],
            },
            Err(Check::Refused(reason)) => refusal(run, producer, &name, reason),
            Err(Check::Unavailable(error)) => return Err(error),
        };
        match permit
            .admit(
                batch,
                guard.take().expect("source guard owned until queue send"),
                std::sync::Arc::new(|| Ok(())),
            )
            .await
        {
            Ok(_) => {}
            Err(WriteError::Stopped) => return Err("the writer has stopped".into()),
            Err(WriteError::Refused(reason)) => {
                // Handled meanwhile by a redelivery: answer from the record.
                if let Some(stored) = self.stored(producer, &name).await? {
                    return Ok(acknowledgment(stored));
                }
                // Admission refused the content (a NUL in a payload, an
                // impossible value): record the frame as refused.
                self.writer
                    .submit(
                        Priority::Live,
                        refusal(
                            run,
                            producer,
                            &name,
                            format!("admission refused it: {reason}"),
                        ),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        self.stored(producer, &name)
            .await?
            .map(acknowledgment)
            .ok_or_else(|| format!("frame {name} from {producer} was not recorded"))
    }

    /// Answer a have request: the recorded acknowledgments of these frames.
    pub async fn have(
        &mut self,
        producer: &str,
        request: &Have,
    ) -> Result<Vec<Acknowledgment>, String> {
        Ok(
            commit_log::frames(&mut self.conn, producer, &request.frames)
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .map(acknowledgment)
                .collect(),
        )
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
            .map_err(|e| Check::Refused(e.to_string()))?;
        if !present {
            return Err(Check::Refused(format!(
                "material {} names bytes {} that are not in the content store; place them first",
                m.id, m.blob
            )));
        }
        Ok((
            Some(SourceProgress {
                native_pending: None,
                erased_replay: None,
                excision_basis: None,
                image: None,
                image_revision: None,
                material_keys: Vec::new(),
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
            let known: Vec<Uuid> =
                sqlx::query_scalar("SELECT id FROM raw.materials WHERE id = ANY($1)")
                    .bind(&cited)
                    .fetch_all(&mut self.conn)
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
                    ordinal: e.ordinal,
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

enum Check {
    /// The frame itself is wrong; it is recorded as refused.
    Refused(String),
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

fn refusal(run: RunId, producer: &str, frame: &str, reason: String) -> Batch {
    Batch {
        run,
        kind: BatchKind::Ingress(Box::new(IngressFrame {
            publication: None,
            producer: producer.to_owned(),
            frame: frame.to_owned(),
            material: None,
            refusal: Some(reason),
        })),
        records: vec![],
        archive: vec![],
    }
}

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

/// Consume the ingress stream and answer have requests until `stop` is set.
pub async fn run(
    client: async_nats::Client,
    mut ingest: Ingest,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    if let Err(error) = ingest.cleanup_bytes().await {
        eprintln!("sinexd: producer private-spool cleanup delayed: {error}");
    }
    let js = crate::publisher::context(client.clone());
    let consumer = ensure_stream(&js, &ingest.subjects).await?;
    let mut frames = consumer
        .messages()
        .await
        .map_err(|e| format!("ingress messages: {e}"))?;
    let mut have = client
        .subscribe(ingest.subjects.have_all())
        .await
        .map_err(|e| format!("have subscription: {e}"))?;
    let mut retire = client
        .subscribe(ingest.subjects.retire_all())
        .await
        .map_err(|e| format!("retirement subscription: {e}"))?;
    client.flush().await.map_err(|e| e.to_string())?;
    loop {
        tokio::select! {
            _ = stop.changed() => {
                if *stop.borrow() {
                    return Ok(());
                }
            }
            message = frames.next() => {
                let Some(message) = message else {
                    return Err("the ingress stream ended".into());
                };
                let message = message.map_err(|e| format!("ingress: {e}"))?;
                let Some(producer) = ingest.subjects.producer_of(&message.subject).map(str::to_owned) else {
                    // Not a subject a producer can send on: drop it for good.
                    let _ = message.ack_with(AckKind::Term).await;
                    continue;
                };
                match ingest.handle(&producer, &message.payload).await {
                    Ok(ack) => {
                        let body = serde_json::to_vec(&ack).map_err(|e| e.to_string())?;
                        let _ = client.publish(ingest.subjects.ack(&producer), body.into()).await;
                        message.ack().await.map_err(|e| format!("ack: {e}"))?;
                    }
                    Err(error) => {
                        eprintln!("sinexd: ingress frame from {producer} delayed: {error}");
                        let _ = message.ack_with(AckKind::Nak(Some(Duration::from_secs(5)))).await;
                    }
                }
            }
            request = retire.next() => {
                let Some(request)=request else {return Err("retirement subscription ended".into())};
                let (Some(reply),Some(producer))=(request.reply.clone(),ingest.subjects.producer_of(&request.subject)) else {continue};
                let result=match serde_json::from_slice::<sinex_protocol::RetireBatches>(&request.payload) {
                    Ok(proof)=>ingest.writer.retire(producer.to_owned(),proof,ingest.blobs.clone()).await.map_err(|e|e.to_string()),
                    Err(e)=>Err(format!("not a retirement proof: {e}")),
                };
                let answer=result.unwrap_or_else(|e|sinex_protocol::Retirement{refusal:Some(e),..Default::default()});
                let _=client.publish(reply,serde_json::to_vec(&answer).map_err(|e|e.to_string())?.into()).await;
            }
            request = have.next() => {
                let Some(request) = request else {
                    return Err("the have subscription ended".into());
                };
                let (Some(reply), Some(producer)) = (
                    request.reply.clone(),
                    ingest.subjects.producer_of(&request.subject).map(str::to_owned),
                ) else {
                    continue;
                };
                let answer = match serde_json::from_slice::<Have>(&request.payload) {
                    Ok(have) => ingest.have(&producer, &have).await.map(|acks| serde_json::json!(acks)),
                    Err(error) => Ok(serde_json::json!({ "refusal": format!("not a have request: {error}") })),
                };
                let body = match answer {
                    Ok(value) => value,
                    Err(error) => serde_json::json!({ "refusal": error }),
                };
                let _ = client.publish(reply, body.to_string().into()).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;

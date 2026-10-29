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
pub(crate) mod authority;
mod bytes;
mod confirmed_consumer;
mod coverage;
pub(crate) mod fetch;
pub(crate) mod replay;
mod signals;

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
    pub public_context: Option<crate::public_context::Provider>,
    pub transport_client: Option<async_nats::Client>,
    pub grants: std::sync::Arc<fetch::ProducerGrants>,
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
        grants: std::sync::Arc<fetch::ProducerGrants>,
    ) -> Result<Self, String> {
        Ok(Self {
            public_context: None,
            transport_client: None,
            grants,
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
        let content_hash = match decoded.as_ref() {
            Ok(Ingress::LargeFrame { blob, .. }) => blob.clone(),
            _ => format!("blake3:{}", blake3::hash(body).to_hex()),
        };
        if let Some(stored) = self.stored(producer, &name).await? {
            if stored
                .outcome
                .get("frame_content_hash")
                .and_then(Value::as_str)
                .is_some_and(|saved| saved != content_hash)
            {
                return Ok(Acknowledgment {
                    number: Some(stored.number),
                    frame: name.clone(),
                    material: None,
                    material_extents: vec![],
                    admitted: vec![],
                    outcome: serde_json::json!({"refusal_kind":FrameRefusal::Conflict}),
                    refusal: Some("frame_body_conflict".into()),
                    commit_seq: None,
                });
            }
            self.discard_recorded(producer, &stored, decoded.as_ref().ok());
            return Ok(acknowledgment(stored));
        }
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
        let source_authority = if let Some(source) = native_source(details) {
            self.native_declaration(producer, source).await.map(drop)
        } else {
            Ok(())
        };
        let run = if matches!(details, Some(Ingress::Signals { .. })) {
            self.signal_run(producer).await?
        } else {
            self.run(
                producer,
                if source_authority.is_ok() {
                    native_source(details)
                } else {
                    None
                },
            )
            .await?
        };
        let checked = if let Err(check) = source_authority {
            Err(check)
        } else if let Some(reason) = configuration {
            Err(Check::Configuration(reason))
        } else {
            match &decoded {
                Err(check) => Err(check.clone()),
                Ok(Ingress::RegisterKinds {
                    declarations: offered,
                    ..
                }) => match self.check_declarations(producer, offered) {
                    Ok(()) => {
                        declarations = offered.clone();
                        Ok((None, vec![]))
                    }
                    Err(error) => Err(error),
                },
                Ok(Ingress::ReplaySequence { sequence, .. }) => {
                    return self
                        .replay_sequence(producer, &name, run, *sequence.clone(), content_hash)
                        .await;
                }
                Ok(Ingress::Material { material, .. }) => {
                    match self.store_material(producer, &name, material).await {
                        Ok(()) => self.material(producer, material),
                        Err(check) => Err(check),
                    }
                }
                Ok(frame @ Ingress::Signals { .. }) => {
                    self.signals_frame(producer, frame, offered.as_ref(), body)
                        .await
                }
                Ok(Ingress::Events {
                    source,
                    events,
                    failures,
                    ..
                }) => self
                    .events(producer, source.as_ref(), events, failures, false)
                    .await
                    .map(|records| (None, records)),
                Ok(chunk @ Ingress::Chunk { .. }) => self
                    .receive_chunk(producer, chunk)
                    .await
                    .map(|()| (None, vec![])),
                Ok(Ingress::LargeFrame { .. }) => unreachable!("large frame was decoded"),
            }
        };
        let checked = match checked {
            Ok(value) => match self.coverage_claim(producer, details).await {
                Ok(coverage) => Ok((value, coverage)),
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };
        let mut batch = match checked {
            Ok(((progress, records), coverage)) => Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    content_hash: Some(content_hash.clone()),
                    receipt_number: None,
                    coverage,
                    native_source: native_source(details).cloned(),
                    source_fingerprint: native_source(details).and_then(|source| {
                        self.grants
                            .get(producer)?
                            .write
                            .as_ref()?
                            .source_declarations
                            .iter()
                            .find(|decl| &decl.source == source)
                            .map(|decl| decl.fingerprint())
                    }),
                    host: self
                        .grants
                        .get(producer)
                        .and_then(|grant| grant.write.as_ref())
                        .and_then(|write| write.host.clone()),
                    family: self
                        .grants
                        .get(producer)
                        .and_then(|grant| grant.write.as_ref())
                        .map(|write| write.family.clone()),
                    write_kind_prefixes: self
                        .grants
                        .get(producer)
                        .and_then(|grant| grant.write.as_ref())
                        .map(|write| write.kind_prefixes.clone())
                        .unwrap_or_default(),
                    assign_orders: assigned_orders(details, &records),
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
        if let BatchKind::Ingress(frame) = &mut batch.kind {
            frame.content_hash = Some(content_hash.clone());
        }
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
                let mut refused = self
                    .refusal(
                        run,
                        producer,
                        &name,
                        details,
                        Check::Refused(format!("admission refused it: {reason}")),
                    )
                    .await?;
                if let BatchKind::Ingress(frame) = &mut refused.kind {
                    frame.content_hash = Some(content_hash.clone());
                }
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
                Some(
                    FrameRefusal::Content
                        | FrameRefusal::Bytes
                        | FrameRefusal::Configuration
                        | FrameRefusal::Excised
                        | FrameRefusal::PartiallyExcised
                        | FrameRefusal::Conflict
                )
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
        let family = match authority::write(&self.grants, producer) {
            Ok(write) => write.family.clone(),
            Err(reason) => return Ok(Some(reason)),
        };
        let privacy =
            sinex_store::privacy::load(&mut self.conn, [producer, family.as_str()].into_iter())
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
    async fn run(
        &mut self,
        producer: &str,
        source: Option<&sinex_model::SourceRef>,
    ) -> Result<RunId, String> {
        let key = format!(
            "{producer}/{}",
            source
                .map(|source| format!("{}/{}", source.name, source.version))
                .unwrap_or_else(|| "canonical".into())
        );
        if let Some(run) = self.runs.get(&key) {
            return Ok(*run);
        }
        let mut emitted = if let Some(source) = source {
            sinex_store::source_declarations::load(&mut self.conn, source)
                .await
                .map_err(|error| error.to_string())?
                .ok_or("native source declaration disappeared")?
                .kinds
                .into_iter()
                .map(|kind| kind.kind.name)
                .collect::<Vec<_>>()
        } else {
            vec![]
        };
        emitted.sort();
        emitted.dedup();
        let emits = emitted.iter().map(String::as_str).collect::<Vec<_>>();
        let name = source
            .map(|source| source.name.as_str())
            .unwrap_or_else(|| {
                self.grants
                    .get(producer)
                    .and_then(|grant| grant.write.as_ref())
                    .map_or(producer, |write| write.family.as_str())
            });
        let version = source
            .map(|source| source.version.to_string())
            .unwrap_or_else(|| "1".into());
        let run = runs::start(
            &mut self.conn,
            &runs::Manifest {
                name,
                kind: if source.is_some() {
                    "source"
                } else {
                    "producer"
                },
                version: &version,
                build: crate::build_identity::current()?,
                emits: &emits,
            },
            self.grants
                .get(producer)
                .and_then(|grant| grant.write.as_ref())
                .and_then(|write| write.host.as_deref())
                .unwrap_or(""),
        )
        .await
        .map_err(|e| e.to_string())?;
        self.runs.insert(key, run);
        Ok(run)
    }

    /// An indirect frame stands for the frame stored as its bytes; every other
    /// frame stands for itself.
    fn material(
        &self,
        producer: &str,
        m: &MaterialFrame,
    ) -> Result<(Option<SourceProgress>, Vec<RecordOutcome>), Check> {
        if m.parser == "signals" {
            return Err(Check::Configuration(
                "host signal material requires the typed signal ingress".into(),
            ));
        }
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
                source: Some(m.source.clone()),
                host: self
                    .grants
                    .get(producer)
                    .and_then(|grant| grant.write.as_ref())
                    .and_then(|write| write.host.clone()),
                data_keys: Vec::new(),
                binding: format!(
                    "ingress:{producer}:{}:{}",
                    m.source.name,
                    self.grants
                        .get(producer)
                        .and_then(|grant| grant.write.as_ref())
                        .and_then(|write| write.host.as_deref())
                        .map(|host| blake3::hash(host.as_bytes()).to_hex().to_string())
                        .unwrap_or_else(|| "unattested".into())
                ),
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

    async fn native_declaration(
        &mut self,
        producer: &str,
        source: &sinex_model::SourceRef,
    ) -> Result<sinex_model::SourceDeclaration, Check> {
        let write = authority::write(&self.grants, producer).map_err(Check::Configuration)?;
        if !authority::matches(&write.source_prefixes, &source.name) {
            return Err(Check::Configuration(
                "native source is outside the configured credential capability".into(),
            ));
        }
        let declaration = sinex_store::source_declarations::load(&mut self.conn, source)
            .await
            .map_err(|error| Check::Unavailable(error.to_string()))?
            .ok_or_else(|| {
                Check::Configuration(
                    "native source version is not installed in the owner catalog".into(),
                )
            })?;
        if declaration.family != write.family {
            return Err(Check::Configuration(
                "native source belongs to another configured family".into(),
            ));
        }
        Ok(declaration)
    }

    fn check_declarations(
        &self,
        producer: &str,
        declarations: &[sinex_model::KindDeclaration],
    ) -> Result<(), Check> {
        let write = authority::write(&self.grants, producer).map_err(Check::Configuration)?;
        for declaration in declarations {
            if declaration.declaring_producer != write.family
                || !authority::matches(&write.kind_prefixes, &declaration.kind.name)
                || !write
                    .source_declarations
                    .iter()
                    .flat_map(|source| &source.kinds)
                    .chain(&write.canonical_kinds)
                    .any(|known| {
                        known.kind == declaration.kind
                            && known.definition_hash() == declaration.definition_hash()
                    })
            {
                return Err(Check::Configuration(
                    "declaration is outside the configured logical family".into(),
                ));
            }
        }
        Ok(())
    }

    async fn events(
        &mut self,
        producer: &str,
        source: Option<&sinex_model::SourceRef>,
        events: &[ProducerEvent],
        failures: &[ProducerFailure],
        replay: bool,
    ) -> Result<Vec<RecordOutcome>, Check> {
        let family = authority::write(&self.grants, producer)
            .map_err(Check::Configuration)?
            .family
            .clone();
        for event in events {
            authority::event(&self.grants, producer, &event.kind.name, event.class)
                .map_err(Check::Configuration)?;
        }
        let references = events
            .iter()
            .map(|event| event.kind.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let declarations = sinex_schema::kinds::load(&mut self.conn, &references)
            .await
            .map_err(|error| Check::Unavailable(error.to_string()))?;
        if let Some(reference) = references
            .iter()
            .find(|reference| !declarations.contains_key(reference))
        {
            return Err(Check::Refused(format!(
                "namespace_refused: exact kind declaration {}@{} is not registered",
                reference.name, reference.version
            )));
        }
        if declarations.values().any(|declaration| {
            declaration.declaring_producer != "sinex" && declaration.declaring_producer != family
        }) {
            return Err(Check::Refused(
                "kind belongs to another logical family".into(),
            ));
        }
        let native = match source {
            Some(source) => Some(self.native_declaration(producer, source).await?),
            None => None,
        };
        let write = authority::write(&self.grants, producer).map_err(Check::Configuration)?;
        for declaration in declarations.values() {
            let allowed = match &native {
                Some(native) => native.allows(&declaration.kind, &declaration.definition_hash()),
                None => write.canonical_kinds.iter().any(|known| {
                    known.kind == declaration.kind
                        && known.definition_hash() == declaration.definition_hash()
                }),
            };
            if !allowed {
                return Err(Check::Configuration(
                    "event is outside its exact owner source or canonical declaration".into(),
                ));
            }
        }
        if source.is_none()
            && (!failures.is_empty() || events.iter().any(|event| event.location.is_some()))
        {
            return Err(Check::Configuration(
                "material readings require an explicit registered native source".into(),
            ));
        }
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
        if !cited.is_empty() {
            let ownership = sinex_store::producer_reads::material_owners(&mut self.conn, &cited)
                .await
                .map_err(|error| Check::Unavailable(error.to_string()))?;
            if cited.iter().any(|id| {
                !ownership.iter().any(|owner| {
                    owner.0 == Some(*id) && owner.1.as_deref() == Some(family.as_str())
                })
            }) {
                return Err(Check::Refused("material family ownership is unavailable or belongs to another family; explicit mapping is required for historical material".into()));
            }
        }
        if let Some(source) = source {
            let known = sinex_store::producer_reads::material_sources(&mut self.conn, &cited)
                .await
                .map_err(|error| Check::Unavailable(error.to_string()))?;
            for material in &cited {
                if !known.iter().any(|(id, name, version)| {
                    id == material
                        && name.as_deref() == Some(source.name.as_str())
                        && (replay || *version == Some(i64::from(source.version)))
                }) {
                    return Err(Check::Configuration("material and event native sources differ or historical source authority is unknown".into()));
                }
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
                source
                    .map(|source| source.name.clone())
                    .unwrap_or_else(|| family.clone()),
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

fn assigned_orders(frame: Option<&Ingress>, records: &[RecordOutcome]) -> Vec<EventId> {
    let Some(Ingress::Events { events, .. }) = frame else {
        return Vec::new();
    };
    let stored = records.iter().flat_map(|record| match record {
        RecordOutcome::Events { events, .. } => events.as_slice(),
        RecordOutcome::Failure { .. } => &[],
    });
    events
        .iter()
        .zip(stored)
        .filter_map(|(offered, stored)| offered.reading_order.is_none().then_some(stored.id))
        .collect()
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

fn native_source(frame: Option<&Ingress>) -> Option<&sinex_model::SourceRef> {
    match frame? {
        Ingress::Material { material, .. } => Some(&material.source),
        Ingress::Events { source, .. } => source.as_ref(),
        Ingress::ReplaySequence { sequence, .. } => Some(&sequence.source),
        _ => None,
    }
}
fn frame_source(frame: Option<&Ingress>) -> Option<String> {
    if matches!(frame, Some(Ingress::Signals { .. })) {
        return Some(sinex_sources::sources::signals::SPEC.name.into());
    }
    native_source(frame).map(|source| source.name.clone())
}

fn frame_event_types(frame: Option<&Ingress>) -> Vec<String> {
    let mut names = match frame {
        Some(Ingress::Signals {
            signals,
            receipts,
            emergency,
            ..
        }) => {
            let mut names = Vec::new();
            if !signals.is_empty() {
                names.push("signal.intent".into());
            }
            if !receipts.is_empty() {
                names.push("signal.receipt".into());
            }
            if !emergency.is_empty() {
                names.extend(
                    [
                        "signal.emergency",
                        "signal.delivery",
                        "signal.desktop_receipt",
                    ]
                    .into_iter()
                    .map(str::to_owned),
                );
            }
            names
        }
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
            content_hash: None,
            receipt_number: None,
            coverage: None,
            native_source: None,
            source_fingerprint: None,
            host: None,
            family: None,
            write_kind_prefixes: vec![],
            assign_orders: vec![],
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
        number: None,
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
        number: Some(s.number),
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
    pub client: async_nats::Client,
    pub public_context: crate::public_context::Provider,
    pub consumer_admin: std::sync::Arc<tokio::sync::Mutex<()>>,
    pub read_grants: std::sync::Arc<fetch::ProducerGrants>,
    pub subjects: Subjects,
    pub writer: WriterHandle,
    pub reads: sqlx::PgPool,
    pub blobs: Blobs,
}

#[derive(serde::Serialize)]
#[serde(untagged)]
enum ResponseBody {
    Context(sinex_protocol::PublicContext),
    Welcome(ProducerWelcome),
    Consumer(sinex_protocol::ConfirmedConsumerReply),
    Event(sinex_protocol::FetchEventReply),
    Material(sinex_protocol::FetchMaterialReply),
    Recorded(sinex_protocol::RecordedPage),
    Materials(sinex_protocol::MaterialsPage),
    Replay(sinex_protocol::ReplayReply),
    Excise(ExciseReply),
    Error { refusal: String },
}

impl Requests {
    async fn connect(
        &self,
        producer: &str,
        hello: &ProducerHello,
    ) -> Result<ProducerWelcome, String> {
        let write = authority::write(&self.read_grants, producer).ok();
        let mut conn = self.reads.acquire().await.map_err(|e| e.to_string())?;
        let family = write.map(|write| write.family.as_str()).unwrap_or(producer);
        let privacy = sinex_store::privacy::load(&mut conn, [producer, family].into_iter())
            .await
            .map_err(|e| e.to_string())?;
        let refusal = if hello.version != sinex_protocol::PRODUCER_VERSION {
            Some(format!(
                "producer protocol version {} is unsupported; expected {}",
                hello.version,
                sinex_protocol::PRODUCER_VERSION
            ))
        } else if !hello.event_types.is_empty() && write.is_none() {
            Some("producer has no configured write family".into())
        } else if write.is_some_and(|write| {
            !write.classes.contains(&hello.class)
                || hello
                    .event_types
                    .iter()
                    .any(|kind| !authority::matches(&write.kind_prefixes, kind))
        }) {
            let write = write.expect("capability branch requires a write grant");
            let undelegated = hello
                .event_types
                .iter()
                .filter(|kind| !authority::matches(&write.kind_prefixes, kind))
                .collect::<Vec<_>>();
            Some(format!(
                "producer hello is outside its write kind or class capability: offered_class={:?}, class_authorized={}, undelegated_kinds={undelegated:?}",
                hello.class,
                write.classes.contains(&hello.class)
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
                                || write.is_some_and(|write| {
                                    declaration.declaring_producer == write.family
                                }))
                    })
                })
                .map(|name| format!("producer event type {name} is not registered for {producer}"))
        };
        let authorized = refusal.is_none();
        Ok(ProducerWelcome {
            family: authorized
                .then(|| write.map(|write| write.family.clone()))
                .flatten(),
            canonical_publisher: authorized
                && write.is_some_and(|write| !write.canonical_prefixes.is_empty()),
            refusal,
        })
    }

    /// Answer a recorded query: the recorded acknowledgments of these frames.
    async fn recorded(
        &self,
        producer: &str,
        request: &RecordedQuery,
    ) -> Result<sinex_protocol::RecordedPage, String> {
        let mut conn = self
            .reads
            .acquire()
            .await
            .map_err(|error| error.to_string())?;
        let (rows, next) = commit_log::frames_page(
            &mut conn,
            producer,
            &request.frames,
            &request.numbers,
            request.after,
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(sinex_protocol::RecordedPage {
            acknowledgments: rows.into_iter().map(acknowledgment).collect(),
            next,
        })
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
        let ids = rows.iter().map(|row| row.material_id).collect::<Vec<_>>();
        let sources = sinex_store::producer_reads::material_sources(&mut conn, &ids)
            .await
            .map_err(|error| error.to_string())?;
        rows.into_iter()
            .map(|row| {
                let (_, name, version) = sources
                    .iter()
                    .find(|(id, _, _)| *id == row.material_id)
                    .ok_or(
                    "material source authority is unknown; explicit historical mapping required",
                )?;
                let source = sinex_model::SourceRef {
                    name: name.clone().ok_or("material source authority is unknown")?,
                    version: u32::try_from(version.ok_or("material source version is unknown")?)
                        .map_err(|error| error.to_string())?,
                };
                Ok(ProducerMaterial {
                    extents: vec![row.extent],
                    number: row.number,
                    frame: row.frame,
                    material: MaterialFrame {
                        source,
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
    async fn answer(&self, subject: &str, payload: &[u8]) -> Option<ResponseBody> {
        let producer = self.subjects.producer_of(subject)?.to_owned();
        let response = if subject == self.subjects.public_context(&producer) {
            let response = if !self
                .read_grants
                .get(&producer)
                .is_some_and(|grant| grant.public_context)
            {
                sinex_protocol::PublicContext::Refused {
                    reason: "public_context_not_authorized".into(),
                }
            } else {
                match serde_json::from_slice::<sinex_protocol::PublicContextRequest>(payload) {
                    Err(error) => sinex_protocol::PublicContext::Refused {
                        reason: error.to_string(),
                    },
                    Ok(request) => self.public_context_snapshot(&producer, &request).await,
                }
            };
            ResponseBody::Context(response)
        } else if subject == self.subjects.connect(&producer) {
            match serde_json::from_slice::<ProducerHello>(payload) {
                Ok(hello) => {
                    ResponseBody::Welcome(self.connect(&producer, &hello).await.unwrap_or_else(
                        |error| ProducerWelcome {
                            refusal: Some(error),
                            ..Default::default()
                        },
                    ))
                }
                Err(error) => ResponseBody::Welcome(ProducerWelcome {
                    refusal: Some(error.to_string()),
                    ..Default::default()
                }),
            }
        } else if subject == self.subjects.confirmed_consumer(&producer) {
            match serde_json::from_slice::<sinex_protocol::ConfirmedConsumer>(payload) {
                Ok(request) => {
                    ResponseBody::Consumer(self.confirmed_consumer(&producer, &request).await)
                }
                Err(error) => ResponseBody::Consumer(sinex_protocol::ConfirmedConsumerReply {
                    refusal: Some(error.to_string()),
                    ..Default::default()
                }),
            }
        } else if subject == self.subjects.fetch_event(&producer) {
            match serde_json::from_slice::<sinex_protocol::FetchEvent>(payload) {
                Ok(request) => ResponseBody::Event(self.fetch_event(&producer, &request).await),
                Err(error) => ResponseBody::Event(sinex_protocol::FetchEventReply {
                    refusal: Some(error.to_string()),
                    refusal_kind: Some(sinex_protocol::FetchRefusalKind::Invalid),
                    ..Default::default()
                }),
            }
        } else if subject == self.subjects.fetch_material(&producer) {
            match serde_json::from_slice::<sinex_protocol::FetchMaterial>(payload) {
                Ok(request) => {
                    ResponseBody::Material(self.fetch_material(&producer, &request).await)
                }
                Err(error) => ResponseBody::Material(sinex_protocol::FetchMaterialReply {
                    refusal: Some(error.to_string()),
                    refusal_kind: Some(sinex_protocol::FetchRefusalKind::Invalid),
                    ..Default::default()
                }),
            }
        } else if subject == self.subjects.recorded(&producer) {
            match serde_json::from_slice::<RecordedQuery>(payload) {
                Ok(query) => match self.recorded(&producer, &query).await {
                    Ok(page) => ResponseBody::Recorded(page),
                    Err(error) => ResponseBody::Error { refusal: error },
                },
                Err(error) => ResponseBody::Error {
                    refusal: error.to_string(),
                },
            }
        } else if subject == self.subjects.materials(&producer) {
            match serde_json::from_slice::<Materials>(payload) {
                Ok(query) => match self.materials(&producer, &query).await {
                    Ok(materials) => {
                        let next = (materials.len() == query.limit as usize
                            && !materials.is_empty())
                        .then(|| materials.last().expect("nonempty page").number);
                        ResponseBody::Materials(sinex_protocol::MaterialsPage { materials, next })
                    }
                    Err(error) => ResponseBody::Error { refusal: error },
                },
                Err(error) => ResponseBody::Error {
                    refusal: error.to_string(),
                },
            }
        } else if subject == self.subjects.replay_scope(&producer) {
            match serde_json::from_slice::<sinex_protocol::ReplayQuery>(payload) {
                Ok(sinex_protocol::ReplayQuery::Inputs { cache, cursor }) => {
                    ResponseBody::Replay(sinex_protocol::ReplayReply::Inputs {
                        result: self
                            .replay_inputs(&producer, &cache, cursor.as_deref())
                            .await,
                    })
                }
                Ok(sinex_protocol::ReplayQuery::Materials { query }) => {
                    let family = match authority::write(&self.read_grants, &producer) {
                        Ok(write) => write.family.clone(),
                        Err(error) => return Some(ResponseBody::Error { refusal: error }),
                    };
                    let mut conn = match self.reads.acquire().await {
                        Ok(conn) => conn,
                        Err(error) => {
                            return Some(ResponseBody::Error {
                                refusal: error.to_string(),
                            });
                        }
                    };
                    match replay::select_page(&mut conn, &producer, &family, &query).await {
                        Ok(page) => {
                            ResponseBody::Replay(sinex_protocol::ReplayReply::Materials { page })
                        }
                        Err(error) => ResponseBody::Error {
                            refusal: error.to_string(),
                        },
                    }
                }
                Err(error) => ResponseBody::Error {
                    refusal: error.to_string(),
                },
            }
        } else {
            match serde_json::from_slice::<Excise>(payload) {
                Ok(request) => {
                    ResponseBody::Excise(self.excise(&producer, &request).await.unwrap_or_else(
                        |error| ExciseReply {
                            refusal: Some(error),
                            ..Default::default()
                        },
                    ))
                }
                Err(error) => ResponseBody::Excise(ExciseReply {
                    refusal: Some(error.to_string()),
                    ..Default::default()
                }),
            }
        };
        Some(response)
    }
}

/// Consume the ingress stream, and answer producer requests beside it, until
/// `stop` is set.
pub(crate) async fn run(
    client: async_nats::Client,
    mut ingest: Ingest,
    requests: Requests,
    mut stop: watch::Receiver<bool>,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), String> {
    ingest.public_context = Some(requests.public_context.clone());
    ingest.transport_client = Some(client.clone());
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
        result = serve(client, requests, ready) => result,
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
                if let Some(number) = ack.number {
                    let hint = sinex_protocol::ReceiptHint {
                        number,
                        frame_hash: blake3::hash(ack.frame.as_bytes()).to_hex().to_string(),
                    };
                    let _ = client
                        .publish(
                            ingest.subjects.ack(&producer),
                            serde_json::to_vec(&hint)
                                .expect("receipt hint serializes")
                                .into(),
                        )
                        .await;
                }
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
async fn serve(
    client: async_nats::Client,
    requests: Requests,
    ready: tokio::sync::oneshot::Sender<()>,
) -> Result<(), String> {
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
        subscribe(requests.subjects.public_context_all()).await?,
        subscribe(requests.subjects.confirmed_consumer_all()).await?,
        subscribe(requests.subjects.recorded_all()).await?,
        subscribe(requests.subjects.materials_all()).await?,
        subscribe(requests.subjects.excise_all()).await?,
        subscribe(requests.subjects.replay_scope_all()).await?,
        subscribe(requests.subjects.fetch_event_all()).await?,
        subscribe(requests.subjects.fetch_material_all()).await?,
    ]);
    client.flush().await.map_err(|e| e.to_string())?;
    // The API socket becomes available only after the broker has acknowledged
    // every producer request subscription on this connection.
    ready
        .send(())
        .map_err(|_| "producer request startup receiver closed".to_owned())?;
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
        let Some(producer) = requests.subjects.producer_of(&request.subject) else {
            continue;
        };
        let prefix = format!("{}.", requests.subjects.inbox_prefix(producer));
        if !reply.starts_with(&prefix) || reply.contains('*') || reply.contains('>') {
            continue;
        }

        let slot = slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| e.to_string())?;
        let (client, requests) = (client.clone(), requests.clone());
        running.spawn(async move {
            let _ = sinex_transport::replies::respond_json(
                &client,
                reply.to_string(),
                requests.answer(&request.subject, &request.payload),
                Duration::from_secs(30),
            )
            .await;
            drop(slot);
        });
    }
}

#[cfg(test)]
mod tests;

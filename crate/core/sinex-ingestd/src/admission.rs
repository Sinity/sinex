//! Reusable event admission boundary for ingestd.
//!
//! This module owns validation and persistence that are not intrinsically tied
//! to NATS message settlement. The `JetStream` consumer remains responsible for
//! ACK/NAK/DLQ and confirmation publishing, while finite staged parsers can call
//! this service directly before they have a transport shape of their own.

use crate::validator::{IngestEventValidator, ValidationResult};
use crate::{IngestdResult, SinexError};
use sinex_db::DbPool;
use sinex_db::repositories::{DbPoolExt, StreamBatchRow};
use sinex_primitives::constants::limits::MAX_EVENT_PAYLOAD_BYTES;
use sinex_primitives::events::Event;
use sinex_primitives::events::admission::{ACCEPTED_ENVELOPE_VERSIONS, AdmittedEventIntent};
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::{Id, JsonValue, Timestamp, Uuid};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::time::{Duration, timeout};
use tracing::{error, warn};

const DB_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const RECENT_ID_CACHE_SIZE: usize = 50_000;

/// SQLSTATE classes that indicate a deterministic row-level persistence fault.
const SQLSTATE_DATA_EXCEPTION_CLASS: &str = "22";
const SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS: &str = "23";

/// Error-class marker for deferred source-material FK violations.
const ERROR_CLASS_SOURCE_MATERIAL_FK: &str = "source_material_fk_violation";
const EVENTS_SOURCE_MATERIAL_ID_FKEY: &str = "events_source_material_id_fkey";
const EVENTS_PAYLOAD_SCHEMA_ID_FKEY: &str = "events_payload_schema_id_fkey";

/// Event accepted by the admission boundary and ready for persistence.
#[derive(Debug, Clone)]
pub struct AdmittedEvent {
    pub event: Event<JsonValue>,
    pub event_id: Uuid,
    pub metadata: Option<CandidateEventMetadata>,
}

/// Parser-side metadata attached before an event crosses the admission boundary.
///
/// These fields deliberately describe the parser/source observation rather than
/// the durable transport. Some fields map onto existing event columns today
/// (`parser_semantics_version`, `operation_id`); the rest are carried with the
/// admitted value so staged-parser callers have one typed place to prove what
/// they knew before persistence.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CandidateEventMetadata {
    pub source_material_id: Option<Uuid>,
    pub stable_natural_key: Option<String>,
    pub parser_source_unit_id: Option<String>,
    pub parser_semantics_version: Option<String>,
    pub timestamp_derivation_evidence: Option<String>,
    pub privacy_context: Option<String>,
    pub privacy_profile: Option<String>,
    pub operation_id: Option<Uuid>,
}

/// Candidate event from a staged parser or other non-NATS caller.
#[derive(Debug, Clone)]
pub struct CandidateEvent {
    pub event: Event<JsonValue>,
    pub metadata: CandidateEventMetadata,
}

impl CandidateEvent {
    #[must_use]
    pub fn new(event: Event<JsonValue>, metadata: CandidateEventMetadata) -> Self {
        Self { event, metadata }
    }
}

/// Result of attempting to admit a candidate event.
#[derive(Debug)]
pub enum AdmissionDecision {
    Admitted(AdmittedEvent),
    Transformed(AdmittedEvent),
    Suppressed(AdmissionRejection),
    QuarantineNeeded(AdmissionRejection),
    Rejected(AdmissionRejection),
}

/// Coarse reason for an event being rejected before persistence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectionKind {
    PayloadTooLarge,
    InvalidUtf8,
    StructuralJson,
    EventDeserialization,
    EnvelopeDeserialization,
    EnvelopeValidation,
    MissingTimestamp,
    PastTimestamp,
    FutureTimestamp,
    NegativeAnchor,
    SchemaValidation,
    CandidateMetadata,
    PrivacyPolicy,
    QuarantinePolicy,
    MissingEventId,
    InvalidEventId,
}

/// A rejected candidate event with a stable kind and operator-facing reason.
#[derive(Debug)]
pub struct AdmissionRejection {
    pub kind: AdmissionRejectionKind,
    pub reason: String,
}

impl AdmissionRejection {
    fn new(kind: AdmissionRejectionKind, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
}

/// Persistence result for an admitted batch.
#[derive(Debug)]
pub struct AdmissionPersistResult {
    pub inserted_ids: Option<Vec<Uuid>>,
    pub attempted_event_ids: Vec<Uuid>,
    pub duplicate_event_ids: Vec<Uuid>,
    pub tombstoned_event_ids: Vec<Uuid>,
    pub tombstoned_events_rejected: usize,
}

impl AdmissionPersistResult {
    fn skipped_plan(plan: &AdmissionBatchPlan) -> Self {
        Self {
            inserted_ids: None,
            attempted_event_ids: plan.attempted_event_ids(),
            duplicate_event_ids: plan.cached_duplicate_event_ids.clone(),
            tombstoned_event_ids: plan.tombstoned_event_ids.clone(),
            tombstoned_events_rejected: plan.tombstoned_event_ids.len(),
        }
    }

    fn persisted_plan(plan: &AdmissionBatchPlan, inserted_ids: Vec<Uuid>) -> Self {
        let inserted_set: HashSet<_> = inserted_ids.iter().copied().collect();
        let mut duplicate_event_ids = plan.cached_duplicate_event_ids.clone();
        duplicate_event_ids.extend(plan.batch_duplicate_event_ids.iter().copied());
        duplicate_event_ids.extend(
            plan.events
                .iter()
                .map(|event| event.event_id)
                .filter(|event_id| !inserted_set.contains(event_id)),
        );
        Self {
            inserted_ids: Some(inserted_ids),
            attempted_event_ids: plan.attempted_event_ids(),
            duplicate_event_ids,
            tombstoned_event_ids: plan.tombstoned_event_ids.clone(),
            tombstoned_events_rejected: plan.tombstoned_event_ids.len(),
        }
    }
}

/// Per-batch disposition after duplicate-cache and tombstone admission filters.
#[derive(Debug, Clone)]
pub struct AdmissionBatchPlan {
    pub events: Vec<AdmittedEvent>,
    pub cached_duplicate_event_ids: Vec<Uuid>,
    pub batch_duplicate_event_ids: Vec<Uuid>,
    pub tombstoned_event_ids: Vec<Uuid>,
    cacheable_event_ids: Vec<Uuid>,
}

impl AdmissionBatchPlan {
    #[must_use]
    pub fn attempted_event_ids(&self) -> Vec<Uuid> {
        self.events.iter().map(|event| event.event_id).collect()
    }

    #[must_use]
    pub fn success_duplicate_event_ids(&self, inserted_ids: &[Uuid]) -> Vec<Uuid> {
        let inserted_set: HashSet<_> = inserted_ids.iter().copied().collect();
        let mut duplicate_event_ids = self.cached_duplicate_event_ids.clone();
        duplicate_event_ids.extend(self.batch_duplicate_event_ids.iter().copied());
        duplicate_event_ids.extend(
            self.events
                .iter()
                .map(|event| event.event_id)
                .filter(|event_id| !inserted_set.contains(event_id)),
        );
        duplicate_event_ids
    }

    #[must_use]
    pub fn cacheable_event_ids(&self) -> &[Uuid] {
        &self.cacheable_event_ids
    }
}

struct CacheFilterResult<'a> {
    events: Vec<&'a AdmittedEvent>,
    cached_duplicate_event_ids: Vec<Uuid>,
    batch_duplicate_event_ids: Vec<Uuid>,
}

struct TombstoneFilterResult<'a> {
    events: Vec<&'a AdmittedEvent>,
    tombstoned_event_ids: Vec<Uuid>,
}

#[derive(Debug, Clone)]
struct RecentIdCache {
    capacity: usize,
    order: VecDeque<Uuid>,
    set: HashSet<Uuid>,
}

impl RecentIdCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            set: HashSet::with_capacity(capacity),
        }
    }

    fn contains(&self, id: &Uuid) -> bool {
        if self.capacity == 0 {
            return false;
        }
        self.set.contains(id)
    }

    fn insert(&mut self, id: Uuid) {
        if self.capacity == 0 {
            return;
        }
        if self.set.insert(id) {
            self.order.push_back(id);
            while self.order.len() > self.capacity {
                if let Some(evicted) = self.order.pop_front() {
                    self.set.remove(&evicted);
                }
            }
        }
    }
}

/// Validates candidate events and persists admitted batches through
/// `EventRepository::insert_stream_batch`.
pub struct AdmissionService {
    pool: DbPool,
    validator: Arc<RwLock<IngestEventValidator>>,
    recent_id_cache: Mutex<RecentIdCache>,
    fail_once: Option<Arc<AtomicBool>>,
    db_failures_remaining: Option<Arc<AtomicUsize>>,
    future_ts_skew: time::Duration,
    ts_orig_lower_bound: Timestamp,
}

impl AdmissionService {
    pub fn new(pool: DbPool, validator: Arc<RwLock<IngestEventValidator>>) -> Self {
        Self {
            pool,
            validator,
            recent_id_cache: Mutex::new(RecentIdCache::new(RECENT_ID_CACHE_SIZE)),
            fail_once: None,
            db_failures_remaining: None,
            future_ts_skew: time::Duration::hours(1),
            ts_orig_lower_bound: Timestamp::from_const(time::macros::datetime!(
                2000-01-01 00:00:00 UTC
            )),
        }
    }

    #[must_use]
    pub fn with_future_ts_skew(mut self, skew: time::Duration) -> Self {
        self.future_ts_skew = skew;
        self
    }

    #[must_use]
    pub fn with_ts_orig_lower_bound(mut self, lower_bound: Timestamp) -> Self {
        self.ts_orig_lower_bound = lower_bound;
        self
    }

    #[must_use]
    pub fn with_test_fail_once(mut self, fail_once: Option<Arc<AtomicBool>>) -> Self {
        self.fail_once = fail_once;
        self
    }

    #[must_use]
    pub fn with_test_db_failures(mut self, failures_remaining: Option<Arc<AtomicUsize>>) -> Self {
        self.db_failures_remaining = failures_remaining;
        self
    }

    pub fn set_future_ts_skew(&mut self, skew: time::Duration) {
        self.future_ts_skew = skew;
    }

    pub fn set_ts_orig_lower_bound(&mut self, lower_bound: Timestamp) {
        self.ts_orig_lower_bound = lower_bound;
    }

    /// Admit a candidate event already decoded into the canonical event model.
    pub async fn admit_event(&self, event: Event<JsonValue>) -> IngestdResult<AdmissionDecision> {
        self.admit_event_with_metadata(event, None).await
    }

    /// Admit a staged-parser candidate with parser/source metadata.
    pub async fn admit_candidate(
        &self,
        candidate: CandidateEvent,
    ) -> IngestdResult<AdmissionDecision> {
        let CandidateEvent {
            mut event,
            metadata,
        } = candidate;

        if let Some(expected_material_id) = metadata.source_material_id {
            match &event.provenance {
                Provenance::Material { id, .. } if *id.as_uuid() == expected_material_id => {}
                Provenance::Material { id, .. } => {
                    return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                        AdmissionRejectionKind::CandidateMetadata,
                        format!(
                            "candidate source_material_id {expected_material_id} does not match event provenance {}",
                            id.as_uuid()
                        ),
                    )));
                }
                Provenance::Synthesis { .. } => {
                    return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                        AdmissionRejectionKind::CandidateMetadata,
                        "candidate source_material_id cannot be attached to synthesis provenance",
                    )));
                }
            }
        }

        if let Some(parser_semantics_version) = metadata.parser_semantics_version.as_deref() {
            match event.semantics_version.as_deref() {
                Some(existing) if existing != parser_semantics_version => {
                    return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                        AdmissionRejectionKind::CandidateMetadata,
                        format!(
                            "candidate parser_semantics_version {parser_semantics_version} does not match event semantics_version {existing}"
                        ),
                    )));
                }
                None => event.semantics_version = Some(parser_semantics_version.to_string()),
                Some(_) => {}
            }
        }

        if let Some(operation_id) = metadata.operation_id {
            match event.created_by_operation_id {
                Some(existing) if existing != operation_id => {
                    return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                        AdmissionRejectionKind::CandidateMetadata,
                        format!(
                            "candidate operation_id {operation_id} does not match event created_by_operation_id {existing}"
                        ),
                    )));
                }
                None => event.created_by_operation_id = Some(operation_id),
                Some(_) => {}
            }
        }

        self.admit_event_with_metadata(event, Some(metadata)).await
    }

    async fn admit_event_with_metadata(
        &self,
        event: Event<JsonValue>,
        metadata: Option<CandidateEventMetadata>,
    ) -> IngestdResult<AdmissionDecision> {
        if event.ts_orig.is_none() {
            warn!(event_id = ?event.id, "Event validation failed: missing ts_orig");
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::MissingTimestamp,
                "Validation failed: missing ts_orig",
            )));
        }

        if let Some(ts_orig) = event.ts_orig {
            let now = Timestamp::now();
            if ts_orig < self.ts_orig_lower_bound {
                error!(
                    target: "sinex_metrics",
                    metric = "ingestd.admission_rejections_total",
                    kind = "past_timestamp",
                    event_id = ?event.id,
                    source = %event.source,
                    event_type = %event.event_type,
                    ts_orig = %ts_orig,
                    lower_bound = %self.ts_orig_lower_bound,
                    "Event ts_orig predates lower bound"
                );
                return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::PastTimestamp,
                    format!(
                        "ts_orig {ts_orig} predates lower bound {} (implausibly old)",
                        self.ts_orig_lower_bound
                    ),
                )));
            }
            if ts_orig > now + self.future_ts_skew {
                let latest_expected = now + self.future_ts_skew;
                error!(
                    target: "sinex_metrics",
                    metric = "ingestd.admission_rejections_total",
                    kind = "future_timestamp",
                    event_id = ?event.id,
                    source = %event.source,
                    event_type = %event.event_type,
                    ts_orig = %ts_orig,
                    latest_expected = %latest_expected,
                    skew_seconds = (ts_orig - now).whole_seconds(),
                    "Event ts_orig is implausibly far in the future"
                );
                return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::FutureTimestamp,
                    format!(
                        "ts_orig {ts_orig} exceeds latest expected {latest_expected} by {} seconds (implausibly future)",
                        (ts_orig - now).whole_seconds()
                    ),
                )));
            }
        }

        if let Some(anchor_byte) = event.get_anchor_byte()
            && anchor_byte < 0
        {
            error!(
                target: "sinex_metrics",
                metric = "ingestd.admission_rejections_total",
                kind = "negative_anchor",
                event_id = ?event.id,
                source = %event.source,
                event_type = %event.event_type,
                anchor_byte,
                "Event has negative anchor_byte"
            );
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::NegativeAnchor,
                format!("Invalid anchor_byte: {anchor_byte} (must be >= 0)"),
            )));
        }

        let validated_schema_id = match self.validate_event(&event).await {
            Ok(schema_id) => schema_id,
            Err(error) => {
                warn!(event_id = ?event.id, "Event validation failed: {}", error);
                return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::SchemaValidation,
                    format!("Validation failed: {error}"),
                )));
            }
        };

        let mut event = event;
        if let Some(schema_id) = validated_schema_id {
            event.payload_schema_id = Some(schema_id);
        }

        let event_id = if let Some(id) = event.id {
            *id.as_uuid()
        } else {
            error!(
                target: "sinex_metrics",
                metric = "ingestd.admission_rejections_total",
                kind = "missing_event_id",
                "Event missing required ID"
            );
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::MissingEventId,
                "Missing event ID",
            )));
        };
        if !is_uuid_v7(&event_id) {
            error!(
                target: "sinex_metrics",
                metric = "ingestd.admission_rejections_total",
                kind = "invalid_event_id",
                event_id = %event_id,
                source = %event.source,
                event_type = %event.event_type,
                uuid_version = event_id.get_version_num(),
                uuid_variant = ?event_id.get_variant(),
                "Event ID is not UUIDv7 - violates hypertable partition contract"
            );
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::InvalidEventId,
                format!(
                    "Invalid event ID: {event_id} is UUID version {} with variant {:?}, expected RFC4122 UUIDv7",
                    event_id.get_version_num(),
                    event_id.get_variant()
                ),
            )));
        }

        Ok(AdmissionDecision::Admitted(AdmittedEvent {
            event,
            event_id,
            metadata,
        }))
    }

    /// Admit a canonical event encoded as bytes from a durable transport.
    pub async fn admit_bytes(&self, payload: &[u8]) -> IngestdResult<AdmissionDecision> {
        if payload.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::PayloadTooLarge,
                format!(
                    "Event payload too large: {} bytes (limit: {} bytes)",
                    payload.len(),
                    MAX_EVENT_PAYLOAD_BYTES
                ),
            )));
        }

        let payload_str = match std::str::from_utf8(payload) {
            Ok(value) => value,
            Err(error) => {
                return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::InvalidUtf8,
                    format!("Event payload is not valid UTF-8: {error}"),
                )));
            }
        };

        if let Err(error) = serde_json::from_slice::<serde_json::Value>(payload) {
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::EventDeserialization,
                format!("Parse error: {error}"),
            )));
        }

        if let Err(error) = sinex_primitives::validation::validate_json(payload_str) {
            return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::StructuralJson,
                format!("Event payload failed structural validation: {error}"),
            )));
        }

        let event: Event<JsonValue> = match serde_json::from_slice(payload) {
            Ok(event) => event,
            Err(error) => {
                return Ok(AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::EventDeserialization,
                    format!("Invalid timestamp or field format: {error}"),
                )));
            }
        };

        self.admit_event(event).await
    }

    /// Admit bytes that may contain an `AdmittedEventIntent` envelope.
    ///
    /// First attempts to deserialize as an envelope. If successful, validates
    /// the envelope (version, required fields) and admits each event inside.
    /// If the payload is not an envelope (missing `envelope_version` field),
    /// falls back to the legacy single-event deserialization path.
    pub async fn admit_intent_bytes(
        &self,
        payload: &[u8],
    ) -> IngestdResult<Vec<AdmissionDecision>> {
        if payload.len() > MAX_EVENT_PAYLOAD_BYTES {
            return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::PayloadTooLarge,
                format!(
                    "Event payload too large: {} bytes (limit: {} bytes)",
                    payload.len(),
                    MAX_EVENT_PAYLOAD_BYTES
                ),
            ))]);
        }

        let payload_str = match std::str::from_utf8(payload) {
            Ok(value) => value,
            Err(error) => {
                return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::InvalidUtf8,
                    format!("Event payload is not valid UTF-8: {error}"),
                ))]);
            }
        };

        if let Err(error) = serde_json::from_slice::<serde_json::Value>(payload) {
            return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::EventDeserialization,
                format!("Parse error: {error}"),
            ))]);
        }

        if let Err(error) = sinex_primitives::validation::validate_json(payload_str) {
            return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                AdmissionRejectionKind::StructuralJson,
                format!("Event payload failed structural validation: {error}"),
            ))]);
        }

        // Try to deserialize as an AdmittedEventIntent envelope first.
        // Detection heuristic: presence of "envelope_version" field.
        let is_envelope = payload_str.contains("\"envelope_version\"");

        if is_envelope {
            let intent: AdmittedEventIntent = match serde_json::from_slice(payload) {
                Ok(intent) => intent,
                Err(error) => {
                    return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                        AdmissionRejectionKind::EnvelopeDeserialization,
                        format!("Failed to deserialize admission envelope: {error}"),
                    ))]);
                }
            };

            // Validate the envelope.
            if let Err(error) = intent.validate() {
                return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::EnvelopeValidation,
                    format!("Admission envelope validation failed: {error}"),
                ))]);
            }

            if !ACCEPTED_ENVELOPE_VERSIONS.contains(&intent.envelope_version.as_str()) {
                return Ok(vec![AdmissionDecision::Rejected(AdmissionRejection::new(
                    AdmissionRejectionKind::EnvelopeValidation,
                    format!(
                        "Envelope version {} is not accepted. Accepted versions: {:?}",
                        intent.envelope_version, ACCEPTED_ENVELOPE_VERSIONS
                    ),
                ))]);
            }

            // Admit each event in the envelope.
            let mut decisions = Vec::with_capacity(intent.events.len());
            for event in intent.events {
                decisions.push(self.admit_event(event).await?);
            }
            return Ok(decisions);
        }

        // Fall back to legacy single-event deserialization.
        let decision = self.admit_bytes(payload).await?;
        Ok(vec![decision])
    }

    pub async fn persist_batch(
        &self,
        batch: &[AdmittedEvent],
    ) -> IngestdResult<AdmissionPersistResult> {
        let refs: Vec<&AdmittedEvent> = batch.iter().collect();
        self.persist_batch_refs(&refs).await
    }

    pub async fn plan_persistence_batch(
        &self,
        batch: &[AdmittedEvent],
    ) -> IngestdResult<AdmissionBatchPlan> {
        let refs: Vec<&AdmittedEvent> = batch.iter().collect();
        self.plan_persistence_batch_refs(&refs).await
    }

    pub async fn plan_persistence_batch_refs(
        &self,
        batch: &[&AdmittedEvent],
    ) -> IngestdResult<AdmissionBatchPlan> {
        let tombstone_filter = self.filter_tombstoned_batch(batch).await?;
        let cache_filter = self.filter_cached_batch(&tombstone_filter.events).await;
        let cacheable_event_ids = tombstone_filter
            .events
            .iter()
            .map(|event| event.event_id)
            .collect();

        Ok(AdmissionBatchPlan {
            events: cache_filter.events.into_iter().cloned().collect(),
            cached_duplicate_event_ids: cache_filter.cached_duplicate_event_ids,
            batch_duplicate_event_ids: cache_filter.batch_duplicate_event_ids,
            tombstoned_event_ids: tombstone_filter.tombstoned_event_ids,
            cacheable_event_ids,
        })
    }

    /// Persist admitted events through `EventRepository::insert_stream_batch()`.
    ///
    /// The repository owns all routing decisions (`QueryBuilder` for small
    /// batches, COPY for large material-only batches, REPEATABLE READ for
    /// synthesis batches). The recent-ID cache acts as a prefilter only.
    pub async fn persist_batch_refs(
        &self,
        batch: &[&AdmittedEvent],
    ) -> IngestdResult<AdmissionPersistResult> {
        let plan = self.plan_persistence_batch_refs(batch).await?;
        self.persist_plan(&plan).await
    }

    pub async fn persist_plan(
        &self,
        plan: &AdmissionBatchPlan,
    ) -> IngestdResult<AdmissionPersistResult> {
        if plan.events.is_empty() {
            return Ok(AdmissionPersistResult::skipped_plan(plan));
        }

        if let Some(fail_flag) = &self.fail_once
            && fail_flag.swap(false, Ordering::SeqCst)
        {
            return Err(SinexError::database("forced transient failure"));
        }

        if let Some(remaining) = &self.db_failures_remaining
            && remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    current.checked_sub(1)
                })
                .is_ok()
        {
            return Err(SinexError::database("forced persistent failure"));
        }

        let to_persist: Vec<&AdmittedEvent> = plan.events.iter().collect();
        let rows = admitted_to_stream_rows(&to_persist)?;
        let insert_result = timeout(
            DB_WRITE_TIMEOUT,
            self.pool.events().insert_stream_batch(&rows),
        )
        .await
        .map_err(|_| {
            error!(
                target: "sinex_metrics",
                metric = "ingestd.batch_insert_timeouts_total",
                batch_size = to_persist.len(),
                timeout_seconds = DB_WRITE_TIMEOUT.as_secs(),
                "Timed out waiting for batch insert to complete"
            );
            SinexError::database(format!(
                "Persisting batch timed out after {DB_WRITE_TIMEOUT:?}"
            ))
        })?;

        let insert_result = match insert_result {
            Err(ref error) if is_payload_schema_fk_violation(error) => {
                let schema_stripped_count = rows
                    .iter()
                    .filter(|row| row.payload_schema_id.is_some())
                    .count();
                warn!(
                    batch_size = to_persist.len(),
                    schema_stripped_count,
                    "INSERT hit FK violation on payload_schema_id; retrying without schema IDs on affected rows"
                );
                let mut rows_without_schema = rows.clone();
                for row in &mut rows_without_schema {
                    if row.payload_schema_id.is_some() {
                        row.payload_schema_id = None;
                    }
                }
                timeout(
                    DB_WRITE_TIMEOUT,
                    self.pool.events().insert_stream_batch(&rows_without_schema),
                )
                .await
                .map_err(|_| {
                    SinexError::database(format!(
                        "Persisting batch (schema-id-stripped retry) timed out after {DB_WRITE_TIMEOUT:?}"
                    ))
                })?
            }
            other => other,
        };

        let result = insert_result.map_err(|error| {
            if is_source_material_fk_violation_for_stream_batch(&error, &rows) {
                warn!(
                    batch_size = to_persist.len(),
                    "INSERT hit FK violation (source_material not yet registered); will retry"
                );
            } else {
                error!(
                    target: "sinex_metrics",
                    metric = "ingestd.batch_persistence_failures_total",
                    error = %error,
                    "Failed to persist events batch"
                );
            }
            error
        })?;

        let inserted_ids = require_inserted_ids(result.inserted_ids, to_persist.len())?;
        self.remember_event_ids(plan.cacheable_event_ids()).await;
        Ok(AdmissionPersistResult::persisted_plan(plan, inserted_ids))
    }

    #[must_use]
    pub fn is_source_material_fk_violation_for_admitted_batch(
        error: &SinexError,
        batch: &[&AdmittedEvent],
    ) -> bool {
        has_explicit_source_material_fk_marker(error)
            || (is_foreign_key_violation(error) && batch_depends_only_on_source_material_fk(batch))
    }

    #[must_use]
    pub fn is_source_material_fk_violation_for_stream_batch(
        error: &SinexError,
        batch: &[StreamBatchRow],
    ) -> bool {
        is_source_material_fk_violation_for_stream_batch(error, batch)
    }

    #[must_use]
    pub fn is_isolatable_persistence_failure(error: &SinexError) -> bool {
        is_isolatable_batch_persistence_failure(error)
    }

    async fn validate_event(&self, event: &Event<JsonValue>) -> IngestdResult<Option<Uuid>> {
        let guard = self.validator.read().await;
        let validation =
            guard.validate_payload_for(&event.source, &event.event_type, &event.payload);
        let strict_mode = guard.is_strict_mode();
        resolve_validation_result(validation, strict_mode, &event.source, &event.event_type)
    }

    async fn filter_cached_batch<'a>(&self, batch: &[&'a AdmittedEvent]) -> CacheFilterResult<'a> {
        let cached_ids = {
            let cache = self.recent_id_cache.lock().await;
            cache.clone()
        };
        let mut seen = HashSet::new();
        let mut cached_duplicate_event_ids = Vec::new();
        let mut batch_duplicate_event_ids = Vec::new();
        let mut events = Vec::with_capacity(batch.len());
        for event in batch {
            if cached_ids.contains(&event.event_id) {
                cached_duplicate_event_ids.push(event.event_id);
            } else if !seen.insert(event.event_id) {
                batch_duplicate_event_ids.push(event.event_id);
            } else {
                events.push(*event);
            }
        }
        CacheFilterResult {
            events,
            cached_duplicate_event_ids,
            batch_duplicate_event_ids,
        }
    }

    async fn remember_event_ids(&self, event_ids: &[Uuid]) {
        let mut cache = self.recent_id_cache.lock().await;
        for event_id in event_ids {
            cache.insert(*event_id);
        }
    }

    async fn filter_tombstoned_batch<'a>(
        &self,
        batch: &[&'a AdmittedEvent],
    ) -> IngestdResult<TombstoneFilterResult<'a>> {
        if batch.is_empty() {
            return Ok(TombstoneFilterResult {
                events: Vec::new(),
                tombstoned_event_ids: Vec::new(),
            });
        }

        let ids: Vec<Id<Event>> = batch
            .iter()
            .map(|event| Id::from_uuid(event.event_id))
            .collect();
        let tombstoned_ids = self
            .pool
            .events()
            .filter_tombstoned(&ids)
            .await
            .map_err(|error| {
                error!(
                    target: "sinex_metrics",
                    metric = "ingestd.tombstone_query_failures_total",
                    error = %error,
                    "Failed to query event_tombstones during batch persistence"
                );
                SinexError::database("tombstone query failed")
                    .with_context("batch_size", batch.len().to_string())
            })?;

        if tombstoned_ids.is_empty() {
            return Ok(TombstoneFilterResult {
                events: batch.to_vec(),
                tombstoned_event_ids: Vec::new(),
            });
        }

        let tombstoned_event_ids: Vec<Uuid> =
            tombstoned_ids.iter().map(|id| *id.as_uuid()).collect();
        warn!(
            count = tombstoned_event_ids.len(),
            "Rejected {} tombstoned event(s) during ingestion",
            tombstoned_event_ids.len()
        );

        Ok(TombstoneFilterResult {
            events: batch
                .iter()
                .filter(|event| !tombstoned_ids.contains(&Id::from_uuid(event.event_id)))
                .copied()
                .collect(),
            tombstoned_event_ids,
        })
    }
}

fn admitted_to_stream_rows(batch: &[&AdmittedEvent]) -> IngestdResult<Vec<StreamBatchRow>> {
    batch
        .iter()
        .map(|admitted| {
            let event = &admitted.event;
            let (
                source_event_ids,
                source_material_id,
                offset_start,
                offset_end,
                offset_kind,
                anchor_byte,
            ) = sinex_db::repositories::events::conversions::extract_provenance(event)?;

            Ok(StreamBatchRow {
                id: admitted.event_id,
                source: event.source.clone(),
                event_type: event.event_type.clone(),
                ts_orig: event.ts_orig.ok_or_else(|| {
                    SinexError::validation("validated event missing ts_orig")
                        .with_context("event_id", admitted.event_id.to_string())
                        .with_context("source", event.source.as_str().to_string())
                        .with_context("event_type", event.event_type.as_str().to_string())
                })?,
                host: event.host.clone(),
                payload: event.payload.clone(),
                source_material_id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
                source_event_ids,
                payload_schema_id: event.payload_schema_id,
                source_run_id: event.source_run_id,
                associated_blob_ids: event.associated_blob_ids.clone(),
                anchor_payload_hash: event.anchor_payload_hash.clone(),
                temporal_policy: event.temporal_policy.map(|policy| policy.to_string()),
                semantics_version: event.semantics_version.clone(),
                scope_key: event.scope_key.clone(),
                equivalence_key: event.equivalence_key.clone(),
                created_by_operation_id: event.created_by_operation_id,
                node_model: event.node_model.map(|model| model.to_string()),
            })
        })
        .collect()
}

fn resolve_validation_result(
    validation: ValidationResult,
    strict_mode: bool,
    source: &sinex_primitives::domain::EventSource,
    event_type: &sinex_primitives::domain::EventType,
) -> IngestdResult<Option<Uuid>> {
    match validation {
        ValidationResult::Valid { schema_id } => Ok(Some(schema_id)),
        ValidationResult::Skipped => Ok(None),
        ValidationResult::NoSchema => {
            if strict_mode {
                Err(SinexError::validation(format!(
                    "Strict validation enabled: event has no registered schema (source={source}, event_type={event_type})"
                ))
                .with_operation("admission.validate_event")
                .with_context("strict_mode", "enabled"))
            } else {
                Ok(None)
            }
        }
        ValidationResult::SchemaNotFound { schema_id } => {
            warn!(
                schema_id = %schema_id,
                source = %source,
                event_type = %event_type,
                "Schema referenced by validator lookup is missing from cache; accepting event without payload schema id"
            );
            Ok(None)
        }
        ValidationResult::Invalid { errors } => Err(SinexError::validation(format!(
            "Schema validation failed: {}",
            errors.join(", ")
        ))
        .with_operation("admission.validate_event")),
    }
}

fn require_inserted_ids(
    inserted_ids: Option<Vec<Uuid>>,
    attempted_rows: usize,
) -> IngestdResult<Vec<Uuid>> {
    inserted_ids.ok_or_else(|| {
        SinexError::invalid_state(format!(
            "Event repository omitted inserted_ids for a non-empty stream batch of {attempted_rows} row(s)"
        ))
    })
}

fn is_uuid_v7(value: &Uuid) -> bool {
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

fn is_foreign_key_violation(error: &SinexError) -> bool {
    error
        .context_map()
        .get("sqlstate")
        .is_some_and(|value| value == "23503")
}

fn is_source_material_fk_constraint_name(value: &str) -> bool {
    value == EVENTS_SOURCE_MATERIAL_ID_FKEY
        || value
            .strip_suffix(EVENTS_SOURCE_MATERIAL_ID_FKEY)
            .is_some_and(|prefix| prefix.ends_with('_'))
}

fn has_explicit_source_material_fk_marker(error: &SinexError) -> bool {
    error
        .context_map()
        .get("error_class")
        .is_some_and(|value| value == ERROR_CLASS_SOURCE_MATERIAL_FK)
        || error
            .context_map()
            .get("constraint")
            .is_some_and(|value| is_source_material_fk_constraint_name(value))
}

fn batch_depends_only_on_source_material_fk(batch: &[&AdmittedEvent]) -> bool {
    batch.iter().all(|admitted| {
        matches!(admitted.event.provenance, Provenance::Material { .. })
            && admitted.event.payload_schema_id.is_none()
            && admitted.event.source_run_id.is_none()
    })
}

fn rows_depend_only_on_source_material_fk(batch: &[StreamBatchRow]) -> bool {
    batch.iter().all(|row| {
        row.source_material_id.is_some()
            && row
                .source_event_ids
                .as_ref()
                .is_none_or(std::vec::Vec::is_empty)
            && row.payload_schema_id.is_none()
            && row.source_run_id.is_none()
    })
}

fn is_source_material_fk_violation_for_stream_batch(
    error: &SinexError,
    batch: &[StreamBatchRow],
) -> bool {
    has_explicit_source_material_fk_marker(error)
        || (is_foreign_key_violation(error) && rows_depend_only_on_source_material_fk(batch))
}

fn is_payload_schema_fk_violation(error: &SinexError) -> bool {
    if !is_foreign_key_violation(error) {
        return false;
    }
    error
        .context_map()
        .get("constraint")
        .is_some_and(|constraint| {
            constraint == EVENTS_PAYLOAD_SCHEMA_ID_FKEY
                || constraint.contains(EVENTS_PAYLOAD_SCHEMA_ID_FKEY)
        })
}

fn is_isolatable_batch_persistence_failure(error: &SinexError) -> bool {
    if has_explicit_source_material_fk_marker(error)
        || sinex_db::query_helpers::is_retryable_db_error(error)
    {
        return false;
    }

    if is_foreign_key_violation(error) {
        return true;
    }

    error.context_map().get("sqlstate").is_some_and(|value| {
        value.starts_with(SQLSTATE_DATA_EXCEPTION_CLASS)
            || value.starts_with(SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS)
    })
}

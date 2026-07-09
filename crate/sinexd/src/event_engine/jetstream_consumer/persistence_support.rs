//! Persistence classification types and source-material failure helpers.

use async_nats::jetstream;
use sinex_primitives::error::SinexErrorKind;
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::{JsonValue, Uuid};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::event_engine::{EventEngineResult, SinexError};

/// SQLSTATE for foreign-key violation.
pub(super) const SQLSTATE_DATA_EXCEPTION_CLASS: &str = "22";
pub(super) const SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS: &str = "23";
/// SQLSTATE 54xxx = program_limit_exceeded (includes 54023 too_many_arguments).
/// Bisecting a batch that exceeds PostgreSQL's 65,535 bound-parameter ceiling
/// produces sub-batches small enough to succeed — treat as isolatable.
pub(super) const SQLSTATE_PROGRAM_LIMIT_EXCEEDED_CLASS: &str = "54";

/// Error-class marker for deferred source-material FK violations.
pub(super) const ERROR_CLASS_SOURCE_MATERIAL_FK: &str = "source_material_fk_violation";
pub(super) const EVENTS_SOURCE_MATERIAL_ID_FKEY: &str = "events_source_material_id_fkey";
const NON_LIVE_DERIVED_PARENT_ERROR_FRAGMENT: &str = "non-live source_event_ids";

pub(super) fn is_source_material_fk_constraint_name(value: &str) -> bool {
    value == EVENTS_SOURCE_MATERIAL_ID_FKEY
        || value
            .strip_suffix(EVENTS_SOURCE_MATERIAL_ID_FKEY)
            .is_some_and(|prefix| prefix.ends_with('_'))
}

/// Hard guard for producer-supplied event IDs.
///
/// All producer-minted event IDs must be RFC4122 `UUIDv7`. `event_engine` rejects every ID
/// that does not meet this requirement before it reaches the hypertable partition key.
#[cfg(test)]
pub(super) fn is_uuid_v7(value: &Uuid) -> bool {
    value.get_version_num() == 7 && value.get_variant() == uuid::Variant::RFC4122
}

pub(super) fn is_foreign_key_violation(err: &SinexError) -> bool {
    // Per #751 F32: classify FK violations by SQLSTATE (23503 foreign_key_violation)
    // instead of inspecting rendered error text. SQLSTATE is always set when errors
    // flow through sinex_db::db_error(), which extracts pg errcode from the sqlx error.
    err.context_map()
        .get("sqlstate")
        .is_some_and(|value| value == "23503")
}

pub(super) fn has_explicit_source_material_fk_marker(err: &SinexError) -> bool {
    err.context_map()
        .get("error_class")
        .is_some_and(|value| value == ERROR_CLASS_SOURCE_MATERIAL_FK)
        || err
            .context_map()
            .get("constraint")
            .is_some_and(|value| is_source_material_fk_constraint_name(value))
}

pub(super) fn batch_depends_only_on_source_material_fk(batch: &[&PreparedEvent]) -> bool {
    batch.iter().all(|prepared| {
        matches!(prepared.event.provenance, Provenance::Material { .. })
            && prepared.event.payload_schema_id.is_none()
            && prepared.event.module_run_id.is_none()
    })
}

pub(super) fn is_source_material_fk_violation_for_prepared_batch(
    err: &SinexError,
    batch: &[&PreparedEvent],
) -> bool {
    has_explicit_source_material_fk_marker(err)
        || (is_foreign_key_violation(err) && batch_depends_only_on_source_material_fk(batch))
}

pub(super) fn is_non_live_derived_parent_validation(err: &SinexError) -> bool {
    err.kind() == SinexErrorKind::Validation
        && err
            .to_string()
            .contains(NON_LIVE_DERIVED_PARENT_ERROR_FRAGMENT)
}

pub(super) fn is_isolatable_batch_persistence_failure(err: &SinexError) -> bool {
    if has_explicit_source_material_fk_marker(err)
        || sinex_db::query_helpers::is_retryable_db_error(err)
    {
        return false;
    }

    if is_non_live_derived_parent_validation(err) {
        return true;
    }

    if is_foreign_key_violation(err) {
        return true;
    }

    err.context_map().get("sqlstate").is_some_and(|value| {
        value.starts_with(SQLSTATE_DATA_EXCEPTION_CLASS)
            || value.starts_with(SQLSTATE_INTEGRITY_CONSTRAINT_VIOLATION_CLASS)
            || value.starts_with(SQLSTATE_PROGRAM_LIMIT_EXCEEDED_CLASS)
    })
}

#[derive(Debug)]
pub(super) struct PersistBatchResult {
    pub(super) inserted_ids: Option<Vec<Uuid>>,
    pub(super) duplicate_event_ids: Vec<Uuid>,
    pub(super) tombstoned_event_ids: Vec<Uuid>,
    /// The FINAL persisted+redacted event image for every event_id in the
    /// attempted batch (sinex-z8p). This is what actually got written to
    /// Postgres (or, for a cached duplicate, would have been written had it
    /// not already existed) — confirmation must publish exactly this, never
    /// the pre-redaction `PreparedEvent.event` the caller parsed off the wire.
    pub(super) redacted_events: HashMap<Uuid, Event<JsonValue>>,
}

#[derive(Debug)]
pub(super) struct PersistBatchFailure {
    pub(super) error: SinexError,
    pub(super) attempted_event_ids: Vec<Uuid>,
    pub(super) duplicate_event_ids: Vec<Uuid>,
    pub(super) tombstoned_event_ids: Vec<Uuid>,
}

pub(super) struct PreparedEvent {
    pub(super) event: Event<JsonValue>,
    pub(super) parsed_id: Uuid,
    pub(super) message: jetstream::Message,
    /// Shared with every other `PreparedEvent` derived from the same
    /// physical raw JetStream message (sinex-r6d.12). Settlement of the
    /// underlying message goes through this, never `message.ack()`/
    /// `ack_with()` directly — see [`RawEnvelopeSettlement`].
    pub(super) settlement: Arc<RawEnvelopeSettlement>,
}

/// Terminal outcome for one child (admission decision) derived from a
/// physical raw JetStream message, reported to that message's shared
/// [`RawEnvelopeSettlement`].
#[derive(Debug, Clone, Copy)]
pub(super) enum ChildOutcome {
    /// Durable and terminal: persisted+confirmed, durably DLQed, durably
    /// tombstoned, or durably suppressed. Safe to let the shared message be
    /// removed from the stream once every child reports `Safe`.
    Safe,
    /// This child needs the whole envelope redelivered (transient DB
    /// failure, source material not yet ready, DLQ publish failed after
    /// retries...). Optional delay mirrors `AckKind::Nak(Some(delay))`.
    Retry(Option<Duration>),
}

/// Coordinates ACK/NAK of one physical raw JetStream message across every
/// admission decision (child) derived from it.
///
/// sinex-r6d.12: `prepare_events` used to act on the shared raw message
/// directly per-child — a rejected child could ACK+DLQ the message while an
/// admitted sibling was still sitting unprocessed in memory; if that sibling
/// later needed a NAK, NAKing its message clone was a no-op because the
/// underlying JetStream delivery was already permanently acked via the
/// rejected sibling's path. This primitive makes that impossible by
/// construction: the message settles exactly once, only after EVERY child
/// has reported a terminal outcome, and NAKs (real redelivery, never silent
/// loss) if any child needed it — regardless of how many children there are
/// or what order they settle in. A singleton `EventIntent` degenerates to
/// "wait for the one child, then ack/nak", so every raw-ingress path uses
/// this uniformly rather than special-casing multi-child envelopes.
#[derive(Debug)]
pub(super) struct RawEnvelopeSettlement {
    message: jetstream::Message,
    remaining: AtomicUsize,
    needs_redelivery: AtomicBool,
    redelivery_delay: std::sync::Mutex<Option<Duration>>,
}

impl RawEnvelopeSettlement {
    /// `child_count` is the total number of admission decisions this
    /// message produced (admitted, transformed, suppressed, rejected,
    /// quarantined — every one of them must report exactly once). A
    /// `child_count` of 0 must never reach here; callers ack an empty-intent
    /// message directly instead of constructing a settlement for it.
    pub(super) fn new(message: jetstream::Message, child_count: usize) -> Arc<Self> {
        debug_assert!(child_count > 0, "RawEnvelopeSettlement requires at least one child");
        Arc::new(Self {
            message,
            remaining: AtomicUsize::new(child_count),
            needs_redelivery: AtomicBool::new(false),
            redelivery_delay: std::sync::Mutex::new(None),
        })
    }

    /// Report one child's terminal outcome. Once every child has reported,
    /// the shared message is ACKed (all `Safe`) or NAKed (any `Retry`) —
    /// exactly once, regardless of settlement order. A duplicate report past
    /// `child_count` is a logic error (debug-asserted) but degrades safely
    /// in release builds: it is simply ignored rather than double-settling.
    pub(super) async fn settle_child(&self, outcome: ChildOutcome) -> EventEngineResult<()> {
        if let ChildOutcome::Retry(delay) = outcome {
            self.needs_redelivery.store(true, Ordering::SeqCst);
            if let Some(delay) = delay
                && let Ok(mut guard) = self.redelivery_delay.lock()
            {
                guard.get_or_insert(delay);
            }
        }

        let prev = self.remaining.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            n.checked_sub(1)
        });
        let Ok(prev) = prev else {
            debug_assert!(false, "settle_child called more times than child_count");
            return Ok(());
        };
        if prev != 1 {
            return Ok(());
        }

        if self.needs_redelivery.load(Ordering::SeqCst) {
            let delay = self.redelivery_delay.lock().ok().and_then(|guard| *guard);
            self.message
                .ack_with(jetstream::AckKind::Nak(delay))
                .await
                .map_err(|e| {
                    SinexError::network("Failed to NAK raw envelope after child settlement")
                        .with_source(e)
                })
        } else {
            self.message.ack().await.map_err(|e| {
                SinexError::network("Failed to ACK raw envelope after child settlement")
                    .with_source(e)
            })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceMaterialSettlement {
    Deferred,
    RoutedToDlq,
}

pub(super) fn source_material_unavailable_error(
    prepared: &PreparedEvent,
    material_id: Option<Uuid>,
    persistence_error: Option<&SinexError>,
    threshold: i64,
) -> String {
    let material = material_id.map_or_else(|| "unknown".to_string(), |id| id.to_string());
    let base = format!(
        "Source material {material} was not registered after {threshold} deliveries for event {} (source={}, event_type={})",
        prepared.parsed_id, prepared.event.source, prepared.event.event_type
    );

    if let Some(error) = persistence_error {
        format!("{base}; persistence error: {error}")
    } else {
        base
    }
}

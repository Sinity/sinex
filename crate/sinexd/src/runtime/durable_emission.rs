//! Durable emission receipt types (sinex-r6d.11 — types-only slice).
//!
//! Transcribes the ratified API sketch from the 2026-07-07 GPT-Pro red-team
//! report (`.agent/scratch/new-gpt-pro/01-batch-a-receipt-red-team.report.md`,
//! section B) into real Rust types. Pure data model only — no backend
//! implementation, no wiring into any real call site. This is deliberately
//! the SAME scoping discipline as [`sinex_primitives::commit_frontier`]
//! (sinex-4as3): land the shape first, land it correctly, and let the
//! reorder work (sinex-r6d.4, sinex-vxu, sinex-r6d.7, sinex-w4i) consume it
//! later as its own reviewable, individually-tested change.
//!
//! # Why this exists before the reorder
//!
//! `EventEmitter::emit` is an in-memory mpsc handoff, yet three progress
//! markers across the codebase currently treat it as a durable commit:
//! adapter-source cursor checkpoints, automaton `process_batch` checkpoints,
//! and invalidation acks. This bead's fix is ONE shared notion of "durable":
//! a [`DurableEmissionReceipt`] whose [`DurableEmissionReceipt::unlocks_progress`]
//! is true only after every item in the request reached a terminal,
//! crash-recoverable outcome — never after a bare mpsc send or an unacked
//! NATS publish alone.
//!
//! # Progress-unlocking vs non-progress states
//!
//! [`EmissionReceiptState`] deliberately contains BOTH progress-unlocking
//! variants (a superset/refinement of
//! [`sinex_primitives::commit_frontier::TerminalOutcome`] — this module adds
//! the caller-visible detail like storage lane, dedup reason, and debt id
//! that the pure ordering primitive doesn't need to know about) and
//! non-progress variants (`RawAccepted`, `Deferred`, `FailedTransient`,
//! `Prepared`, `Submitted`). A cursor/checkpoint/ack MUST NEVER advance on a
//! non-progress state.
//!
//! # Not done here (see sinex-r6d.11's own AC for the rest)
//!
//! - No backend implementation (the ratified design is (A) settlement-channel
//!   receipt over the existing NATS→event-engine admission/persist/confirm
//!   pipeline — this needs the pipeline's admission/redaction/equivalence/FK
//!   gates as the single chokepoint, which is real wiring work).
//! - No `request_id` producer-crash-recovery ledger.
//! - No caller integration (source cursors, automaton checkpoints,
//!   invalidation acks all still act exactly as before this module was
//!   added — this module has zero behavior change on its own).
//! - `allow_spool_backend` / `SpoolAcceptedLossless` is included in the type
//!   shape per the ratified design, but any real backend must gate it behind
//!   sinex-r6d.5 (already landed — the recovery spool is lossless) before
//!   ever constructing that variant.

use sinex_db::repositories::EventStorageLane;
use sinex_primitives::events::Event;
use sinex_primitives::{JsonValue, Uuid};

/// A caller's request to durably emit a batch of events as a single
/// progress atom. `events` share one [`ProgressAtom`] — see
/// [`DurableEmissionReceipt::unlocks_progress`] for why the receipt is
/// all-or-nothing with respect to progress, not per-event.
#[derive(Debug, Clone)]
pub struct DurableEmissionRequest {
    pub origin: EmissionOrigin,
    pub required_level: ReceiptLevel,
    pub progress_atom: ProgressAtom,
    pub events: Vec<Event<JsonValue>>,
    /// Whether the caller permits `SpoolAcceptedLossless` as a valid
    /// terminal outcome for this request. Backends must treat this as
    /// `false` unconditionally until the backend itself is proven to route
    /// only through a lossless spool (sinex-r6d.5) — this flag alone does
    /// not grant that proof.
    pub allow_spool_backend: bool,
}

/// Which subsystem originated a [`DurableEmissionRequest`] — carried for
/// observability/metrics labeling, not for dispatch (dispatch is by
/// backend, see [`ReceiptBackend`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionOrigin {
    SourceAdapter,
    AutomatonBridge,
    Invalidation,
    WindowedTimer,
    Reflection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptLevel {
    /// The event-engine admission pipeline has durably settled every item
    /// (persisted, suppressed-with-evidence, tombstoned, or durable debt).
    AdmissionSettled,
    /// Weaker than `AdmissionSettled` — durably recoverable at the transport
    /// layer (e.g. a lossless spool write) but not yet admission-settled.
    /// Must never be used to advance a cursor/checkpoint/ack without an
    /// explicit, reviewed proof that the specific caller's recovery path
    /// tolerates it.
    TransportRecoverable,
}

/// The caller-defined unit of progress a [`DurableEmissionRequest`] covers.
/// Each variant corresponds to one of sinex-r6d.11's four caller families
/// (plus reflection, which never unlocks progress at all).
#[derive(Debug, Clone)]
pub enum ProgressAtom {
    SourceRecord {
        source_id: String,
        material_id: Uuid,
        anchor_byte: i64,
        cursor_after: JsonValue,
    },
    AutomatonInputBatch {
        automaton: String,
        input_event_ids: Vec<Uuid>,
    },
    InvalidationScope {
        automaton: String,
        operation_uuid: Uuid,
        scope_keys: Vec<String>,
    },
    WindowFlush {
        automaton: String,
        flush_id: Uuid,
    },
    /// Self-observation/telemetry. Always paired with `EmissionOrigin::Reflection`
    /// and a best-effort backend — see this module's top-level doc.
    ReflectionTelemetry,
}

/// Which backend actually settled a [`DurableEmissionReceipt`]. The Direct
/// and NATS backends must expose the same receipt state machine (the
/// ratified design's point: `RuntimeResult<()>` erases suppression/tombstone
/// outcomes today, which is part of what this bead fixes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptBackend {
    Direct,
    Nats,
}

/// Why an event was suppressed rather than persisted — carried on
/// [`EmissionReceiptState::Suppressed`] so a receipt can distinguish
/// "this is fine, occurrence already exists" from other suppression
/// classes without the caller re-deriving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuppressionReason {
    EquivalenceKeyDuplicate,
    CachedDuplicate,
    BatchDuplicate,
    Tombstoned,
}

/// The receipt for one durable-emission request: `request_id` is the
/// producer-crash-recovery handle (a restarted producer reconciles
/// in-flight receipts by this id instead of blindly re-emitting — the
/// ledger itself is not implemented by this module, see the top-level doc),
/// `items` carries one [`EmissionItemReceipt`] per event in the request.
#[derive(Debug, Clone)]
pub struct DurableEmissionReceipt {
    pub request_id: Uuid,
    pub atom: ProgressAtom,
    pub items: Vec<EmissionItemReceipt>,
    pub backend: ReceiptBackend,
}

impl DurableEmissionReceipt {
    /// True only when EVERY item reached a progress-unlocking terminal
    /// state ([`EmissionReceiptState::is_progress_unlocking`]). A caller
    /// (source cursor, automaton checkpoint, invalidation ack, window flush
    /// state save) must gate its own progress advance on this returning
    /// `true` for the receipt covering that exact atom — never on a partial
    /// or non-progress result, and never on having merely called the emit
    /// function.
    #[must_use]
    pub fn unlocks_progress(&self) -> bool {
        !self.items.is_empty() && self.items.iter().all(|item| item.state.is_progress_unlocking())
    }

    /// Diagnostic accessor: the first item, if any, that did NOT reach a
    /// progress-unlocking state — what a caller should log/attach as
    /// context when `unlocks_progress()` is false.
    #[must_use]
    pub fn first_non_progress(&self) -> Option<&EmissionItemReceipt> {
        self.items.iter().find(|item| !item.state.is_progress_unlocking())
    }
}

/// The terminal (or still-pending) state of one event within a
/// [`DurableEmissionReceipt`].
#[derive(Debug, Clone)]
pub struct EmissionItemReceipt {
    pub event_id: Option<Uuid>,
    pub state: EmissionReceiptState,
}

/// Every state an emitted event can be in when a receipt is produced.
/// See this module's top-level doc for the progress-unlocking / non-progress
/// split — [`EmissionReceiptState::is_progress_unlocking`] is the single
/// source of truth for which variants belong to which side; do not
/// re-derive that classification at call sites.
#[derive(Debug, Clone)]
pub enum EmissionReceiptState {
    // ---- Progress-unlocking (crash-recoverable, terminal) ----
    /// Persisted in the target DB lane; if also published to confirmed-events,
    /// that publish was durably accepted before the raw message was acked
    /// (matches the existing sinex-z8p confirmed-publish-gates-raw-ack order).
    PersistedConfirmed {
        lane: EventStorageLane,
        inserted: bool,
        confirmed_sequence: Option<u64>,
    },
    /// Intentionally received and suppressed, with durable/admission
    /// evidence backing the decision — not a bare unaccounted skip.
    Suppressed {
        reason: SuppressionReason,
        existing_event_id: Option<Uuid>,
    },
    /// Rejected, quarantined, malformed, or processing-failure state,
    /// durably persisted to an operator-visible debt/DLQ/quarantine record
    /// before progress advances.
    DurableDebt { debt_id: Uuid, reason: String },
    /// Durably accepted into a LOSSLESS local spool (sinex-r6d.5). A capped
    /// or discarding spool must never produce this variant — see
    /// `allow_spool_backend` on [`DurableEmissionRequest`].
    SpoolAcceptedLossless {
        segment: String,
        offset: u64,
        parent_dir_synced: bool,
    },
    /// The caller's computation legitimately produced no output for this
    /// item and there is nothing further to settle.
    NoOutputSettled,

    // ---- Non-progress (never sufficient for cursor/checkpoint/ack movement) ----
    /// Event IDs/defaults assigned, schema validated, not yet submitted.
    Prepared,
    /// Queued to a backend with no durable recovery guarantee yet.
    Submitted,
    /// JetStream accepted the raw intent; DB/admission terminal state is
    /// still unknown. This is exactly the state r6d.4/vxu/r6d.7 currently
    /// (incorrectly) treat as sufficient to advance a cursor/checkpoint/ack.
    RawAccepted { stream: String, sequence: u64 },
    /// Not yet resolvable — e.g. source material not registered yet. May be
    /// recoverable in raw JetStream, but progress must wait for terminal
    /// settlement or a durable pending-debt policy.
    Deferred { reason: String },
    /// No cursor/checkpoint/ack movement is permitted on this outcome.
    FailedTransient { error: String },
}

impl EmissionReceiptState {
    /// The single source of truth for whether this state permits a
    /// cursor/checkpoint/ack to advance over the item it describes.
    #[must_use]
    pub fn is_progress_unlocking(&self) -> bool {
        matches!(
            self,
            Self::PersistedConfirmed { .. }
                | Self::Suppressed { .. }
                | Self::DurableDebt { .. }
                | Self::SpoolAcceptedLossless { .. }
                | Self::NoOutputSettled
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(state: EmissionReceiptState) -> EmissionItemReceipt {
        EmissionItemReceipt {
            event_id: Some(Uuid::now_v7()),
            state,
        }
    }

    fn receipt(items: Vec<EmissionItemReceipt>) -> DurableEmissionReceipt {
        DurableEmissionReceipt {
            request_id: Uuid::now_v7(),
            atom: ProgressAtom::ReflectionTelemetry,
            items,
            backend: ReceiptBackend::Direct,
        }
    }

    #[test]
    fn empty_receipt_never_unlocks_progress() {
        assert!(!receipt(vec![]).unlocks_progress());
    }

    #[test]
    fn all_progress_unlocking_items_unlock_progress() {
        let r = receipt(vec![
            item(EmissionReceiptState::PersistedConfirmed {
                lane: EventStorageLane::Activity,
                inserted: true,
                confirmed_sequence: Some(1),
            }),
            item(EmissionReceiptState::NoOutputSettled),
        ]);
        assert!(r.unlocks_progress());
        assert!(r.first_non_progress().is_none());
    }

    #[test]
    fn a_single_non_progress_item_blocks_the_whole_receipt() {
        let r = receipt(vec![
            item(EmissionReceiptState::PersistedConfirmed {
                lane: EventStorageLane::Activity,
                inserted: true,
                confirmed_sequence: Some(1),
            }),
            item(EmissionReceiptState::RawAccepted {
                stream: "SINEX_RAW_EVENTS".to_string(),
                sequence: 42,
            }),
        ]);
        assert!(
            !r.unlocks_progress(),
            "a partial receipt must never unlock progress, even with one terminal item"
        );
        assert!(matches!(
            r.first_non_progress().expect("one non-progress item").state,
            EmissionReceiptState::RawAccepted { .. }
        ));
    }

    #[test]
    fn every_progress_unlocking_variant_is_classified_correctly() {
        let progress_unlocking = [
            EmissionReceiptState::PersistedConfirmed {
                lane: EventStorageLane::Reflection,
                inserted: false,
                confirmed_sequence: None,
            },
            EmissionReceiptState::Suppressed {
                reason: SuppressionReason::EquivalenceKeyDuplicate,
                existing_event_id: None,
            },
            EmissionReceiptState::DurableDebt {
                debt_id: Uuid::now_v7(),
                reason: "test".to_string(),
            },
            EmissionReceiptState::SpoolAcceptedLossless {
                segment: "seg-0".to_string(),
                offset: 0,
                parent_dir_synced: true,
            },
            EmissionReceiptState::NoOutputSettled,
        ];
        for state in progress_unlocking {
            assert!(state.is_progress_unlocking(), "{state:?} must be progress-unlocking");
        }

        let non_progress = [
            EmissionReceiptState::Prepared,
            EmissionReceiptState::Submitted,
            EmissionReceiptState::RawAccepted {
                stream: "s".to_string(),
                sequence: 0,
            },
            EmissionReceiptState::Deferred {
                reason: "not ready".to_string(),
            },
            EmissionReceiptState::FailedTransient {
                error: "boom".to_string(),
            },
        ];
        for state in non_progress {
            assert!(!state.is_progress_unlocking(), "{state:?} must NOT be progress-unlocking");
        }
    }
}

//! Durable emission receipt types (sinex-r6d.11).
//!
//! Transcribes the ratified API sketch from the 2026-07-07 GPT-Pro red-team
//! report (`.agent/scratch/new-gpt-pro/01-batch-a-receipt-red-team.report.md`,
//! section B) into real Rust types, plus (as of the second slice)
//! [`SettlementRegistry`] — a per-event-id waiter map that a future
//! `emit_batch_durable()` will use to await [`EmissionReceiptState`]
//! resolution. Pure primitives only — no backend implementation, no wiring
//! into any real call site. This is deliberately the SAME scoping discipline
//! as [`sinex_primitives::commit_frontier`] (sinex-4as3): land the shape
//! first, land it correctly, and let the reorder work (sinex-r6d.4,
//! sinex-vxu, sinex-r6d.7, sinex-w4i) consume it later as its own
//! reviewable, individually-tested change.
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
//! # `SettlementRegistry`: the future backend's notification primitive
//!
//! [`SettlementRegistry`] is the mechanism a future `emit_batch_durable()`
//! will use to turn "some other task, at some later point, learns this
//! event's terminal [`EmissionReceiptState`]" into an `await`able value. The
//! natural future hook point is the ~15 `settle_child(` call sites in
//! `event_engine::jetstream_consumer::persist`/`prepare` (see
//! `persistence_support::RawEnvelopeSettlement`) — each already knows the
//! exact settlement outcome for the event it just persisted/suppressed/
//! DLQ'd; wiring `registry.resolve(parsed_id, state)` in next to each of
//! those call sites, and threading a shared `SettlementRegistry` through
//! `JetStreamConsumer`'s construction (`service_container.rs`, `main.rs`,
//! every place a `JetStreamConsumer` gets built), is explicitly NOT part of
//! this slice — see "Not done here" below.
//!
//! Registration/resolution contract:
//!
//! - A caller `register()`s once per event **before** emitting, getting back
//!   a `oneshot::Receiver<EmissionReceiptState>`.
//! - The (future) settlement call site calls `resolve(event_id, state)`
//!   exactly once when that event reaches a terminal outcome. `resolve` is a
//!   cheap no-op (`false`) for the overwhelmingly common case of an event id
//!   nobody registered for — ordinary event traffic will eventually call
//!   `resolve()` for every persisted/suppressed/DLQ'd event regardless of
//!   whether any receipt caller is waiting, so a miss must never be treated
//!   as an error.
//! - **Cleanup invariant**: the ONLY sanctioned way to wait on registered
//!   receivers is [`SettlementRegistry::await_batch`]. It removes its own
//!   registry entries when it gives up (timeout or a dropped sender), so
//!   nothing leaks in the `DashMap` as long as every `register()` is
//!   eventually awaited through it. A caller that registers and then never
//!   awaits (via this helper or a manual `cancel()`) leaks that one entry
//!   until `resolve()` happens to be called for the same id later.
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
//! - No wiring of [`SettlementRegistry`] into `settle_child`'s ~15 call
//!   sites or `JetStreamConsumer`'s construction — `register`/`resolve` are
//!   never called from real event-engine code by this slice.
//! - No `emit_batch_durable()` — [`SettlementRegistry::await_batch`] is the
//!   assembly primitive it will call, not the function itself.
//! - `allow_spool_backend` / `SpoolAcceptedLossless` is included in the type
//!   shape per the ratified design, but any real backend must gate it behind
//!   sinex-r6d.5 (already landed — the recovery spool is lossless) before
//!   ever constructing that variant.

use std::time::Duration;

use dashmap::DashMap;
use sinex_db::repositories::EventStorageLane;
use sinex_primitives::events::Event;
use sinex_primitives::{JsonValue, Uuid};
use tokio::sync::oneshot;

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

/// Per-event-id waiter map: a future settlement call site resolves an event
/// id to its terminal [`EmissionReceiptState`]; a future emission-side
/// caller awaits it. See this module's top-level "`SettlementRegistry`"
/// section for the registration/resolution/cleanup contract.
///
/// Cheap to clone (inner `Arc`), matching this crate's existing idiom for
/// shared runtime coordination state (e.g.
/// `event_engine::material_ready_set::MaterialReadySet`) rather than an
/// `Arc<Self>`-wrapped constructor.
#[derive(Debug, Clone, Default)]
pub struct SettlementRegistry {
    waiters: std::sync::Arc<DashMap<Uuid, oneshot::Sender<EmissionReceiptState>>>,
}

impl SettlementRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register interest in `event_id`'s eventual settlement, returning the
    /// receiver half. Must be called before the event is emitted, and the
    /// returned receiver should be awaited via [`Self::await_batch`] (see
    /// the cleanup invariant on this module's top-level doc) rather than
    /// directly, unless the caller has its own equivalent cleanup path.
    ///
    /// Registering the same `event_id` twice replaces the previous sender —
    /// the earlier receiver then observes a closed channel (`RecvError`).
    /// No caller in this codebase does this today; documented rather than
    /// guarded against, since guarding would require an error return for a
    /// case with no real caller yet.
    pub fn register(&self, event_id: Uuid) -> oneshot::Receiver<EmissionReceiptState> {
        let (tx, rx) = oneshot::channel();
        self.waiters.insert(event_id, tx);
        rx
    }

    /// Resolve `event_id` to its terminal `state`, waking anyone awaiting
    /// it. Returns `true` if a waiter was found and removed, `false` if
    /// nobody had registered for this id (the common case — most events
    /// have no receipt caller) or the entry was already resolved/cancelled.
    /// Never treats "the receiver was already dropped" (nobody is awaiting
    /// it anymore) as an error — [`oneshot::Sender::send`]'s `Err` in that
    /// case is intentionally discarded.
    pub fn resolve(&self, event_id: Uuid, state: EmissionReceiptState) -> bool {
        match self.waiters.remove(&event_id) {
            Some((_, tx)) => {
                let _ = tx.send(state);
                true
            }
            None => false,
        }
    }

    /// Remove `event_id`'s registration without resolving it (the receiver,
    /// if still held, then observes a closed channel). A no-op if nothing
    /// is registered for this id. Exposed for callers with their own
    /// cleanup path outside [`Self::await_batch`]; `await_batch` itself
    /// calls this internally on timeout/drop, so callers that always go
    /// through it never need to call this directly.
    pub fn cancel(&self, event_id: Uuid) {
        self.waiters.remove(&event_id);
    }

    /// Number of currently in-flight (unresolved, uncancelled)
    /// registrations. Test/diagnostic accessor.
    #[must_use]
    pub fn len(&self) -> usize {
        self.waiters.len()
    }

    /// `true` iff there are no in-flight registrations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.waiters.is_empty()
    }

    /// Await a batch of `(event_id, receiver)` pairs concurrently (not
    /// serially — preserves the "no per-event await serialization" AC),
    /// each bounded by `per_item_timeout`, and assemble the results into
    /// one [`EmissionItemReceipt`] per pair in input order. This is the
    /// primitive a future `emit_batch_durable()` will call after
    /// `register()`-ing every event in a [`DurableEmissionRequest`] and
    /// submitting them to a backend.
    ///
    /// A receiver that does not resolve within `per_item_timeout`, or whose
    /// sender was dropped without resolving (e.g. the registry's owner shut
    /// down), maps to [`EmissionReceiptState::FailedTransient`] — never
    /// silently treated as success, so [`DurableEmissionReceipt::unlocks_progress`]
    /// correctly stays `false` for a receipt containing it. Every such case
    /// also removes the id's registry entry (see this module's cleanup
    /// invariant), so a caller that always awaits through this method never
    /// leaks a `DashMap` entry.
    ///
    /// A timed-out or dropped sibling never corrupts the outcome of the
    /// OTHER pairs in the same batch — each pair resolves (or times out)
    /// completely independently.
    pub async fn await_batch(
        &self,
        waiters: Vec<(Uuid, oneshot::Receiver<EmissionReceiptState>)>,
        per_item_timeout: Duration,
    ) -> Vec<EmissionItemReceipt> {
        let pending = waiters.into_iter().map(|(event_id, rx)| async move {
            match tokio::time::timeout(per_item_timeout, rx).await {
                Ok(Ok(state)) => EmissionItemReceipt {
                    event_id: Some(event_id),
                    state,
                },
                Ok(Err(_)) => {
                    // Sender dropped without resolving: the registry entry may
                    // or may not have already been removed by whoever dropped
                    // it, so cancel() is a no-op in the already-removed case.
                    self.cancel(event_id);
                    EmissionItemReceipt {
                        event_id: Some(event_id),
                        state: EmissionReceiptState::FailedTransient {
                            error: "settlement registry dropped before resolving".to_string(),
                        },
                    }
                }
                Err(_elapsed) => {
                    self.cancel(event_id);
                    EmissionItemReceipt {
                        event_id: Some(event_id),
                        state: EmissionReceiptState::FailedTransient {
                            error: "settlement wait timed out".to_string(),
                        },
                    }
                }
            }
        });
        futures::future::join_all(pending).await
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

#[cfg(test)]
mod settlement_registry_tests {
    // `TestResult` appears only as the declared return type in `#[sinex_test]`
    // signatures below; the macro replaces that annotation during expansion (same
    // import-free precedent as `dlq_retry_test.rs`), so importing the alias here
    // triggers an unused-import warning.
    use xtask::sandbox::sinex_test;

    use super::*;

    fn debt(reason: &str) -> EmissionReceiptState {
        EmissionReceiptState::DurableDebt {
            debt_id: Uuid::now_v7(),
            reason: reason.to_string(),
        }
    }

    #[test]
    fn register_then_resolve_delivers_exact_state_to_the_receiver() {
        let registry = SettlementRegistry::new();
        let event_id = Uuid::now_v7();
        let mut rx = registry.register(event_id);

        assert!(registry.resolve(event_id, EmissionReceiptState::NoOutputSettled));

        match rx.try_recv() {
            Ok(EmissionReceiptState::NoOutputSettled) => {}
            other => panic!("expected NoOutputSettled, got {other:?}"),
        }
    }

    #[test]
    fn resolve_with_no_matching_registration_returns_false_without_panicking() {
        let registry = SettlementRegistry::new();
        assert!(!registry.resolve(Uuid::now_v7(), EmissionReceiptState::NoOutputSettled));
    }

    #[test]
    fn two_in_flight_event_ids_resolve_independently_with_no_cross_talk() {
        let registry = SettlementRegistry::new();
        let id_a = Uuid::now_v7();
        let id_b = Uuid::now_v7();
        let mut rx_a = registry.register(id_a);
        let mut rx_b = registry.register(id_b);

        assert!(registry.resolve(id_b, debt("b-debt")));
        assert!(registry.resolve(id_a, EmissionReceiptState::NoOutputSettled));

        match rx_a.try_recv() {
            Ok(EmissionReceiptState::NoOutputSettled) => {}
            other => panic!("id_a: expected NoOutputSettled, got {other:?}"),
        }
        match rx_b.try_recv() {
            Ok(EmissionReceiptState::DurableDebt { reason, .. }) => assert_eq!(reason, "b-debt"),
            other => panic!("id_b: expected DurableDebt(b-debt), got {other:?}"),
        }
    }

    #[test]
    fn duplicate_resolve_for_the_same_id_is_a_no_op_second_time() {
        let registry = SettlementRegistry::new();
        let event_id = Uuid::now_v7();
        let _rx = registry.register(event_id);

        assert!(registry.resolve(event_id, EmissionReceiptState::NoOutputSettled));
        assert!(!registry.resolve(event_id, debt("second-call-should-be-ignored")));
        assert!(registry.is_empty(), "entry must be removed after the first resolve");
    }

    #[test]
    fn resolving_after_the_receiver_was_dropped_directly_does_not_panic() {
        let registry = SettlementRegistry::new();
        let event_id = Uuid::now_v7();
        let rx = registry.register(event_id);
        drop(rx);

        // The registration still exists (nothing removed it) — resolve() finds and
        // removes it, `Sender::send` silently no-ops on the closed channel, and the
        // call must not panic.
        assert!(registry.resolve(event_id, EmissionReceiptState::NoOutputSettled));
        assert!(registry.is_empty());
    }

    #[sinex_test]
    async fn await_batch_all_resolve_before_timeout_collects_every_state() -> TestResult<()> {
        let registry = SettlementRegistry::new();
        let id_a = Uuid::now_v7();
        let id_b = Uuid::now_v7();
        let rx_a = registry.register(id_a);
        let rx_b = registry.register(id_b);

        // Resolve both before awaiting — a oneshot channel buffers the sent value,
        // so this is deterministic (no scheduling race with await_batch).
        registry.resolve(id_a, EmissionReceiptState::NoOutputSettled);
        registry.resolve(id_b, debt("b-debt"));

        let items = registry
            .await_batch(vec![(id_a, rx_a), (id_b, rx_b)], Duration::from_millis(200))
            .await;

        assert_eq!(items.len(), 2);
        assert!(matches!(
            items[0],
            EmissionItemReceipt {
                event_id: Some(id),
                state: EmissionReceiptState::NoOutputSettled,
            } if id == id_a
        ));
        assert!(matches!(
            &items[1],
            EmissionItemReceipt {
                event_id: Some(id),
                state: EmissionReceiptState::DurableDebt { reason, .. },
            } if *id == id_b && reason == "b-debt"
        ));
        assert!(registry.is_empty(), "await_batch must not leave entries behind");
        Ok(())
    }

    #[sinex_test]
    async fn await_batch_one_timeout_does_not_corrupt_sibling_results_and_blocks_unlocks_progress()
    -> TestResult<()> {
        let registry = SettlementRegistry::new();
        let resolved_id = Uuid::now_v7();
        let stuck_id = Uuid::now_v7();
        let rx_resolved = registry.register(resolved_id);
        let rx_stuck = registry.register(stuck_id);

        registry.resolve(resolved_id, EmissionReceiptState::NoOutputSettled);
        // stuck_id is deliberately never resolved — its receiver must time out.

        let items = registry
            .await_batch(
                vec![(resolved_id, rx_resolved), (stuck_id, rx_stuck)],
                Duration::from_millis(75),
            )
            .await;

        assert_eq!(items.len(), 2);
        assert!(
            matches!(items[0].state, EmissionReceiptState::NoOutputSettled),
            "the resolved sibling must keep its real state: {:?}",
            items[0]
        );
        assert!(
            matches!(items[1].state, EmissionReceiptState::FailedTransient { .. }),
            "the stuck item must be FailedTransient, not silently dropped: {:?}",
            items[1]
        );
        assert!(
            registry.is_empty(),
            "await_batch must cancel the timed-out registration, not leak it"
        );

        // Prove the r6d.11 "partial == not a commit permission" rule holds for a
        // receipt assembled from a batch containing one timeout.
        let receipt = DurableEmissionReceipt {
            request_id: Uuid::now_v7(),
            atom: ProgressAtom::AutomatonInputBatch {
                automaton: "test-automaton".to_string(),
                input_event_ids: vec![resolved_id, stuck_id],
            },
            items,
            backend: ReceiptBackend::Direct,
        };
        assert!(
            !receipt.unlocks_progress(),
            "one non-progress (timed-out) item must block the whole receipt"
        );
        assert!(matches!(
            receipt.first_non_progress().expect("one non-progress item").state,
            EmissionReceiptState::FailedTransient { .. }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn resolve_after_await_batch_timeout_is_a_noop_because_the_entry_is_already_gone()
    -> TestResult<()> {
        let registry = SettlementRegistry::new();
        let event_id = Uuid::now_v7();
        let rx = registry.register(event_id);

        let items = registry
            .await_batch(vec![(event_id, rx)], Duration::from_millis(50))
            .await;
        assert!(matches!(
            items[0].state,
            EmissionReceiptState::FailedTransient { .. }
        ));

        // A late-arriving settlement (e.g. a slow persist that finally completes
        // after the caller gave up) must not panic and must report "nobody was
        // waiting" honestly, since await_batch already removed the registration.
        assert!(!registry.resolve(event_id, EmissionReceiptState::NoOutputSettled));
        Ok(())
    }
}

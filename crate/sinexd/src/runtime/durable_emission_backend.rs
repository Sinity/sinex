//! `emit_batch_durable()`: the receipt-producing entry point over the
//! existing mpsc emission path (sinex-r6d.11).
//!
//! # Why this is a separate module from `durable_emission`
//!
//! [`crate::runtime::durable_emission`] is deliberately I/O-free — pure
//! types plus [`crate::runtime::durable_emission::SettlementRegistry`], the
//! same scoping discipline as [`sinex_primitives::commit_frontier`]. This
//! module is the one place that actually depends on the emission machinery
//! ([`crate::runtime::stream::EventEmitter`]) to submit events,
//! rather than making `durable_emission.rs` itself depend on transport code.
//! Keeping the pure-type module free of `EventEmitter` avoids a layering
//! risk: `handles.rs` is lower-level runtime plumbing that many modules
//! depend on, and a future caller-family (e.g. the automaton bridge) may
//! want `handles.rs` to depend on `durable_emission`'s types directly
//! (constructing receipts inline) — if `durable_emission.rs` already
//! depended back on `handles.rs` for `emit_batch_durable`, that would be a
//! cycle. Routing the I/O-dependent assembly through this sibling module
//! instead keeps the dependency edge one-directional:
//! `durable_emission_backend -> {durable_emission, stream::handles}`, never
//! the reverse.
//!
//! # Not done here
//!
//! This module makes `emit_batch_durable()` exist, correctly, and callable
//! — it does NOT wire it into any real caller (source cursor, automaton
//! checkpoint, invalidation ack). That caller integration is the next slice
//! (sinex-vxu et al.), which needs its own reviewed reordering of each
//! caller's checkpoint-then-emit sequence into emit-then-checkpoint. See
//! `durable_emission.rs`'s top-level doc and sinex-r6d.11's bead notes.

use std::collections::HashMap;
use std::time::Duration;

use sinex_primitives::{Id, Uuid};

use crate::runtime::durable_emission::{
    DurableEmissionReceipt, DurableEmissionRequest, EmissionItemReceipt, EmissionReceiptState,
    ReceiptBackend, SettlementRegistry,
};
use crate::runtime::stream::EventEmitter;

/// Durably emit every event in `request` as one progress atom, returning a
/// receipt whose [`DurableEmissionReceipt::unlocks_progress`] is `true` only
/// once every event reached a progress-unlocking terminal
/// [`EmissionReceiptState`] via `registry`.
///
/// # Ordering contract (the critical part)
///
/// Every event's id is registered with `registry` **before** that event is
/// handed to `emitter.emit()`. Registering after emission would race: the
/// event-engine admission/persist/confirm pipeline could settle the event
/// (and call `registry.resolve()`) before this function's `register()` call
/// ever runs, permanently losing that resolution (nothing would be waiting,
/// so `resolve()` is a documented no-op in that case) — the receipt would
/// then hang until `per_item_timeout` for an event that had, in reality,
/// already settled. Registering first closes that window: the receiver
/// exists before the event can possibly reach a terminal state.
///
/// # Backend
///
/// This is backend (A) from the ratified design: a settlement-channel
/// receipt over the existing NATS→event-engine admission/persist/confirm
/// pipeline (`registry` is the [`crate::event_engine::jetstream_consumer::JetStreamConsumer`]'s
/// `settlement_registry`, and settlement happens via the `settle_child(`
/// call sites in `persist.rs`/`prepare.rs`). The receipt's
/// [`ReceiptBackend`] is therefore always [`ReceiptBackend::Nats`] — this
/// function never constructs [`ReceiptBackend::Direct`] receipts.
///
/// # Emission-failure handling
///
/// If `emitter.emit()` itself fails (e.g. the mpsc channel is closed, or
/// schema validation rejects the event) for a given event, that event will
/// never reach the event-engine and therefore will never be resolved by any
/// `settle_child(` site. Rather than let it sit registered until
/// `per_item_timeout` expires, its registration is cancelled immediately and
/// its item receipt is set to [`EmissionReceiptState::FailedTransient`]
/// directly — [`DurableEmissionReceipt::unlocks_progress`] still correctly
/// reports `false` for a receipt containing it either way, but this avoids
/// an unnecessary wait for an outcome that is already known.
///
/// # Concurrency
///
/// Both the registration/emission loop and the settlement wait
/// ([`SettlementRegistry::await_batch`]) run every event concurrently (no
/// per-event `await` serialization), matching the "batching/pipelining
/// preserved" acceptance criterion — `emitter.emit()` is itself just an
/// `mpsc::Sender::send`, so concurrent emission does not bypass or race the
/// downstream `EventBatcher`'s own batching window.
pub async fn emit_batch_durable(
    registry: &SettlementRegistry,
    emitter: &EventEmitter,
    mut request: DurableEmissionRequest,
    per_item_timeout: Duration,
) -> DurableEmissionReceipt {
    let request_id = Uuid::now_v7();

    // Assign a final id to every event and register interest in its
    // settlement BEFORE any event is emitted (see the ordering contract
    // above). `event_ids` preserves the request's original event order so
    // the final receipt's `items` can be reassembled in that same order.
    let mut event_ids: Vec<Uuid> = Vec::with_capacity(request.events.len());
    let mut waiters: Vec<(Uuid, tokio::sync::oneshot::Receiver<EmissionReceiptState>)> =
        Vec::with_capacity(request.events.len());
    for event in &mut request.events {
        let id = *event.id.get_or_insert_with(Id::new).as_uuid();
        event_ids.push(id);
        waiters.push((id, registry.register(id)));
    }

    // Emit every event concurrently. This borrows `emitter` for the
    // duration of the join rather than spawning tasks, so no `'static`
    // bound or extra clone is needed.
    let emit_results: Vec<(Uuid, Result<(), sinex_primitives::SinexError>)> =
        futures::future::join_all(request.events.into_iter().zip(event_ids.iter().copied()).map(
            |(event, event_id)| async move { (event_id, emitter.emit(event).await) },
        ))
        .await;

    // Events whose emit() call itself failed never reach the event engine,
    // so nothing will ever call registry.resolve() for them — cancel the
    // registration immediately instead of waiting out the full timeout, and
    // record their outcome directly.
    let mut immediate: HashMap<Uuid, EmissionItemReceipt> = HashMap::new();
    for (event_id, result) in emit_results {
        if let Err(error) = result {
            registry.cancel(event_id);
            immediate.insert(
                event_id,
                EmissionItemReceipt {
                    event_id: Some(event_id),
                    state: EmissionReceiptState::FailedTransient {
                        error: error.to_string(),
                    },
                },
            );
        }
    }

    let awaited_waiters: Vec<_> = waiters
        .into_iter()
        .filter(|(event_id, _)| !immediate.contains_key(event_id))
        .collect();
    let awaited = registry.await_batch(awaited_waiters, per_item_timeout).await;
    let mut awaited_by_id: HashMap<Uuid, EmissionItemReceipt> = awaited
        .into_iter()
        .filter_map(|item| item.event_id.map(|id| (id, item)))
        .collect();

    let items: Vec<EmissionItemReceipt> = event_ids
        .into_iter()
        .map(|id| {
            immediate.remove(&id).or_else(|| awaited_by_id.remove(&id)).unwrap_or_else(|| {
                // Unreachable in practice: every id in `event_ids` was either
                // recorded in `immediate` (emit failed) or awaited through
                // `await_batch` (emit succeeded, so it's in `awaited_by_id`
                // unconditionally, including the timeout/dropped case which
                // await_batch itself maps to FailedTransient). Kept as an
                // honest non-panicking fallback rather than an `expect()`.
                EmissionItemReceipt {
                    event_id: Some(id),
                    state: EmissionReceiptState::FailedTransient {
                        error: "emit_batch_durable lost bookkeeping for this event id"
                            .to_string(),
                    },
                }
            })
        })
        .collect();

    DurableEmissionReceipt {
        request_id,
        atom: request.progress_atom,
        items,
        backend: ReceiptBackend::Nats,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sinex_primitives::DynamicPayload;
    use tokio::sync::mpsc;
    use xtask::sandbox::sinex_test;

    use super::*;
    use crate::runtime::durable_emission::{ProgressAtom, SuppressionReason};

    fn material_event(anchor_byte: i64) -> sinex_primitives::events::Event<sinex_primitives::JsonValue> {
        let material_id: sinex_primitives::Id<sinex_primitives::events::SourceMaterial> =
            sinex_primitives::Id::new();
        DynamicPayload::new(
            "test-source",
            "test.emit_batch_durable",
            json!({"anchor_byte": anchor_byte}),
        )
        .from_material_at(material_id, anchor_byte)
        .build()
        .expect("valid material-provenance event")
    }

    fn request(events: Vec<sinex_primitives::events::Event<sinex_primitives::JsonValue>>) -> DurableEmissionRequest {
        DurableEmissionRequest {
            origin: crate::runtime::durable_emission::EmissionOrigin::SourceAdapter,
            required_level: crate::runtime::durable_emission::ReceiptLevel::AdmissionSettled,
            progress_atom: ProgressAtom::SourceRecord {
                source_id: "test-source".to_string(),
                material_id: Uuid::now_v7(),
                anchor_byte: 0,
                cursor_after: sinex_primitives::JsonValue::Null,
            },
            events,
            allow_spool_backend: false,
        }
    }

    #[sinex_test]
    async fn register_happens_before_emit_no_race_with_immediate_resolve() -> TestResult<()> {
        // A channel with capacity 0 would still let `send` succeed once a
        // receiver is polling; instead prove the ordering directly: resolve
        // every incoming event the instant it's observed on the receiver
        // side, from a task that starts polling only after emit_batch_durable
        // has begun. If register() ran after emit(), this would race and
        // could drop the resolution (nobody registered yet) — repeated runs
        // would then intermittently time out. Asserting zero timeouts across
        // a real batch is the load-bearing proof.
        let (tx, mut rx) = mpsc::channel(8);
        let emitter = EventEmitter::new(tx, false);
        let registry = SettlementRegistry::new();

        let events = vec![material_event(0), material_event(1), material_event(2)];
        let req = request(events);

        let registry_for_settler = registry.clone();
        let settler = tokio::spawn(async move {
            let mut settled = 0;
            while let Some(event) = rx.recv().await {
                let id = *event.id.expect("emit() assigns an id").as_uuid();
                registry_for_settler.resolve(id, EmissionReceiptState::NoOutputSettled);
                settled += 1;
                if settled == 3 {
                    break;
                }
            }
        });

        let receipt = emit_batch_durable(&registry, &emitter, req, Duration::from_millis(500)).await;
        settler.await.expect("settler task did not panic");

        assert_eq!(receipt.items.len(), 3);
        assert!(
            receipt.unlocks_progress(),
            "every item should resolve to NoOutputSettled well within the timeout if \
             register() truly ran before emit(): {:?}",
            receipt.items
        );
        assert_eq!(receipt.backend, ReceiptBackend::Nats);
        Ok(())
    }

    #[sinex_test]
    async fn receipt_reflects_real_mixed_settlement_outcomes() -> TestResult<()> {
        let (tx, mut rx) = mpsc::channel(8);
        let emitter = EventEmitter::new(tx, false);
        let registry = SettlementRegistry::new();

        let events = vec![material_event(10), material_event(11)];
        let req = request(events);

        let registry_for_settler = registry.clone();
        let settler = tokio::spawn(async move {
            let mut ids = Vec::new();
            while let Some(event) = rx.recv().await {
                ids.push(*event.id.expect("emit() assigns an id").as_uuid());
                if ids.len() == 2 {
                    break;
                }
            }
            // First event: persisted+confirmed. Second: suppressed (tombstoned).
            registry_for_settler.resolve(
                ids[0],
                EmissionReceiptState::PersistedConfirmed {
                    lane: sinex_db::repositories::EventStorageLane::Activity,
                    inserted: true,
                    confirmed_sequence: None,
                },
            );
            registry_for_settler.resolve(
                ids[1],
                EmissionReceiptState::Suppressed {
                    reason: SuppressionReason::Tombstoned,
                    existing_event_id: None,
                },
            );
        });

        let receipt = emit_batch_durable(&registry, &emitter, req, Duration::from_millis(500)).await;
        settler.await.expect("settler task did not panic");

        assert_eq!(receipt.items.len(), 2);
        assert!(
            receipt.unlocks_progress(),
            "PersistedConfirmed + Suppressed are both progress-unlocking: {:?}",
            receipt.items
        );
        assert!(matches!(
            receipt.items[0].state,
            EmissionReceiptState::PersistedConfirmed { inserted: true, .. }
        ));
        assert!(matches!(
            receipt.items[1].state,
            EmissionReceiptState::Suppressed {
                reason: SuppressionReason::Tombstoned,
                ..
            }
        ));
        Ok(())
    }

    #[sinex_test]
    async fn a_never_settled_event_times_out_and_blocks_unlocks_progress() -> TestResult<()> {
        let (tx, mut rx) = mpsc::channel(8);
        let emitter = EventEmitter::new(tx, false);
        let registry = SettlementRegistry::new();

        let events = vec![material_event(20)];
        let req = request(events);

        // Drain the channel but never resolve anything — simulates an event
        // that reached the mpsc handoff but never settled (e.g. the consumer
        // crashed before admission).
        let drainer = tokio::spawn(async move {
            let _ = rx.recv().await;
        });

        let receipt = emit_batch_durable(&registry, &emitter, req, Duration::from_millis(80)).await;
        drainer.await.expect("drainer task did not panic");

        assert_eq!(receipt.items.len(), 1);
        assert!(!receipt.unlocks_progress());
        assert!(matches!(
            receipt.items[0].state,
            EmissionReceiptState::FailedTransient { .. }
        ));
        assert!(
            registry.is_empty(),
            "await_batch must cancel the timed-out registration, not leak it"
        );
        Ok(())
    }

    #[sinex_test]
    async fn emit_failure_short_circuits_without_waiting_for_the_full_timeout() -> TestResult<()> {
        // Dropping the receiver immediately makes every `emit()` call fail
        // (`mpsc::Sender::send` errors once the channel is closed).
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let emitter = EventEmitter::new(tx, false);
        let registry = SettlementRegistry::new();

        let events = vec![material_event(30)];
        let req = request(events);

        let started = tokio::time::Instant::now();
        let receipt = emit_batch_durable(&registry, &emitter, req, Duration::from_secs(30)).await;
        let elapsed = started.elapsed();

        assert_eq!(receipt.items.len(), 1);
        assert!(!receipt.unlocks_progress());
        assert!(matches!(
            receipt.items[0].state,
            EmissionReceiptState::FailedTransient { .. }
        ));
        assert!(
            elapsed < Duration::from_secs(5),
            "an emit() failure must be reported immediately, not after the 30s per_item_timeout: {elapsed:?}"
        );
        assert!(registry.is_empty(), "a failed emit must cancel its registration, not leak it");
        Ok(())
    }
}

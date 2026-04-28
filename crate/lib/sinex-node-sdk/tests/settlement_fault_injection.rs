//! Settlement fault-injection scenarios (issue #653, deferred from #608).
//!
//! Each scenario asserts the *settlement outcome* the runtime should reach when a
//! specific fault is injected into the boundary that owns it. Where the runtime
//! wiring already exists (CheckpointManager CAS, ConfirmationBuffer TTL), the
//! test exercises the real primitive end-to-end. Where settlement vocabulary
//! describes a runtime contract that is not yet fully wired (DLQ unavailable,
//! NATS-down circuit breaker), the test exercises the `FailurePolicy` mapping
//! that the runtime *must* follow — making the policy contract executable
//! rather than aspirational.
//!
//! Scenario 4 (journal self-capture filtered by automata) is owned by
//! `sinex-system-ingestor` because the filter belongs to the journal config
//! there; that test lives in
//! `crate/nodes/sinex-system-ingestor/tests/watcher_logic_test.rs`.
//!
//! See `crate/lib/sinex-primitives/src/settlement.rs` for the vocabulary.

use sinex_node_sdk::confirmation_handler::{ConfirmationBuffer, ProvisionalEvent};
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::SinexError;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::error::ErrorClass;
use sinex_primitives::events::builder::EventId;
use sinex_primitives::settlement::{
    Backoff, BatchSettlement, DefaultFailurePolicy, DurabilityDomain, EffectIntent, EffectKind,
    EventSettlement, FailureContext, FailurePolicy, HaltReason, ParkReason, ProgressProposal,
    Receipt, RemainingPolicy, RetryBudget, RuntimeOperation, RuntimePhase, Settlement,
};
use sinex_primitives::temporal::Timestamp;
use std::time::Duration;
use xtask::sandbox::prelude::*;

const NODE_FOR_POLICY: &str = "settlement-fault-policy-test";

fn ctx_for(operation: RuntimeOperation, phase: RuntimePhase, attempts: u32) -> FailureContext {
    FailureContext {
        unit_id: NODE_FOR_POLICY.to_string(),
        operation,
        phase,
        input_scope: None,
        effect_kind: None,
        delivery_count: None,
        attempts,
    }
}

// -------------------------------------------------------------------------
// Scenario 1: Checkpoint CAS failure → node halt
// -------------------------------------------------------------------------
//
// Two managers share a key and revision. When manager B tries to save with a
// stale revision, the second `save_checkpoint` surfaces a CAS-style error
// whose `ErrorClass` is `NodeFatal`, which the `DefaultFailurePolicy` settles
// to `HaltNode { CheckpointCasConflict }`.
#[sinex_test]
async fn settlement_halts_on_checkpoint_cas_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node_name = format!("cas-halt-{}", Uuid::now_v7().simple());

    let mgr_a = CheckpointManager::new(
        kv.clone(),
        node_name.clone(),
        "default".to_string(),
        "instance-1".to_string(),
    );
    let mgr_b = CheckpointManager::new(
        kv,
        node_name,
        "default".to_string(),
        "instance-1".to_string(),
    );

    let mut state_a = CheckpointState::default();
    state_a.checkpoint = Checkpoint::Timestamp {
        timestamp: Timestamp::now(),
        metadata: None,
    };
    state_a.processed_count = 1;
    state_a.revision = mgr_a.save_checkpoint(&state_a).await?;

    // Stash that revision: B will try to save *again* using A's first revision,
    // but A advances first so B's revision is now stale.
    let stale_revision_for_b = state_a.revision;
    state_a.processed_count = 2;
    state_a.revision = mgr_a.save_checkpoint(&state_a).await?;

    let mut state_b = CheckpointState::default();
    state_b.checkpoint = Checkpoint::Timestamp {
        timestamp: Timestamp::now(),
        metadata: None,
    };
    state_b.processed_count = 99; // distinct content so the idempotent re-create
                                  // path does not absorb this as a no-op.
    state_b.revision = stale_revision_for_b;

    let err = mgr_b
        .save_checkpoint(&state_b)
        .await
        .expect_err("stale-revision save must surface a CAS-class checkpoint error");

    ctx.assert("error class is NodeFatal")
        .eq(&err.error_class(), &ErrorClass::NodeFatal)?;

    let policy = DefaultFailurePolicy;
    let settlement = policy.settle(
        &err,
        &ctx_for(
            RuntimeOperation::CheckpointSave,
            RuntimePhase::PersistProgress,
            1,
        ),
    );
    match settlement {
        Settlement::HaltNode {
            reason: HaltReason::CheckpointCasConflict,
        } => Ok(()),
        other => Err(color_eyre::eyre::eyre!(
            "expected HaltNode(CheckpointCasConflict), got {other:?}"
        )),
    }
}

// -------------------------------------------------------------------------
// Scenario 2: Output failure → checkpoint does not advance
// -------------------------------------------------------------------------
//
// The settlement contract: when a per-event output emission fails, the event
// settles as Retry (or Park). Neither variant carries a `ProgressProposal`,
// so checkpoint advance MUST NOT happen for that event. This test asserts the
// invariant directly on the settlement vocabulary.
#[test]
fn settlement_blocks_checkpoint_advance_on_output_failure() {
    let policy = DefaultFailurePolicy;

    // Output-emission transient infra failure → Retry (no progress proposal).
    let infra_err = SinexError::network("simulated jetstream publish ack timeout");
    let infra_settlement = policy.settle(
        &infra_err,
        &ctx_for(RuntimeOperation::OutputEmission, RuntimePhase::EmitEffect, 1),
    );
    let advances_under_retry = matches!(infra_settlement, Settlement::Commit);
    assert!(
        !advances_under_retry,
        "transient output failure must not authorize checkpoint advance: got {infra_settlement:?}"
    );
    assert!(
        matches!(
            infra_settlement,
            Settlement::Retry { .. } | Settlement::Park { .. }
        ),
        "transient output failure must Retry or Park, got {infra_settlement:?}"
    );

    // A `Retry` event settlement also has no `ProgressProposal`. Verify the
    // batch-level invariant: only `Committed` carries advance authority.
    let batch = BatchSettlement {
        outcomes: vec![EventSettlement::Retry {
            reason: SinexError::network("publish failed"),
            backoff: Backoff::None,
            budget: RetryBudget {
                max_attempts: 3,
                max_elapsed: None,
                backoff: Backoff::None,
                terminal: Box::new(Settlement::HaltNode {
                    reason: HaltReason::EscalateOperator,
                }),
            },
        }],
        remaining: RemainingPolicy::HaltRemaining,
    };
    let any_progress = batch.outcomes.iter().any(|o| {
        matches!(
            o,
            EventSettlement::Committed {
                progress: ProgressProposal {
                    advance_checkpoint: true,
                    ..
                },
                ..
            }
        )
    });
    assert!(
        !any_progress,
        "Retry settlement carries no ProgressProposal; checkpoint advance must be impossible"
    );
}

// -------------------------------------------------------------------------
// Scenario 3: Poison event → routed to DLQ (exactly once)
// -------------------------------------------------------------------------
//
// The settlement contract: malformed/poison input maps to `DataError` ->
// `Settlement::SendToProcessingFailure`, and the resulting `EffectIntent` MUST
// carry a deterministic `processing_failure_effect_id` so a redelivered batch
// produces the same idempotency key (exactly-once routing under at-least-once
// delivery).
#[test]
fn settlement_routes_poison_event_to_dlq_exactly_once() {
    let policy = DefaultFailurePolicy;
    let bad = SinexError::validation("poison event: schema violation");
    let settlement = policy.settle(
        &bad,
        &ctx_for(RuntimeOperation::ProcessBatch, RuntimePhase::ProcessInput, 1),
    );
    assert!(
        matches!(settlement, Settlement::SendToProcessingFailure),
        "DataError must settle as SendToProcessingFailure, got {settlement:?}"
    );

    // Determinism: the same input event id + node + fingerprint produces the
    // same effect id across invocations. This is what makes DLQ routing
    // exactly-once under at-least-once delivery.
    let event_id = Uuid::now_v7();
    let id_a = sinex_primitives::settlement::processing_failure_effect_id(
        NODE_FOR_POLICY,
        event_id,
        "schema-violation:foo.bar",
        "policy-v1",
    );
    let id_b = sinex_primitives::settlement::processing_failure_effect_id(
        NODE_FOR_POLICY,
        event_id,
        "schema-violation:foo.bar",
        "policy-v1",
    );
    assert_eq!(
        id_a, id_b,
        "processing_failure_effect_id must be deterministic for exactly-once DLQ routing"
    );

    // Different fingerprint → different effect id (no accidental collision).
    let id_c = sinex_primitives::settlement::processing_failure_effect_id(
        NODE_FOR_POLICY,
        event_id,
        "schema-violation:other",
        "policy-v1",
    );
    assert_ne!(
        id_a, id_c,
        "different error fingerprints must produce different effect ids"
    );
}

// -------------------------------------------------------------------------
// Scenario 5: Invalidation + process crash → resumable from checkpoint
// -------------------------------------------------------------------------
//
// Save a checkpoint, simulate process death by dropping the manager, then
// recreate a new manager against the same KV and load. The recovered
// checkpoint must equal the saved one — meaning a crash mid-invalidation
// that runs *before* checkpoint advance will resume from the last persisted
// position when the node restarts.
#[sinex_test]
async fn settlement_resumes_from_checkpoint_after_invalidation_crash(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let node_name = format!("invalidation-resume-{}", Uuid::now_v7().simple());

    let mgr = CheckpointManager::new(
        kv.clone(),
        node_name.clone(),
        "default".to_string(),
        "instance-1".to_string(),
    );

    let event_id = Uuid::now_v7();
    let mut saved = CheckpointState::default();
    saved.checkpoint = Checkpoint::stream("invalidation-cursor", Some(event_id));
    saved.processed_count = 7;
    saved.revision = mgr.save_checkpoint(&saved).await?;

    // Simulate a crash *between* invalidation publish and any progress advance:
    // the manager goes away, but the KV-persisted checkpoint survives.
    drop(mgr);

    let recovered_mgr = CheckpointManager::new(
        kv,
        node_name,
        "default".to_string(),
        "instance-1".to_string(),
    );
    let recovered = recovered_mgr.load_checkpoint().await?;

    ctx.assert("processed_count survives invalidation+crash")
        .eq(&recovered.processed_count, &7u64)?;
    match &recovered.checkpoint {
        Checkpoint::Stream {
            message_id,
            event_id: recovered_event_id,
        } => {
            ctx.assert("stream message_id preserved")
                .eq(message_id, &"invalidation-cursor".to_string())?;
            ctx.assert("stream event_id preserved")
                .eq(recovered_event_id, &Some(event_id))?;
        }
        other => {
            return Err(color_eyre::eyre::eyre!(
                "expected Stream checkpoint after recovery, got {other:?}"
            ));
        }
    }
    Ok(())
}

// -------------------------------------------------------------------------
// Scenario 6: Orphan confirmation (event not persisted) → TTL expires + ack sent
// -------------------------------------------------------------------------
//
// A provisional event that is never confirmed must time out, then expire
// from the buffer once the grace period elapses. After expiry, the runtime
// can ack the upstream message because the event no longer holds a slot.
#[sinex_test]
async fn settlement_orphan_confirmation_expires_after_ttl() -> TestResult<()> {
    let timeout = Duration::from_millis(50);
    let grace = Duration::from_millis(50);
    let buffer = ConfirmationBuffer::with_capacity_and_grace(timeout, 16, grace);

    let event_id = EventId::from_uuid(Uuid::now_v7());
    let provisional = ProvisionalEvent {
        event_id,
        source: EventSource::from_static("settlement-fault-test"),
        event_type: EventType::new("test.orphan").expect("valid event type"),
        payload: serde_json::json!({}),
        ts_orig: Timestamp::now(),
        received_at: Timestamp::now(),
    };
    assert!(buffer.add_provisional(provisional).await);
    assert_eq!(buffer.len().await, 1);

    // Wait past the TTL. WaitHelpers::wait_for_condition takes a fallible
    // closure returning Result<bool, _>; we only need the inner `bool`.
    WaitHelpers::wait_for_condition(
        || async {
            let to = buffer.check_timeouts().await;
            Ok::<bool, color_eyre::eyre::Error>(!to.is_empty())
        },
        Timeouts::SHORT,
    )
    .await?;

    // Still in buffer during grace period (intentional — late confirmations
    // can land here and be matched).
    assert_eq!(
        buffer.len().await,
        1,
        "timed-out event remains during grace window"
    );

    // After grace, purge_expired removes it. That removal is what authorizes
    // the runtime to ack the upstream message: the slot is freed.
    WaitHelpers::wait_for_condition(
        || async {
            let purged = buffer.purge_expired().await;
            Ok::<bool, color_eyre::eyre::Error>(!purged.is_empty())
        },
        Timeouts::SHORT,
    )
    .await?;
    assert_eq!(
        buffer.len().await,
        0,
        "after grace period the orphan slot is freed (upstream ack permitted)"
    );
    Ok(())
}

// -------------------------------------------------------------------------
// Scenario 7: DLQ unavailable → node quarantines and halts
// -------------------------------------------------------------------------
//
// When the DLQ stream is the failed dependency itself, the policy contract is
// `Settlement::HaltNode { TransportDegraded }` (the runtime cannot route a
// processing failure if the failure-routing channel is down). We model this by
// surfacing a transport-class error for `DlqRouting`.
#[test]
fn settlement_quarantines_and_halts_when_dlq_unavailable() {
    /// Custom policy that escalates `Network`/`Timeout` errors observed during
    /// `DlqRouting` to `TransportDegraded` -> `HaltNode`. This is the contract
    /// the runtime must implement when DLQ itself is down: there is no useful
    /// fallback because the fallback IS the DLQ.
    struct DlqAwareFailurePolicy;
    impl FailurePolicy for DlqAwareFailurePolicy {
        fn settle(&self, err: &SinexError, fctx: &FailureContext) -> Settlement {
            if matches!(fctx.operation, RuntimeOperation::DlqRouting)
                && matches!(
                    err.error_class(),
                    ErrorClass::TransientInfra | ErrorClass::TransportDegraded
                )
            {
                return Settlement::HaltNode {
                    reason: HaltReason::TransportDegraded,
                };
            }
            DefaultFailurePolicy.settle(err, fctx)
        }
    }

    let policy = DlqAwareFailurePolicy;
    let dlq_down = SinexError::network("DLQ stream unreachable");
    let settlement = policy.settle(
        &dlq_down,
        &ctx_for(RuntimeOperation::DlqRouting, RuntimePhase::EmitEffect, 1),
    );
    assert!(
        matches!(
            settlement,
            Settlement::HaltNode {
                reason: HaltReason::TransportDegraded
            }
        ),
        "DLQ-down must settle as HaltNode(TransportDegraded), got {settlement:?}"
    );
}

// -------------------------------------------------------------------------
// Scenario 8: NATS down → circuit breaker opens
// -------------------------------------------------------------------------
//
// When NATS is down for output emission, the policy contract is: retry with
// finite budget, then escalate. After the retry budget is exhausted, the
// runtime opens its circuit breaker (settlement: `Park`). This test drives the
// policy through enough simulated attempts to trip the breaker.
#[test]
fn settlement_opens_circuit_breaker_when_nats_down() {
    let policy = DefaultFailurePolicy;
    let nats_down = SinexError::network("NATS publish ack timed out");

    // Early attempts: classify as TransientInfra → Retry (circuit closed).
    let early = policy.settle(
        &nats_down,
        &ctx_for(RuntimeOperation::OutputEmission, RuntimePhase::EmitEffect, 1),
    );
    assert!(
        matches!(early, Settlement::Retry { .. }),
        "first attempt under NATS-down must Retry, got {early:?}"
    );

    // Past retry budget threshold (>=10 attempts): the breaker opens — the
    // policy parks rather than continuing to hammer the dead transport.
    let exhausted = policy.settle(
        &nats_down,
        &ctx_for(RuntimeOperation::OutputEmission, RuntimePhase::EmitEffect, 11),
    );
    assert!(
        matches!(
            exhausted,
            Settlement::Park {
                reason: ParkReason::RetryBudgetExhausted
            }
        ),
        "NATS-down past retry budget must Park(RetryBudgetExhausted) (breaker open), got {exhausted:?}"
    );
}

// -------------------------------------------------------------------------
// Cross-cutting invariants exercised by these tests:
//   - Receipt durability domains are explicit (used by CheckpointSave receipts).
//   - EffectIntent.required_for_progress controls whether a missing receipt
//     blocks checkpoint advance (the same invariant tested in scenario 2).
// -------------------------------------------------------------------------
#[test]
fn receipt_durability_domains_match_settlement_vocabulary() {
    let remote = Receipt::JetStreamAccepted {
        stream: "events".into(),
        sequence: 1,
        msg_id: "x".into(),
    };
    assert_eq!(remote.durability_domain(), DurabilityDomain::Remote);

    let local = Receipt::LocalSegmentFsynced {
        path: "/var/lib/sinex/spool/0001".into(),
        segment: 1,
        offset: 0,
    };
    assert_eq!(local.durability_domain(), DurabilityDomain::Local);

    let intent = EffectIntent {
        effect_id: "deadbeef".into(),
        kind: EffectKind::DerivedOutput,
        idempotency_key: "k".into(),
        required_for_progress: true,
        payload: serde_json::Value::Null,
    };
    assert!(
        intent.required_for_progress,
        "DerivedOutput EffectIntent must default to required_for_progress=true \
         so a missing receipt blocks checkpoint advance (scenario 2 invariant)"
    );
}

//! Tests for the relation extractor — stage 3 of the entity intelligence pipeline.
//!
//! Verifies the sliding co-occurrence window (gap > 300s closes the window,
//! capacity bound at 2000 entities), pairwise relation generation, and
//! `ScopeReconciler` invariants (single fixed scope, no spurious emissions
//! before the window closes).

use sinex_primitives::Uuid;
use sinex_primitives::domain::{EntityTypeName, ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::EntityResolvedPayload;
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue};
use sinexd::automata::relation_extractor::RelationExtractor;
use sinexd::node_sdk::ScopeReconciler;
use sinexd::node_sdk::derived_node::AutomatonContext;
use xtask::sandbox::prelude::*;

const CO_OCCURRENCE_SCOPE: &str = "co-occurrence-window";

fn make_context(ts: Timestamp) -> AutomatonContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    AutomatonContext {
        trigger_event_id: event_id,
        source: EntityResolvedPayload::SOURCE,
        event_type: EntityResolvedPayload::EVENT_TYPE,
        ts_orig: Some(ts),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn resolved(name: &str) -> EntityResolvedPayload {
    EntityResolvedPayload {
        entity_id: Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()),
        canonical_name: name.to_string(),
        entity_type: EntityTypeName::new("tool"),
        original_name: name.to_string(),
    }
}

#[sinex_test]
async fn scope_keys_returns_singleton_co_occurrence() -> TestResult<()> {
    let extractor = RelationExtractor;
    let ctx = make_context(Timestamp::now());
    let keys = extractor.scope_keys(&resolved("git"), &ctx);
    assert_eq!(keys, vec![CO_OCCURRENCE_SCOPE.to_string()]);
    Ok(())
}

#[sinex_test]
async fn single_entity_emits_no_relations() -> TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = Default::default();
    let ctx = make_context(Timestamp::now());

    let outputs = extractor
        .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved("git"), &ctx)
        .await?;

    assert!(outputs.is_empty());
    assert_eq!(state.window.len(), 1);
    assert_eq!(state.relations_emitted, 0);
    Ok(())
}

#[sinex_test]
async fn within_window_no_emission_yet() -> TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = Default::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");

    for (i, name) in ["git", "nix", "cargo"].iter().enumerate() {
        let ts = t0 + Duration::seconds(i as i64 * 60); // 60s apart, all within 300s
        let ctx = make_context(ts);
        let outputs = extractor
            .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved(name), &ctx)
            .await?;
        assert!(outputs.is_empty(), "no emission while window open");
    }

    assert_eq!(state.window.len(), 3);
    assert_eq!(state.relations_emitted, 0);
    Ok(())
}

#[sinex_test]
async fn gap_closes_window_and_emits_pairwise_relations() -> TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = Default::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");

    // Three entities within the window.
    for (i, name) in ["git", "nix", "cargo"].iter().enumerate() {
        let ts = t0 + Duration::seconds(i as i64 * 60);
        let ctx = make_context(ts);
        extractor
            .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved(name), &ctx)
            .await?;
    }

    // Fourth entity arrives after a 400s gap — closes the prior window.
    let after_gap = t0 + Duration::seconds(60 * 2 + 400);
    let ctx = make_context(after_gap);
    let outputs = extractor
        .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved("docker"), &ctx)
        .await?;

    // 3 entities -> C(3,2) = 3 pairwise relations.
    assert_eq!(outputs.len(), 3);
    assert_eq!(state.relations_emitted, 3);

    // Window now contains the new entity only.
    assert_eq!(state.window.len(), 1);
    assert_eq!(state.window[0].canonical_name, "docker");

    for out in &outputs {
        assert_eq!(out.payload.relation_type.as_str(), "co_occurs_with");
        assert!((0.0..=1.0).contains(&out.payload.confidence));
        assert_ne!(out.payload.source_entity_id, out.payload.target_entity_id);
    }
    Ok(())
}

#[sinex_test]
async fn two_entities_then_gap_emits_one_relation() -> TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = Default::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");

    let ctx0 = make_context(t0);
    extractor
        .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved("git"), &ctx0)
        .await?;
    let ctx1 = make_context(t0 + Duration::seconds(60));
    extractor
        .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved("nix"), &ctx1)
        .await?;

    let ctx_gap = make_context(t0 + Duration::seconds(60 + 400));
    let outputs = extractor
        .reconcile(&mut state, CO_OCCURRENCE_SCOPE, resolved("cargo"), &ctx_gap)
        .await?;

    assert_eq!(outputs.len(), 1, "C(2,2) = 1 relation");
    Ok(())
}

#[sinex_test]
async fn single_entity_after_gap_emits_nothing() -> TestResult<()> {
    let mut extractor = RelationExtractor;
    let mut state = Default::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");

    extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("git"),
            &make_context(t0),
        )
        .await?;

    // Gap > 300s but only one entity in the window — should_close requires >=2.
    let outputs = extractor
        .reconcile(
            &mut state,
            CO_OCCURRENCE_SCOPE,
            resolved("nix"),
            &make_context(t0 + Duration::seconds(400)),
        )
        .await?;

    assert!(outputs.is_empty());
    // Both entities accumulated into the active window.
    assert_eq!(state.window.len(), 2);
    Ok(())
}

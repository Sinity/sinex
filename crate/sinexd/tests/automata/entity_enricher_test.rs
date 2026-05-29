//! Tests for the entity enricher — stage 4 of the entity intelligence pipeline.
//!
//! Verifies per-entity scope keying, accumulated temporal statistics
//! (`first_seen`, `last_seen`, `occurrence_count`, `active_hours` histogram),
//! dirty-entity tracking, periodic sweep emission via `reconcile_interval_secs`,
//! and `entity_type` → `EntityCategory` refinement.

use sinexd::node_sdk::ScopeReconciler;
use sinexd::node_sdk::derived_node::AutomatonContext;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EntityTypeName, ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::{EntityCategory, EntityResolvedPayload};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{Id, JsonValue};
use sinex_process::automata::entity_enricher::{EnricherConfig, EnricherState, EntityEnricher};
use xtask::sandbox::prelude::*;

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

fn resolved(entity_type: &str, canonical_name: &str) -> EntityResolvedPayload {
    EntityResolvedPayload {
        entity_id: Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{entity_type}:{canonical_name}").as_bytes(),
        ),
        canonical_name: canonical_name.to_string(),
        entity_type: EntityTypeName::new(entity_type),
        original_name: canonical_name.to_string(),
    }
}

fn enricher_with_interval(secs: i64) -> EntityEnricher {
    EntityEnricher {
        config: EnricherConfig {
            reconcile_interval_secs: secs,
        },
    }
}

#[sinex_test]
async fn scope_key_is_entity_id() -> TestResult<()> {
    let enricher = EntityEnricher::default();
    let ctx = make_context(Timestamp::now());
    let payload = resolved("tool", "git");
    let keys = enricher.scope_keys(&payload, &ctx);
    assert_eq!(keys, vec![payload.entity_id.to_string()]);
    Ok(())
}

#[sinex_test]
async fn first_observation_creates_stats_and_emits_immediately() -> TestResult<()> {
    // last_sweep is None → first reconcile triggers sweep regardless of interval.
    let mut enricher = enricher_with_interval(300);
    let mut state = EnricherState::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
    let payload = resolved("tool", "git");
    let scope = payload.entity_id.to_string();

    let outputs = enricher
        .reconcile(&mut state, &scope, payload.clone(), &make_context(t0))
        .await?;

    assert_eq!(outputs.len(), 1);
    let stats = state.entities.get(&scope).expect("entity stats");
    assert_eq!(stats.occurrence_count, 1);
    assert_eq!(stats.first_seen, t0);
    assert_eq!(stats.last_seen, t0);
    assert_eq!(outputs[0].payload.refined_category, EntityCategory::Tool);
    Ok(())
}

#[sinex_test]
async fn subsequent_observations_accumulate_without_emitting_within_interval() -> TestResult<()> {
    let mut enricher = enricher_with_interval(300);
    let mut state = EnricherState::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
    let payload = resolved("tool", "git");
    let scope = payload.entity_id.to_string();

    enricher
        .reconcile(&mut state, &scope, payload.clone(), &make_context(t0))
        .await?;

    // Second observation 60s later — inside the 300s interval, no emit.
    let outputs = enricher
        .reconcile(
            &mut state,
            &scope,
            payload.clone(),
            &make_context(t0 + Duration::seconds(60)),
        )
        .await?;

    assert!(outputs.is_empty());
    let stats = state.entities.get(&scope).unwrap();
    assert_eq!(stats.occurrence_count, 2);
    assert!(state.dirty_entities.contains(&payload.entity_id));
    Ok(())
}

#[sinex_test]
async fn sweep_emits_after_interval_elapses() -> TestResult<()> {
    let mut enricher = enricher_with_interval(300);
    let mut state = EnricherState::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
    let payload = resolved("tool", "git");
    let scope = payload.entity_id.to_string();

    // First observation: sweep happens (last_sweep was None).
    enricher
        .reconcile(&mut state, &scope, payload.clone(), &make_context(t0))
        .await?;
    // Quick follow-up: still inside the interval, no sweep.
    enricher
        .reconcile(
            &mut state,
            &scope,
            payload.clone(),
            &make_context(t0 + Duration::seconds(60)),
        )
        .await?;
    // Now jump past the interval.
    let outputs = enricher
        .reconcile(
            &mut state,
            &scope,
            payload.clone(),
            &make_context(t0 + Duration::seconds(400)),
        )
        .await?;

    assert_eq!(outputs.len(), 1);
    let snapshot = &outputs[0].payload;
    assert_eq!(snapshot.occurrence_count, 3);
    assert_eq!(snapshot.refined_category, EntityCategory::Tool);
    assert!(state.dirty_entities.is_empty(), "sweep clears dirty list");
    Ok(())
}

#[sinex_test]
async fn category_refinement_maps_known_types() -> TestResult<()> {
    let cases = [
        ("tool", EntityCategory::Tool),
        ("url", EntityCategory::Website),
        ("file", EntityCategory::Document),
        ("person", EntityCategory::Person),
        ("project", EntityCategory::Project),
    ];

    for (entity_type, expected) in cases {
        let mut enricher = enricher_with_interval(300);
        let mut state = EnricherState::default();
        let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
        let payload = resolved(entity_type, "subject");
        let scope = payload.entity_id.to_string();

        let outputs = enricher
            .reconcile(&mut state, &scope, payload, &make_context(t0))
            .await?;

        assert_eq!(outputs.len(), 1, "{entity_type}: should emit");
        assert_eq!(
            outputs[0].payload.refined_category, expected,
            "{entity_type} -> {expected:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn unknown_type_falls_back_to_document_category() -> TestResult<()> {
    let mut enricher = enricher_with_interval(300);
    let mut state = EnricherState::default();
    let t0 = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
    let payload = resolved("klingon-warship", "bird-of-prey");
    let scope = payload.entity_id.to_string();

    let outputs = enricher
        .reconcile(&mut state, &scope, payload, &make_context(t0))
        .await?;

    assert_eq!(
        outputs[0].payload.refined_category,
        EntityCategory::Document
    );
    Ok(())
}

#[sinex_test]
async fn active_hours_histogram_accumulates() -> TestResult<()> {
    // Use a long interval so subsequent observations accumulate without emitting.
    let mut enricher = enricher_with_interval(86_400);
    let mut state = EnricherState::default();
    // Pick a base time at the start of an hour for predictability.
    let base = Timestamp::from_unix_timestamp(1_800_000_000).expect("valid ts");
    let payload = resolved("tool", "git");
    let scope = payload.entity_id.to_string();

    // First call: also performs sweep (last_sweep None).
    enricher
        .reconcile(&mut state, &scope, payload.clone(), &make_context(base))
        .await?;
    // Three more observations within the same hour.
    for offset in [60_i64, 120, 180] {
        enricher
            .reconcile(
                &mut state,
                &scope,
                payload.clone(),
                &make_context(base + Duration::seconds(offset)),
            )
            .await?;
    }

    let stats = state.entities.get(&scope).unwrap();
    assert_eq!(stats.occurrence_count, 4);
    let total_active: u64 = stats.active_hours.values().sum();
    assert_eq!(total_active, 4);
    Ok(())
}

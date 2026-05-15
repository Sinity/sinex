//! Tests for the entity resolver — the second stage of the entity intelligence pipeline.
//!
//! Verifies deterministic UUIDv5 identity assignment, type-aware canonicalization,
//! deduplication via the persistent `known_entities` map, and `WindowedNode` semantics
//! (accumulate stages a pending payload, window_complete flips, emit returns + clears it).

use sinex_node_sdk::WindowedNode;
use sinex_node_sdk::derived_node::DerivedTriggerContext;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EntityTypeName, ProcessingMode, TriggerKind};
use sinex_primitives::events::payloads::{EntityExtractedPayload, EntityResolvedPayload};
use sinex_primitives::events::{Event, EventPayload};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{Id, JsonValue};
use sinex_process::automata::entity_resolver::{EntityResolver, ResolverState};
use xtask::sandbox::prelude::*;

fn make_context() -> DerivedTriggerContext {
    let event_id: Id<Event<JsonValue>> = Id::new();
    DerivedTriggerContext {
        trigger_event_id: event_id,
        source: EntityExtractedPayload::SOURCE,
        event_type: EntityExtractedPayload::EVENT_TYPE,
        ts_orig: Some(Timestamp::now()),
        ts_coided: event_id.timestamp(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
    }
}

fn extracted(entity_type: &str, raw_name: &str) -> EntityExtractedPayload {
    EntityExtractedPayload {
        entity_type: EntityTypeName::new(entity_type),
        raw_name: raw_name.to_string(),
        confidence: 0.95,
    }
}

async fn drive(
    resolver: &mut EntityResolver,
    state: &mut ResolverState,
    input: EntityExtractedPayload,
) -> Option<EntityResolvedPayload> {
    let ctx = make_context();
    resolver
        .accumulate(state, input, &ctx)
        .await
        .expect("accumulate");
    if resolver.window_complete(state) {
        resolver
            .emit(state, &ctx)
            .await
            .expect("emit")
            .map(|out| out.payload)
    } else {
        None
    }
}

#[sinex_test]
async fn accumulate_stages_pending_and_window_completes() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();

    let ctx = make_context();
    resolver
        .accumulate(&mut state, extracted("tool", "git"), &ctx)
        .await?;

    assert!(resolver.window_complete(&state));
    assert_eq!(state.candidates_processed, 1);
    Ok(())
}

#[sinex_test]
async fn emit_clears_pending_and_returns_resolved_payload() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();

    let payload = drive(&mut resolver, &mut state, extracted("tool", "git"))
        .await
        .expect("first emission");
    assert_eq!(payload.entity_type, EntityTypeName::new("tool"));
    assert_eq!(payload.canonical_name, "git");
    assert_eq!(payload.original_name, "git");
    assert!(!resolver.window_complete(&state));
    Ok(())
}

#[sinex_test]
async fn duplicate_entity_skipped_via_dedup_map() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();

    drive(&mut resolver, &mut state, extracted("tool", "git"))
        .await
        .expect("first");
    let second = drive(&mut resolver, &mut state, extracted("tool", "git")).await;

    assert!(second.is_none(), "duplicate must not produce a new resolved payload");
    assert_eq!(state.candidates_processed, 1);
    assert_eq!(state.known_entities.len(), 1);
    Ok(())
}

#[sinex_test]
async fn identity_is_deterministic_uuidv5() -> TestResult<()> {
    let payload_a = {
        let mut r = EntityResolver::default();
        let mut s = ResolverState::default();
        drive(&mut r, &mut s, extracted("tool", "git")).await.unwrap()
    };
    let payload_b = {
        let mut r = EntityResolver::default();
        let mut s = ResolverState::default();
        drive(&mut r, &mut s, extracted("tool", "git")).await.unwrap()
    };

    assert_eq!(
        payload_a.entity_id, payload_b.entity_id,
        "same (type, canonical_name) must yield the same UUIDv5"
    );

    let expected = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"tool:git");
    assert_eq!(payload_a.entity_id, expected);
    Ok(())
}

#[sinex_test]
async fn tool_canonicalization_lowercases_and_trims() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();
    let out = drive(&mut resolver, &mut state, extracted("tool", "  GIT  "))
        .await
        .unwrap();
    assert_eq!(out.canonical_name, "git");
    assert_eq!(out.original_name, "  GIT  ");
    Ok(())
}

#[sinex_test]
async fn url_canonicalization_normalizes_host() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();
    let out = drive(
        &mut resolver,
        &mut state,
        extracted("url", "https://www.Example.COM/path/to/page"),
    )
    .await
    .unwrap();
    assert_eq!(out.canonical_name, "example.com");
    Ok(())
}

#[sinex_test]
async fn file_canonicalization_preserves_path_case() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();
    let out = drive(
        &mut resolver,
        &mut state,
        extracted("file", "/Home/User/Notes.md"),
    )
    .await
    .unwrap();
    assert_eq!(out.canonical_name, "/Home/User/Notes.md");
    Ok(())
}

#[sinex_test]
async fn different_types_with_same_name_are_distinct_entities() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();

    let tool = drive(&mut resolver, &mut state, extracted("tool", "git"))
        .await
        .unwrap();
    let url = drive(&mut resolver, &mut state, extracted("url", "git"))
        .await
        .unwrap();

    assert_ne!(tool.entity_id, url.entity_id);
    assert_eq!(state.known_entities.len(), 2);
    Ok(())
}

#[sinex_test]
async fn emit_returns_none_when_no_pending() -> TestResult<()> {
    let mut resolver = EntityResolver::default();
    let mut state = ResolverState::default();
    let ctx = make_context();
    let result = resolver.emit(&mut state, &ctx).await?;
    assert!(result.is_none());
    Ok(())
}

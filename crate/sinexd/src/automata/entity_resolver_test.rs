use super::*;
use sinex_primitives::Timestamp;
use sinex_primitives::domain::{EventSource, EventType, ProcessingMode, TriggerKind};
use sinex_primitives::events::Event;
use sinex_primitives::temporal::Duration;
use sinex_primitives::{Id, JsonValue};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn resolved_entity_provenance_uses_trigger_event_not_entity_id() -> TestResult<()> {
    let mut resolver = EntityResolver;
    let mut state = ResolverState::default();
    let context = AutomatonContext::timer_flush(Timestamp::now())?;
    let trigger_id = context.trigger_uuid();

    resolver
        .accumulate(
            &mut state,
            EntityExtractedPayload {
                entity_type: EntityTypeName::new("tool"),
                raw_name: "Nix".to_string(),
                confidence: 0.9,
            },
            &context,
        )
        .await?;

    let output = resolver
        .emit(&mut state, &context)
        .await?
        .expect("unique extracted entity should resolve");

    assert_eq!(output.source_event_ids, vec![trigger_id]);
    assert_ne!(output.source_event_ids, vec![output.payload.entity_id]);
    assert_eq!(output.payload.entity_id.get_version_num(), 5);
    assert_eq!(trigger_id.get_version_num(), 7);
    Ok(())
}

/// sinex-audit-entity-unbounded-maps: `known_entities` must never grow past
/// `MAX_KNOWN_ENTITIES`, even when the automaton observes far more distinct
/// entities than that. If the eviction guard in `accumulate` were removed
/// (or a caller reverted `MAX_KNOWN_ENTITIES` back to `usize::MAX`), this
/// test fails because `state.known_entities.len()` would grow to
/// `MAX_KNOWN_ENTITIES + 500` instead of staying capped.
#[sinex_test]
async fn known_entities_map_is_bounded_under_high_cardinality() -> TestResult<()> {
    let mut resolver = EntityResolver;
    let mut state = ResolverState::default();
    let base = Timestamp::now();

    let overflow = 500usize;
    for i in 0..(MAX_KNOWN_ENTITIES + overflow) {
        let context = AutomatonContext::timer_flush(base + Duration::seconds(i as i64))?;
        resolver
            .accumulate(
                &mut state,
                EntityExtractedPayload {
                    entity_type: EntityTypeName::new("tool"),
                    raw_name: format!("tool-{i}"),
                    confidence: 0.9,
                },
                &context,
            )
            .await?;
        // Each accumulate stages exactly one pending resolution; drain it so
        // the loop behaves like the real pipeline (one input -> one emit).
        resolver.emit(&mut state, &context).await?;
    }

    assert!(
        state.known_entities.len() <= MAX_KNOWN_ENTITIES,
        "known_entities grew to {} which exceeds the {} bound -- the eviction guard is not \
         bounding the map",
        state.known_entities.len(),
        MAX_KNOWN_ENTITIES,
    );

    // The earliest-inserted entities should have been evicted as stalest;
    // the most recently inserted ones should still be present.
    let evicted_key = canonical_key(&EntityTypeName::new("tool"), "tool-0");
    assert!(
        !state.known_entities.contains_key(&evicted_key),
        "the stalest entity should have been evicted, not retained"
    );
    let retained_key = canonical_key(
        &EntityTypeName::new("tool"),
        &format!("tool-{}", MAX_KNOWN_ENTITIES + overflow - 1),
    );
    assert!(
        state.known_entities.contains_key(&retained_key),
        "the most recently resolved entity should be retained, not evicted"
    );

    Ok(())
}

fn context_with_ts_orig(ts_orig: Option<Timestamp>) -> AutomatonContext {
    AutomatonContext {
        trigger_event_id: Id::<Event<JsonValue>>::new(),
        source: EventSource::from_static("test.source"),
        event_type: EventType::from_static("test.type"),
        ts_orig,
        ts_coided: Timestamp::now(),
        processing_mode: ProcessingMode::Live,
        trigger_kind: TriggerKind::NewEvent,
        created_by_operation_id: None,
        trigger_material_id: None,
        trigger_anchor_byte: None,
    }
}

/// sinex-g0ve (closed invalid): the original citation of this file's line 146
/// (`touch_time = context.ts_orig.unwrap_or_else(Timestamp::now)`) as ts_orig
/// fabrication was doubly wrong -- `touch_time` never reaches an emitted
/// event's `ts_orig` at all. It only orders the in-memory eviction cache
/// (`KnownEntity::last_touched`, already doc-commented as an intentional
/// fallback so eviction never blocks on a missing source timestamp). The
/// emitted `entity.resolved` event's `ts_orig` comes from
/// `DerivedOutput::windowed_now`, which always synthesizes wall-clock time
/// regardless of `touch_time` or `context.ts_orig` -- proven below.
#[sinex_test]
async fn accumulate_with_missing_context_ts_orig_does_not_affect_emitted_event_ts_orig()
-> TestResult<()> {
    let mut resolver = EntityResolver;
    let mut state = ResolverState::default();
    let context = context_with_ts_orig(None);

    resolver
        .accumulate(
            &mut state,
            EntityExtractedPayload {
                entity_type: EntityTypeName::new("tool"),
                raw_name: "Nix".to_string(),
                confidence: 0.9,
            },
            &context,
        )
        .await?;

    // touch_time fell back to wall-clock without panicking or leaving the
    // cache entry unset -- the entity was accepted into known_entities.
    let key = canonical_key(&EntityTypeName::new("tool"), "nix");
    assert!(
        state.known_entities.contains_key(&key),
        "entity should still be tracked when the trigger context carries no ts_orig"
    );

    let before = Timestamp::now();
    let output = resolver
        .emit(&mut state, &context)
        .await?
        .expect("unique extracted entity should resolve");
    let after = Timestamp::now();

    assert!(
        output.ts_orig >= before && output.ts_orig <= after,
        "emitted entity.resolved ts_orig should be wall-clock synthesis time regardless of \
         context.ts_orig, got {:?} outside [{before:?}, {after:?}]",
        output.ts_orig
    );
    Ok(())
}

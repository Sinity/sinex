use super::*;
use sinex_primitives::Timestamp;
use sinex_primitives::temporal::Duration;
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

use super::*;
use sinex_primitives::domain::EntityTypeName;
use sinex_primitives::temporal::Duration;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn enriched_entity_provenance_uses_trigger_event_not_entity_id() -> TestResult<()> {
    let mut enricher = EntityEnricher::default();
    let mut state = EnricherState::default();
    let now = Timestamp::now();
    let context = AutomatonContext::timer_flush(now)?;
    let trigger_id = context.trigger_uuid();

    let entity_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"tool:nix");
    let outputs = enricher
        .reconcile(
            &mut state,
            &entity_id.to_string(),
            EntityResolvedPayload {
                entity_id,
                canonical_name: "nix".to_string(),
                entity_type: EntityTypeName::new("tool"),
                original_name: "Nix".to_string(),
            },
            &context,
        )
        .await?;

    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].source_event_ids, vec![trigger_id]);
    assert_ne!(outputs[0].source_event_ids, vec![entity_id]);
    assert_eq!(entity_id.get_version_num(), 5);
    assert_eq!(trigger_id.get_version_num(), 7);
    Ok(())
}

/// sinex-audit-entity-unbounded-maps: `entities` must never grow past
/// `MAX_TRACKED_ENTITIES`, even when far more distinct entities are observed
/// than that. If the eviction guard in `reconcile` were removed (or
/// `MAX_TRACKED_ENTITIES` reverted to `usize::MAX`), this test fails because
/// `state.entities.len()` would grow to `MAX_TRACKED_ENTITIES + 500` instead
/// of staying capped.
#[sinex_test]
async fn entities_map_is_bounded_under_high_cardinality() -> TestResult<()> {
    let mut enricher = EntityEnricher::default();
    let mut state = EnricherState::default();
    let base = Timestamp::now();

    let overflow = 500usize;
    let mut first_entity_id = None;
    let mut last_entity_id = None;
    for i in 0..(MAX_TRACKED_ENTITIES + overflow) {
        let now = base + Duration::seconds(i as i64);
        let context = AutomatonContext::timer_flush(now)?;
        let entity_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, format!("tool:tool-{i}").as_bytes());
        if i == 0 {
            first_entity_id = Some(entity_id);
        }
        last_entity_id = Some(entity_id);

        enricher
            .reconcile(
                &mut state,
                &entity_id.to_string(),
                EntityResolvedPayload {
                    entity_id,
                    canonical_name: format!("tool-{i}"),
                    entity_type: EntityTypeName::new("tool"),
                    original_name: format!("tool-{i}"),
                },
                &context,
            )
            .await?;
    }

    assert!(
        state.entities.len() <= MAX_TRACKED_ENTITIES,
        "entities grew to {} which exceeds the {} bound -- the eviction guard is not bounding \
         the map",
        state.entities.len(),
        MAX_TRACKED_ENTITIES,
    );

    let first_key = first_entity_id.expect("loop ran at least once").to_string();
    assert!(
        !state.entities.contains_key(&first_key),
        "the stalest entity should have been evicted, not retained"
    );
    let last_key = last_entity_id.expect("loop ran at least once").to_string();
    assert!(
        state.entities.contains_key(&last_key),
        "the most recently observed entity should be retained, not evicted"
    );

    Ok(())
}

use super::*;
use sinex_primitives::domain::EntityTypeName;
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

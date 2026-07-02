use super::*;
use sinex_primitives::Timestamp;
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

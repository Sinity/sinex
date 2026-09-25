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

/// sinex-nbi.5 regression: verify that the enricher's active-hours histogram
/// uses the operator's civil hour and that emitted metadata names that same
/// timezone. Explicit Europe/Warsaw fixed-offset and DST cases below keep
/// timezone behavior deterministic regardless of the process environment.
#[sinex_test]
async fn active_hours_buckets_by_operator_local_hour_not_utc_sinex_nbi_5() -> TestResult<()> {
    use crate::automata::civil::{civil_hour_of_day, floor_to_civil_day, floor_to_civil_hour};
    use sinex_primitives::temporal::parse_rfc3339;

    // Winter 23:30 UTC is midnight hour 0 in Europe/Warsaw (UTC+1).
    let now = parse_rfc3339("2026-01-15T23:30:00Z").expect("valid timestamp");
    assert_eq!(civil_hour_of_day(now, "Europe/Warsaw"), Some(0));

    let local_hour_start = floor_to_civil_hour(now);
    let local_day_start = floor_to_civil_day(now);
    let expected_local_hour =
        ((local_hour_start.unix_timestamp() - local_day_start.unix_timestamp()) / 3600) as u8;
    let utc_hour = ((now.unix_timestamp() / 3600) % 24) as u8;
    let mut enricher = EntityEnricher::default();
    let mut state = EnricherState::default();
    let context = AutomatonContext::timer_flush(now)?;
    let entity_id = Uuid::new_v5(&Uuid::NAMESPACE_OID, b"tool:nbi5-fixture");

    let outputs = enricher
        .reconcile(
            &mut state,
            &entity_id.to_string(),
            EntityResolvedPayload {
                entity_id,
                canonical_name: "nbi5-fixture".to_string(),
                entity_type: EntityTypeName::new("tool"),
                original_name: "nbi5-fixture".to_string(),
            },
            &context,
        )
        .await?;

    let stats = state
        .entities
        .get(&entity_id.to_string())
        .expect("entity must be tracked after reconcile");

    assert!(
        stats.active_hours.contains_key(&expected_local_hour),
        "sinex-nbi.5: expected the operator-local hour {expected_local_hour} to be bucketed, \
         got buckets {:?} (raw UTC hour {utc_hour} was bucketed instead)",
        stats.active_hours.keys().collect::<Vec<_>>()
    );
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].payload.tz_id,
        crate::automata::civil::operator_tz(),
        "payload must identify the canonical civil timezone"
    );
    assert_eq!(outputs[0].semantics_version.as_deref(), Some("2.0.0"));

    Ok(())
}

/// Both occurrences of Warsaw's repeated 02:00 fall-back hour map to hour 2.
#[sinex_test]
async fn civil_hour_of_day_handles_warsaw_fall_back() -> TestResult<()> {
    use crate::automata::civil::civil_hour_of_day;
    use sinex_primitives::temporal::parse_rfc3339;

    let first = parse_rfc3339("2024-10-27T00:30:00Z").expect("valid timestamp");
    let second = parse_rfc3339("2024-10-27T01:30:00Z").expect("valid timestamp");
    assert_eq!(civil_hour_of_day(first, "Europe/Warsaw"), Some(2));
    assert_eq!(civil_hour_of_day(second, "Europe/Warsaw"), Some(2));
    Ok(())
}

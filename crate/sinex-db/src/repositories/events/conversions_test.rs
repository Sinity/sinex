// Inline because these conversions depend directly on generated row shapes.
use super::*;
use crate::schema::defs::events::EventRecord;
use sinex_primitives::temporal;
use xtask::sandbox::sinex_test;

fn base_event_record() -> EventRecord {
    let now = temporal::now();
    EventRecord {
        id: uuid::Uuid::now_v7(),
        source: "test.source".to_string(),
        event_type: "test.event".to_string(),
        host: "test-host".to_string(),
        payload: serde_json::json!({"ok": true}),
        ts_orig: now,
        ts_orig_subnano: None,
        ts_quality: None,
        ts_coided: now,
        ts_persisted: now,
        source_material_id: Some(uuid::Uuid::now_v7()),
        anchor_byte: Some(0),
        offset_start: None,
        offset_end: None,
        offset_kind: None,
        source_event_ids: None,
        associated_blob_ids: None,
        payload_schema_id: None,
        module_run_id: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        anchor_payload_hash: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    }
}

#[sinex_test]
async fn test_try_to_event_rejects_invalid_temporal_policy() -> xtask::sandbox::TestResult<()> {
    let mut record = base_event_record();
    record.temporal_policy = Some("not-a-policy".to_string());

    let error = record.try_to_event().unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("event record has invalid temporal_policy"));
    assert!(rendered.contains("value: not-a-policy"));
    Ok(())
}

#[sinex_test]
async fn test_try_to_event_rejects_invalid_automaton_model() -> xtask::sandbox::TestResult<()> {
    let mut record = base_event_record();
    record.automaton_model = Some("not-a-model".to_string());

    let error = record.try_to_event().unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("event record has invalid automaton_model"));
    assert!(rendered.contains("value: not-a-model"));
    Ok(())
}

#[sinex_test]
async fn test_try_to_event_rejects_invalid_product_class() -> xtask::sandbox::TestResult<()> {
    let mut record = base_event_record();
    record.product_class = Some("not-a-product-class".to_string());

    let error = record.try_to_event().unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("event record has invalid product_class"));
    assert!(rendered.contains("value: not-a-product-class"));
    Ok(())
}

#[sinex_test]
async fn test_try_to_event_rejects_invalid_claim_support() -> xtask::sandbox::TestResult<()> {
    let mut record = base_event_record();
    record.claim_support = Some(serde_json::json!({"not": "a claim support vector"}));

    let error = record.try_to_event().unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("event record has invalid claim_support"));
    Ok(())
}

/// sinex-8cr.2: `try_to_event` must parse a concrete non-null
/// `product_class`/`claim_support`/`derivation_declaration_id`/
/// `derivation_epoch_id`/`derivation_lane_id`/`adjudication_event_id` back
/// into their typed `Event<T>` counterparts, not just pass `None` through.
#[sinex_test]
async fn test_try_to_event_parses_derivation_control_plane_fields()
-> xtask::sandbox::TestResult<()> {
    use sinex_primitives::derivation::{
        AdjudicationStatus, ClaimSupport, ClaimTemporalQuality, DerivedProductClass,
        SourceCoverage, SupportLevel,
    };

    let claim_support = ClaimSupport::unreviewed(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        5,
        2,
        1,
        0,
    );
    let epoch_id = uuid::Uuid::now_v7();
    let lane_id = uuid::Uuid::now_v7();

    let mut record = base_event_record();
    record.product_class = Some(DerivedProductClass::AnalysisClaim.as_str().to_string());
    record.claim_support = Some(serde_json::to_value(&claim_support)?);
    record.derivation_declaration_id = Some("sinex.test.conversion".to_string());
    record.derivation_epoch_id = Some(epoch_id);
    record.derivation_lane_id = Some(lane_id);

    let event = record.try_to_event()?;
    assert_eq!(event.product_class, Some(DerivedProductClass::AnalysisClaim));
    assert_eq!(event.claim_support, Some(claim_support));
    assert_eq!(
        event.claim_support.as_ref().map(ClaimSupport::adjudication),
        Some(AdjudicationStatus::Unreviewed)
    );
    assert_eq!(
        event.derivation_declaration_id.as_deref(),
        Some("sinex.test.conversion")
    );
    assert_eq!(event.derivation_epoch_id, Some(epoch_id));
    assert_eq!(event.derivation_lane_id, Some(lane_id));
    assert_eq!(event.adjudication_event_id, None);
    Ok(())
}

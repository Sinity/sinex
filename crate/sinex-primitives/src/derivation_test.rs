#![allow(clippy::unwrap_used)]

use super::*;
use crate::Uuid;
use xtask::sandbox::sinex_test;

// ─── DerivedProductClass ───────────────────────────────────────────────────

#[sinex_test]
async fn derived_product_class_serde_round_trips_exact_vocabulary() -> TestResult<()> {
    let expected = [
        (DerivedProductClass::CanonicalDerivedEvent, "canonical_derived_event"),
        (DerivedProductClass::ProjectionRow, "projection_row"),
        (DerivedProductClass::AnalysisClaim, "analysis_claim"),
        (DerivedProductClass::ReportArtifact, "report_artifact"),
        (DerivedProductClass::SemanticCandidate, "semantic_candidate"),
        (DerivedProductClass::OperatorJudgment, "operator_judgment"),
    ];

    for (variant, wire) in expected {
        assert_eq!(variant.as_str(), wire);
        assert_eq!(variant.to_string(), wire);

        let json = serde_json::to_value(variant).unwrap();
        assert_eq!(json, serde_json::Value::String(wire.to_string()));

        let round_tripped: DerivedProductClass = serde_json::from_value(json).unwrap();
        assert_eq!(round_tripped, variant);
    }

    Ok(())
}

#[sinex_test]
async fn derived_product_class_only_canonical_is_default_input_eligible() -> TestResult<()> {
    assert!(DerivedProductClass::CanonicalDerivedEvent.default_canonical_input_eligible());
    assert!(!DerivedProductClass::ProjectionRow.default_canonical_input_eligible());
    assert!(!DerivedProductClass::AnalysisClaim.default_canonical_input_eligible());
    assert!(!DerivedProductClass::ReportArtifact.default_canonical_input_eligible());
    assert!(!DerivedProductClass::SemanticCandidate.default_canonical_input_eligible());
    assert!(!DerivedProductClass::OperatorJudgment.default_canonical_input_eligible());
    Ok(())
}

// ─── ClaimSupport ───────────────────────────────────────────────────────────

#[sinex_test]
async fn claim_support_default_is_unknown_and_unreviewed() -> TestResult<()> {
    // Candidate-confidence doctrine: defaults must be unknown/low, never a
    // fabricated Direct/Covered value with empty evidence refs.
    let support = ClaimSupport::default();
    assert_eq!(support, ClaimSupport::unknown());
    assert_eq!(support.support_level(), SupportLevel::Unsupported);
    assert_eq!(support.source_coverage(), SourceCoverage::Unknown);
    assert_eq!(support.temporal_quality(), ClaimTemporalQuality::Unknown);
    assert_eq!(support.adjudication(), AdjudicationStatus::Unreviewed);
    assert_eq!(support.evidence_event_count(), 0);
    assert_eq!(support.evidence_material_count(), 0);
    assert_eq!(support.support_family_count(), 0);
    assert_eq!(support.counterevidence_count(), 0);
    assert_eq!(support.adjudication_event_id(), None);
    assert!(support.is_shape_valid());
    Ok(())
}

#[sinex_test]
async fn claim_support_adjudicated_rejects_unreviewed_status() -> TestResult<()> {
    let err = ClaimSupport::adjudicated(
        SupportLevel::Direct,
        SourceCoverage::Covered,
        ClaimTemporalQuality::RealtimeCapture,
        AdjudicationStatus::Unreviewed,
        Id::<Event>::new(),
        1,
        1,
        0,
        0,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("use ClaimSupport::unreviewed"),
        "expected guidance toward unreviewed(), got: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn claim_support_adjudicated_carries_typed_judgment_event_id() -> TestResult<()> {
    let judgment_event_id = Id::<Event>::from_uuid(Uuid::from_u128(42));
    let support = ClaimSupport::adjudicated(
        SupportLevel::Heuristic,
        SourceCoverage::Partial,
        ClaimTemporalQuality::WindowBoundary,
        AdjudicationStatus::Accepted,
        judgment_event_id,
        3,
        1,
        0,
        1,
    )
    .unwrap();

    assert_eq!(support.adjudication(), AdjudicationStatus::Accepted);
    assert_eq!(support.adjudication_event_id(), Some(judgment_event_id));
    assert!(support.is_shape_valid());
    Ok(())
}

#[sinex_test]
async fn claim_support_deserialized_without_judgment_id_is_shape_invalid() -> TestResult<()> {
    // A wire payload can claim `accepted` without ever going through
    // `ClaimSupport::adjudicated` (private-field construction only blocks
    // Rust struct literals, not serde). `is_shape_valid` is the runtime
    // boundary check that catches this — mirrors the DB trigger invariant.
    let json = serde_json::json!({
        "support_level": "direct",
        "source_coverage": "covered",
        "temporal_quality": "realtime_capture",
        "adjudication": "accepted",
        "evidence_event_count": 1,
        "evidence_material_count": 0,
        "support_family_count": 0,
        "counterevidence_count": 0
    });
    let support: ClaimSupport = serde_json::from_value(json).unwrap();
    assert_eq!(support.adjudication(), AdjudicationStatus::Accepted);
    assert_eq!(support.adjudication_event_id(), None);
    assert!(
        !support.is_shape_valid(),
        "an accepted vector without an adjudication_event_id must be shape-invalid"
    );
    Ok(())
}

#[sinex_test]
async fn claim_support_template_instantiate_produces_unreviewed_vector() -> TestResult<()> {
    let template = ClaimSupportTemplate::new(
        SupportLevel::Convergent,
        SourceCoverage::Covered,
        ClaimTemporalQuality::InheritParent,
    );
    let support = template.instantiate(4, 2, 1, 0);

    assert_eq!(support.support_level(), SupportLevel::Convergent);
    assert_eq!(support.source_coverage(), SourceCoverage::Covered);
    assert_eq!(support.temporal_quality(), ClaimTemporalQuality::InheritParent);
    assert_eq!(support.adjudication(), AdjudicationStatus::Unreviewed);
    assert_eq!(support.evidence_event_count(), 4);
    assert_eq!(support.evidence_material_count(), 2);
    assert_eq!(support.support_family_count(), 1);
    assert_eq!(support.adjudication_event_id(), None);
    Ok(())
}

#[sinex_test]
async fn claim_support_template_unknown_baseline_matches_claim_support_unknown() -> TestResult<()>
{
    let instantiated = ClaimSupportTemplate::UNKNOWN.instantiate(0, 0, 0, 0);
    assert_eq!(instantiated, ClaimSupport::unknown());
    Ok(())
}

// ─── DerivationOutputDeclaration ────────────────────────────────────────────

const fn valid_derived_output_declaration() -> DerivationOutputDeclaration {
    DerivationOutputDeclaration {
        declaration_id: "test.fixture.canonical",
        owner: "test-fixture",
        product_class: DerivedProductClass::CanonicalDerivedEvent,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("test-fixture"),
        output_event_type: Some("test.fixture.output"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "v1",
        input_eligibility: InputEligibility::DefaultCanonicalInput,
        default_support: ClaimSupportTemplate::UNKNOWN,
        verification_command: "xtask test -p sinex-primitives -E 'test(fixture)'",
    }
}

#[sinex_test]
async fn derivation_output_declaration_valid_fixture_passes_validate() -> TestResult<()> {
    valid_derived_output_declaration().validate().unwrap();
    Ok(())
}

#[sinex_test]
async fn derivation_output_declaration_requires_output_identity_for_derived_output_surface()
-> TestResult<()> {
    let mut declaration = valid_derived_output_declaration();
    declaration.output_source = None;
    let err = declaration.validate().unwrap_err();
    assert!(
        err.to_string().contains("output_source"),
        "expected an output_source complaint, got: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn derivation_output_declaration_rejects_output_identity_on_non_derived_output_surface()
-> TestResult<()> {
    let mut declaration = valid_derived_output_declaration();
    declaration.write_surface = DerivationWriteSurface::ArtifactWriter;
    declaration.artifact_kind = Some("test.fixture.artifact");
    // output_source/output_event_type are still Some from the base fixture,
    // which the biconditional check must reject for a non-derived-output
    // write surface.
    let err = declaration.validate().unwrap_err();
    assert!(err.to_string().contains("output_source"));
    Ok(())
}

#[sinex_test]
async fn derivation_output_declaration_projection_row_requires_projection_kind() -> TestResult<()>
{
    let mut declaration = valid_derived_output_declaration();
    declaration.product_class = DerivedProductClass::ProjectionRow;
    declaration.write_surface = DerivationWriteSurface::ProjectionWriter;
    declaration.output_source = None;
    declaration.output_event_type = None;
    let err = declaration.validate().unwrap_err();
    assert!(err.to_string().contains("projection_kind"));

    declaration.projection_kind = Some("test.fixture.projection");
    declaration.validate().unwrap();
    Ok(())
}

// ─── DerivationScope ────────────────────────────────────────────────────────

#[sinex_test]
async fn derivation_scope_input_set_hash_is_uniform_across_every_variant() -> TestResult<()> {
    let scopes = [
        DerivationScope::EventSet {
            input_ids: vec!["evt-1".to_string()],
            input_set_hash: "hash-event-set".to_string(),
        },
        DerivationScope::SourceMaterialSet {
            input_ids: vec!["mat-1".to_string()],
            input_set_hash: "hash-material-set".to_string(),
        },
        DerivationScope::DocumentChunkSet {
            input_ids: vec!["chunk-1".to_string()],
            input_set_hash: "hash-chunk-set".to_string(),
        },
        DerivationScope::StreamCheckpoint {
            stream: "core.events.confirmed".to_string(),
            filter_subjects: vec!["entity.>".to_string()],
            start_seq: 0,
            end_seq: Some(100),
            coverage_window: None,
            input_set_hash: "hash-stream-checkpoint".to_string(),
        },
        DerivationScope::TimeWindow {
            bucket: "hourly".to_string(),
            start: Timestamp::UNIX_EPOCH,
            end: Timestamp::UNIX_EPOCH,
            input_set_hash: "hash-time-window".to_string(),
        },
        DerivationScope::ScopeReconcilerKey {
            scope_key: "entity:abc".to_string(),
            input_set_hash: "hash-scope-reconciler".to_string(),
        },
        DerivationScope::ProjectionScope {
            projection_kind: "documents".to_string(),
            scope_key: "doc:abc".to_string(),
            input_set_hash: "hash-projection-scope".to_string(),
        },
    ];

    let expected_hashes = [
        "hash-event-set",
        "hash-material-set",
        "hash-chunk-set",
        "hash-stream-checkpoint",
        "hash-time-window",
        "hash-scope-reconciler",
        "hash-projection-scope",
    ];
    let expected_models = [
        "event_set",
        "source_material_set",
        "document_chunk_set",
        "stream_checkpoint",
        "time_window",
        "scope_reconciler_key",
        "projection_scope",
    ];

    for ((scope, expected_hash), expected_model) in scopes
        .iter()
        .zip(expected_hashes.iter())
        .zip(expected_models.iter())
    {
        assert_eq!(scope.input_set_hash(), *expected_hash);
        assert_eq!(scope.scope_model(), *expected_model);
    }

    Ok(())
}

#[sinex_test]
async fn derivation_scope_stream_checkpoint_distinguishes_frozen_vs_open_ended() -> TestResult<()>
{
    let live = DerivationScope::StreamCheckpoint {
        stream: "core.events.confirmed".to_string(),
        filter_subjects: vec!["entity.>".to_string()],
        start_seq: 0,
        end_seq: None,
        coverage_window: None,
        input_set_hash: "hash-live".to_string(),
    };
    let frozen = DerivationScope::StreamCheckpoint {
        stream: "core.events.confirmed".to_string(),
        filter_subjects: vec!["entity.>".to_string()],
        start_seq: 0,
        end_seq: Some(500),
        coverage_window: Some(TstzRange::new(Timestamp::UNIX_EPOCH, Timestamp::UNIX_EPOCH).unwrap()),
        input_set_hash: "hash-frozen".to_string(),
    };

    assert!(live.is_open_ended_stream());
    assert!(!frozen.is_open_ended_stream());

    // Non-StreamCheckpoint variants are never "open-ended stream".
    let batch = DerivationScope::EventSet {
        input_ids: vec![],
        input_set_hash: "hash-batch".to_string(),
    };
    assert!(!batch.is_open_ended_stream());

    Ok(())
}

#[sinex_test]
async fn derivation_scope_serde_round_trips_stream_checkpoint_tag() -> TestResult<()> {
    let scope = DerivationScope::StreamCheckpoint {
        stream: "core.events.confirmed".to_string(),
        filter_subjects: vec!["entity.>".to_string(), "relation.>".to_string()],
        start_seq: 10,
        end_seq: Some(20),
        coverage_window: None,
        input_set_hash: "hash-round-trip".to_string(),
    };

    let json = serde_json::to_value(&scope).unwrap();
    assert_eq!(json["kind"], "stream_checkpoint");
    assert_eq!(json["start_seq"], 10);
    assert_eq!(json["end_seq"], 20);

    let round_tripped: DerivationScope = serde_json::from_value(json).unwrap();
    assert_eq!(round_tripped, scope);
    Ok(())
}

// ─── TstzRange ───────────────────────────────────────────────────────────

#[sinex_test]
async fn tstz_range_rejects_start_after_end() -> TestResult<()> {
    let later = Timestamp::now();
    let err = TstzRange::new(later, Timestamp::UNIX_EPOCH).unwrap_err();
    assert!(err.to_string().contains("start must not be after end"));
    Ok(())
}

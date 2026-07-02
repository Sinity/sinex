#![allow(clippy::unwrap_used)]

use super::*;
use crate::events::SourceMaterial;
use crate::events::builder::Provenance;
use crate::ids::Id;
use crate::{EventSource, EventType, HostName};
use serde_json::json;
use xtask::sandbox::sinex_test;

fn material_event(
    source: &str,
    event_type: &str,
    ts: Option<Timestamp>,
    quality: Option<TemporalSourceType>,
    payload: JsonValue,
) -> Event<JsonValue> {
    Event {
        id: Some(Id::<Event<JsonValue>>::new()),
        source: EventSource::new(source).unwrap(),
        event_type: EventType::new(event_type).unwrap(),
        payload,
        ts_orig: ts,
        ts_quality: quality,
        host: HostName::new("test-host").unwrap(),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::<SourceMaterial>::new(), 0, None, None),
        anchor_payload_hash: None,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
    }
}

fn at(secs: i64) -> Timestamp {
    Timestamp::from_unix_timestamp(1_700_000_000 + secs).unwrap()
}

#[sinex_test]
async fn observed_range_from_event_maps_quality_ladder() -> TestResult<()> {
    let exact = material_event(
        "shell.atuin",
        "command.executed",
        Some(at(0)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({}),
    );
    let range = ObservedRange::from_event(&exact);
    assert_eq!(range.basis, TimeBasis::SourceIntrinsic);
    assert_eq!(range.quality, TimeQuality::Exact);

    let staged = material_event(
        "fs.watcher",
        "file.created",
        Some(at(0)),
        Some(TemporalSourceType::StagedAt),
        json!({}),
    );
    let range = ObservedRange::from_event(&staged);
    assert_eq!(range.basis, TimeBasis::StagingTime);
    assert_eq!(range.quality, TimeQuality::Coarse);

    let untimed = material_event("fs.watcher", "file.created", None, None, json!({}));
    let range = ObservedRange::from_event(&untimed);
    assert_eq!(range.basis, TimeBasis::MaterialAnchor);
    assert_eq!(range.quality, TimeQuality::Unknown);
    assert!(!range.is_timed());

    Ok(())
}

#[sinex_test]
async fn overlaps_and_gap_semantics() -> TestResult<()> {
    let a = ObservedRange::point(at(0), TimeBasis::SourceIntrinsic, TimeQuality::Exact);
    let b = ObservedRange::point(at(0), TimeBasis::SourceIntrinsic, TimeQuality::Exact);
    assert!(a.overlaps(&b));
    assert_eq!(a.gap_to(&b), Some(Duration::ZERO));

    let c = ObservedRange::point(at(120), TimeBasis::SourceIntrinsic, TimeQuality::Exact);
    assert!(!a.overlaps(&c));
    assert_eq!(a.gap_to(&c), Some(Duration::seconds(120)));

    let untimed = ObservedRange::unknown(TimeBasis::MaterialAnchor);
    assert!(!a.overlaps(&untimed));
    assert_eq!(a.gap_to(&untimed), None);

    Ok(())
}

/// Deterministic fixture from the design doc: commands run while discussing a
/// topic, with one contradiction and one caveat.
#[sinex_test]
async fn within_relation_assembles_evidence_window_with_contradiction_and_caveat()
-> TestResult<()> {
    // Seed: a "discussing topic X" event.
    let seed = material_event(
        "chat.session",
        "message.sent",
        Some(at(0)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({ "topic": "X" }),
    );
    // Candidates: two commands near the seed, one far away, one untimed.
    let near = material_event(
        "shell.atuin",
        "command.executed",
        Some(at(30)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({ "command": "cargo test" }),
    );
    let far = material_event(
        "shell.atuin",
        "command.executed",
        Some(at(7200)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({ "command": "git push" }),
    );
    let untimed = material_event(
        "shell.atuin",
        "command.executed",
        None,
        None,
        json!({ "command": "unknown when" }),
    );

    let expr = EventRelationExpr::Within { within_secs: 300 };
    let window = expr
        .evaluate(&[seed], &[near.clone(), far, untimed])
        .with_contradiction(
            SinexObjectRef::new(SinexObjectKind::Event, "contradiction-1"),
            ObservedRange::point(at(60), TimeBasis::SourceIntrinsic, TimeQuality::Exact),
            "operator: this command argues against topic X",
        );

    // Only the near command supports.
    assert_eq!(window.support_refs.len(), 1);
    assert_eq!(window.support_refs[0].role, EvidenceRole::Support);
    // The untimed candidate became a coverage caveat.
    assert!(
        window
            .caveats
            .iter()
            .any(|c| c.id == "evidence.timing_unknown")
    );
    // The contradiction was recorded (explicit, never inferred).
    assert_eq!(window.contradiction_refs.len(), 1);
    assert_eq!(
        window.contradiction_refs[0].role,
        EvidenceRole::Contradiction
    );
    // The trace explains every inclusion.
    assert!(
        window
            .expansion_trace
            .steps
            .iter()
            .any(|s| s.kind == ExpansionStepKind::SeedMatched)
    );
    assert!(
        window
            .expansion_trace
            .steps
            .iter()
            .any(|s| s.kind == ExpansionStepKind::RelationIncluded)
    );
    assert!(
        window
            .expansion_trace
            .steps
            .iter()
            .any(|s| s.kind == ExpansionStepKind::CoverageGapCaveat)
    );

    Ok(())
}

#[sinex_test]
async fn same_field_relation_matches_on_payload_and_source() -> TestResult<()> {
    let seed = material_event(
        "git.commit",
        "commit.created",
        Some(at(0)),
        Some(TemporalSourceType::IntrinsicContent),
        json!({ "repo": "sinex" }),
    );
    let same_repo = material_event(
        "git.commit",
        "commit.created",
        Some(at(99999)),
        Some(TemporalSourceType::IntrinsicContent),
        json!({ "repo": "sinex" }),
    );
    let other_repo = material_event(
        "git.commit",
        "commit.created",
        Some(at(99999)),
        Some(TemporalSourceType::IntrinsicContent),
        json!({ "repo": "polylogue" }),
    );

    let expr = EventRelationExpr::Same {
        field: SameField::Payload("repo".to_string()),
    };
    let window = expr.evaluate(&[seed], &[same_repo, other_repo]);
    // Same-field relation is timing-independent: only the matching repo supports,
    // and the far-future timestamp does not exclude it.
    assert_eq!(window.support_refs.len(), 1);

    Ok(())
}

#[sinex_test]
async fn sequence_relation_flags_out_of_order_and_span() -> TestResult<()> {
    let a = material_event(
        "a",
        "a.evt",
        Some(at(0)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({}),
    );
    let b = material_event(
        "b",
        "b.evt",
        Some(at(60)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({}),
    );
    let c = material_event(
        "c",
        "c.evt",
        Some(at(120)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({}),
    );

    let ok = EventRelationExpr::Sequence { within_secs: 300 }
        .evaluate(&[a.clone(), b.clone(), c.clone()], &[]);
    assert_eq!(ok.support_refs.len(), 3);
    assert!(!ok.caveats.iter().any(|c| c.id == "sequence.out_of_order"));
    assert!(!ok.caveats.iter().any(|c| c.id == "sequence.span_exceeded"));

    let too_long = EventRelationExpr::Sequence { within_secs: 30 }
        .evaluate(&[a.clone(), b.clone(), c.clone()], &[]);
    assert!(
        too_long
            .caveats
            .iter()
            .any(|c| c.id == "sequence.span_exceeded")
    );

    let out_of_order =
        EventRelationExpr::Sequence { within_secs: 300 }.evaluate(&[c, b, a], &[]);
    assert!(
        out_of_order
            .caveats
            .iter()
            .any(|c| c.id == "sequence.out_of_order")
    );

    Ok(())
}

#[sinex_test]
async fn evidence_window_renders_as_view_envelope_with_caveats() -> TestResult<()> {
    let seed = material_event(
        "s",
        "s.evt",
        Some(at(0)),
        Some(TemporalSourceType::RealtimeCapture),
        json!({}),
    );
    let window = EventRelationExpr::Overlaps
        .evaluate(&[seed], &[])
        .with_caveat("test.caveat", "demonstration caveat");
    let envelope = window.into_view("sinexctl.relations");
    let value = serde_json::to_value(&envelope).unwrap();
    assert_eq!(value["source_surface"], "sinexctl.relations");
    assert_eq!(value["payload"]["query"]["relation"], "overlaps");
    // Window caveats are lifted onto the envelope.
    assert!(
        value["caveats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == "test.caveat")
    );

    Ok(())
}

#[sinex_test]
async fn causal_footprint_fixture_models_machine_session_as_evidence_window() -> TestResult<()>
{
    let fixture = fixtures::machine_session_causal_footprint();
    let window = fixture.evidence_window();

    assert_eq!(fixture.source_behavior, "machine session causal footprint");
    assert_eq!(
        fixture.native_owner_surface,
        "sinex.relations.evidence_window"
    );
    assert_eq!(window.query, EventRelationExpr::Within { within_secs: 300 });
    assert_eq!(window.seed_refs.len(), 1);
    assert_eq!(
        window.support_refs.len(),
        3,
        "xtask, rust build, and co-present agent evidence should support the footprint"
    );
    assert!(
        window
            .support_refs
            .iter()
            .all(|evidence| evidence.role == EvidenceRole::Support)
    );
    assert!(
        window
            .support_refs
            .iter()
            .any(|evidence| evidence.object.label.as_deref()
                == Some("dev.xtask · xtask.invoked"))
    );
    assert!(window.support_refs.iter().any(|evidence| {
        evidence.object.label.as_deref() == Some("machine.scope · build.scope.completed")
    }));
    assert!(window.support_refs.iter().any(|evidence| {
        evidence.object.label.as_deref()
            == Some("polylogue.agent-session · agent.session.active")
    }));
    assert!(
        window.caveats.iter().any(|caveat| {
            caveat.id == "privacy.evidence_suppressed"
                && caveat
                    .ref_
                    .as_ref()
                    .is_some_and(|object| object.kind == SinexObjectKind::SourceMaterial)
        }),
        "privacy-limited source material should remain a caveated object ref"
    );
    assert!(
        window
            .expansion_trace
            .steps
            .iter()
            .any(|step| step.kind == ExpansionStepKind::CoverageGapCaveat)
    );

    Ok(())
}

#[sinex_test]
async fn causal_footprint_fixture_renders_as_view_not_canonical_event() -> TestResult<()> {
    let fixture = fixtures::machine_session_causal_footprint();
    let envelope = fixture.view();
    let value = serde_json::to_value(&envelope).unwrap();

    assert_eq!(value["source_surface"], "sinex.relations.evidence_window");
    assert_eq!(value["payload"]["query"]["relation"], "within");
    assert_eq!(
        crate::declared_output_kind("relations.evidence_window"),
        Some(crate::OutputKind::EphemeralView)
    );
    assert!(
        !crate::declared_output_kind("relations.evidence_window")
            .unwrap()
            .is_canonical_event()
    );
    assert!(
        value["caveats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|caveat| caveat["id"] == "privacy.evidence_suppressed")
    );

    Ok(())
}

#[sinex_test]
async fn relation_expr_roundtrips_through_json_with_tag() -> TestResult<()> {
    let expr = EventRelationExpr::Before { max_gap_secs: 90 };
    let value = serde_json::to_value(&expr).unwrap();
    assert_eq!(value["relation"], "before");
    assert_eq!(value["max_gap_secs"], 90);
    let back: EventRelationExpr = serde_json::from_value(value).unwrap();
    assert_eq!(back, expr);

    Ok(())
}

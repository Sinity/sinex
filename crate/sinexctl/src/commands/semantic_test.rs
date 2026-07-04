use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::rpc::semantic::{
    SemanticEpochListResponse, SemanticLaneDiffsListResponse, SemanticLaneListResponse,
    SemanticLaneOutputsListResponse,
};
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::sinex_test;

fn lane_id() -> Uuid {
    Uuid::from_u128(0x1111)
}

#[sinex_test]
async fn epoch_list_empty_envelope_names_absent_registry() -> xtask::TestResult<()> {
    let envelope = semantic_epoch_list_envelope(
        SemanticEpochListResponse { epochs: Vec::new() },
        100,
    );
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite semantic epoch envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.semantic.epoch.list");
    assert_eq!(parsed["query_echo"]["limit"], 100);
    assert_eq!(parsed["caveats"][0]["id"], "source.absent");
    assert_eq!(
        parsed["caveats"][0]["ref"]["command_hint"],
        "sinexctl semantic epoch list"
    );
    Ok(())
}

#[sinex_test]
async fn lane_list_limit_hit_marks_partial_window() -> xtask::TestResult<()> {
    let envelope = semantic_lane_list_envelope(
        SemanticLaneListResponse {
            lanes: vec![serde_json::json!({
                "lane_id": lane_id(),
                "name": "candidate",
                "status": "planned",
            })],
        },
        Some("planned"),
        1,
    );
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert_eq!(envelope.source_surface, "sinexctl.semantic.lane.list");
    assert_eq!(envelope.query_echo.as_ref().unwrap()["status"], "planned");
    assert!(caveat_ids.contains(&"window.partial"));
    Ok(())
}

#[sinex_test]
async fn lane_outputs_empty_envelope_names_absent_outputs() -> xtask::TestResult<()> {
    let envelope = semantic_lane_outputs_envelope(
        SemanticLaneOutputsListResponse {
            lane_id: lane_id(),
            outputs: Vec::new(),
        },
        50,
    );
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite semantic lane outputs envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["source_surface"], "sinexctl.semantic.lane.outputs");
    assert_eq!(parsed["query_echo"]["lane_id"], lane_id().to_string());
    assert_eq!(parsed["caveats"][0]["id"], "source.absent");
    assert_eq!(
        parsed["caveats"][0]["ref"]["rpc_method"],
        "semantic.lane.outputs.list"
    );
    Ok(())
}

#[sinex_test]
async fn lane_diffs_empty_envelope_names_absent_comparison_evidence() -> xtask::TestResult<()> {
    let envelope = semantic_lane_diffs_envelope(
        SemanticLaneDiffsListResponse {
            lane_id: lane_id(),
            diffs: Vec::new(),
        },
        20,
    );

    assert_eq!(envelope.source_surface, "sinexctl.semantic.lane.diffs");
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "source.absent");
    assert!(
        envelope.caveats[0]
            .message
            .contains("comparison evidence is absent")
    );
    Ok(())
}

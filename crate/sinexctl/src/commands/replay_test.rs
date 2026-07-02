use super::{
    MaterialReplayabilityScorecard, Replayability, format_per_material_scorecard_table,
    format_replay_preview_table, preview_total_events, truncate_head_chars,
    truncate_tail_chars, weakness_dimensions,
};
use serde_json::json;
use sinex_primitives::rpc::replay::{
    ReplayCheckpoint, ReplayOperation, ReplayScope, ReplayState,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn preview_total_events_accepts_valid_counts() -> TestResult<()> {
    assert_eq!(preview_total_events(&json!({ "total_events": 0 }))?, 0);
    assert_eq!(preview_total_events(&json!({ "total_events": 42 }))?, 42);
    Ok(())
}

#[sinex_test]
async fn truncate_helpers_handle_multi_byte_utf8() -> TestResult<()> {
    // Mix of 1-byte ASCII, 2-byte (e), 3-byte (β), 4-byte (𝛼) characters.
    // Byte slicing here would panic at the 12-byte / len-25 boundaries
    // when those land in the middle of a code point — char-based
    // truncation must always succeed.
    let s = "/home/usér/φιλε-βυcket/path/𝛼-final-segment-with-extra-padding";
    // Just verify the calls don't panic and the return is non-empty.
    let head = truncate_head_chars(s, 12);
    assert!(!head.is_empty());
    let tail = truncate_tail_chars(s, 26, 25);
    assert!(!tail.is_empty());

    // Short strings are returned unchanged (no ellipsis).
    let short = "abc";
    assert_eq!(truncate_head_chars(short, 12), "abc");
    assert_eq!(truncate_tail_chars(short, 26, 25), "abc");

    // Length above threshold gets ellipsis.
    let long = "x".repeat(40);
    assert!(truncate_head_chars(&long, 12).ends_with('…'));
    assert!(truncate_tail_chars(&long, 26, 25).starts_with('…'));
    Ok(())
}

#[sinex_test]
async fn preview_total_events_rejects_missing_field() -> TestResult<()> {
    let error = preview_total_events(&json!({})).expect_err("missing total_events must fail");
    assert!(error.to_string().contains("total_events"));
    Ok(())
}

#[sinex_test]
async fn preview_total_events_rejects_non_numeric_field() -> TestResult<()> {
    let error = preview_total_events(&json!({ "total_events": "zero" }))
        .expect_err("non-numeric total_events must fail");
    assert!(error.to_string().contains("total_events"));
    Ok(())
}

#[sinex_test]
async fn replay_preview_table_surfaces_failed_safety_analysis() -> TestResult<()> {
    let operation = ReplayOperation {
        operation_id: "op-1".to_string(),
        state: ReplayState::Previewed,
        scope: ReplayScope {
            source_name: "terminal.zsh-history".to_string(),
            time_window: None,
            material_filter: None,
            filters: std::collections::HashMap::new(),
            source_id: None,
            source_material_id: None,
            parser_id: None,
            parser_version: None,
        },
        preview_summary: None,
        checkpoint: ReplayCheckpoint {
            processed_events: 0,
            total_events: 0,
            last_event_id: None,
            batch_number: 0,
            savepoint_id: None,
            updated_at: "2026-04-04T00:00:00Z".to_string(),
        },
        actor: "tester".to_string(),
        created_at: "2026-04-04T00:00:00Z".to_string(),
        approved_by: None,
        approved_at: None,
        executor_module: None,
        started_at: None,
        finished_at: None,
        outcome: None,
        error_details: None,
    };
    let preview = json!({
        "total_events": 3,
        "anchor_churn_pct": null,
        "time_quality_flip_pct": null,
        "max_observed_depth": 7,
        "schema_boundary_crossed": true,
        "replay_gates": {
            "gates": [
                {
                    "name": "anchor_churn_threshold_percent",
                    "tripped": false,
                    "advisory": true,
                    "observed": "not measured (advisory)",
                    "override_flag": "--allow-anchor-churn"
                },
                {
                    "name": "require_force_on_schema_mismatch",
                    "tripped": true,
                    "override_flag": "--force-schema-mismatch"
                }
            ]
        },
        "safety_analysis": {
            "status": "failed",
            "error": "integrity analyzer unavailable",
            "warning": "Cascade impact could not be determined. Approve with caution."
        }
    });

    let rendered = format_replay_preview_table(&operation, &preview);

    assert!(rendered.contains("Safety Warning: analysis failed"));
    assert!(rendered.contains("Anchor Churn: not measured"));
    assert!(rendered.contains("Time Quality Flips: not measured"));
    assert!(rendered.contains("Max Cascade Depth: 7"));
    assert!(rendered.contains("Schema Boundary: true"));
    assert!(
        rendered.contains(
            "Gates Tripped: require_force_on_schema_mismatch (--force-schema-mismatch)"
        )
    );
    assert!(rendered.contains("Safety Error:   integrity analyzer unavailable"));
    assert!(rendered.contains(
        "Safety Detail:  Cascade impact could not be determined. Approve with caution."
    ));
    Ok(())
}

fn make_scorecard(
    material_id: &str,
    source: &str,
    status: sinex_primitives::MaterialStatus,
    replayability: Replayability,
) -> MaterialReplayabilityScorecard {
    MaterialReplayabilityScorecard {
        material_id: material_id.to_string(),
        source_identifier: source.to_string(),
        material_kind: "annex".to_string(),
        status,
        replayability,
    }
}

#[sinex_test]
async fn weakness_dimensions_lists_failed_axes_only() -> TestResult<()> {
    // All-green scorecard reports no weaknesses.
    let strong = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Completed,
        true,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
        Some(1024),
    );
    assert!(weakness_dimensions(&strong).is_empty());

    // Sensing material with no blob and inferred timing must surface
    // blob, timing, and anchor as weakness axes.
    let weak = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Sensing,
        false,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
        None,
    );
    let dims = weakness_dimensions(&weak);
    assert!(dims.contains(&"blob"));
    assert!(dims.contains(&"timing"));
    assert!(dims.contains(&"anchor"));
    Ok(())
}

#[sinex_test]
async fn per_material_scorecard_table_contains_aggregate_row() -> TestResult<()> {
    // Two materials with distinct replayability shapes — one strong,
    // one weak — should compose into an aggregate row that names the
    // material count and a midpoint score.
    let strong = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Completed,
        true,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Intrinsic,
        Some(2048),
    );
    let weak = Replayability::from_material_facts(
        sinex_primitives::MaterialStatus::Sensing,
        false,
        sinex_primitives::domain::SourceMaterialTimingInfoType::Inferred,
        None,
    );
    let rows = vec![
        make_scorecard(
            "mat-a-uuid",
            "/path/strong.csv",
            sinex_primitives::MaterialStatus::Completed,
            strong,
        ),
        make_scorecard(
            "mat-b-uuid",
            "/path/weak.csv",
            sinex_primitives::MaterialStatus::Sensing,
            weak,
        ),
    ];

    let rendered = format_per_material_scorecard_table(&rows);
    assert!(rendered.contains("Per-Material Replayability:"));
    assert!(rendered.contains("MATERIAL"));
    assert!(rendered.contains("WEAKNESSES"));
    // Both rows present (truncated material id prefix).
    assert!(rendered.contains("mat-a-uuid"));
    assert!(rendered.contains("mat-b-uuid"));
    // Aggregate row mentions the material count.
    assert!(rendered.contains("aggregate; 2 materials"));
    // Weak row surfaces the dimension labels in the WEAKNESSES column.
    assert!(rendered.contains("blob") || rendered.contains("timing"));
    Ok(())
}

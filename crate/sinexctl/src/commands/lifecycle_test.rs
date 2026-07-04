use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::domain::DataTier;
use sinex_primitives::rpc::lifecycle::{
    LifecycleStatusResponse, TierStatus, TombstoneOperation, TombstoneOperationPhase,
};
use sinex_primitives::views::{ReadinessCaveatId, VIEW_ENVELOPE_SCHEMA_VERSION};
use xtask::sandbox::prelude::*;

fn empty_tier(tier: DataTier) -> TierStatus {
    TierStatus {
        tier,
        event_count: 0,
        oldest_ts: None,
        newest_ts: None,
        distinct_sources: 0,
    }
}

fn fixture_tombstone_operation(id: &str) -> TombstoneOperation {
    TombstoneOperation {
        operation_id: id.to_string(),
        phase: TombstoneOperationPhase::Pending,
        state: TombstoneOperationState::Pending,
        before: None,
        source: None,
        event_ids: None,
        limit: 100,
        reason: "fixture".to_string(),
        cascade_analysis: None,
        created_by: "test".to_string(),
        created_at: "2026-07-04T00:00:00Z".to_string(),
        expires_at: "2026-07-04T01:00:00Z".to_string(),
        approved_by: None,
        approved_at: None,
        started_at: None,
        finished_at: None,
        tombstoned_count: None,
        error_details: None,
    }
}

#[sinex_test]
async fn lifecycle_status_envelope_caveats_empty_tiers() -> TestResult<()> {
    let response = LifecycleStatusResponse {
        tiers: vec![
            empty_tier(DataTier::Live),
            empty_tier(DataTier::Archive),
            empty_tier(DataTier::Tombstone),
        ],
        total_events: 0,
    };

    let envelope = lifecycle_status_envelope(response);

    assert_eq!(envelope.source_surface, "sinexctl.ops.lifecycle.status");
    assert_eq!(envelope.payload.total_events, 0);
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(
        envelope.caveats[0].id,
        ReadinessCaveatId::CoverageUnmeasurable.as_str()
    );
    Ok(())
}

#[sinex_test]
async fn lifecycle_status_envelope_renders_finite_json() -> TestResult<()> {
    let response = LifecycleStatusResponse {
        tiers: vec![TierStatus {
            tier: DataTier::Live,
            event_count: 7,
            oldest_ts: Some("2026-07-04T00:00:00Z".to_string()),
            newest_ts: Some("2026-07-04T00:01:00Z".to_string()),
            distinct_sources: 2,
        }],
        total_events: 7,
    };
    let envelope = lifecycle_status_envelope(response);

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.lifecycle.status");
    assert_eq!(parsed["payload"]["total_events"], 7);
    assert_eq!(parsed["payload"]["tiers"][0]["tier"], "live");
    Ok(())
}

#[sinex_test]
async fn tombstone_list_envelope_caveats_empty_operation_log() -> TestResult<()> {
    let response = TombstoneListResponse {
        operations: Vec::new(),
    };

    let envelope = tombstone_list_envelope(response, Some(TombstoneOperationState::Pending), 20);

    assert_eq!(
        envelope.source_surface,
        "sinexctl.ops.lifecycle.tombstone.list"
    );
    assert_eq!(envelope.payload.operations.len(), 0);
    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(
        envelope.caveats[0].id,
        ReadinessCaveatId::SourceAbsent.as_str()
    );
    assert_eq!(envelope.query_echo.as_ref().unwrap()["state"], "pending");
    Ok(())
}

#[sinex_test]
async fn tombstone_list_envelope_renders_operations() -> TestResult<()> {
    let response = TombstoneListResponse {
        operations: vec![fixture_tombstone_operation("op-1")],
    };
    let envelope = tombstone_list_envelope(response, None, 20);

    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(
        parsed["source_surface"],
        "sinexctl.ops.lifecycle.tombstone.list"
    );
    assert_eq!(parsed["payload"]["operations"][0]["operation_id"], "op-1");
    assert_eq!(parsed["query_echo"]["limit"], 20);
    Ok(())
}

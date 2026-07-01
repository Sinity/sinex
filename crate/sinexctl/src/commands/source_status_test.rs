#![allow(clippy::unwrap_used)]

use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::views::{
    ActionAvailability, CaveatView, SourceCoverageListView, SourcePrivacyPosture,
    SourceResourceBudgetView, VIEW_ENVELOPE_SCHEMA_VERSION,
};
use xtask::sandbox::sinex_test;

fn fixture_source() -> SourceCoverageView {
    SourceCoverageView {
        source_id: "fixture.source".to_string(),
        namespace: "fixture".to_string(),
        event_types: vec!["fixture/fixture.event".to_string()],
        readiness: SourceCoverageReadiness::Ready,
        continuity: SourceCoverageContinuity::Active,
        last_material_at: None,
        last_event_at: None,
        material_count: 2,
        event_count: 3,
        binding_count: 1,
        accepted_binding_count: 1,
        proposed_binding_count: 0,
        gaps: Vec::new(),
        caveats: Vec::new(),
        privacy: SourcePrivacyPosture {
            tier: "sensitive".to_string(),
            context: "command".to_string(),
            proposed: false,
        },
        resource_budget: None,
        modes: vec![fixture_mode()],
        actions: Vec::new(),
    }
}

fn fixture_mode() -> SourceModeStatusView {
    SourceModeStatusView {
        mode_id: "fixture.mode".to_string(),
        binding_id: "binding.fixture.mode".to_string(),
        implementation: "fixture-implementation".to_string(),
        adapter: "FixtureAdapter".to_string(),
        output_event_type: "fixture.event".to_string(),
        proposed: false,
        runner_pack: "staged".to_string(),
        runtime_shape: "on_demand".to_string(),
        checkpoint_family: "file_cursor".to_string(),
        material_lifecycle: "retain_raw".to_string(),
        transport: "direct".to_string(),
        delivery: "synchronous".to_string(),
        ordering: "input_order".to_string(),
        replayable: true,
        dlq: false,
        backpressure: false,
        privacy_context: "metadata".to_string(),
        resource_budget: SourceResourceBudgetView {
            resource_profile: "bounded_file".to_string(),
            work_class: "bulk_import".to_string(),
            steady_memory_mib: 16,
            burst_memory_mib: 32,
            cpu_weight: 10,
            max_input_bytes_per_sec: None,
            max_input_events_per_sec: None,
            max_pending_material_bytes: 1024,
            max_pending_candidates: 16,
            max_unacked_transport_messages: None,
            batch_size: Some(8),
            flush_interval_ms: None,
            checkpoint_interval_ms: None,
            pressure_actions: vec!["pause".to_string()],
        },
        runtime_observed: None,
        runtime_live: None,
        last_heartbeat_at: None,
        last_output_at: None,
        recent_output_count: None,
        provider_operation_status: None,
        provider_auth_state: None,
        provider_network_state: None,
        provider_sync_state: None,
        provider_rate_limit_state: None,
        provider_failure_class: None,
        provider_required_action: None,
        provider_retry_after_secs: None,
        provider_reconnect_state: None,
        provider_operation_id: None,
        provider_coverage_ref: None,
        provider_debt_ref: None,
        mailbox_projection_message_count: None,
        mailbox_projection_thread_count: None,
        mailbox_projection_body_bytes: None,
        mailbox_projection_attachment_count: None,
        mailbox_projection_attachment_observed_count: None,
        mailbox_projection_last_observed_at: None,
        actions: Vec::new(),
    }
}

fn fixture_source_with_id(source_id: &str) -> SourceCoverageView {
    SourceCoverageView {
        source_id: source_id.to_string(),
        ..fixture_source()
    }
}

fn fixture_source_with_id_and_namespace(source_id: &str, namespace: &str) -> SourceCoverageView {
    SourceCoverageView {
        source_id: source_id.to_string(),
        namespace: namespace.to_string(),
        ..fixture_source()
    }
}

#[sinex_test]
async fn table_renderer_shows_source_coverage_view_fields() -> xtask::TestResult<()> {
    let mut source = fixture_source();
    source.caveats.push(CaveatView {
        id: "source.runtime_bridge.unobserved".to_string(),
        message: "bridge is declared but no records have been observed".to_string(),
        ref_: None,
    });
    source.actions.push(ActionAvailability::read(
        "terminal.activity.check",
        "Check Bridge",
        ActionAvailabilityState::Enabled,
    ));
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![source]),
    );

    let table = format_sources_status_table(&envelope);

    assert!(table.contains("fixture.source"));
    assert!(table.contains("Sources: total=1 ready=1"));
    assert!(table.contains("ready"));
    assert!(table.contains("active"));
    assert!(table.contains("accepted:1"));
    assert!(table.contains("fixture.mode:accepted/on_demand/direct"));
    assert!(table.contains("fixture/fixture.event"));
    assert!(table.contains("source.runtime_bridge.unobserved"));
    assert!(table.contains("terminal.activity.check:enabled"));
    Ok(())
}

#[sinex_test]
async fn machine_render_preserves_envelope_schema() -> xtask::TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![fixture_source()]),
    );

    let json =
        render_finite_envelope(&envelope, OutputFormat::Json)?.expect("json renders envelope");
    let value: serde_json::Value = serde_json::from_str(&json)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["payload"]["count"], 1);
    assert_eq!(value["payload"]["summary"]["total_sources"], 1);
    assert_eq!(value["payload"]["summary"]["readiness"]["ready"], 1);
    assert_eq!(value["payload"]["summary"]["continuity"]["active"], 1);
    assert_eq!(
        value["payload"]["sources"][0]["source_id"],
        "fixture.source"
    );
    Ok(())
}

#[sinex_test]
async fn status_filter_detection_ignores_empty_values() -> xtask::TestResult<()> {
    assert!(!source_status_has_filter(&None, &None));
    assert!(!source_status_has_filter(&Some(String::new()), &None));
    assert!(!source_status_has_filter(&None, &Some(String::new())));
    assert!(source_status_has_filter(
        &Some("browser.history".to_string()),
        &None
    ));
    assert!(source_status_has_filter(
        &None,
        &Some("browser".to_string())
    ));
    Ok(())
}

#[sinex_test]
async fn status_filter_keeps_matching_source_and_envelope_metadata() -> xtask::TestResult<()> {
    let mut envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![
            fixture_source_with_id("terminal.kitty-osc-live"),
            fixture_source_with_id("browser.history"),
        ]),
    )
    .with_query_echo(serde_json::json!({ "from": "gateway" }));
    envelope.caveats.push(CaveatView {
        id: "source.coverage.partial".to_string(),
        message: "fixture top-level caveat".to_string(),
        ref_: None,
    });

    let filtered = filter_sources_status_envelope(
        envelope,
        &Some("terminal.kitty-osc-live".to_string()),
        &None,
    );

    assert_eq!(filtered.source_surface, "sinexctl.sources.status");
    assert_eq!(filtered.caveats.len(), 1);
    assert_eq!(filtered.payload.count, 1);
    assert_eq!(filtered.payload.summary.total_sources, 1);
    assert_eq!(
        filtered.payload.summary.readiness.get("ready"),
        Some(&1_usize)
    );
    assert_eq!(
        filtered.payload.sources[0].source_id,
        "terminal.kitty-osc-live"
    );
    assert_eq!(
        filtered.query_echo,
        Some(serde_json::json!({
            "from": "gateway",
            "source": "terminal.kitty-osc-live",
            "family": null
        }))
    );
    Ok(())
}

#[sinex_test]
async fn status_filter_renders_empty_match_as_finite_view() -> xtask::TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![fixture_source_with_id("browser.history")]),
    );

    let filtered = filter_sources_status_envelope(envelope, &Some("terminal".to_string()), &None);

    assert_eq!(filtered.payload.count, 0);
    assert_eq!(filtered.payload.summary.total_sources, 0);
    assert!(filtered.payload.summary.readiness.is_empty());
    assert!(filtered.payload.sources.is_empty());
    assert_eq!(
        format_sources_status_table(&filtered),
        "No sources registered."
    );
    Ok(())
}

#[sinex_test]
async fn status_filter_keeps_matching_family_and_web_namespace() -> xtask::TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![
            fixture_source_with_id_and_namespace("browser.history", "web"),
            fixture_source_with_id_and_namespace("raindrop-bookmarks", "web"),
            fixture_source_with_id_and_namespace("terminal.atuin-history", "terminal"),
        ]),
    );

    let filtered = filter_sources_status_envelope(envelope, &None, &Some("browser".to_string()));

    let source_ids = filtered
        .payload
        .sources
        .iter()
        .map(|source| source.source_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(source_ids, vec!["browser.history", "raindrop-bookmarks"]);
    assert_eq!(filtered.payload.count, 2);
    assert_eq!(filtered.payload.summary.total_sources, 2);
    assert_eq!(
        filtered.query_echo,
        Some(serde_json::json!({ "source": null, "family": "browser" }))
    );
    Ok(())
}

#[sinex_test]
async fn finite_machine_render_rejects_ndjson() -> xtask::TestResult<()> {
    let envelope = ViewEnvelope::new(
        "sinexctl.sources.status",
        SourceCoverageListView::new(vec![fixture_source()]),
    );

    let err = render_finite_envelope(&envelope, OutputFormat::Ndjson).unwrap_err();

    assert!(err.to_string().contains("ndjson"));
    Ok(())
}

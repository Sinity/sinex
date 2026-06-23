use clap::Args;
use console::style;
use sinex_primitives::views::{
    ActionAvailabilityState, SourceCoverageContinuity, SourceCoverageListView,
    SourceCoverageReadiness, SourceCoverageView, SourceModeStatusView, ViewEnvelope,
};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, print_finite_envelope};
use crate::model::OutputFormat;

/// Show source coverage/readiness status.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Show all source status
    sinexctl sources status

    # Show one source package mode
    sinexctl sources status terminal.kitty-osc-live

    # Emit machine-readable status
    sinexctl sources status terminal.kitty-osc-live --format json
")]
pub struct SourceStatusCommand {
    /// Optional source id or substring to inspect.
    source: Option<String>,
}

impl SourceStatusCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let envelope =
            filter_sources_status_envelope(client.sources_status_view().await?, &self.source);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(envelope, format_sources_status_table).display(&format)?;
        Ok(())
    }
}

fn filter_sources_status_envelope(
    mut envelope: ViewEnvelope<SourceCoverageListView>,
    source: &Option<String>,
) -> ViewEnvelope<SourceCoverageListView> {
    let Some(filter) = source.as_deref().filter(|value| !value.is_empty()) else {
        return envelope;
    };
    envelope.payload.sources = envelope
        .payload
        .sources
        .into_iter()
        .filter(|source| source.source_id.contains(filter))
        .collect::<Vec<_>>();
    envelope.payload.count = envelope.payload.sources.len();
    envelope.query_echo = Some(serde_json::json!({ "source": filter }));
    envelope
}

fn readiness_label(readiness: SourceCoverageReadiness) -> console::StyledObject<&'static str> {
    match readiness {
        SourceCoverageReadiness::Ready => style("ready").green(),
        SourceCoverageReadiness::Proposed => style("proposed").cyan(),
        SourceCoverageReadiness::MissingMaterial => style("missing-material").yellow(),
        SourceCoverageReadiness::MissingEvents => style("missing-events").yellow(),
        SourceCoverageReadiness::MissingBinding => style("missing-binding").red(),
    }
}

fn continuity_label(continuity: SourceCoverageContinuity) -> console::StyledObject<&'static str> {
    match continuity {
        SourceCoverageContinuity::Active => style("active").green(),
        SourceCoverageContinuity::MaterialOnly => style("material-only").yellow(),
        SourceCoverageContinuity::EventOnly => style("event-only").yellow(),
        SourceCoverageContinuity::Gapped => style("gapped").red(),
        SourceCoverageContinuity::Unknown => style("unknown").dim(),
    }
}

fn format_optional_timestamp(value: Option<&sinex_primitives::Timestamp>) -> String {
    value.map_or_else(|| style("-").dim().to_string(), ToString::to_string)
}

fn event_types_summary(source: &SourceCoverageView) -> String {
    match source.event_types.len() {
        0 => style("-").dim().to_string(),
        1 => source.event_types[0].clone(),
        n => format!("{} (+{})", source.event_types[0], n - 1),
    }
}

fn caveats_summary(source: &SourceCoverageView) -> String {
    match source.caveats.len() {
        0 => style("-").dim().to_string(),
        1 => source.caveats[0].id.clone(),
        n => format!("{} (+{})", source.caveats[0].id, n - 1),
    }
}

fn actions_summary(source: &SourceCoverageView) -> String {
    let action_ids = source
        .actions
        .iter()
        .map(|action| format!("{}:{}", action.id, action_state_label(action.state)))
        .collect::<Vec<_>>();
    match action_ids.len() {
        0 => style("-").dim().to_string(),
        1 => action_ids[0].clone(),
        n => format!("{} (+{})", action_ids[0], n - 1),
    }
}

fn modes_summary(source: &SourceCoverageView) -> String {
    let summaries = source.modes.iter().map(mode_summary).collect::<Vec<_>>();
    match summaries.len() {
        0 => style("-").dim().to_string(),
        1 => summaries[0].clone(),
        n => format!("{} (+{})", summaries[0], n - 1),
    }
}

fn mode_summary(mode: &SourceModeStatusView) -> String {
    let state = if mode.proposed {
        "proposed"
    } else {
        "accepted"
    };
    let provider = mode
        .provider_required_action
        .as_deref()
        .or(mode.provider_reconnect_state.as_deref())
        .or(mode.provider_failure_class.as_deref())
        .map(|state| format!("/provider:{state}"))
        .unwrap_or_default();
    let projection = mode
        .mailbox_projection_message_count
        .map(|count| format!("/mailbox:{count}msg"))
        .unwrap_or_default();
    format!(
        "{}:{}/{}/{}",
        mode.mode_id, state, mode.runtime_shape, mode.transport
    ) + &provider
        + &projection
}

const fn action_state_label(state: ActionAvailabilityState) -> &'static str {
    match state {
        ActionAvailabilityState::Enabled => "enabled",
        ActionAvailabilityState::Disabled => "disabled",
        ActionAvailabilityState::Target => "target",
        ActionAvailabilityState::Loading => "loading",
        ActionAvailabilityState::Dangerous => "dangerous",
        ActionAvailabilityState::Partial => "partial",
        ActionAvailabilityState::Unavailable => "unavailable",
    }
}

fn format_sources_status_table(envelope: &ViewEnvelope<SourceCoverageListView>) -> String {
    if envelope.payload.sources.is_empty() {
        return "No sources registered.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "READY",
        "CONTINUITY",
        "EVENTS",
        "MATERIALS",
        "LAST EVENT",
        "LAST MATERIAL",
        "PRIVACY",
        "MODES",
        "TYPES",
        "CAVEATS",
        "ACTIONS",
    ]);

    for source in &envelope.payload.sources {
        builder.push_record([
            source.source_id.clone(),
            readiness_label(source.readiness).to_string(),
            continuity_label(source.continuity).to_string(),
            source.event_count.to_string(),
            source.material_count.to_string(),
            format_optional_timestamp(source.last_event_at.as_ref()),
            format_optional_timestamp(source.last_material_at.as_ref()),
            format!("{}/{}", source.privacy.tier, source.privacy.context),
            modes_summary(source),
            event_types_summary(source),
            caveats_summary(source),
            actions_summary(source),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

#[cfg(test)]
mod tests {
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
            live_binding_count: 1,
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
        assert!(table.contains("ready"));
        assert!(table.contains("active"));
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
        assert_eq!(
            value["payload"]["sources"][0]["source_id"],
            "fixture.source"
        );
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

        let filtered =
            filter_sources_status_envelope(envelope, &Some("terminal.kitty-osc-live".to_string()));

        assert_eq!(filtered.source_surface, "sinexctl.sources.status");
        assert_eq!(filtered.caveats.len(), 1);
        assert_eq!(filtered.payload.count, 1);
        assert_eq!(
            filtered.payload.sources[0].source_id,
            "terminal.kitty-osc-live"
        );
        assert_eq!(
            filtered.query_echo,
            Some(serde_json::json!({ "source": "terminal.kitty-osc-live" }))
        );
        Ok(())
    }

    #[sinex_test]
    async fn status_filter_renders_empty_match_as_finite_view() -> xtask::TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.status",
            SourceCoverageListView::new(vec![fixture_source_with_id("browser.history")]),
        );

        let filtered = filter_sources_status_envelope(envelope, &Some("terminal".to_string()));

        assert_eq!(filtered.payload.count, 0);
        assert!(filtered.payload.sources.is_empty());
        assert_eq!(
            format_sources_status_table(&filtered),
            "No sources registered."
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
}

use clap::Args;
use console::style;
use serde_json::Map;
use sinex_primitives::sources::source_identity_matches_family;
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

    # Show one source family
    sinexctl sources status --family browser

    # Emit machine-readable status
    sinexctl sources status terminal.kitty-osc-live --format json
")]
pub struct SourceStatusCommand {
    /// Optional source id or substring to inspect.
    source: Option<String>,

    /// Optional source family filter (e.g. "terminal", "browser", "chat").
    #[arg(long)]
    family: Option<String>,

    /// Compute exact lifetime event counts instead of bounded presence probes.
    #[arg(long)]
    exact_counts: bool,
}

impl SourceStatusCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let envelope = filter_sources_status_envelope(
            client
                .sources_status_view_filtered(
                    self.source.clone(),
                    self.family.clone(),
                    self.exact_counts,
                )
                .await?,
            &self.source,
            &self.family,
        );
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
    family: &Option<String>,
) -> ViewEnvelope<SourceCoverageListView> {
    let source_filter = source.as_deref().filter(|value| !value.is_empty());
    let family_filter = family.as_deref().filter(|value| !value.is_empty());
    if source_filter.is_none() && family_filter.is_none() {
        return envelope;
    };
    envelope.payload.sources = envelope
        .payload
        .sources
        .into_iter()
        .filter(|item| {
            source_filter.is_none_or(|filter| item.source_id.contains(filter))
                && family_filter.is_none_or(|family| source_status_matches_family(item, family))
        })
        .collect::<Vec<_>>();
    envelope.payload.refresh_summary();
    let mut query_echo = envelope
        .query_echo
        .take()
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_else(Map::new);
    query_echo.insert("source".to_string(), serde_json::json!(source_filter));
    query_echo.insert("family".to_string(), serde_json::json!(family_filter));
    envelope.query_echo = Some(serde_json::Value::Object(query_echo));
    envelope
}

fn source_status_matches_family(source: &SourceCoverageView, family: &str) -> bool {
    source_identity_matches_family(&source.source_id, &source.namespace, family)
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

fn binding_summary(source: &SourceCoverageView) -> String {
    if source.proposed_binding_count == 0 {
        format!("accepted:{}", source.accepted_binding_count)
    } else {
        format!(
            "accepted:{}/proposed:{}",
            source.accepted_binding_count, source.proposed_binding_count
        )
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
        "BINDINGS",
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
            binding_summary(source),
            format!("{}/{}", source.privacy.tier, source.privacy.context),
            modes_summary(source),
            event_types_summary(source),
            caveats_summary(source),
            actions_summary(source),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    format!(
        "{}\n{}",
        source_status_summary_line(&envelope.payload),
        table
    )
}

fn source_status_summary_line(view: &SourceCoverageListView) -> String {
    let summary = &view.summary;
    format!(
        "Sources: total={} ready={} proposed={} missing_material={} active={} gapped={} eventful={} materialized={} accepted_bindings={} proposed_bindings={} events={} materials={}",
        summary.total_sources,
        summary.readiness.get("ready").copied().unwrap_or_default(),
        summary
            .readiness
            .get("proposed")
            .copied()
            .unwrap_or_default(),
        summary
            .readiness
            .get("missing_material")
            .copied()
            .unwrap_or_default(),
        summary
            .continuity
            .get("active")
            .copied()
            .unwrap_or_default(),
        summary
            .continuity
            .get("gapped")
            .copied()
            .unwrap_or_default(),
        summary.eventful_sources,
        summary.materialized_sources,
        summary.accepted_bindings,
        summary.proposed_bindings,
        summary.total_events,
        summary.total_materials
    )
}

#[cfg(test)]
#[path = "source_status_test.rs"]
mod tests;

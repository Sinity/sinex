use clap::Args;
use console::style;
use sinex_primitives::views::{
    SourceCoverageContinuity, SourceCoverageListView, SourceCoverageReadiness, SourceCoverageView,
    ViewEnvelope,
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

    # Emit machine-readable status
    sinexctl sources status --format json
")]
pub struct SourceStatusCommand {}

impl SourceStatusCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let envelope = client.sources_status_view().await?;
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(envelope, format_sources_status_table).display(&format)?;
        Ok(())
    }
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
        "TYPES",
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
            event_types_summary(source),
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
        SourceCoverageListView, SourcePrivacyPosture, VIEW_ENVELOPE_SCHEMA_VERSION,
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
            actions: Vec::new(),
        }
    }

    #[sinex_test]
    async fn table_renderer_shows_source_coverage_view_fields() -> xtask::TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.status",
            SourceCoverageListView::new(vec![fixture_source()]),
        );

        let table = format_sources_status_table(&envelope);

        assert!(table.contains("fixture.source"));
        assert!(table.contains("ready"));
        assert!(table.contains("active"));
        assert!(table.contains("fixture/fixture.event"));
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

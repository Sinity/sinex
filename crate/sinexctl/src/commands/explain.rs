use clap::Args;
use color_eyre::Result;
use console::style;
use serde_json::json;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::query::{LineageDirection, LineageQuery, LineageResult};
use sinex_primitives::views::ViewEnvelope;
use sinex_primitives::{Event, Id};

use crate::client::GatewayClient;
use crate::fmt::render_envelope;
use crate::model::OutputFormat;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl events explain 019d95af-fcd8-7aa3-b5b7-65da9e28dc12
")]
pub struct ExplainCommand {
    /// Event ID to explain (`UUIDv7`)
    pub event_id: String,
}

impl ExplainCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let event_id: Id<Event<serde_json::Value>> =
            Id::from_uuid(self.event_id.parse().map_err(|e| {
                color_eyre::eyre::eyre!("Invalid event ID '{}': {}", self.event_id, e)
            })?);

        let query = LineageQuery {
            event_id,
            direction: LineageDirection::Both,
            max_depth: 1,
        };

        let result = client.trace_lineage(query).await?;

        if let Some(output) = render_explain_machine_output(&result, &self.event_id, format)? {
            println!("{output}");
            return Ok(());
        }

        let event = &result.root;

        println!();
        println!("{}", style("Event Details").bold().cyan());
        println!("{}", style("═".repeat(60)).dim());

        println!();
        print_field(
            "ID",
            &event
                .id
                .as_ref()
                .map_or("?".to_string(), std::string::ToString::to_string),
        );
        print_field("Source", event.source.as_ref());
        print_field("Event Type", event.event_type.as_ref());
        print_field("Host", event.host.as_ref());
        if let Some(ts) = &event.ts_orig {
            print_field("Timestamp (orig)", &ts.inner().to_string());
        }

        println!();
        println!("{}", style("Provenance").bold().cyan());
        println!("{}", style("─".repeat(60)).dim());

        match &event.provenance {
            Provenance::Material {
                id,
                anchor_byte,
                offset_start,
                offset_end,
                ..
            } => {
                print_field("Type", "Material (from source data)");
                print_field("Material ID", &id.to_string());
                print_field("Anchor Byte", &anchor_byte.to_string());
                if let Some(start) = offset_start {
                    print_field(
                        "Offset Range",
                        &format!("{}..{}", start, offset_end.unwrap_or(*start)),
                    );
                }
            }
            Provenance::Derived {
                source_event_ids, ..
            } => {
                print_field("Type", "Derived (derived from other events)");
                print_field("Parent Count", &source_event_ids.len().to_string());
                for pid in source_event_ids {
                    println!("  {:<20} {}", style("  └─").dim(), pid);
                }
            }
        }

        if let Some(model) = &event.automaton_model {
            print_field("RuntimeModule Model", &format!("{model:?}"));
        }
        if let Some(scope) = &event.scope_key {
            print_field("Scope Key", scope);
        }
        if let Some(equiv) = &event.equivalence_key {
            print_field("Equivalence Key", equiv);
        }
        if let Some(schema_id) = &event.payload_schema_id {
            print_field("Schema ID", &schema_id.to_string());
        }

        if !result.ancestors.is_empty() {
            println!();
            println!("{}", style("Ancestors").bold().cyan());
            println!("{}", style("─".repeat(60)).dim());
            for node in &result.ancestors {
                println!(
                    "  depth={} {} {} [{}]",
                    node.depth,
                    style(
                        node.event
                            .id
                            .as_ref()
                            .map_or("?".to_string(), std::string::ToString::to_string)
                    )
                    .dim(),
                    style(node.event.event_type.as_ref()).yellow(),
                    node.event.source.as_ref(),
                );
            }
        }

        if !result.descendants.is_empty() {
            println!();
            println!("{}", style("Descendants").bold().cyan());
            println!("{}", style("─".repeat(60)).dim());
            for node in &result.descendants {
                println!(
                    "  depth={} {} {} [{}]",
                    node.depth,
                    style(
                        node.event
                            .id
                            .as_ref()
                            .map_or("?".to_string(), std::string::ToString::to_string)
                    )
                    .dim(),
                    style(node.event.event_type.as_ref()).yellow(),
                    node.event.source.as_ref(),
                );
            }
        }

        println!();
        println!("{}", style("Payload").bold().cyan());
        println!("{}", style("─".repeat(60)).dim());
        let pretty = serde_json::to_string_pretty(&event.payload)
            .unwrap_or_else(|_| format!("{:?}", event.payload));
        for line in pretty.lines() {
            println!("  {line}");
        }
        println!();

        Ok(())
    }
}

fn render_explain_machine_output(
    result: &LineageResult,
    event_id: &str,
    format: OutputFormat,
) -> Result<Option<String>> {
    match format {
        OutputFormat::Table => Ok(None),
        OutputFormat::Json | OutputFormat::Yaml => {
            let envelope = ViewEnvelope::new("sinexctl.events.explain", result)
                .with_query_echo(json!({ "event_id": event_id }));
            render_envelope(&envelope, &result.ancestors, format)
        }
        OutputFormat::Ndjson | OutputFormat::Dot => Err(color_eyre::eyre::eyre!(
            "explain is a finite view; use json, yaml, or table"
        )),
    }
}

fn print_field(label: &str, value: &str) {
    println!("  {:<20} {}", style(format!("{label}:")).dim(), value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::query::LineageNode;
    use sinex_primitives::testing::event_fixture;
    use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
    use xtask::sandbox::prelude::sinex_test;

    fn lineage_fixture() -> LineageResult {
        let root = event_fixture(
            sinex_primitives::EventSource::from_static("test"),
            sinex_primitives::EventType::from_static("test.root"),
            json!({ "message": "root" }),
        );
        let parent = event_fixture(
            sinex_primitives::EventSource::from_static("test"),
            sinex_primitives::EventType::from_static("test.parent"),
            json!({ "message": "parent" }),
        );

        LineageResult {
            root,
            ancestors: vec![LineageNode {
                event: parent,
                depth: 1,
            }],
            descendants: Vec::new(),
            material_links: Vec::new(),
        }
    }

    #[sinex_test]
    async fn explain_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
        let result = lineage_fixture();
        let output = render_explain_machine_output(
            &result,
            "01912345-6789-7abc-def0-123456789abc",
            OutputFormat::Json,
        )?
        .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
        let value: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.events.explain");
        assert_eq!(
            value["query_echo"]["event_id"],
            "01912345-6789-7abc-def0-123456789abc"
        );
        assert_eq!(value["payload"]["root"]["event_type"], "test.root");
        assert_eq!(value["payload"]["ancestors"][0]["depth"], 1);
        Ok(())
    }

    #[sinex_test]
    async fn explain_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
        let result = lineage_fixture();
        let err = render_explain_machine_output(&result, "id", OutputFormat::Ndjson);
        assert!(err.is_err(), "explain must remain a finite view");
        Ok(())
    }
}

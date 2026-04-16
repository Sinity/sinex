use clap::Args;
use color_eyre::Result;
use console::style;
use sinex_primitives::events::builder::Provenance;
use sinex_primitives::query::{LineageDirection, LineageQuery};
use sinex_primitives::{Event, Id};

use crate::client::GatewayClient;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl explain 019d95af-fcd8-7aa3-b5b7-65da9e28dc12
")]
pub struct ExplainCommand {
    /// Event ID to explain (`UUIDv7`)
    pub event_id: String,
}

impl ExplainCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
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
            Provenance::Synthesis {
                source_event_ids, ..
            } => {
                print_field("Type", "Synthesis (derived from other events)");
                print_field("Parent Count", &source_event_ids.len().to_string());
                for pid in source_event_ids {
                    println!("  {:<20} {}", style("  └─").dim(), pid);
                }
            }
        }

        if let Some(model) = &event.node_model {
            print_field("Node Model", &format!("{model:?}"));
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

fn print_field(label: &str, value: &str) {
    println!("  {:<20} {}", style(format!("{label}:")).dim(), value);
}

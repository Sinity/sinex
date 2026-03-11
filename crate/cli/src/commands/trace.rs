use clap::{Args, ValueEnum};
use console::style;
use serde_json::Value as JsonValue;
use sinex_primitives::events::{Event, Provenance};
use sinex_primitives::ids::Id;
use sinex_primitives::query::{LineageDirection, LineageNode, LineageQuery, LineageResult};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

/// Trace event provenance chain
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Trace full provenance (ancestors + descendants) for an event
    sinexctl trace 01912345-6789-7abc-def0-123456789abc

    # Trace only ancestors (towards raw materials)
    sinexctl trace 01912345-... --direction ancestors

    # Trace only descendants (derived events)
    sinexctl trace 01912345-... --direction descendants

    # Limit traversal depth
    sinexctl trace 01912345-... --max-depth 3

    # Output as JSON for piping
    sinexctl trace 01912345-... -f json
")]
pub struct TraceCommand {
    /// Event ID to trace provenance for (UUID)
    event_id: Id<Event<JsonValue>>,

    /// Direction to traverse the provenance chain
    #[arg(long, short = 'd', value_enum, default_value = "both")]
    direction: DirectionArg,

    /// Maximum traversal depth (clamped to 50)
    #[arg(long, short = 'n', default_value = "10")]
    max_depth: u32,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

/// CLI-facing direction enum (maps to `LineageDirection`)
#[derive(Debug, Clone, Copy, ValueEnum)]
enum DirectionArg {
    Ancestors,
    Descendants,
    Both,
}

impl From<DirectionArg> for LineageDirection {
    fn from(d: DirectionArg) -> Self {
        match d {
            DirectionArg::Ancestors => Self::Ancestors,
            DirectionArg::Descendants => Self::Descendants,
            DirectionArg::Both => Self::Both,
        }
    }
}

impl TraceCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let query = LineageQuery {
            event_id: self.event_id,
            direction: self.direction.into(),
            max_depth: self.max_depth,
        };

        let result = client.trace_lineage(query).await?;

        match self.format {
            OutputFormat::Table => render_tree(&result),
            OutputFormat::Json => println!("{}", format_json(&result)?),
            OutputFormat::Yaml => println!("{}", format_yaml(&result)?),
        }

        Ok(())
    }
}

/// Render the lineage result as a tree to stdout.
fn render_tree(result: &LineageResult) {
    // Root event
    let root = &result.root;
    println!(
        "{} {} {}",
        style("Root:").bold(),
        format_event_summary(root),
        format_provenance_tag(root),
    );

    // Ancestors
    if !result.ancestors.is_empty() {
        println!("  {}:", style("Ancestors").cyan().bold());
        render_nodes(&result.ancestors);
    }

    // Descendants
    if !result.descendants.is_empty() {
        println!("  {}:", style("Descendants").cyan().bold());
        render_nodes(&result.descendants);
    }

    if result.ancestors.is_empty() && result.descendants.is_empty() {
        println!("  {}", style("No provenance links found.").dim());
    }
}

/// Render a slice of lineage nodes with tree connectors.
fn render_nodes(nodes: &[LineageNode]) {
    for (i, node) in nodes.iter().enumerate() {
        let is_last = i == nodes.len() - 1;
        let connector = if is_last { "└─" } else { "├─" };
        println!(
            "    {connector} {} {} {}",
            style(format!("[{}]", node.depth)).dim(),
            format_event_summary(&node.event),
            format_provenance_tag(&node.event),
        );
    }
}

/// Format a single event as a compact summary line.
fn format_event_summary(event: &Event<JsonValue>) -> String {
    let id_short = event.id.as_ref().map_or_else(
        || "????????".to_string(),
        |id| {
            let s = id.to_string();
            s[..8.min(s.len())].to_string()
        },
    );

    let timestamp = event.ts_orig.map_or_else(
        || "no timestamp".to_string(),
        |ts| {
            ts.format(time::macros::format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second]"
            ))
            .unwrap_or_else(|_| "invalid".to_string())
        },
    );

    format!(
        "{} {}{}{} {}",
        style(id_short).yellow(),
        style("[").dim(),
        style(format!("{}/{}", event.source, event.event_type)).green(),
        style("]").dim(),
        style(timestamp).dim(),
    )
}

/// Format a provenance tag for display.
fn format_provenance_tag(event: &Event<JsonValue>) -> String {
    match &event.provenance {
        Provenance::Material { .. } => style("(material)").blue().to_string(),
        Provenance::Synthesis { .. } => style("(synthesis)").magenta().to_string(),
        _ => style("(unknown)").dim().to_string(),
    }
}

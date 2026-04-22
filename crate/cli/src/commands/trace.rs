use clap::{Args, ValueEnum};
use console::style;
use serde_json::Value as JsonValue;
use sinex_primitives::Uuid;
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

    # Output as Graphviz DOT for rendering
    sinexctl trace 01912345-... -f dot | dot -Tsvg > provenance.svg

    # Live-poll and re-render every 5 seconds
    sinexctl trace 01912345-... --follow 5
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

    /// Poll and re-render the provenance chain every N seconds (default: 3)
    #[arg(long, value_name = "SECS", default_missing_value = "3", num_args = 0..=1)]
    follow: Option<u64>,
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

        let result = client.trace_lineage(query.clone()).await?;
        self.render(&result)?;

        if let Some(interval_secs) = self.follow {
            let interval_secs = if interval_secs == 0 { 3 } else { interval_secs };
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            // First tick fires immediately; skip it so we don't double-render.
            interval.tick().await;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let result = client.trace_lineage(query.clone()).await?;
                        // Clear terminal if stdout is a TTY.
                        if atty::is(atty::Stream::Stdout) {
                            // Move cursor to top-left and clear screen.
                            print!("\x1B[2J\x1B[1;1H");
                        }
                        self.render(&result)?;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn render(&self, result: &LineageResult) -> Result<()> {
        match self.format {
            OutputFormat::Table => render_tree(result),
            OutputFormat::Json => println!("{}", format_json(result)?),
            OutputFormat::Yaml => println!("{}", format_yaml(result)?),
            OutputFormat::Dot => println!("{}", render_dot(result)),
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

    if result.ancestors.is_empty()
        && result.descendants.is_empty()
        && result.material_links.is_empty()
    {
        println!("  {}", style("No provenance links found.").dim());
    }

    if !result.material_links.is_empty() {
        println!("  {}:", style("Material evidence").cyan().bold());
        for link in &result.material_links {
            println!(
                "    {} {} {} {}",
                style(short_uuid(link.from_material_id)).yellow(),
                style("--").dim(),
                style(&link.relation_type).blue(),
                style(format!("--> {}", short_uuid(link.to_material_id))).yellow(),
            );
        }
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
    }
}

fn event_material_id(event: &Event<JsonValue>) -> Option<Uuid> {
    match &event.provenance {
        Provenance::Material { id, .. } => Some(id.to_uuid()),
        Provenance::Synthesis { .. } => None,
    }
}

fn short_uuid(id: Uuid) -> String {
    let value = id.to_string();
    value[..8.min(value.len())].to_string()
}

/// Render the lineage result as a Graphviz DOT graph.
///
/// Material events are rendered with a light-blue fill; synthesis events with a
/// light-yellow fill.  Edges flow from parent → child (i.e., ancestor → root →
/// descendant).
fn render_dot(result: &LineageResult) -> String {
    let mut out = String::from("digraph provenance {\n  rankdir=TB;\n");

    // Helper: emit a node declaration.
    let node_decl = |event: &Event<JsonValue>| -> String {
        let id_full = event
            .id
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string);
        let id_short = &id_full[..8.min(id_full.len())];

        let timestamp = event.ts_orig.map_or_else(
            || "no timestamp".to_string(),
            |ts| {
                ts.format(time::macros::format_description!(
                    "[year]-[month]-[day] [hour]:[minute]:[second]"
                ))
                .unwrap_or_else(|_| "invalid".to_string())
            },
        );

        let label = format!(
            "{}\\n{}/{}\\n{}",
            id_short, event.source, event.event_type, timestamp
        );

        let (fill_color, extra) = match &event.provenance {
            Provenance::Material { .. } => ("lightblue", ""),
            Provenance::Synthesis { .. } => ("lightyellow", ""),
        };

        format!(
            "  \"{id_full}\" [label=\"{label}\" shape=box style=filled fillcolor={fill_color}{extra}];\n"
        )
    };

    // Emit all node declarations.
    out.push_str(&node_decl(&result.root));
    for node in &result.ancestors {
        out.push_str(&node_decl(&node.event));
    }
    for node in &result.descendants {
        out.push_str(&node_decl(&node.event));
    }

    let root_id = result
        .root
        .id
        .as_ref()
        .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string);

    // Ancestor edges: ancestor → root (or ancestor → child closer to root).
    // We emit edges based on depth ordering: depth N+1 → depth N → root.
    // For simplicity we emit each ancestor → root; the DOT layout handles depth
    // positioning via rankdir=TB.
    for node in &result.ancestors {
        let anc_id = node
            .event
            .id
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string);
        out.push_str(&format!(
            "  \"{anc_id}\" -> \"{root_id}\" [label=\"ancestor\"];\n"
        ));
    }

    // Descendant edges: root → descendant.
    for node in &result.descendants {
        let desc_id = node
            .event
            .id
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string);
        out.push_str(&format!(
            "  \"{root_id}\" -> \"{desc_id}\" [label=\"synthesis\"];\n"
        ));
    }

    for event in std::iter::once(&result.root)
        .chain(result.ancestors.iter().map(|node| &node.event))
        .chain(result.descendants.iter().map(|node| &node.event))
    {
        if let Some(material_id) = event_material_id(event) {
            let event_id = event
                .id
                .as_ref()
                .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string);
            out.push_str(&format!(
                "  \"material:{material_id}\" [label=\"material\\n{}\" shape=note style=filled fillcolor=lightcyan];\n",
                short_uuid(material_id)
            ));
            out.push_str(&format!(
                "  \"material:{material_id}\" -> \"{event_id}\" [label=\"material\" style=dotted];\n"
            ));
        }
    }

    for link in &result.material_links {
        out.push_str(&format!(
            "  \"material:{}\" [label=\"material\\n{}\" shape=note style=filled fillcolor=lightcyan];\n",
            link.from_material_id,
            short_uuid(link.from_material_id)
        ));
        out.push_str(&format!(
            "  \"material:{}\" [label=\"material\\n{}\" shape=note style=filled fillcolor=lightcyan];\n",
            link.to_material_id,
            short_uuid(link.to_material_id)
        ));
        out.push_str(&format!(
            "  \"material:{}\" -> \"material:{}\" [label=\"{}\" style=dashed color=gray50];\n",
            link.from_material_id, link.to_material_id, link.relation_type
        ));
    }

    out.push('}');
    out
}

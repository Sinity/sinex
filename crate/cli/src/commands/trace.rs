use clap::{Args, ValueEnum};
use console::style;
use serde_json::Value as JsonValue;
use sinex_primitives::Uuid;
use sinex_primitives::events::{Event, Provenance};
use sinex_primitives::ids::Id;
use sinex_primitives::query::{LineageDirection, LineageNode, LineageQuery, LineageResult};
use std::collections::{BTreeSet, HashSet};
use std::io::IsTerminal;

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
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let query = LineageQuery {
            event_id: self.event_id,
            direction: self.direction.into(),
            max_depth: self.max_depth,
        };

        let result = client.trace_lineage(query.clone()).await?;
        self.render(&result, format)?;

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
                        if std::io::stdout().is_terminal() {
                            // Move cursor to top-left and clear screen.
                            print!("\x1B[2J\x1B[1;1H");
                        }
                        self.render(&result, format)?;
                    }
                    _ = tokio::signal::ctrl_c() => {
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    fn render(&self, result: &LineageResult, format: OutputFormat) -> Result<()> {
        match format {
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
/// light-yellow fill. Edges use provenance directly when both endpoints are in
/// the lineage result: material edges are dotted blue, synthesis edges are solid
/// purple, and auxiliary source-material links are dashed gray.
fn render_dot(result: &LineageResult) -> String {
    let mut out = String::from(
        "digraph provenance {\n  rankdir=TB;\n  graph [fontname=\"Inter\"];\n  node [fontname=\"Inter\"];\n  edge [fontname=\"Inter\"];\n",
    );

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

        let label = escape_dot_label(&format!(
            "{}\\n{}/{}\\n{}",
            id_short, event.source, event.event_type, timestamp
        ));

        let (fill_color, extra) = match &event.provenance {
            Provenance::Material { .. } => ("#d9ecff", ""),
            Provenance::Synthesis { .. } => ("#fff2bf", ""),
        };

        format!(
            "  \"{}\" [label=\"{label}\" shape=box style=filled fillcolor=\"{fill_color}\"{extra}];\n",
            escape_dot_id(&id_full)
        )
    };

    out.push_str("  subgraph cluster_events {\n    label=\"events\";\n    color=\"#d0d7de\";\n");

    // Emit all node declarations.
    out.push_str(&node_decl(&result.root));
    for node in &result.ancestors {
        out.push_str(&node_decl(&node.event));
    }
    for node in &result.descendants {
        out.push_str(&node_decl(&node.event));
    }
    out.push_str("  }\n");

    let events: Vec<&Event<JsonValue>> = std::iter::once(&result.root)
        .chain(result.ancestors.iter().map(|node| &node.event))
        .chain(result.descendants.iter().map(|node| &node.event))
        .collect();
    let event_ids: HashSet<Id<Event<JsonValue>>> =
        events.iter().filter_map(|event| event.id).collect();
    let mut emitted_edges = BTreeSet::new();
    let mut emitted_material_nodes = HashSet::new();

    for event in &events {
        emit_synthesis_edges(event, &event_ids, &mut emitted_edges, &mut out);
        if let Some(material_id) = event_material_id(event) {
            emit_material_node(material_id, &mut emitted_material_nodes, &mut out);
            let event_id = event_dot_id(event);
            out.push_str(&format!(
                "  \"material:{}\" -> \"{event_id}\" [label=\"material\" style=dotted color=\"#0969da\" fontcolor=\"#0969da\"];\n",
                escape_dot_id(&material_id.to_string())
            ));
        }
    }

    for link in &result.material_links {
        emit_material_node(link.from_material_id, &mut emitted_material_nodes, &mut out);
        emit_material_node(link.to_material_id, &mut emitted_material_nodes, &mut out);
        out.push_str(&format!(
            "  \"material:{}\" -> \"material:{}\" [label=\"{}\" style=dashed color=\"#6e7781\" fontcolor=\"#6e7781\"];\n",
            escape_dot_id(&link.from_material_id.to_string()),
            escape_dot_id(&link.to_material_id.to_string()),
            escape_dot_label(&link.relation_type)
        ));
    }

    out.push_str(
        "  subgraph cluster_legend {\n    label=\"legend\";\n    color=\"#d0d7de\";\n    \"legend:material\" [label=\"material event\" shape=box style=filled fillcolor=\"#d9ecff\"];\n    \"legend:synthesis\" [label=\"synthesis event\" shape=box style=filled fillcolor=\"#fff2bf\"];\n    \"legend:source\" [label=\"source material\" shape=note style=filled fillcolor=\"#ddf4ff\"];\n    \"legend:source\" -> \"legend:material\" [label=\"material\" style=dotted color=\"#0969da\" fontcolor=\"#0969da\"];\n    \"legend:material\" -> \"legend:synthesis\" [label=\"synthesis\" color=\"#8250df\" fontcolor=\"#8250df\"];\n  }\n",
    );

    out.push('}');
    out
}

fn emit_synthesis_edges(
    event: &Event<JsonValue>,
    event_ids: &HashSet<Id<Event<JsonValue>>>,
    emitted_edges: &mut BTreeSet<(String, String)>,
    out: &mut String,
) {
    let Some(to_id) = event.id else {
        return;
    };
    let Provenance::Synthesis {
        source_event_ids, ..
    } = &event.provenance
    else {
        return;
    };

    for from_id in source_event_ids {
        if !event_ids.contains(from_id) {
            continue;
        }

        let from = from_id.to_string();
        let to = to_id.to_string();
        if !emitted_edges.insert((from.clone(), to.clone())) {
            continue;
        }

        out.push_str(&format!(
            "  \"{}\" -> \"{}\" [label=\"synthesis\" color=\"#8250df\" fontcolor=\"#8250df\"];\n",
            escape_dot_id(&from),
            escape_dot_id(&to)
        ));
    }
}

fn emit_material_node(material_id: Uuid, emitted: &mut HashSet<Uuid>, out: &mut String) {
    if !emitted.insert(material_id) {
        return;
    }

    out.push_str(&format!(
        "  \"material:{}\" [label=\"material\\n{}\" shape=note style=filled fillcolor=\"#ddf4ff\"];\n",
        escape_dot_id(&material_id.to_string()),
        escape_dot_label(&short_uuid(material_id))
    ));
}

fn event_dot_id(event: &Event<JsonValue>) -> String {
    escape_dot_id(
        &event
            .id
            .as_ref()
            .map_or_else(|| "unknown".to_string(), std::string::ToString::to_string),
    )
}

fn escape_dot_id(value: &str) -> String {
    escape_dot_label(value)
}

fn escape_dot_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sinex_primitives::events::{DynamicPayload, SourceMaterial};
    use sinex_primitives::ids::Id;
    use sinex_primitives::query::SourceMaterialLinkInfo;

    fn material_event(source: &str, event_type: &str) -> Event<JsonValue> {
        let mut event = DynamicPayload::new(source, event_type, json!({}))
            .from_material(Id::<SourceMaterial>::new())
            .build()
            .expect("material test event should build");
        event.id = Some(Id::new());
        event
    }

    fn synthesis_event(
        source: &str,
        event_type: &str,
        parents: impl IntoIterator<Item = Id<Event<JsonValue>>>,
    ) -> Event<JsonValue> {
        let mut event = DynamicPayload::new(source, event_type, json!({}))
            .from_parents(parents)
            .expect("synthesis test event should accept non-empty parents")
            .build()
            .expect("synthesis test event should build");
        event.id = Some(Id::new());
        event
    }

    fn event_id(event: &Event<JsonValue>) -> String {
        event
            .id
            .expect("test event should have an id")
            .to_string()
    }

    #[test]
    fn dot_renderer_uses_provenance_edges_instead_of_flattening_to_root() {
        let ancestor = material_event("fs", "file.created");
        let root = synthesis_event(
            "process",
            "document.parsed",
            [ancestor.id.expect("ancestor id")],
        );
        let descendant = synthesis_event(
            "process",
            "document.chunked",
            [root.id.expect("root id")],
        );

        let dot = render_dot(&LineageResult {
            root: root.clone(),
            ancestors: vec![LineageNode {
                event: ancestor.clone(),
                depth: 1,
            }],
            descendants: vec![LineageNode {
                event: descendant.clone(),
                depth: 1,
            }],
            material_links: Vec::new(),
        });

        let ancestor_id = event_id(&ancestor);
        let root_id = event_id(&root);
        let descendant_id = event_id(&descendant);

        assert!(
            dot.contains(&format!("\"{ancestor_id}\" -> \"{root_id}\"")),
            "DOT should render the ancestor event as the root's synthesis parent"
        );
        assert!(
            dot.contains(&format!("\"{root_id}\" -> \"{descendant_id}\"")),
            "DOT should render the root event as the descendant's synthesis parent"
        );
        assert!(
            dot.contains("color=\"#8250df\""),
            "synthesis edges should be visually distinct"
        );
    }

    #[test]
    fn dot_renderer_includes_material_evidence_and_legend() {
        let root = material_event("fs", "file.created");
        let from_material_id = Id::<SourceMaterial>::new().to_uuid();
        let to_material_id = Id::<SourceMaterial>::new().to_uuid();

        let dot = render_dot(&LineageResult {
            root,
            ancestors: Vec::new(),
            descendants: Vec::new(),
            material_links: vec![SourceMaterialLinkInfo {
                from_material_id,
                to_material_id,
                relation_type: "derived_from".to_string(),
                metadata: json!({}),
                created_at: sinex_primitives::Timestamp::now(),
            }],
        });

        assert!(
            dot.contains("label=\"legend\""),
            "DOT output should explain visual edge semantics"
        );
        assert!(
            dot.contains("style=dotted color=\"#0969da\""),
            "material provenance edge should be dotted and blue"
        );
        assert!(
            dot.contains("style=dashed color=\"#6e7781\""),
            "source-material evidence link should be dashed and gray"
        );
    }
}

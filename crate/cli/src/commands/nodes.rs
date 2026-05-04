use clap::Args;
use color_eyre::Result;
use console::style;
use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::temporal::Timestamp;

use crate::client::GatewayClient;
use crate::fmt::format_heartbeat_age;
use crate::model::{NodeRole, OutputFormat};

/// List running nodes with status, health, and uptime.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # List all running nodes
    sinexctl nodes

    # Filter by capture nodes
    sinexctl nodes --role capture

    # Only synthesis (automata) nodes
    sinexctl nodes --role synthesis
")]
pub struct NodesCommand {
    /// Filter by role
    #[arg(long)]
    role: Option<NodeRole>,
}

impl NodesCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let nodes = client.list_nodes(self.role).await?;

        let enriched: Vec<EnrichedNodeInfo> = nodes
            .into_iter()
            .map(|info| {
                let now = Timestamp::now();
                let healthy = info
                    .last_heartbeat
                    .is_some_and(|hb| (now - hb).whole_seconds() < 60);
                let stale = info.last_heartbeat.is_some() && !healthy;
                EnrichedNodeInfo {
                    info,
                    healthy,
                    stale,
                }
            })
            .collect();

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                let payload = serde_json::json!({
                    "nodes": enriched.iter().map(|n| serde_json::json!({
                        "instance_id": n.info.instance_id.as_str(),
                        "node_type": n.info.node_type.to_string(),
                        "hostname": n.info.hostname.as_ref().map(|h| h.as_str()),
                        "healthy": n.healthy,
                        "stale": n.stale,
                        "last_heartbeat": n.info.last_heartbeat.map(|hb| hb.format_rfc3339()),
                        "is_leader": n.info.is_leader,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", serde_json::to_string_pretty(&payload)?);
            }
            OutputFormat::Yaml => {
                let payload = serde_json::json!({
                    "nodes": enriched.iter().map(|n| serde_json::json!({
                        "instance_id": n.info.instance_id.as_str(),
                        "node_type": n.info.node_type.to_string(),
                        "hostname": n.info.hostname.as_ref().map(|h| h.as_str()),
                        "healthy": n.healthy,
                        "stale": n.stale,
                        "last_heartbeat": n.info.last_heartbeat.map(|hb| hb.format_rfc3339()),
                        "is_leader": n.info.is_leader,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", crate::fmt::format_yaml(&payload)?);
            }
            OutputFormat::Table => {
                render_nodes_table(&enriched);
            }
        }

        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────
// Enriched node wrapper
// ─────────────────────────────────────────────────────────────

struct EnrichedNodeInfo {
    info: InstanceInfo,
    healthy: bool,
    stale: bool,
}

// ─────────────────────────────────────────────────────────────
// Terminal table rendering
// ─────────────────────────────────────────────────────────────

fn render_nodes_table(nodes: &[EnrichedNodeInfo]) {
    if nodes.is_empty() {
        println!("{}", style("No nodes found.").dim());
        return;
    }

    let total = nodes.len();
    let healthy_count = nodes.iter().filter(|n| n.healthy).count();
    let stale_count = nodes.iter().filter(|n| n.stale).count();
    let unknown_count = total - healthy_count - stale_count;

    // Summary header
    let mut summary_parts = Vec::new();
    if healthy_count > 0 {
        summary_parts.push(format!("{} healthy", style(healthy_count).green()));
    }
    if stale_count > 0 {
        summary_parts.push(format!("{} stale", style(stale_count).yellow()));
    }
    if unknown_count > 0 {
        summary_parts.push(format!("{} unknown", style(unknown_count).dim()));
    }
    println!(
        "{} node{}: {}",
        style(total).bold(),
        if total == 1 { "" } else { "s" },
        summary_parts.join(", ")
    );

    println!("{}", style("─".repeat(90)).dim());

    // Column headers
    println!(
        "  {:<32} {:<14} {:<10} {:<12} {:<10} {}",
        "NAME", "TYPE", "HEALTH", "LAST SEEN", "HOST", "LEADER"
    );
    println!(
        "  {:-<32} {:-<14} {:-<10} {:-<12} {:-<10} {:-<6}",
        "", "", "", "", "", ""
    );

    for node in nodes {
        let name = node.info.instance_id.as_str();
        let node_type = node.info.node_type.to_string().to_lowercase();

        let health = if node.healthy {
            style("healthy").green()
        } else if node.stale {
            style("stale").yellow()
        } else {
            style("unknown").dim()
        };

        let last_seen = node
            .info
            .last_heartbeat
            .as_ref()
            .map(|hb| format_heartbeat_age(hb))
            .unwrap_or_else(|| "never".to_string());

        let host = node
            .info
            .hostname
            .as_ref()
            .map(|h| h.as_str())
            .unwrap_or("—");

        let leader = if node.info.is_leader {
            style("yes").cyan()
        } else {
            style("—").dim()
        };

        println!(
            "  {:<32} {:<14} {:<16} {:<12} {:<10} {}",
            name, node_type, health, last_seen, host, leader
        );
    }
}

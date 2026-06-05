use clap::Args;
use color_eyre::Result;
use console::style;
use sinex_primitives::rpc::{
    automata::AutomataStatusResponse, coordination::InstanceInfo, system::SystemHealthResponse,
    telemetry::RecentActivityEntry,
};
use sinex_primitives::temporal::Timestamp;

use crate::client::GatewayClient;
use crate::fmt::format_heartbeat_age;
use crate::model::OutputFormat;

/// Show what's happening right now — recent activity, active modules, current status.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Quick dashboard
    sinexctl now

    # JSON format for scripting
    sinexctl now -f json | jq '.health.status'
")]
pub struct NowCommand;

impl NowCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        // Fetch everything in parallel.
        let (health, modules, recent, automata) = tokio::try_join!(
            async { client.health().await },
            async { client.list_runtime(None).await },
            async { client.telemetry_recent_activity(Some(10)).await },
            async { client.automata_status(60, 300).await },
        )?;

        let snapshot = NowSnapshot {
            health,
            modules: modules.clone(),
            recent,
            automata,
        };

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", serde_json::to_string_pretty(&snapshot)?);
            }
            OutputFormat::Yaml => {
                println!("{}", serde_yml::to_string(&snapshot)?);
            }
            OutputFormat::Table => {
                render_table(&snapshot);
            }
        }

        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────
// Snapshot for structured output
// ─────────────────────────────────────────────────────────────

#[derive(serde::Serialize)]
struct NowSnapshot {
    health: SystemHealthResponse,
    modules: Vec<InstanceInfo>,
    recent: Vec<RecentActivityEntry>,
    automata: AutomataStatusResponse,
}

// ─────────────────────────────────────────────────────────────
// Terminal rendering
// ─────────────────────────────────────────────────────────────

fn render_table(snapshot: &NowSnapshot) {
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "unknown".to_string());
    println!("{}  {}", style("Now").bold().cyan(), style(&now).dim());
    println!("{}", style("═".repeat(60)).dim());

    // ── Health signals ──────────────────────────────────────────

    let now_ts = Timestamp::now();
    let node_count = snapshot.modules.len();
    let healthy_nodes = snapshot
        .modules
        .iter()
        .filter(|n| {
            n.last_heartbeat
                .is_some_and(|hb| (now_ts - hb).whole_seconds() < 60)
        })
        .count();

    let runtime_status_label = if healthy_nodes == node_count && node_count > 0 {
        style("healthy").green()
    } else if healthy_nodes > 0 {
        style("degraded").yellow()
    } else if node_count == 0 {
        style("no modules").dim()
    } else {
        style("unhealthy").red()
    };

    let gateway_icon = if snapshot.health.healthy {
        style("✓").green()
    } else {
        style("✗").red()
    };
    let gateway_status = snapshot.health.status.to_string().to_lowercase();
    println!("  Gateway:  {gateway_icon} {gateway_status}");
    if !snapshot.health.degradation_reasons.is_empty() {
        for reason in &snapshot.health.degradation_reasons {
            println!(
                "            {} {}",
                style("!").yellow(),
                style(reason).yellow()
            );
        }
    }

    println!(
        "  Nodes:    {} ({}/{} healthy)",
        runtime_status_label,
        style(healthy_nodes).bold(),
        style(node_count).dim()
    );
    println!(
        "  Automata: {} registered, {} live",
        style(snapshot.automata.automata.len()).bold(),
        style(snapshot.automata.automata.iter().filter(|a| a.live).count()).bold()
    );

    // ── Recent activity ─────────────────────────────────────────

    println!();
    println!("{}", style("Recent Activity").bold());
    if snapshot.recent.is_empty() {
        println!("  (no recent activity)");
    } else {
        for entry in &snapshot.recent {
            let ts = entry
                .timestamp
                .as_deref()
                .map_or("unknown", |t| &t[..t.len().min(19)]);
            let detail = entry.detail.as_deref().unwrap_or("");
            let detail_display = if detail.len() > 50 {
                format!("{}...", &detail[..47])
            } else {
                detail.to_string()
            };
            let activity = if let Some(ctx) = &entry.context {
                format!("{} ({})", entry.activity_type, ctx)
            } else {
                entry.activity_type.clone()
            };
            println!(
                "  {}  {}  {}",
                style(ts).dim(),
                style(activity).cyan(),
                detail_display
            );
        }
    }

    // ── Active modules ────────────────────────────────────────────

    if !snapshot.modules.is_empty() {
        println!();
        println!("{}", style("Active Nodes").bold());
        println!(
            "  {:<30} {:<14} {:<10}  {:<10}  LDR",
            "NAME", "TYPE", "STATUS", "LAST SEEN"
        );
        println!(
            "  {:-<30} {:-<14} {:-<10}  {:-<10}  {:-<3}",
            "", "", "", "", ""
        );

        for node in &snapshot.modules {
            let age = node
                .last_heartbeat
                .as_ref()
                .map_or_else(|| "never".to_string(), format_heartbeat_age);

            let status = if node
                .last_heartbeat
                .is_some_and(|hb| (now_ts - hb).whole_seconds() < 60)
            {
                style("healthy").green()
            } else if node.last_heartbeat.is_some() {
                style("stale").yellow()
            } else {
                style("unknown").dim()
            };

            let leader = if node.is_leader {
                style("L").cyan()
            } else {
                style(" ").dim()
            };

            println!(
                "  {:<30} {:<14} {:<10}  {:<10}  {}",
                node.instance_id.as_str(),
                node.module_kind.to_string().to_lowercase(),
                status,
                age,
                leader
            );
        }
    }

    // ── Automata summary ────────────────────────────────────────

    if !snapshot.automata.automata.is_empty() {
        println!();
        println!("{}", style("Automata").bold());
        println!("  {:<30} {:<12} {:<10}  EVENTS", "NAME", "LIVE", "STATUS");
        println!("  {:-<30} {:-<12} {:-<10}  {:-<8}", "", "", "", "");

        for automaton in &snapshot.automata.automata {
            let live = if automaton.live {
                style("yes").green()
            } else {
                style("no").dim()
            };
            let run_status = automaton.run_status.as_deref().unwrap_or("—");
            let events = automaton
                .events_processed_current_run
                .map_or_else(|| "—".to_string(), |n| n.to_string());

            println!(
                "  {:<30} {:<12} {:<10}  {}",
                automaton.module_name.as_str(),
                live,
                style(run_status).dim(),
                events
            );
        }
    }
}

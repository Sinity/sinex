use clap::Args;
use color_eyre::Result;
use console::style;
use sinex_primitives::rpc::coordination::InstanceInfo;
use sinex_primitives::temporal::Timestamp;

use crate::client::GatewayClient;
use crate::fmt::format_heartbeat_age;
use crate::model::{OutputFormat, RuntimeModuleRole};

/// List running modules with status, health, and uptime.
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # List all running modules
    sinexctl modules

    # Filter by capture modules
    sinexctl modules --role capture

    # Only derived (automata) modules
    sinexctl modules --role derived
")]
pub struct RuntimePresenceCommand {
    /// Filter by role
    #[arg(long)]
    role: Option<RuntimeModuleRole>,
}

impl RuntimePresenceCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let modules = client.list_runtime(self.role).await?;

        let enriched: Vec<EnrichedRuntimeInfo> = modules
            .into_iter()
            .map(|info| {
                let now = Timestamp::now();
                let healthy = info
                    .last_heartbeat
                    .is_some_and(|hb| (now - hb).whole_seconds() < 60);
                let stale = info.last_heartbeat.is_some() && !healthy;
                EnrichedRuntimeInfo {
                    info,
                    healthy,
                    stale,
                }
            })
            .collect();

        match format {
            OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
                let payload = serde_json::json!({
                    "modules": enriched.iter().map(|n| serde_json::json!({
                        "instance_id": n.info.instance_id.as_str(),
                        "module_kind": n.info.module_kind.to_string(),
                        "hostname": n.info.hostname.as_ref().map(sinex_primitives::HostName::as_str),
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
                    "modules": enriched.iter().map(|n| serde_json::json!({
                        "instance_id": n.info.instance_id.as_str(),
                        "module_kind": n.info.module_kind.to_string(),
                        "hostname": n.info.hostname.as_ref().map(sinex_primitives::HostName::as_str),
                        "healthy": n.healthy,
                        "stale": n.stale,
                        "last_heartbeat": n.info.last_heartbeat.map(|hb| hb.format_rfc3339()),
                        "is_leader": n.info.is_leader,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", crate::fmt::format_yaml(&payload)?);
            }
            OutputFormat::Table => {
                render_modules_table(&enriched);
            }
        }

        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────
// Enriched runtime-module wrapper
// ─────────────────────────────────────────────────────────────

struct EnrichedRuntimeInfo {
    info: InstanceInfo,
    healthy: bool,
    stale: bool,
}

// ─────────────────────────────────────────────────────────────
// Terminal table rendering
// ─────────────────────────────────────────────────────────────

fn render_modules_table(modules: &[EnrichedRuntimeInfo]) {
    if modules.is_empty() {
        println!("{}", style("No modules found.").dim());
        return;
    }

    let total = modules.len();
    let healthy_count = modules.iter().filter(|module| module.healthy).count();
    let stale_count = modules.iter().filter(|module| module.stale).count();
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
        "{} module{}: {}",
        style(total).bold(),
        if total == 1 { "" } else { "s" },
        summary_parts.join(", ")
    );

    println!("{}", style("─".repeat(90)).dim());

    // Column headers
    println!(
        "  {:<32} {:<14} {:<10} {:<12} {:<10} LEADER",
        "NAME", "TYPE", "HEALTH", "LAST SEEN", "HOST"
    );
    println!(
        "  {:-<32} {:-<14} {:-<10} {:-<12} {:-<10} {:-<6}",
        "", "", "", "", "", ""
    );

    for module in modules {
        let name = module.info.instance_id.as_str();
        let module_kind = module.info.module_kind.to_string().to_lowercase();

        let health = if module.healthy {
            style("healthy").green()
        } else if module.stale {
            style("stale").yellow()
        } else {
            style("unknown").dim()
        };

        let last_seen = module
            .info
            .last_heartbeat
            .as_ref()
            .map_or_else(|| "never".to_string(), format_heartbeat_age);

        let host = module
            .info
            .hostname
            .as_ref()
            .map_or("—", sinex_primitives::HostName::as_str);

        let leader = if module.info.is_leader {
            style("yes").cyan()
        } else {
            style("—").dim()
        };

        println!("  {name:<32} {module_kind:<14} {health:<16} {last_seen:<12} {host:<10} {leader}");
    }
}

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{HistoryDb, InvocationStatus};
use anyhow::Result;
use console::style;

#[derive(Debug, Clone, clap::Args)]
pub struct StatusCommand {
    /// Service to check (default: all)
    pub service: Option<String>,
    /// Watch for changes
    #[arg(short, long)]
    pub watch: bool,
}

impl XtaskCommand for StatusCommand {
    fn name(&self) -> &str {
        "status"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if ctx.is_human() {
            println!(
                "{}",
                style("━━━━━━━━━━━━━━━━ WORKSPACE STATUS ━━━━━━━━━━━━━━━━").bold()
            );
        }

        // 1. Infrastructure Health
        println!("\n{}", style("Infrastructure:").bold());

        // Postgres
        let pg_ready = std::process::Command::new("pg_isready")
            .arg("-q")
            .status()
            .is_ok_and(|s| s.success());
        println!(
            "  {:<12} {}",
            "Postgres",
            if pg_ready {
                style("online").green()
            } else {
                style("offline").red()
            }
        );

        // NATS (check computed port from env)
        let nats_port = std::env::var("SINEX_DEV_NATS_PORT")
            .ok()
            .and_then(|s| s.parse::<u16>().ok())
            .unwrap_or(4222);
        let nats_ready = std::net::TcpStream::connect(format!("127.0.0.1:{}", nats_port)).is_ok();
        println!(
            "  {:<12} {}",
            "NATS",
            if nats_ready {
                style("online").green()
            } else {
                style("offline").red()
            }
        );

        // 2. Active Services
        println!("\n{}", style("Active Services:").bold());
        let services = ["sinex-gateway", "sinex-ingestd"];
        for svc in services {
            let running = std::process::Command::new("pgrep")
                .arg("-f")
                .arg(svc)
                .output()
                .is_ok_and(|o| !o.stdout.is_empty());
            println!(
                "  {:<12} {}",
                svc,
                if running {
                    style("running").green()
                } else {
                    style("stopped").dim()
                }
            );
        }

        // 3. Recent Activity
        let history = open_history_db()?;
        println!("\n{}", style("Recent activity:").bold());
        let recent = history.get_recent(10, None)?;
        for inv in &recent {
            let status_style = match inv.status {
                InvocationStatus::Success => style("success").green(),
                InvocationStatus::Failed => style("failed").red(),
                InvocationStatus::Running => style("running").yellow(),
                InvocationStatus::Cancelled => style("cancelled").dim(),
            };
            println!(
                "  {:<15} {:<10} ({:.1}s) {}",
                inv.command,
                status_style,
                inv.duration_secs.unwrap_or(0.0),
                inv.started_at
                    .format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            );
        }

        // 4. Warnings
        println!("\n{}", style("Warnings:").bold());
        let last_fail = recent.iter().find(|i| i.status == InvocationStatus::Failed);
        if let Some(fail) = last_fail {
            println!("  ⚠ Last run of '{}' failed.", fail.command);
        } else {
            let mut has_warning = false;
            if !pg_ready {
                println!("  ⚠ Postgres is offline. Some commands will fail.");
                has_warning = true;
            }
            if !nats_ready {
                println!("  ⚠ NATS is offline. Real-time features won't work.");
                has_warning = true;
            }
            if !has_warning {
                println!("  ✓ No recent failures or infrastructure issues.");
            }
        }

        Ok(CommandResult::success())
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::default()
    }
}

fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

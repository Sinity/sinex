//! Status command - show environment status

use anyhow::Result;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::{HistoryDb, InvocationStatus};

/// Status command configuration
pub struct StatusCommand {
    pub watch: bool,
}

impl XtaskCommand for StatusCommand {
    fn name(&self) -> &str {
        "status"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        loop {
            if self.watch {
                // Clear screen for watch mode
                print!("\x1B[2J\x1B[H");
            }

            ctx.heading("environment status");

            // Database status
            let db_ok = Command::new("psql")
                .args(["-c", "SELECT 1"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false);

            let db_sym = if db_ok { "✓" } else { "✗" };
            println!(
                "  Database: {} {}",
                db_sym,
                if db_ok { "connected" } else { "unavailable" }
            );

            // NATS status
            let nats_url =
                std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "localhost:4222".into());
            let nats_ok = std::net::TcpStream::connect_timeout(
                &nats_url
                    .trim_start_matches("nats://")
                    .parse()
                    .unwrap_or_else(|_| "127.0.0.1:4222".parse().unwrap()),
                std::time::Duration::from_secs(1),
            )
            .is_ok();

            let nats_sym = if nats_ok { "✓" } else { "✗" };
            println!(
                "  NATS:     {} {}",
                nats_sym,
                if nats_ok { &nats_url } else { "unavailable" }
            );

            // Git status
            if let Ok(output) = Command::new("git")
                .args(["branch", "--show-current"])
                .output()
            {
                if output.status.success() {
                    let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    let dirty = Command::new("git")
                        .args(["status", "--porcelain"])
                        .output()
                        .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
                        .unwrap_or(0);

                    print!("  Git:      {}", branch);
                    if dirty > 0 {
                        print!(" ({} dirty)", dirty);
                    }
                    println!();
                }
            }

            // History info
            if let Ok(db) = open_history_db() {
                if let Ok(Some(last_check)) = db.get_last("check") {
                    let status_sym = match last_check.status {
                        InvocationStatus::Success => "✓",
                        InvocationStatus::Failed => "✗",
                        _ => "?",
                    };
                    println!(
                        "  Build:    {} {:?} ({})",
                        status_sym,
                        last_check.status,
                        last_check.started_at.format("%H:%M")
                    );
                }
                if let Ok(Some(last_test)) = db.get_last("test") {
                    let status_sym = match last_test.status {
                        InvocationStatus::Success => "✓",
                        InvocationStatus::Failed => "✗",
                        _ => "?",
                    };
                    println!(
                        "  Test:     {} {:?} ({})",
                        status_sym,
                        last_test.status,
                        last_test.started_at.format("%H:%M")
                    );
                }
            }

            if !self.watch {
                break;
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
        }

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("devenv".to_string()),
            timeout: None,
            modifies_state: false,
            track_in_history: false,
        }
    }
}

/// Open the history database.
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_name() {
        let cmd = StatusCommand { watch: false };
        assert_eq!(cmd.name(), "status");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = StatusCommand { watch: false };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("devenv".to_string()));
        assert!(!metadata.modifies_state);
        assert!(!metadata.track_in_history);
    }

    #[test]
    fn test_watch_flag() {
        let cmd = StatusCommand { watch: true };
        assert!(cmd.watch);
    }
}

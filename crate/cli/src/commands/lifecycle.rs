//! Data lifecycle management commands.
//!
//! Implements the "Principled Forgetting" three-tier data lifecycle:
//!
//! ```text
//! Live (core.events) ←→ Archive (audit.archived_events) → Tombstone (core.event_tombstones)
//! ```
//!
//! - **status**: Show tier sizes, age distributions
//! - **archive**: Move live events to archive (with cascade)
//! - **restore**: Move archived events back to live (with cascade)
//! - **tombstone**: Move archived events to tombstones (one-way, permanent!)

use clap::{Args, Subcommand};
use humantime::parse_duration;
use std::time::Duration;

use crate::client::GatewayClient;
use crate::fmt::{with_spinner_result, CommandOutput};
use crate::model::OutputFormat;
use crate::Result;

/// Data lifecycle management
#[derive(Debug, Subcommand)]
#[command(after_help = "\
PHILOSOPHY:
    Sinex embraces 'Principled Forgetting' - explicit, auditable data lifecycle management.
    No automatic silent deletion. You control when data moves between tiers.

TIERS:
    Live (core.events)        - Full data, real-time queries
    Archive (audit.archived_events) - Full data preserved, can restore
    Tombstone (core.event_tombstones) - Skeleton only, permanent (data gone)

EXAMPLES:
    # Show lifecycle status
    sinexctl lifecycle status

    # Archive old events
    sinexctl lifecycle archive --before 30d --source terminal

    # Restore archived events
    sinexctl lifecycle restore <event_id>

    # Tombstone archived events (PERMANENT - data is gone!)
    sinexctl lifecycle tombstone --before 365d --yes-i-understand-data-is-gone
")]
pub enum LifecycleCommands {
    /// Show lifecycle tier status (event counts, age distributions)
    Status(LifecycleStatusCommand),

    /// Archive live events (move to audit.archived_events)
    Archive(LifecycleArchiveCommand),

    /// Restore archived events back to live
    Restore(LifecycleRestoreCommand),

    /// Tombstone archived events (PERMANENT - data is gone!)
    Tombstone(LifecycleTombstoneCommand),
}

impl LifecycleCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Status(cmd) => cmd.execute(client).await,
            Self::Archive(cmd) => cmd.execute(client).await,
            Self::Restore(cmd) => cmd.execute(client).await,
            Self::Tombstone(cmd) => cmd.execute(client).await,
        }
    }
}

/// Show lifecycle tier status
#[derive(Debug, Args)]
pub struct LifecycleStatusCommand {
    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl LifecycleStatusCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = with_spinner_result(
            "Fetching lifecycle status...".to_string(),
            "Lifecycle status retrieved",
            client.lifecycle_status(),
        )
        .await?;

        CommandOutput::single(response, format_status_table).display(&self.format)?;
        Ok(())
    }
}

/// Archive live events
#[derive(Debug, Args)]
pub struct LifecycleArchiveCommand {
    /// Archive events older than this duration (e.g., "30d", "90d", "1y")
    #[arg(long, value_parser = parse_duration_arg)]
    before: Option<Duration>,

    /// Filter by source
    #[arg(long)]
    source: Option<String>,

    /// Archive specific event IDs
    #[arg(long, num_args = 1..)]
    ids: Option<Vec<String>>,

    /// Maximum number of events to archive (default: 1000)
    #[arg(long, default_value = "1000")]
    limit: i64,

    /// Actually perform the archive (otherwise dry-run)
    #[arg(long)]
    confirm: bool,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl LifecycleArchiveCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let before_str = self.before.map(|d| format!("{}s", d.as_secs()));
        let dry_run = !self.confirm;

        let response = with_spinner_result(
            if dry_run {
                "Analyzing archive operation (dry run)...".to_string()
            } else {
                "Archiving events...".to_string()
            },
            if dry_run {
                "Archive analysis complete (dry run)"
            } else {
                "Archive operation complete"
            },
            client.lifecycle_archive(
                self.source.clone(),
                before_str,
                self.ids.clone(),
                self.limit,
                dry_run,
            ),
        )
        .await?;

        CommandOutput::single(response, format_archive_table).display(&self.format)?;

        if dry_run {
            println!();
            println!("This was a DRY RUN. Add --confirm to actually archive events.");
        }

        Ok(())
    }
}

/// Restore archived events back to live
#[derive(Debug, Args)]
pub struct LifecycleRestoreCommand {
    /// Restore specific event IDs
    #[arg(required = true, num_args = 1..)]
    ids: Vec<String>,

    /// Actually perform the restore (otherwise dry-run)
    #[arg(long)]
    confirm: bool,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl LifecycleRestoreCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let dry_run = !self.confirm;

        let response = with_spinner_result(
            if dry_run {
                format!(
                    "Analyzing restore cascade for {} event(s) (dry run)...",
                    self.ids.len()
                )
            } else {
                format!("Restoring {} event(s) with cascade...", self.ids.len())
            },
            if dry_run {
                "Restore analysis complete (dry run)"
            } else {
                "Restore operation complete"
            },
            client.lifecycle_restore(self.ids.clone(), dry_run),
        )
        .await?;

        CommandOutput::single(response, format_restore_table).display(&self.format)?;

        if dry_run {
            println!();
            println!("This was a DRY RUN. Add --confirm to actually restore events.");
        }

        Ok(())
    }
}

/// Tombstone archived events (PERMANENT!)
#[derive(Debug, Args)]
pub struct LifecycleTombstoneCommand {
    /// Tombstone archived events older than this duration
    #[arg(long, value_parser = parse_duration_arg)]
    before: Option<Duration>,

    /// Filter by source
    #[arg(long)]
    source: Option<String>,

    /// Tombstone specific event IDs
    #[arg(long, num_args = 1..)]
    ids: Option<Vec<String>>,

    /// Maximum number of events to tombstone (default: 1000)
    #[arg(long, default_value = "1000")]
    limit: i64,

    /// Reason for tombstoning
    #[arg(long, default_value = "manual tombstone via CLI")]
    reason: String,

    /// REQUIRED: Acknowledge that data will be permanently deleted
    #[arg(long, required = true)]
    yes_i_understand_data_is_gone: bool,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl LifecycleTombstoneCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        if !self.yes_i_understand_data_is_gone {
            return Err(color_eyre::eyre::eyre!(
                "You must acknowledge that tombstoning is PERMANENT.\n\
                 Add --yes-i-understand-data-is-gone to confirm."
            ));
        }

        let before_str = self.before.map(|d| format!("{}s", d.as_secs()));

        // Always do dry run first to show what will happen
        let preview = client
            .lifecycle_tombstone(
                self.source.clone(),
                before_str.clone(),
                self.ids.clone(),
                self.limit,
                self.reason.clone(),
                true, // dry run
            )
            .await?;

        println!();
        println!("⚠️  WARNING: TOMBSTONING IS PERMANENT!");
        println!(
            "   {} events will be reduced to minimal skeletons.",
            preview.cascade_total
        );
        println!("   Payload data will be PERMANENTLY DELETED.");
        println!("   This operation CANNOT be undone.");
        println!();
        println!("Cascade Analysis:");
        println!("  Max depth:        {}", preview.cascade_depth);
        println!("  Total to tombstone: {}", preview.cascade_total);
        println!();

        // Now execute the real operation
        let response = with_spinner_result(
            format!("Tombstoning {} events...", preview.cascade_total),
            "Tombstone operation complete",
            client.lifecycle_tombstone(
                self.source.clone(),
                before_str,
                self.ids.clone(),
                self.limit,
                self.reason.clone(),
                false, // actual execution
            ),
        )
        .await?;

        CommandOutput::single(response, format_tombstone_table).display(&self.format)?;

        println!();
        println!("💀 Data has been permanently deleted.");

        Ok(())
    }
}

/// Parse duration argument (e.g., "30d", "90d", "1y")
fn parse_duration_arg(s: &str) -> std::result::Result<Duration, String> {
    parse_duration(s).map_err(|e| format!("Invalid duration '{}': {}", s, e))
}

// ==================== Table Formatters ====================

fn format_status_table(
    response: &sinex_primitives::rpc::lifecycle::LifecycleStatusResponse,
) -> String {
    let mut output = String::new();
    output.push_str("Data Lifecycle Status\n");
    output.push_str(&format!("{}\n", "═".repeat(70)));
    output.push('\n');

    for tier in &response.tiers {
        let tier_icon = match tier.tier.as_str() {
            "live" => "🟢",
            "archive" => "📦",
            "tombstone" => "💀",
            _ => "  ",
        };

        let tier_name = tier
            .tier
            .chars()
            .next()
            .map(|c| c.to_uppercase().collect::<String>() + &tier.tier[1..])
            .unwrap_or_default();

        output.push_str(&format!("{} {} Tier\n", tier_icon, tier_name));
        output.push_str(&format!(
            "  Events:  {:>12}\n",
            format_count(tier.event_count)
        ));
        output.push_str(&format!("  Sources: {:>12}\n", tier.distinct_sources));

        if let (Some(oldest), Some(newest)) = (&tier.oldest_ts, &tier.newest_ts) {
            output.push_str(&format!("  Oldest:  {}\n", oldest));
            output.push_str(&format!("  Newest:  {}\n", newest));
        } else {
            output.push_str("  (empty)\n");
        }
        output.push('\n');
    }

    output.push_str(&format!("{}\n", "─".repeat(70)));
    output.push_str(&format!(
        "Total events across all tiers: {}\n",
        format_count(response.total_events)
    ));

    output
}

fn format_archive_table(
    response: &sinex_primitives::rpc::lifecycle::LifecycleArchiveResponse,
) -> String {
    let mut output = String::new();

    if response.dry_run {
        output.push_str("Archive Preview (Dry Run)\n");
    } else {
        output.push_str("Archive Complete\n");
    }
    output.push_str(&format!("{}\n", "─".repeat(50)));
    output.push_str(&format!("  Archived:     {}\n", response.archived_count));
    output.push_str(&format!("  Cascade depth: {}\n", response.cascade_depth));
    output.push_str(&format!("  Cascade total: {}\n", response.cascade_total));
    output.push_str(&format!("  Operation ID:  {}\n", response.operation_id));

    output
}

fn format_restore_table(
    response: &sinex_primitives::rpc::lifecycle::LifecycleRestoreResponse,
) -> String {
    let mut output = String::new();

    if response.dry_run {
        output.push_str("Restore Preview (Dry Run)\n");
    } else {
        output.push_str("Restore Complete\n");
    }
    output.push_str(&format!("{}\n", "─".repeat(50)));
    output.push_str(&format!("  Restored:      {}\n", response.restored_count));
    output.push_str(&format!("  Cascade depth: {}\n", response.cascade_depth));
    output.push_str(&format!("  Cascade total: {}\n", response.cascade_total));
    output.push_str(&format!("  Operation ID:  {}\n", response.operation_id));

    output
}

fn format_tombstone_table(
    response: &sinex_primitives::rpc::lifecycle::LifecycleTombstoneResponse,
) -> String {
    let mut output = String::new();

    if response.dry_run {
        output.push_str("Tombstone Preview (Dry Run)\n");
    } else {
        output.push_str("💀 Tombstone Complete (PERMANENT)\n");
    }
    output.push_str(&format!("{}\n", "─".repeat(50)));
    output.push_str(&format!("  Tombstoned:    {}\n", response.tombstoned_count));
    output.push_str(&format!("  Cascade depth: {}\n", response.cascade_depth));
    output.push_str(&format!("  Cascade total: {}\n", response.cascade_total));
    output.push_str(&format!("  Operation ID:  {}\n", response.operation_id));

    output
}

/// Format a count with thousands separators
fn format_count(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }
    result
}

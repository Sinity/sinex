//! Data lifecycle management commands.
//!
//! Implements the "Principled Forgetting" three-tier data lifecycle:
//!
//! ```text
//! Live (core.events) <-> Archive (audit.archived_events) -> Tombstone (core.event_tombstones)
//! ```
//!
//! - **status**: Show tier sizes, age distributions
//! - **archive**: Move live events to archive (with cascade)
//! - **restore**: Move archived events back to live (with cascade)
//! - **tombstone**: Move archived events to tombstones (one-way, permanent!)

use clap::{Args, Subcommand, ValueEnum};
use sinex_primitives::rpc::lifecycle::{
    TombstoneApproveResponse, TombstoneCreateResponse, TombstoneListResponse,
    TombstoneOperationState, TombstonePreviewResponse, TombstoneStatusResponse,
};
use sinex_primitives::utils::timestamp_helpers::parse_relative_std_duration;
use std::time::Duration;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, with_spinner_result};
use crate::model::OutputFormat;

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

    # Two-step tombstone (safer):
    sinexctl lifecycle tombstone create --before 365d --reason 'Annual cleanup'
    sinexctl lifecycle tombstone approve <operation_id> --yes-i-understand-data-is-gone
")]
pub enum LifecycleCommands {
    /// Show lifecycle tier status (event counts, age distributions)
    Status(LifecycleStatusCommand),

    /// Archive live events (move to `audit.archived_events`)
    Archive(LifecycleArchiveCommand),

    /// Restore archived events back to live
    Restore(LifecycleRestoreCommand),

    /// Tombstone archived events (PERMANENT - data is gone!)
    #[command(subcommand)]
    Tombstone(TombstoneCommands),
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

// ─────────────────────────────────────────────────────────────
// Tombstone subcommands (Two-step flow - SEC-003)
// ─────────────────────────────────────────────────────────────

/// Tombstone archived events (PERMANENT!) - Two-step confirmation flow
#[derive(Debug, Subcommand)]
#[command(after_help = "\
TWO-STEP TOMBSTONE FLOW:
    Tombstoning is PERMANENT and cannot be undone. To prevent accidental data loss,
    a two-step confirmation flow is required:

    1. CREATE: Create a tombstone operation with cascade preview
       sinexctl lifecycle tombstone create --before 365d --reason 'Annual cleanup'
       -> Returns operation_id and cascade analysis

    2. APPROVE: Review and approve the operation (must be done within 1 hour)
       sinexctl lifecycle tombstone approve <operation_id> --yes-i-understand-data-is-gone
       -> Executes the tombstone (data is permanently deleted!)

OTHER COMMANDS:
    preview <id>  - Re-view cascade analysis for an operation
    cancel <id>   - Cancel a pending operation
    list          - List all tombstone operations
    status <id>   - Get status of a specific operation
")]
pub enum TombstoneCommands {
    /// Create a new tombstone operation (Step 1)
    Create(TombstoneCreateCommand),

    /// Approve and execute a tombstone operation (Step 2 - PERMANENT!)
    Approve(TombstoneApproveCommand),

    /// Preview cascade analysis for an existing operation
    Preview(TombstonePreviewCommand),

    /// Cancel a pending tombstone operation
    Cancel(TombstoneCancelCommand),

    /// List all tombstone operations
    List(TombstoneListCommand),

    /// Get status of a specific tombstone operation
    Status(TombstoneStatusCommand),
}

impl TombstoneCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::Create(cmd) => cmd.execute(client).await,
            Self::Approve(cmd) => cmd.execute(client).await,
            Self::Preview(cmd) => cmd.execute(client).await,
            Self::Cancel(cmd) => cmd.execute(client).await,
            Self::List(cmd) => cmd.execute(client).await,
            Self::Status(cmd) => cmd.execute(client).await,
        }
    }
}

/// Create a tombstone operation (Step 1)
#[derive(Debug, Args)]
pub struct TombstoneCreateCommand {
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

    /// Reason for tombstoning (required for audit)
    #[arg(long)]
    reason: String,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl TombstoneCreateCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let before_str = self.before.map(|d| format!("{}s", d.as_secs()));

        let response = with_spinner_result(
            "Creating tombstone operation...".to_string(),
            "Tombstone operation created",
            client.tombstone_create(
                self.source.clone(),
                before_str,
                self.ids.clone(),
                self.limit,
                self.reason.clone(),
            ),
        )
        .await?;

        CommandOutput::single(response, format_tombstone_create_table)
            .display(&self.format)?;

        Ok(())
    }
}

/// Approve and execute a tombstone operation (Step 2 - PERMANENT!)
#[derive(Debug, Args)]
pub struct TombstoneApproveCommand {
    /// Operation ID to approve
    operation_id: String,

    /// REQUIRED: Acknowledge that data will be permanently deleted
    #[arg(long, required = true)]
    yes_i_understand_data_is_gone: bool,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl TombstoneApproveCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        if !self.yes_i_understand_data_is_gone {
            return Err(color_eyre::eyre::eyre!(
                "You must acknowledge that tombstoning is PERMANENT.\n\
                 Add --yes-i-understand-data-is-gone to confirm."
            ));
        }

        // First, get the current status to show what will happen
        let status = client.tombstone_status(self.operation_id.clone()).await?;

        // Print the warning banner for table mode
        if matches!(self.format, OutputFormat::Table) {
            println!();
            println!("⚠️  WARNING: TOMBSTONING IS PERMANENT!");
            println!("{}", "═".repeat(60));
            if let Some(analysis) = &status.operation.cascade_analysis {
                println!(
                    "  {} events will be reduced to minimal skeletons.",
                    analysis.cascade_total
                );
            }
            println!("  Payload data will be PERMANENTLY DELETED.");
            println!("  This operation CANNOT be undone.");
            println!("{}", "═".repeat(60));
            println!();
        }

        let response = with_spinner_result(
            "Executing tombstone operation...".to_string(),
            "Tombstone operation complete",
            client.tombstone_approve(self.operation_id.clone(), true),
        )
        .await?;

        CommandOutput::single(response, format_tombstone_approve_table)
            .display(&self.format)?;

        Ok(())
    }
}

/// Preview cascade analysis for an existing operation
#[derive(Debug, Args)]
pub struct TombstonePreviewCommand {
    /// Operation ID to preview
    operation_id: String,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl TombstonePreviewCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = with_spinner_result(
            "Fetching tombstone preview...".to_string(),
            "Preview retrieved",
            client.tombstone_preview(self.operation_id.clone()),
        )
        .await?;

        CommandOutput::single(response, format_tombstone_preview_table)
            .display(&self.format)?;

        Ok(())
    }
}

/// Cancel a pending tombstone operation
#[derive(Debug, Args)]
pub struct TombstoneCancelCommand {
    /// Operation ID to cancel
    operation_id: String,

    /// Optional cancellation reason
    #[arg(long)]
    reason: Option<String>,
}

impl TombstoneCancelCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = with_spinner_result(
            "Cancelling tombstone operation...".to_string(),
            "Operation cancelled",
            client.tombstone_cancel(self.operation_id.clone(), self.reason.clone()),
        )
        .await?;

        println!();
        println!("Tombstone Operation Cancelled");
        println!("  Operation ID: {}", response.operation_id);
        println!("  Status:       {}", response.status);

        Ok(())
    }
}

/// CLI state filter for tombstone operation listing.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum TombstoneStateArg {
    Pending,
    Previewed,
    Executing,
    Completed,
    Cancelled,
    Failed,
    Expired,
}

impl TombstoneStateArg {
    const fn into_state(self) -> TombstoneOperationState {
        match self {
            Self::Pending => TombstoneOperationState::Pending,
            Self::Previewed => TombstoneOperationState::Previewed,
            Self::Executing => TombstoneOperationState::Executing,
            Self::Completed => TombstoneOperationState::Completed,
            Self::Cancelled => TombstoneOperationState::Cancelled,
            Self::Failed => TombstoneOperationState::Failed,
            Self::Expired => TombstoneOperationState::Expired,
        }
    }
}

/// List all tombstone operations
#[derive(Debug, Args)]
pub struct TombstoneListCommand {
    /// Filter by state (pending, previewed, executing, completed, cancelled, failed, expired)
    #[arg(long, value_enum)]
    state: Option<TombstoneStateArg>,

    /// Maximum number of operations to show
    #[arg(long, default_value = "20")]
    limit: i64,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl TombstoneListCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = with_spinner_result(
            "Fetching tombstone operations...".to_string(),
            "Operations retrieved",
            client.tombstone_list(
                self.state.map(TombstoneStateArg::into_state),
                Some(self.limit),
            ),
        )
        .await?;

        if response.operations.is_empty() {
            CommandOutput::<serde_json::Value>::empty("No tombstone operations found.").display(&self.format)?;
            return Ok(());
        }

        CommandOutput::single(response, format_tombstone_list_table)
            .display(&self.format)?;

        Ok(())
    }
}

/// Get status of a specific tombstone operation
#[derive(Debug, Args)]
pub struct TombstoneStatusCommand {
    /// Operation ID to query
    operation_id: String,

    /// Output format
    #[arg(long, short = 'f', value_enum, default_value = "table")]
    format: OutputFormat,
}

impl TombstoneStatusCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let response = with_spinner_result(
            "Fetching operation status...".to_string(),
            "Status retrieved",
            client.tombstone_status(self.operation_id.clone()),
        )
        .await?;

        CommandOutput::single(response, format_tombstone_status_table)
            .display(&self.format)?;

        Ok(())
    }
}

/// Parse duration argument (e.g., "30d", "90d", "1y")
fn parse_duration_arg(s: &str) -> std::result::Result<Duration, String> {
    parse_relative_std_duration(s).ok_or_else(|| format!("Invalid duration '{s}'"))
}

// ==================== Table Formatters ====================

fn format_status_table(
    response: &sinex_primitives::rpc::lifecycle::LifecycleStatusResponse,
) -> String {
    let mut output = String::new();
    output.push_str("Data Lifecycle Status\n");
    output.push_str(&format!("{}\n", "=".repeat(70)));
    output.push('\n');

    for tier in &response.tiers {
        let tier_str = tier.tier.to_string();
        let tier_icon = match tier_str.as_str() {
            "live" => "[L]",
            "archive" => "[A]",
            "tombstone" => "[T]",
            _ => "  ",
        };

        let tier_name = tier_str
            .chars()
            .next()
            .map(|c| c.to_uppercase().collect::<String>() + &tier_str[1..])
            .unwrap_or_default();

        output.push_str(&format!("{tier_icon} {tier_name} Tier\n"));
        output.push_str(&format!(
            "  Events:  {:>12}\n",
            format_count(tier.event_count)
        ));
        output.push_str(&format!("  Sources: {:>12}\n", tier.distinct_sources));

        if let (Some(oldest), Some(newest)) = (&tier.oldest_ts, &tier.newest_ts) {
            output.push_str(&format!("  Oldest:  {oldest}\n"));
            output.push_str(&format!("  Newest:  {newest}\n"));
        } else {
            output.push_str("  (empty)\n");
        }
        output.push('\n');
    }

    output.push_str(&format!("{}\n", "-".repeat(70)));
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
    output.push_str(&format!("{}\n", "-".repeat(50)));
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
    output.push_str(&format!("{}\n", "-".repeat(50)));
    output.push_str(&format!("  Restored:      {}\n", response.restored_count));
    output.push_str(&format!("  Cascade depth: {}\n", response.cascade_depth));
    output.push_str(&format!("  Cascade total: {}\n", response.cascade_total));
    output.push_str(&format!("  Operation ID:  {}\n", response.operation_id));

    output
}

// ==================== Tombstone Table Formatters ====================

fn format_tombstone_create_table(response: &TombstoneCreateResponse) -> String {
    let mut output = String::new();
    output.push_str("\n");
    output.push_str("Tombstone Operation Created\n");
    output.push_str(&"\u{2550}".repeat(60));
    output.push_str("\n\n");
    output.push_str(&format!("  Operation ID: {}\n", response.operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", response.operation.state));
    output.push_str(&format!("  Expires:      {}\n", response.operation.expires_at));
    output.push_str("\n");

    if let Some(analysis) = &response.operation.cascade_analysis {
        output.push_str("Cascade Analysis:\n");
        output.push_str(&format!("  Root events:   {}\n", analysis.root_event_count));
        output.push_str(&format!("  Total cascade: {}\n", analysis.cascade_total));
        output.push_str(&format!("  Max depth:     {}\n", analysis.cascade_depth));
        if !analysis.sample_ids.is_empty() {
            output.push_str(&format!(
                "  Sample IDs:    {} ...\n",
                analysis.sample_ids.first().unwrap_or(&"(none)".to_string())
            ));
        }
    }

    output.push_str("\n");
    let warning_count = response
        .operation
        .cascade_analysis
        .as_ref()
        .map_or(0, |a| a.cascade_total);
    output.push_str(&format!("\u{26a0}\u{fe0f}  This operation will PERMANENTLY DELETE {} events.\n", warning_count));
    output.push_str("\n");
    output.push_str("To approve and execute, run within 1 hour:\n");
    output.push_str(&format!(
        "  sinexctl lifecycle tombstone approve {} --yes-i-understand-data-is-gone\n",
        response.operation.operation_id
    ));
    output.push_str("\n");
    output.push_str("To cancel:\n");
    output.push_str(&format!(
        "  sinexctl lifecycle tombstone cancel {}\n",
        response.operation.operation_id
    ));

    output
}

fn format_tombstone_approve_table(response: &TombstoneApproveResponse) -> String {
    let mut output = String::new();
    output.push_str("\n");
    output.push_str("\u{1f480} Tombstone Complete (PERMANENT)\n");
    output.push_str(&"\u{2500}".repeat(50));
    output.push_str("\n");
    output.push_str(&format!("  Operation ID:  {}\n", response.operation.operation_id));
    output.push_str(&format!("  State:         {:?}\n", response.operation.state));
    if let Some(count) = response.operation.tombstoned_count {
        output.push_str(&format!("  Tombstoned:    {count} events\n"));
    }
    output.push_str("\n");
    output.push_str("Data has been permanently deleted.\n");
    output
}

fn format_tombstone_preview_table(response: &TombstonePreviewResponse) -> String {
    let mut output = String::new();
    output.push_str("\n");
    output.push_str("Tombstone Operation Preview\n");
    output.push_str(&"\u{2550}".repeat(60));
    output.push_str("\n\n");
    output.push_str(&format!("  Operation ID: {}\n", response.operation.operation_id));
    output.push_str(&format!("  State:        {:?}\n", response.operation.state));
    output.push_str(&format!("  Created:      {}\n", response.operation.created_at));
    output.push_str(&format!("  Expires:      {}\n", response.operation.expires_at));
    output.push_str(&format!("  Reason:       {}\n", response.operation.reason));
    output.push_str("\n");

    if let Some(analysis) = &response.operation.cascade_analysis {
        output.push_str("Cascade Analysis:\n");
        output.push_str(&format!("  Root events:   {}\n", analysis.root_event_count));
        output.push_str(&format!("  Total cascade: {}\n", analysis.cascade_total));
        output.push_str(&format!("  Max depth:     {}\n", analysis.cascade_depth));
    }

    output
}

fn format_tombstone_list_table(response: &TombstoneListResponse) -> String {
    let mut output = String::new();
    output.push_str("\n");
    output.push_str("Tombstone Operations\n");
    output.push_str(&"\u{2550}".repeat(100));
    output.push_str("\n");
    output.push_str(&format!(
        "{:<28} {:<12} {:<10} {:<20} Reason\n",
        "Operation ID", "State", "Events", "Created"
    ));
    output.push_str(&"\u{2500}".repeat(100));
    output.push_str("\n");

    for op in &response.operations {
        let event_count = op.cascade_analysis.as_ref().map_or(0, |a| a.cascade_total);
        let reason = if op.reason.len() > 30 {
            format!("{}...", &op.reason[..27])
        } else {
            op.reason.clone()
        };
        output.push_str(&format!(
            "{:<28} {:<12} {:<10} {:<20} {}\n",
            op.operation_id,
            format!("{:?}", op.state),
            event_count,
            &op.created_at[..19],
            reason
        ));
    }

    output
}

fn format_tombstone_status_table(response: &TombstoneStatusResponse) -> String {
    let op = &response.operation;
    let mut output = String::new();
    output.push_str("\n");
    output.push_str("Tombstone Operation Status\n");
    output.push_str(&"\u{2550}".repeat(60));
    output.push_str("\n\n");
    output.push_str(&format!("  Operation ID: {}\n", op.operation_id));
    output.push_str(&format!("  State:        {:?}\n", op.state));
    output.push_str(&format!("  Created by:   {}\n", op.created_by));
    output.push_str(&format!("  Created at:   {}\n", op.created_at));
    output.push_str(&format!("  Expires at:   {}\n", op.expires_at));
    output.push_str(&format!("  Reason:       {}\n", op.reason));
    output.push_str("\n");

    if let Some(by) = &op.approved_by {
        output.push_str(&format!("  Approved by:  {by}\n"));
    }
    if let Some(at) = &op.approved_at {
        output.push_str(&format!("  Approved at:  {at}\n"));
    }
    if let Some(at) = &op.started_at {
        output.push_str(&format!("  Started at:   {at}\n"));
    }
    if let Some(at) = &op.finished_at {
        output.push_str(&format!("  Finished at:  {at}\n"));
    }
    if let Some(count) = op.tombstoned_count {
        output.push_str(&format!("  Tombstoned:   {count} events\n"));
    }
    if let Some(err) = &op.error_details {
        output.push_str(&format!("  Error:        {err}\n"));
    }

    if let Some(analysis) = &op.cascade_analysis {
        output.push_str("\n");
        output.push_str("Cascade Analysis:\n");
        output.push_str(&format!("  Root events:   {}\n", analysis.root_event_count));
        output.push_str(&format!("  Total cascade: {}\n", analysis.cascade_total));
        output.push_str(&format!("  Max depth:     {}\n", analysis.cascade_depth));
    }

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

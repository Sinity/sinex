use clap::Args;
use console::style;
use sinex_primitives::domain::SourceMaterialFormat;
use sinex_primitives::rpc::sources::{
    SourcesCoverageRequest, SourcesCoverageResponse, SourcesListRequest, SourcesListResponse,
    SourcesShowRequest, SourcesShowResponse, SourcesStageRequest, SourcesStageResponse,
};

use sinex_primitives::rpc::sources::{
    SourcesAnnotateRequest, SourcesAnnotateResponse, SourcesArchiveRequest, SourcesArchiveResponse,
    SourcesContinuityRequest, SourcesContinuityResponse,
};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Source material inventory, lifecycle, and diagnostics
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Stage a file as source material
    sinexctl sources stage /path/to/file.csv

    # List all source materials
    sinexctl sources list

    # Show details for a specific material
    sinexctl sources show <uuid>

    # Show temporal coverage
    sinexctl sources coverage

    # Annotate a material with notes and tags
    sinexctl sources annotate <uuid> --notes \"Re-staged after replay\"

    # Archive a material (dry-run preview first)
    sinexctl sources archive <uuid> --dry-run

    # Check temporal continuity for a source
    sinexctl sources continuity --source /path/to/history.db
")]
pub struct SourcesCommand {
    #[command(subcommand)]
    cmd: SourcesSubcommand,
}

impl SourcesCommand {
    #[must_use]
    pub fn subcommand(&self) -> &SourcesSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            SourcesSubcommand::Stage(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::List(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Show(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Coverage(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Annotate(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Archive(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Continuity(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum SourcesSubcommand {
    /// Stage a file as source material
    Stage(StageCommand),
    /// List source materials in the registry
    List(ListCommand),
    /// Show details for a specific source material
    Show(ShowCommand),
    /// Show temporal coverage of source materials
    Coverage(CoverageCommand),
    /// Annotate a source material with notes and tags
    Annotate(AnnotateCommand),
    /// Archive a staged source material (dry-run with --dry-run)
    Archive(ArchiveCommand),
    /// Diagnose temporal continuity and replayability for a source
    Continuity(ContinuityCommand),
}

// ── Stage ──────────────────────────────────────────────────────────────

/// Stage a file as source material
#[derive(Debug, Args)]
pub struct StageCommand {
    /// Path to the file to stage
    file: String,

    /// Human-readable reason for staging
    #[arg(long)]
    reason: Option<String>,

    /// Explicit file material format (jsonl, sqlite, markdown, archive, etc.)
    #[arg(long)]
    format: Option<SourceMaterialFormat>,

    /// Operator tag to attach to the staged material. Can be repeated.
    #[arg(long = "tag")]
    tags: Vec<String>,
}

impl StageCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesStageRequest {
            file_path: self.file.clone(),
            format: self.format,
            timing_info_type: None,
            reason: self.reason.clone(),
            tags: self.tags.clone(),
        };

        let response = client
            .call_raw_rpc("sources.stage", serde_json::to_value(&req)?)
            .await?;
        let stage_response: SourcesStageResponse = serde_json::from_value(response)?;

        CommandOutput::single(stage_response, format_stage_result).display(&format)?;
        Ok(())
    }
}

fn format_stage_result(response: &SourcesStageResponse) -> String {
    format!(
        "Staged source material\n  ID: {}\n  Source: {}\n  Format: {}\n  Timing: {}\n  Total bytes: {}",
        style(&response.material_id).green(),
        style(&response.source_identifier).cyan(),
        style(response.contract.format.to_string()).cyan(),
        style(response.contract.timing.to_string()).cyan(),
        style(
            response
                .total_bytes
                .map_or_else(|| "-".to_string(), |b| b.to_string())
        )
        .yellow(),
    )
}

// ── List ───────────────────────────────────────────────────────────────

/// List source materials in the registry
#[derive(Debug, Args)]
pub struct ListCommand {
    /// Filter by status (completed, sensing, failed, etc.)
    #[arg(long)]
    status: Option<String>,

    /// Maximum number of results
    #[arg(long, default_value_t = 50)]
    limit: i64,
}

impl ListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesListRequest {
            status: self.status.clone(),
            limit: Some(self.limit),
        };

        let response = client
            .call_raw_rpc("sources.list", serde_json::to_value(&req)?)
            .await?;
        let list_response: SourcesListResponse = serde_json::from_value(response)?;

        CommandOutput::single(list_response, format_source_materials_table).display(&format)?;
        Ok(())
    }
}

fn format_source_materials_table(response: &SourcesListResponse) -> String {
    use tabled::{builder::Builder, settings::Style};

    if response.materials.is_empty() {
        return "No source materials found.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "ID",
        "KIND",
        "SOURCE",
        "STATUS",
        "FORMAT",
        "TIMING",
        "SIZE",
        "STAGED AT",
        "STAGED BY",
    ]);

    for m in &response.materials {
        let short_id = format!("{}...", &m.id[..8.min(m.id.len())]);
        let size = m
            .size_bytes
            .map_or_else(|| style("-").dim().to_string(), |b| b.to_string());
        let staged_at = m.staged_at.as_deref().unwrap_or("-");
        let staged_by = m.staged_by.as_deref().unwrap_or("-");

        builder.push_record([
            short_id,
            m.material_kind.clone(),
            m.source_identifier.clone(),
            m.status.clone(),
            m.format
                .map_or_else(|| style("-").dim().to_string(), |format| format.to_string()),
            m.timing_info_type.clone(),
            size,
            staged_at.to_string(),
            staged_by.to_string(),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ── Show ───────────────────────────────────────────────────────────────

/// Show details for a specific source material
#[derive(Debug, Args)]
pub struct ShowCommand {
    /// Source material UUID
    material_id: String,
}

impl ShowCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesShowRequest {
            material_id: self.material_id.clone(),
        };

        let response = client
            .call_raw_rpc("sources.show", serde_json::to_value(&req)?)
            .await?;
        let show_response: SourcesShowResponse = serde_json::from_value(response)?;

        CommandOutput::single(show_response, format_source_material_detail).display(&format)?;
        Ok(())
    }
}

fn format_source_material_detail(response: &SourcesShowResponse) -> String {
    let m = &response.material;
    let mut lines = vec![
        format!("Source Material: {}", style(&m.id).green()),
        format!("  Kind:         {}", m.material_kind),
        format!("  Source:       {}", m.source_identifier),
        format!("  Status:       {}", m.status),
        format!("  Timing:       {}", m.timing_info_type),
        format!(
            "  Format:       {}",
            m.contract
                .as_ref()
                .map_or_else(|| "-".to_string(), |contract| contract.format.to_string())
        ),
        format!("  Staged at:    {}", m.staged_at.as_deref().unwrap_or("-")),
        format!("  Start time:   {}", m.start_time.as_deref().unwrap_or("-")),
        format!("  End time:     {}", m.end_time.as_deref().unwrap_or("-")),
        format!("  Staged by:    {}", m.staged_by.as_deref().unwrap_or("-")),
        format!(
            "  Staged on:    {}",
            m.staged_on_host.as_deref().unwrap_or("-")
        ),
        format!(
            "  Blob ID:      {}",
            m.optional_blob_id.as_deref().unwrap_or("-")
        ),
        format!(
            "  Total bytes:  {}",
            m.total_bytes
                .map_or_else(|| "-".to_string(), |b| b.to_string())
        ),
        format!(
            "  Event count:  {}",
            m.event_count
                .map_or_else(|| "-".to_string(), |c| c.to_string())
        ),
    ];

    if let Some(evidence) = &m.temporal_evidence {
        lines.push(format!("  Timing facts: {}", evidence.ledger_entries));
        if !evidence.source_types.is_empty() {
            lines.push(format!(
                "  Timing kinds: {}",
                evidence.source_types.join(", ")
            ));
        }
    }

    // Add metadata if present
    if !m.metadata.is_null() && m.metadata != serde_json::Value::Object(Default::default()) {
        lines.push(format!(
            "  Metadata:     {}",
            serde_json::to_string_pretty(&m.metadata).unwrap_or_else(|_| "-".to_string())
        ));
    }

    lines.join("\n")
}

// ── Coverage ───────────────────────────────────────────────────────────

/// Show temporal coverage of source materials
#[derive(Debug, Args)]
pub struct CoverageCommand {
    /// Maximum number of buckets
    #[arg(long, default_value_t = 100)]
    limit: i64,
}

impl CoverageCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesCoverageRequest {};

        let response = client
            .call_raw_rpc("sources.coverage", serde_json::to_value(&req)?)
            .await?;
        let coverage_response: SourcesCoverageResponse = serde_json::from_value(response)?;

        CommandOutput::single(coverage_response, format_coverage_table).display(&format)?;
        Ok(())
    }
}

fn format_coverage_table(response: &SourcesCoverageResponse) -> String {
    use tabled::{builder::Builder, settings::Style};

    if response.sources.is_empty() {
        return "No source material coverage data available.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "KIND",
        "EARLIEST",
        "LATEST",
        "EVENTS",
        "MATERIALS",
    ]);

    for bucket in &response.sources {
        let earliest = bucket.earliest_ts.as_deref().unwrap_or("-");
        let latest = bucket.latest_ts.as_deref().unwrap_or("-");
        let events = bucket
            .event_count
            .map_or_else(|| style("-").dim().to_string(), |c| c.to_string());
        let materials = bucket
            .material_count
            .map_or_else(|| style("-").dim().to_string(), |c| c.to_string());

        builder.push_record([
            bucket.source_identifier.clone(),
            bucket.material_kind.clone(),
            earliest.to_string(),
            latest.to_string(),
            events,
            materials,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

// ── Annotate ───────────────────────────────────────────────────────────

/// Annotate a source material with notes and tags
#[derive(Debug, Args)]
pub struct AnnotateCommand {
    /// Source material UUID
    material_id: String,

    /// Free-form notes to attach to the material
    #[arg(long)]
    notes: Option<String>,

    /// Tags to add (can be repeated; duplicates are ignored)
    #[arg(long = "tag")]
    tags: Vec<String>,

    /// Override declared start time (ISO8601)
    #[arg(long)]
    declared_start_time: Option<String>,

    /// Override declared end time (ISO8601)
    #[arg(long)]
    declared_end_time: Option<String>,
}

impl AnnotateCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesAnnotateRequest {
            material_id: self.material_id.clone(),
            notes: self.notes.clone(),
            tags: self.tags.clone(),
            declared_start_time: self.declared_start_time.clone(),
            declared_end_time: self.declared_end_time.clone(),
        };

        let response = client
            .call_raw_rpc("sources.annotate", serde_json::to_value(&req)?)
            .await?;
        let annotate_response: SourcesAnnotateResponse = serde_json::from_value(response)?;

        CommandOutput::single(annotate_response, format_annotate_result).display(&format)?;
        Ok(())
    }
}

fn format_annotate_result(response: &SourcesAnnotateResponse) -> String {
    let mut lines = vec![format!(
        "Annotated source material: {}",
        style(&response.material_id).green()
    )];
    if let Some(reason) = &response.annotations.reason {
        lines.push(format!("  Notes: {}", style(reason).cyan()));
    }
    if !response.annotations.tags.is_empty() {
        lines.push(format!(
            "  Tags:  {}",
            style(response.annotations.tags.join(", ")).yellow()
        ));
    }
    if let Some(start) = &response.annotations.declared_start_time {
        lines.push(format!("  Start: {}", style(start).cyan()));
    }
    if let Some(end) = &response.annotations.declared_end_time {
        lines.push(format!("  End:   {}", style(end).cyan()));
    }
    lines.join("\n")
}

// ── Archive ────────────────────────────────────────────────────────────

/// Archive a staged source material
#[derive(Debug, Args)]
pub struct ArchiveCommand {
    /// Source material UUID to archive
    material_id: String,

    /// Preview cascade without executing (dry-run)
    #[arg(long)]
    dry_run: bool,

    /// Reason for archival (audit)
    #[arg(long)]
    reason: Option<String>,
}

impl ArchiveCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesArchiveRequest {
            material_id: self.material_id.clone(),
            dry_run: self.dry_run,
            reason: self.reason.clone(),
        };

        let response = client
            .call_raw_rpc("sources.archive", serde_json::to_value(&req)?)
            .await?;
        let archive_response: SourcesArchiveResponse = serde_json::from_value(response)?;

        CommandOutput::single(archive_response, format_archive_result).display(&format)?;
        Ok(())
    }
}

fn format_archive_result(response: &SourcesArchiveResponse) -> String {
    let mut lines = vec![format!(
        "{} archive for material {}",
        if response.dry_run {
            "Dry-run"
        } else {
            "Executed"
        },
        style(&response.material_id).green()
    )];
    lines.push(format!(
        "  Cascade count: {}",
        style(response.cascade_count).yellow()
    ));
    if let Some(op_id) = &response.operation_id {
        lines.push(format!(
            "  Operation ID:  {}",
            style(op_id).cyan()
        ));
    }
    if let Some(preview) = &response.preview {
        lines.push(format!(
            "  Preview:       {}",
            style(serde_json::to_string_pretty(preview).unwrap_or_else(|_| "-".to_string()))
                .dim()
        ));
    }
    lines.join("\n")
}

// ── Continuity ─────────────────────────────────────────────────────────

/// Diagnose temporal continuity and replayability for a source
#[derive(Debug, Args)]
pub struct ContinuityCommand {
    /// Source identifier (file path, URI, or source name)
    #[arg(long)]
    source: String,

    /// Optional material kind filter (e.g. "file", "sqlite_db")
    #[arg(long)]
    kind: Option<String>,
}

impl ContinuityCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesContinuityRequest {
            source_identifier: self.source.clone(),
            material_kind: self.kind.clone(),
        };

        let response = client
            .call_raw_rpc("sources.continuity", serde_json::to_value(&req)?)
            .await?;
        let continuity_response: SourcesContinuityResponse = serde_json::from_value(response)?;

        CommandOutput::single(continuity_response, format_continuity_result).display(&format)?;
        Ok(())
    }
}

fn format_continuity_result(response: &SourcesContinuityResponse) -> String {
    let mut lines = vec![format!(
        "Continuity diagnostics for: {}",
        style(&response.source_identifier).green().bold()
    )];

    // Coverage gaps
    if response.coverage_gaps.is_empty() {
        lines.push("  No temporal gaps detected.".to_string());
    } else {
        lines.push(format!(
            "  {} temporal gap(s) detected:",
            style(response.coverage_gaps.len()).yellow()
        ));
        for gap in &response.coverage_gaps {
            let start = gap.gap_start.as_deref().unwrap_or("-");
            let end = gap.gap_end.as_deref().unwrap_or("-");
            let dur = gap
                .gap_duration_seconds
                .map_or_else(|| "-".to_string(), |d| format!("{d}s"));
            lines.push(format!(
                "    {} -> {} (duration: {}) [{}]",
                style(start).dim(),
                style(end).dim(),
                style(dur).yellow(),
                gap.gap_type
            ));
        }
    }

    // Contract status
    let cs = &response.contract_status;
    lines.push(format!(
        "  Coverage contract: {}",
        if cs.has_coverage_contract {
            style("present").green()
        } else {
            style("absent").yellow()
        }
    ));
    if let Some(pct) = cs.actual_coverage_percent {
        lines.push(format!("  Coverage:          {pct:.1}%"));
    }
    if !cs.breaches.is_empty() {
        lines.push("  Breaches:".to_string());
        for breach in &cs.breaches {
            lines.push(format!("    - {}", style(breach).red()));
        }
    }

    // Replayability
    let rp = &response.replayability;
    lines.push(format!(
        "  Replayable:        {}",
        if rp.replayable {
            style("yes").green()
        } else {
            style("no").red()
        }
    ));
    if let Some(reason) = &rp.reason {
        lines.push(format!("  Reason:            {}", style(reason).yellow()));
    }
    lines.push(format!(
        "  Materials staged:  {}",
        style(rp.material_count).cyan()
    ));
    lines.push(format!(
        "  Events reference:  {}",
        style(rp.events_count).cyan()
    ));

    lines.join("\n")
}

use clap::Args;
use console::style;
use serde::{Deserialize, Serialize};
use sinex_primitives::domain::SourceMaterialFormat;
use sinex_primitives::rpc::sources::{
    SourcesCoverageRequest, SourcesCoverageResponse, SourcesListRequest, SourcesListResponse,
    SourcesShowRequest, SourcesShowResponse, SourcesStageRequest, SourcesStageResponse,
};

use sinex_primitives::Timestamp;
use sinex_primitives::parser::SourceId;
use sinex_primitives::rpc::sources::{
    CaveatSeverity, SourceReadiness, SourceReadinessStatus, SourcesReadinessGetRequest,
    SourcesReadinessGetResponse, SourcesReadinessListRequest, SourcesReadinessListResponse,
};
use sinex_primitives::rpc::sources::{SourceCoverageEntry, SourceMaterialSummary};
use sinex_primitives::rpc::sources::{
    SourcesAnnotateRequest, SourcesAnnotateResponse, SourcesArchiveRequest, SourcesArchiveResponse,
    SourcesContinuityRequest, SourcesContinuityResponse, SourcesDriftListRequest,
    SourcesDriftListResponse,
};
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::{
    SourceContinuityReport, SourcesContinuityGetRequest, SourcesContinuityGetResponse,
    SourcesContinuityListRequest, SourcesContinuityListResponse, SourcesExplainGapRequest,
    SourcesExplainGapResponse,
};
use sinex_primitives::views::{
    SourceContinuityDetailView, SourceContinuityGapView, SourceContinuityListView,
    SourceDriftListView, SourceReadinessDetailView, SourceReadinessListView, ViewEnvelope,
};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::{CommandOutput, print_finite_envelope};
use crate::model::OutputFormat;

use super::source_status::SourceStatusCommand;

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
            SourcesSubcommand::Readiness(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Drift(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::ExplainGap(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Cockpit(cmd) => cmd.execute(client, format).await,
            SourcesSubcommand::Status(cmd) => cmd.execute(client, format).await,
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
    /// Report source readiness, cost, freshness, and caveats
    Readiness(ReadinessCommand),
    /// List recent source-shape drift observed by adapter-backed source contracts
    Drift(DriftCommand),
    /// Explain a coverage gap at a specific timestamp
    #[command(name = "explain-gap")]
    ExplainGap(ExplainGapCommand),
    /// Source readiness summary table with status per source
    Cockpit(CockpitCommand),
    /// Source runtime status: run, health, and recent emissions
    Status(SourceStatusCommand),
}

// ── Cockpit ────────────────────────────────────────────────────────────

/// Source readiness table showing status for each registered source.
#[derive(Debug, Args)]
pub struct CockpitCommand {
    /// Filter to sources whose id contains this substring
    #[arg(long)]
    filter: Option<String>,
}

impl CockpitCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        // Surface the readiness table.
        let req = SourcesReadinessGetRequest {
            source_identifier: self.filter.clone().unwrap_or_default(),
            source_family: None,
            stale_after_seconds: None,
        };
        let body: SourcesReadinessGetResponse = client.sources_readiness_get(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.cockpit",
            SourceReadinessDetailView::new(body.readiness.clone()),
        )
        .with_query_echo(serde_json::json!({
            "filter": self.filter,
        }));
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(body, format_readiness_get).display(&format)
    }
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

    /// Explicit file material format (jsonl, sqlite, markdown, archive, etc.).
    ///
    /// Named `--material-format` (not `--format`) to avoid colliding with the
    /// global `--format` output selector: clap registers both under the arg id
    /// `format`, and the typed-access mismatch (SourceMaterialFormat vs
    /// OutputFormat) panicked at runtime on every `sources stage` invocation.
    #[arg(long = "material-format")]
    material_format: Option<SourceMaterialFormat>,

    /// Source binding/package mode to attach to the staged material.
    #[arg(long, value_name = "BINDING")]
    binding: Option<String>,

    /// Operator tag to attach to the staged material. Can be repeated.
    #[arg(long = "tag")]
    tags: Vec<String>,
}

impl StageCommand {
    fn request(&self) -> SourcesStageRequest {
        SourcesStageRequest {
            file_path: self.file.clone(),
            format: self.material_format,
            timing_info_type: None,
            reason: self.reason.clone(),
            tags: self.tags.clone(),
            binding_name: self.binding.clone(),
            with_bytes: true,
        }
    }

    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = self.request();

        let stage_response: SourcesStageResponse = client.sources_stage(req).await?;

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

const SOURCE_MATERIAL_LIST_SCHEMA_VERSION: &str = "sinex.source-material-list/v1";
const SOURCE_COVERAGE_LIST_SCHEMA_VERSION: &str = "sinex.source-coverage-list/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceMaterialListView {
    schema_version: String,
    count: usize,
    materials: Vec<SourceMaterialSummary>,
}

impl SourceMaterialListView {
    fn new(materials: Vec<SourceMaterialSummary>) -> Self {
        let count = materials.len();
        Self {
            schema_version: SOURCE_MATERIAL_LIST_SCHEMA_VERSION.to_string(),
            count,
            materials,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceCoverageListView {
    schema_version: String,
    count: usize,
    sources: Vec<SourceCoverageEntry>,
}

impl SourceCoverageListView {
    fn new(sources: Vec<SourceCoverageEntry>) -> Self {
        let count = sources.len();
        Self {
            schema_version: SOURCE_COVERAGE_LIST_SCHEMA_VERSION.to_string(),
            count,
            sources,
        }
    }
}

impl ListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = SourcesListRequest {
            status: self.status.clone(),
            limit: Some(self.limit),
        };

        let list_response: SourcesListResponse = client.sources_list(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.list",
            SourceMaterialListView::new(list_response.materials.clone()),
        )
        .with_query_echo(serde_json::json!({
            "status": self.status,
            "limit": self.limit,
        }));

        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
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
            m.material_kind.to_string(),
            m.source_identifier.clone(),
            m.status.to_string(),
            m.format
                .map_or_else(|| style("-").dim().to_string(), |format| format.to_string()),
            m.timing_info_type.to_string(),
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

        let show_response: SourcesShowResponse = client.sources_show(req).await?;

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
    if !m.metadata.is_null() && m.metadata != serde_json::Value::Object(serde_json::Map::default())
    {
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

        let coverage_response: SourcesCoverageResponse = client.sources_coverage(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.coverage",
            SourceCoverageListView::new(coverage_response.sources.clone()),
        )
        .with_query_echo(serde_json::json!({
            "limit": self.limit,
        }));

        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
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
            bucket.material_kind.to_string(),
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

        let annotate_response: SourcesAnnotateResponse = client.sources_annotate(req).await?;

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

        let archive_response: SourcesArchiveResponse = client.sources_archive(req).await?;

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
        lines.push(format!("  Operation ID:  {}", style(op_id).cyan()));
    }
    if let Some(preview) = &response.preview {
        lines.push(format!(
            "  Preview:       {}",
            style(serde_json::to_string_pretty(preview).unwrap_or_else(|_| "-".to_string())).dim()
        ));
    }
    lines.join("\n")
}

// ── Continuity ─────────────────────────────────────────────────────────

/// Diagnose temporal continuity and replayability.
///
/// Three modes (resolved by argument shape):
///   `sinexctl sources continuity`               — list reports per source family.
///   `sinexctl sources continuity <FAMILY>`      — report for one source family.
///   `sinexctl sources continuity --source <ID>` — per-identifier diagnostics.
#[derive(Debug, Args)]
pub struct ContinuityCommand {
    /// Source family (e.g. `shell`, `file`, `browser`). Empty -> list mode.
    family: Option<String>,

    /// Per-identifier mode: source identifier (file path, URI, or source name).
    #[arg(long)]
    source: Option<String>,

    /// Per-identifier mode: optional material kind filter.
    #[arg(long)]
    kind: Option<String>,

    /// Restrict listed reports to families staged at or after this RFC3339 timestamp.
    #[arg(long, conflicts_with_all = ["source", "family"])]
    since: Option<String>,
}

impl ContinuityCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        // ── Per-identifier mode ──
        if let Some(source) = &self.source {
            let req = SourcesContinuityRequest {
                source_identifier: source.clone(),
                material_kind: self.kind.clone(),
            };
            let resp: SourcesContinuityResponse = client.sources_continuity(req).await?;
            CommandOutput::single(resp, format_continuity_result).display(&format)?;
            return Ok(());
        }

        // ── Get-by-family mode ──
        if let Some(family_str) = &self.family {
            let family = SourceFamily::new(family_str.clone()).map_err(|e| {
                color_eyre::eyre::eyre!("invalid source family `{family_str}`: {e}")
            })?;
            let req = SourcesContinuityGetRequest {
                source_family: family,
            };
            let resp: SourcesContinuityGetResponse = client.sources_continuity_get(req).await?;
            let envelope = ViewEnvelope::new(
                "sinexctl.sources.continuity",
                SourceContinuityDetailView::new(resp.report.clone()),
            )
            .with_query_echo(serde_json::json!({
                "family": family_str,
            }));
            if print_finite_envelope(&envelope, format)? {
                return Ok(());
            }
            CommandOutput::single(resp, format_continuity_get).display(&format)?;
            return Ok(());
        }

        // ── List mode ──
        let since = self.since.as_deref().map(parse_timestamp).transpose()?;
        let req = SourcesContinuityListRequest { since };
        let resp: SourcesContinuityListResponse = client.sources_continuity_list(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.continuity",
            SourceContinuityListView::new(resp.reports.clone()),
        )
        .with_query_echo(serde_json::json!({
            "since": since.map(|ts| ts.to_string()),
        }));
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(resp, format_continuity_list).display(&format)?;
        Ok(())
    }
}

// ── Explain gap ─────────────────────────────────────────────────────────

/// Explain a coverage gap at a specific timestamp.
#[derive(Debug, Args)]
pub struct ExplainGapCommand {
    /// Source family (e.g. `shell`, `browser`).
    family: String,

    /// Timestamp to explain (RFC3339).
    #[arg(long)]
    at: String,
}

impl ExplainGapCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let family = SourceFamily::new(self.family.clone())
            .map_err(|e| color_eyre::eyre::eyre!("invalid source family: {e}"))?;
        let at = parse_timestamp(&self.at)?;
        let req = SourcesExplainGapRequest {
            source_family: family,
            at,
        };
        let resp: SourcesExplainGapResponse = client.sources_continuity_explain_gap(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.explain_gap",
            SourceContinuityGapView::new(resp.clone()),
        )
        .with_query_echo(serde_json::json!({
            "family": self.family,
            "at": self.at,
        }));
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(resp, format_explain_gap).display(&format)?;
        Ok(())
    }
}

fn parse_timestamp(s: &str) -> Result<Timestamp> {
    sinex_primitives::temporal::parse_rfc3339(s)
        .map_err(|e| color_eyre::eyre::eyre!("invalid RFC3339 timestamp `{s}`: {e}"))
}

fn format_continuity_list(resp: &SourcesContinuityListResponse) -> String {
    if resp.reports.is_empty() {
        return "No source families observed.".to_string();
    }
    let mut lines = vec![format!(
        "{} source families:",
        style(resp.reports.len()).cyan().bold()
    )];
    for r in &resp.reports {
        lines.push(format_report_summary(r));
    }
    lines.join("\n")
}

fn format_continuity_get(resp: &SourcesContinuityGetResponse) -> String {
    match &resp.report {
        Some(r) => format_report_full(r),
        None => "No continuity report — no events observed for this family.".to_string(),
    }
}

fn format_explain_gap(resp: &SourcesExplainGapResponse) -> String {
    let mut lines = vec![format!(
        "Source family: {}    at: {}",
        style(&resp.source_family).green().bold(),
        style(resp.at).dim()
    )];
    lines.push(resp.explanation.clone());
    if let Some(gap) = &resp.gap {
        lines.push(format!(
            "  Gap window: {} -> {}",
            style(gap.from_ts).dim(),
            style(gap.to_ts).dim()
        ));
        lines.push(format!("  Kind:       {:?}", gap.kind));
        if let Some(attr) = &gap.attribution {
            lines.push(format!("  Reason:     {}", style(attr).yellow()));
        }
    }
    lines.join("\n")
}

fn format_report_summary(r: &SourceContinuityReport) -> String {
    let green = r.replayability.green_count();
    let contract_label = if r.is_declared {
        format!("{:?}", r.coverage_contract).to_lowercase()
    } else {
        format!(
            "{} (inferred)",
            format!("{:?}", r.coverage_contract).to_lowercase()
        )
    };
    format!(
        "  {:24} {:24} replayability {}/5  events:{}  materials:{}  gaps:{}  seams:{}",
        r.source_family.as_str(),
        contract_label,
        green,
        r.event_count,
        r.material_count,
        r.gaps.len(),
        r.seams.len()
    )
}

fn format_report_full(r: &SourceContinuityReport) -> String {
    let mut lines = vec![format!(
        "Source family: {}",
        style(r.source_family.as_str()).green().bold()
    )];
    let contract_suffix = if r.is_declared { "" } else { " (inferred)" };
    lines.push(format!(
        "  Coverage contract: {}{}",
        format!("{:?}", r.coverage_contract).to_lowercase(),
        contract_suffix
    ));
    if let Some(start) = r.earliest_ts {
        lines.push(format!("  Earliest:          {start}"));
    }
    if let Some(end) = r.latest_ts {
        lines.push(format!("  Latest:            {end}"));
    }
    lines.push(format!("  Materials:         {}", r.material_count));
    lines.push(format!("  Events:            {}", r.event_count));

    let rp = &r.replayability;
    lines.push(format!(
        "  Replayability:     {}/5",
        style(rp.green_count()).cyan()
    ));
    lines.push(format!(
        "    raw_bytes_preserved : {}",
        checkmark(rp.raw_bytes_preserved)
    ));
    lines.push(format!(
        "    timing_quality      : {}",
        checkmark(rp.timing_quality)
    ));
    lines.push(format!(
        "    anchor_stability    : {}",
        checkmark(rp.anchor_stability)
    ));
    lines.push(format!(
        "    parser_determinism  : {}",
        checkmark(rp.parser_determinism)
    ));
    lines.push(format!(
        "    privacy_safe_replay : {}",
        checkmark(rp.privacy_safe_replay)
    ));
    if !rp.weak_points.is_empty() {
        lines.push("  Weak points:".to_string());
        for w in &rp.weak_points {
            lines.push(format!("    - {}", style(w).yellow()));
        }
    }

    if r.seams.is_empty() {
        lines.push("  Seams:             none".to_string());
    } else {
        lines.push(format!("  Seams ({}):", r.seams.len()));
        for s in &r.seams {
            lines.push(format!(
                "    {:?}  before={}  after={}",
                s.kind,
                s.before_ts
                    .map_or_else(|| "-".to_string(), |t| t.to_string()),
                s.after_ts
                    .map_or_else(|| "-".to_string(), |t| t.to_string()),
            ));
        }
    }

    if r.gaps.is_empty() {
        lines.push("  Gaps:              none".to_string());
    } else {
        lines.push(format!("  Gaps ({}):", r.gaps.len()));
        for g in &r.gaps {
            let kind = format!("{:?}", g.kind).to_lowercase();
            lines.push(format!(
                "    {} -> {}  [{}]  {}",
                style(g.from_ts).dim(),
                style(g.to_ts).dim(),
                style(kind).yellow(),
                g.attribution.as_deref().unwrap_or("")
            ));
        }
    }
    lines.join("\n")
}

fn checkmark(b: bool) -> console::StyledObject<&'static str> {
    if b {
        style("yes").green()
    } else {
        style("no").red()
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

// ── Readiness (#1099) ──────────────────────────────────────────────────

/// Report source readiness, cost, freshness, and caveats
#[derive(Debug, Args)]
pub struct ReadinessCommand {
    /// Optional source identifier; when provided, returns the readiness for
    /// just that source. When omitted, lists readiness for every source.
    source: Option<String>,

    /// Optional source family filter (e.g. "terminal", "browser", "chat").
    #[arg(long)]
    family: Option<String>,

    /// Treat last-success older than this many seconds as `Stale`.
    /// Defaults to 7 days.
    #[arg(long = "stale-after-seconds")]
    stale_after_seconds: Option<i64>,
}

impl ReadinessCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        if let Some(source) = &self.source {
            let req = SourcesReadinessGetRequest {
                source_identifier: source.clone(),
                source_family: self.family.clone(),
                stale_after_seconds: self.stale_after_seconds,
            };
            let body: SourcesReadinessGetResponse = client.sources_readiness_get(req).await?;
            let envelope = ViewEnvelope::new(
                "sinexctl.sources.readiness",
                SourceReadinessDetailView::new(body.readiness.clone()),
            )
            .with_query_echo(serde_json::json!({
                "source": source,
                "family": self.family,
                "stale_after_seconds": self.stale_after_seconds,
            }));
            if print_finite_envelope(&envelope, format)? {
                return Ok(());
            }
            CommandOutput::single(body, format_readiness_get).display(&format)?;
        } else {
            let req = SourcesReadinessListRequest {
                source_family: self.family.clone(),
                stale_after_seconds: self.stale_after_seconds,
            };
            let body: SourcesReadinessListResponse = client.sources_readiness_list(req).await?;
            let envelope = ViewEnvelope::new(
                "sinexctl.sources.readiness",
                SourceReadinessListView::new(body.sources.clone()),
            )
            .with_query_echo(serde_json::json!({
                "family": self.family,
                "stale_after_seconds": self.stale_after_seconds,
            }));
            if print_finite_envelope(&envelope, format)? {
                return Ok(());
            }
            CommandOutput::single(body, format_readiness_list).display(&format)?;
        }
        Ok(())
    }
}

fn status_label(status: SourceReadinessStatus) -> console::StyledObject<&'static str> {
    match status {
        SourceReadinessStatus::Available => style("available").green(),
        SourceReadinessStatus::Partial => style("partial").yellow(),
        SourceReadinessStatus::Stale => style("stale").yellow(),
        SourceReadinessStatus::Error => style("error").red(),
        SourceReadinessStatus::Missing => style("missing").red(),
        SourceReadinessStatus::Blocked => style("blocked").red(),
        SourceReadinessStatus::Disabled => style("disabled").dim(),
        SourceReadinessStatus::Unknown => style("unknown").dim(),
    }
}

fn format_readiness_list(response: &SourcesReadinessListResponse) -> String {
    use tabled::{builder::Builder, settings::Style};

    if response.sources.is_empty() {
        return "No source readiness data available.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "FAMILY",
        "STATUS",
        "COST",
        "MATERIALS",
        "EVENTS",
        "FRESHNESS (s)",
        "CAVEATS",
    ]);

    for r in &response.sources {
        let freshness = r
            .freshness_seconds
            .map_or_else(|| style("-").dim().to_string(), |s| s.to_string());
        let events = r
            .parsed_event_count
            .map_or_else(|| style("-").dim().to_string(), |c| c.to_string());
        let caveat_codes: Vec<&str> = r.caveats.iter().map(|c| c.code.as_str()).collect();
        let caveat_text = if caveat_codes.is_empty() {
            style("-").dim().to_string()
        } else {
            caveat_codes.join(", ")
        };
        builder.push_record([
            r.source_identifier.clone(),
            r.source_family.clone(),
            status_label(r.status).to_string(),
            format!("{:?}", r.cost).to_lowercase(),
            r.material_count.to_string(),
            events,
            freshness,
            caveat_text,
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn format_readiness_get(response: &SourcesReadinessGetResponse) -> String {
    let Some(r) = response.readiness.as_ref() else {
        return "No readiness data for that source.".to_string();
    };
    format_readiness_detail(r)
}

fn format_readiness_detail(r: &SourceReadiness) -> String {
    let mut lines = vec![
        format!(
            "Readiness for: {}",
            style(&r.source_identifier).green().bold()
        ),
        format!("  Family:         {}", r.source_family),
        format!("  Status:         {}", status_label(r.status)),
        format!(
            "  Cost:           {}",
            format!("{:?}", r.cost).to_lowercase()
        ),
        format!("  Materials:      {}", r.material_count),
    ];
    if let Some(c) = r.parsed_event_count {
        lines.push(format!("  Parsed events:  {c}"));
    }
    if let Some(s) = r.freshness_seconds {
        lines.push(format!("  Freshness:      {s}s"));
    }
    if let Some(ts) = &r.last_success_at {
        lines.push(format!("  Last success:   {ts}"));
    }
    if r.caveats.is_empty() {
        lines.push("  Caveats:        none".to_string());
    } else {
        lines.push(format!("  Caveats ({}):", r.caveats.len()));
        for caveat in &r.caveats {
            lines.push(format!(
                "    [{:?}] {} — {}",
                caveat.severity, caveat.code, caveat.message
            ));
        }
    }
    lines.join("\n")
}

// ── Drift (#1103) ─────────────────────────────────────────────────────

/// List recent source-shape drift observed by adapter-backed source contracts
#[derive(Debug, Args)]
pub struct DriftCommand {
    /// Optional source id filter.
    #[arg(long = "source")]
    source_id: Option<String>,

    /// Maximum number of drift observations to return.
    #[arg(long, default_value_t = 50)]
    limit: usize,
}

impl DriftCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let source_id = self.source_id.as_deref().map(SourceId::new).transpose()?;
        let req = SourcesDriftListRequest {
            source_id,
            limit: Some(self.limit),
        };
        let body: SourcesDriftListResponse = client.sources_drift_list(req).await?;
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.drift",
            SourceDriftListView::new(body.drifts.clone()),
        )
        .with_query_echo(serde_json::json!({
            "source": self.source_id,
            "limit": self.limit,
        }));
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        CommandOutput::single(body, format_drift_list).display(&format)?;
        Ok(())
    }
}

fn format_drift_list(response: &SourcesDriftListResponse) -> String {
    use tabled::{builder::Builder, settings::Style};

    if response.drifts.is_empty() {
        return "No checkpointed source-shape drift found.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "SOURCE",
        "IMPACT",
        "CAVEATS",
        "FORMAT",
        "OBSERVED",
        "ADDED",
        "REMOVED",
        "TYPE CHANGES",
        "CHECKPOINT",
    ]);

    for drift in &response.drifts {
        let caveats = drift.readiness_caveats();
        let impact = strongest_caveat_severity(&caveats).map_or_else(
            || "none".to_string(),
            |severity| severity_label(severity).to_string(),
        );
        let caveat_codes = if caveats.is_empty() {
            "none".to_string()
        } else {
            caveats
                .iter()
                .map(|caveat| caveat.code.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        builder.push_record([
            drift.source_id.as_str().to_string(),
            impact,
            caveat_codes,
            drift.format.clone(),
            drift.observed_at.clone(),
            drift.added_keys.len().to_string(),
            drift.removed_keys.len().to_string(),
            drift.type_changes.len().to_string(),
            drift.checkpoint_key.clone(),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}

fn strongest_caveat_severity(
    caveats: &[sinex_primitives::rpc::sources::SourceCaveat],
) -> Option<CaveatSeverity> {
    caveats
        .iter()
        .map(|caveat| caveat.severity)
        .max_by_key(|severity| caveat_severity_rank(*severity))
}

const fn caveat_severity_rank(severity: CaveatSeverity) -> u8 {
    match severity {
        CaveatSeverity::Info => 0,
        CaveatSeverity::Warning => 1,
        CaveatSeverity::Degraded => 2,
        CaveatSeverity::Blocking => 3,
    }
}

const fn severity_label(severity: CaveatSeverity) -> &'static str {
    match severity {
        CaveatSeverity::Info => "info",
        CaveatSeverity::Warning => "warning",
        CaveatSeverity::Degraded => "degraded",
        CaveatSeverity::Blocking => "blocking",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fmt::render_finite_envelope;
    use sinex_primitives::domain::{MaterialStatus, SourceMaterialTimingInfoType};
    use sinex_primitives::parser::ParserId;
    use sinex_primitives::rpc::sources::{
        SourceReadinessCost, SourceReadinessStatus, SourceShapeDriftObservation,
        SourceShapeTypeChange, caveat_codes,
    };
    use sinex_primitives::views::{
        SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION, SOURCE_CONTINUITY_GAP_SCHEMA_VERSION,
        SOURCE_CONTINUITY_LIST_SCHEMA_VERSION, SOURCE_DRIFT_LIST_SCHEMA_VERSION,
        SOURCE_READINESS_DETAIL_SCHEMA_VERSION, SOURCE_READINESS_LIST_SCHEMA_VERSION,
        VIEW_ENVELOPE_SCHEMA_VERSION,
    };
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn drift_table_surfaces_readiness_impact() -> TestResult<()> {
        let response = SourcesDriftListResponse {
            drifts: vec![SourceShapeDriftObservation {
                checkpoint_key: "source.default.fixture".to_string(),
                source_id: SourceId::from_static("browser.history"),
                consumer_group: Some("default".to_string()),
                consumer_name: Some("fixture".to_string()),
                previous_hash: "shape-old".to_string(),
                current_hash: "shape-new".to_string(),
                format: "sqlite_schema".to_string(),
                added_keys: Vec::new(),
                removed_keys: vec!["visit_id".to_string()],
                type_changes: vec![SourceShapeTypeChange {
                    key: "visit_time".to_string(),
                    previous_type: "number".to_string(),
                    current_type: "string".to_string(),
                }],
                required_input_keys: vec!["visit_id".to_string()],
                observed_at: "2026-05-21T07:00:00Z".to_string(),
            }],
        };

        let table = format_drift_list(&response);

        assert!(table.contains("IMPACT"));
        assert!(table.contains("blocking"));
        assert!(table.contains(caveat_codes::PARSER_FIELD_TYPE_CHANGED));
        assert!(table.contains(caveat_codes::PARSER_REQUIRED_FIELD_MISSING));
        Ok(())
    }

    fn fixture_material(id: &str) -> SourceMaterialSummary {
        SourceMaterialSummary {
            id: id.to_string(),
            material_kind: sinex_primitives::MaterialStorageKind::Annex,
            source_identifier: "fixture.source".to_string(),
            status: MaterialStatus::Completed,
            timing_info_type: SourceMaterialTimingInfoType::Intrinsic,
            format: Some(SourceMaterialFormat::Jsonl),
            contract_version: Some(1),
            staged_at: Some("2026-06-01T00:00:00Z".to_string()),
            staged_by: Some("test".to_string()),
            size_bytes: Some(128),
            mime_type: Some("application/jsonl".to_string()),
        }
    }

    fn fixture_coverage(source_identifier: &str) -> SourceCoverageEntry {
        SourceCoverageEntry {
            source_identifier: source_identifier.to_string(),
            material_kind: sinex_primitives::MaterialStorageKind::Annex,
            earliest_ts: Some("2026-06-01T00:00:00Z".to_string()),
            latest_ts: Some("2026-06-01T01:00:00Z".to_string()),
            event_count: Some(7),
            material_count: Some(2),
        }
    }

    fn fixture_readiness(source_identifier: &str) -> SourceReadiness {
        SourceReadiness {
            binding_id: None,
            source_family: "fixture".to_string(),
            source_id: Some(SourceId::from_static("fixture.source")),
            parser_id: Some(ParserId::from_static("fixture.parser")),
            source_identifier: source_identifier.to_string(),
            status: SourceReadinessStatus::Available,
            cost: SourceReadinessCost::LocalFast,
            freshness_seconds: Some(12),
            material_count: 2,
            parsed_event_count: Some(7),
            last_success_at: Some("2026-06-01T00:00:00Z".to_string()),
            caveats: Vec::new(),
            evidence: serde_json::json!({"fixture": true}),
        }
    }

    #[sinex_test]
    async fn stage_request_preserves_package_mode_binding() -> TestResult<()> {
        let command = StageCommand {
            file: "/realm/data/captures/audio/session.json".to_string(),
            reason: Some("operator import".to_string()),
            material_format: Some(SourceMaterialFormat::Json),
            binding: Some("source:media.audio-transcript.audio-bundle-staged".to_string()),
            tags: vec!["media".to_string()],
        };

        let request = command.request();

        assert_eq!(request.file_path, "/realm/data/captures/audio/session.json");
        assert_eq!(request.format, Some(SourceMaterialFormat::Json));
        assert_eq!(request.reason.as_deref(), Some("operator import"));
        assert_eq!(
            request.binding_name.as_deref(),
            Some("source:media.audio-transcript.audio-bundle-staged")
        );
        assert_eq!(request.tags, vec!["media"]);
        assert!(request.with_bytes);
        Ok(())
    }

    #[sinex_test]
    async fn source_material_list_envelope_renders_finite_json_document() -> TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.list",
            SourceMaterialListView::new(vec![fixture_material("material-1")]),
        )
        .with_query_echo(serde_json::json!({
            "status": "completed",
            "limit": 1,
        }));

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.sources.list");
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_MATERIAL_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 1);
        assert_eq!(value["payload"]["materials"][0]["id"], "material-1");
        assert_eq!(value["query_echo"]["status"], "completed");
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_envelope_renders_finite_json_document() -> TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.coverage",
            SourceCoverageListView::new(vec![fixture_coverage("fixture.source")]),
        )
        .with_query_echo(serde_json::json!({
            "limit": 100,
        }));

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.sources.coverage");
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_COVERAGE_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 1);
        assert_eq!(
            value["payload"]["sources"][0]["source_identifier"],
            "fixture.source"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_readiness_list_envelope_renders_finite_json_document() -> TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.readiness",
            SourceReadinessListView::new(vec![fixture_readiness("fixture.source")]),
        )
        .with_query_echo(serde_json::json!({
            "family": "fixture",
            "stale_after_seconds": 60,
        }));

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.sources.readiness");
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_READINESS_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 1);
        assert_eq!(
            value["payload"]["sources"][0]["source_identifier"],
            "fixture.source"
        );
        assert_eq!(value["payload"]["sources"][0]["status"], "available");
        assert_eq!(value["query_echo"]["family"], "fixture");
        Ok(())
    }

    #[sinex_test]
    async fn source_readiness_detail_envelope_renders_finite_json_document() -> TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.readiness",
            SourceReadinessDetailView::new(Some(fixture_readiness("fixture.source"))),
        )
        .with_query_echo(serde_json::json!({
            "source": "fixture.source",
        }));

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_READINESS_DETAIL_SCHEMA_VERSION
        );
        assert_eq!(
            value["payload"]["source"]["source_identifier"],
            "fixture.source"
        );
        assert_eq!(value["query_echo"]["source"], "fixture.source");
        Ok(())
    }

    #[sinex_test]
    async fn source_drift_envelope_renders_finite_json_document() -> TestResult<()> {
        let drift = SourceShapeDriftObservation {
            checkpoint_key: "source.default.fixture".to_string(),
            source_id: SourceId::from_static("browser.history"),
            consumer_group: Some("default".to_string()),
            consumer_name: Some("fixture".to_string()),
            previous_hash: "shape-old".to_string(),
            current_hash: "shape-new".to_string(),
            format: "sqlite_schema".to_string(),
            added_keys: vec!["title".to_string()],
            removed_keys: Vec::new(),
            type_changes: Vec::new(),
            required_input_keys: Vec::new(),
            observed_at: "2026-05-21T07:00:00Z".to_string(),
        };
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.drift",
            SourceDriftListView::new(vec![drift]),
        )
        .with_query_echo(serde_json::json!({
            "source": "browser.history",
            "limit": 1,
        }));

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
        assert_eq!(value["source_surface"], "sinexctl.sources.drift");
        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_DRIFT_LIST_SCHEMA_VERSION
        );
        assert_eq!(value["payload"]["count"], 1);
        assert_eq!(
            value["payload"]["drifts"][0]["source_id"],
            "browser.history"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_continuity_empty_views_render_finite_json_documents() -> TestResult<()> {
        let list = ViewEnvelope::new(
            "sinexctl.sources.continuity",
            SourceContinuityListView::new(Vec::new()),
        );
        let detail = ViewEnvelope::new(
            "sinexctl.sources.continuity",
            SourceContinuityDetailView::new(None),
        );

        let list_json = render_finite_envelope(&list, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let list_value: serde_json::Value = serde_json::from_str(&list_json)?;
        assert_eq!(
            list_value["payload"]["schema_version"],
            SOURCE_CONTINUITY_LIST_SCHEMA_VERSION
        );
        assert_eq!(list_value["payload"]["count"], 0);

        let detail_json = render_finite_envelope(&detail, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let detail_value: serde_json::Value = serde_json::from_str(&detail_json)?;
        assert_eq!(
            detail_value["payload"]["schema_version"],
            SOURCE_CONTINUITY_DETAIL_SCHEMA_VERSION
        );
        assert!(detail_value["payload"].get("report").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn source_continuity_gap_envelope_renders_finite_json_document() -> TestResult<()> {
        let response = SourcesExplainGapResponse {
            source_family: SourceFamily::new("fixture")?,
            at: parse_timestamp("2026-06-01T00:00:00Z")?,
            gap: None,
            explanation: "coverage present".to_string(),
        };
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.explain_gap",
            SourceContinuityGapView::new(response),
        );

        let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
            .expect("json renders finite envelope");
        let value: serde_json::Value = serde_json::from_str(&rendered)?;

        assert_eq!(
            value["payload"]["schema_version"],
            SOURCE_CONTINUITY_GAP_SCHEMA_VERSION
        );
        assert_eq!(
            value["payload"]["explanation"]["explanation"],
            "coverage present"
        );
        Ok(())
    }

    #[sinex_test]
    async fn source_material_table_renderer_stays_on_raw_response() -> TestResult<()> {
        let table = format_source_materials_table(&SourcesListResponse {
            materials: vec![fixture_material("abcdef123456")],
        });

        assert!(table.contains("abcdef12..."));
        assert!(table.contains("fixture.source"));
        assert!(table.contains("completed"));
        Ok(())
    }

    #[sinex_test]
    async fn source_coverage_table_renderer_stays_on_raw_response() -> TestResult<()> {
        let table = format_coverage_table(&SourcesCoverageResponse {
            sources: vec![fixture_coverage("fixture.source")],
        });

        assert!(table.contains("fixture.source"));
        assert!(table.contains("session_document"));
        assert!(table.contains("7"));
        Ok(())
    }

    #[sinex_test]
    async fn finite_source_views_reject_ndjson() -> TestResult<()> {
        let envelope = ViewEnvelope::new(
            "sinexctl.sources.readiness",
            SourceReadinessListView::new(vec![fixture_readiness("fixture.source")]),
        );

        let result = render_finite_envelope(&envelope, OutputFormat::Ndjson);

        assert!(
            result.is_err(),
            "finite source views must not render ndjson"
        );
        assert!(result.unwrap_err().to_string().contains("streaming"));
        Ok(())
    }
}

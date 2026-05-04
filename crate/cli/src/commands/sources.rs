use clap::Args;
use console::style;
use serde::{Deserialize, Serialize};
use tabled::{builder::Builder, settings::Style};

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Source material inventory and staging
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
}

// ── Placeholder RPC types ──────────────────────────────────────────────
// These will move to sinex_primitives::rpc::sources once the gateway
// handlers are implemented (#1008).

#[derive(Debug, Serialize)]
struct StageRequest {
    file_path: String,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StageResponse {
    material_id: String,
    source_identifier: String,
    total_bytes: i64,
}

#[derive(Debug, Serialize)]
struct ListRequest {
    status: Option<String>,
    limit: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SourceMaterialEntry {
    id: String,
    material_kind: String,
    source_identifier: String,
    status: String,
    total_bytes: Option<i64>,
    staged_at: Option<String>,
    staged_by: Option<String>,
    staged_on_host: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ListResponse {
    materials: Vec<SourceMaterialEntry>,
}

#[derive(Debug, Serialize)]
struct ShowRequest {
    material_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
struct SourceMaterialDetail {
    id: String,
    material_kind: String,
    source_identifier: String,
    status: String,
    timing_info_type: String,
    #[allow(dead_code)]
    metadata: serde_json::Value,
    staged_at: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    staged_by: Option<String>,
    staged_on_host: Option<String>,
    optional_blob_id: Option<String>,
    total_bytes: Option<i64>,
    event_count: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ShowResponse {
    material: SourceMaterialDetail,
}

#[derive(Debug, Serialize)]
struct CoverageRequest {
    #[allow(dead_code)]
    limit: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoverageBucket {
    source_identifier: String,
    material_kind: String,
    earliest_ts: Option<String>,
    latest_ts: Option<String>,
    event_count: Option<i64>,
    material_count: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CoverageResponse {
    buckets: Vec<CoverageBucket>,
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
}

impl StageCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let req = StageRequest {
            file_path: self.file.clone(),
            reason: self.reason.clone(),
        };

        let response = client
            .call_raw_rpc("sources.stage", serde_json::to_value(&req)?)
            .await?;
        let stage_response: StageResponse = serde_json::from_value(response)?;

        CommandOutput::single(stage_response, format_stage_result).display(&format)?;
        Ok(())
    }
}

fn format_stage_result(response: &StageResponse) -> String {
    format!(
        "Staged source material\n  ID: {}\n  Source: {}\n  Total bytes: {}",
        style(&response.material_id).green(),
        style(&response.source_identifier).cyan(),
        style(response.total_bytes).yellow(),
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
        let req = ListRequest {
            status: self.status.clone(),
            limit: Some(self.limit),
        };

        let response = client
            .call_raw_rpc("sources.list", serde_json::to_value(&req)?)
            .await?;
        let list_response: ListResponse = serde_json::from_value(response)?;

        CommandOutput::single(list_response, format_source_materials_table).display(&format)?;
        Ok(())
    }
}

fn format_source_materials_table(response: &ListResponse) -> String {
    if response.materials.is_empty() {
        return "No source materials found.".to_string();
    }

    let mut builder = Builder::new();
    builder.push_record([
        "ID",
        "KIND",
        "SOURCE",
        "STATUS",
        "SIZE",
        "STAGED AT",
        "STAGED BY",
    ]);

    for m in &response.materials {
        let short_id = format!("{}...", &m.id[..8.min(m.id.len())]);
        let size = m
            .total_bytes
            .map_or_else(|| style("-").dim().to_string(), |b| b.to_string());
        let staged_at = m
            .staged_at
            .as_deref()
            .unwrap_or("-");
        let staged_by = m
            .staged_by
            .as_deref()
            .unwrap_or("-");

        builder.push_record([
            short_id,
            m.material_kind.clone(),
            m.source_identifier.clone(),
            m.status.clone(),
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
        let req = ShowRequest {
            material_id: self.material_id.clone(),
        };

        let response = client
            .call_raw_rpc("sources.show", serde_json::to_value(&req)?)
            .await?;
        let show_response: ShowResponse = serde_json::from_value(response)?;

        CommandOutput::single(show_response, format_source_material_detail).display(&format)?;
        Ok(())
    }
}

fn format_source_material_detail(response: &ShowResponse) -> String {
    let m = &response.material;
    let mut lines = vec![
        format!("Source Material: {}", style(&m.id).green()),
        format!("  Kind:         {}", m.material_kind),
        format!("  Source:       {}", m.source_identifier),
        format!("  Status:       {}", m.status),
        format!("  Timing:       {}", m.timing_info_type),
        format!(
            "  Staged at:    {}",
            m.staged_at.as_deref().unwrap_or("-")
        ),
        format!(
            "  Start time:   {}",
            m.start_time.as_deref().unwrap_or("-")
        ),
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
        let req = CoverageRequest {
            limit: Some(self.limit),
        };

        let response = client
            .call_raw_rpc("sources.coverage", serde_json::to_value(&req)?)
            .await?;
        let coverage_response: CoverageResponse = serde_json::from_value(response)?;

        CommandOutput::single(coverage_response, format_coverage_table).display(&format)?;
        Ok(())
    }
}

fn format_coverage_table(response: &CoverageResponse) -> String {
    if response.buckets.is_empty() {
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

    for bucket in &response.buckets {
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

//! `sinexctl declare` — manual canonical declarations.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::Serialize;
use sinex_primitives::events::payloads::{HealthQuantity, HealthTimingQuality};
use sinex_primitives::rpc::health::{
    HealthDeclarationResponse, HealthEffectRecordRequest, HealthIntakeRecordRequest,
};
use sinex_primitives::rpc::tasks::{TaskCreateRequest, TaskEventResponse};

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::validation::parse_time_input;

#[derive(Debug, Args)]
pub struct DeclareCommand {
    #[command(subcommand)]
    cmd: DeclareSubcommand,
}

impl DeclareCommand {
    #[must_use]
    pub fn subcommand(&self) -> &DeclareSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            DeclareSubcommand::Health(cmd) => cmd.execute(client, format).await,
            DeclareSubcommand::Task(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum DeclareSubcommand {
    /// Manually declare a health intake or effect observation.
    Health(DeclareHealthCommand),
    /// Manually declare a canonical task.
    Task(DeclareTaskCommand),
}

#[derive(Debug, Args)]
pub struct DeclareHealthCommand {
    #[command(subcommand)]
    cmd: DeclareHealthSubcommand,
}

impl DeclareHealthCommand {
    #[must_use]
    pub fn subcommand(&self) -> &DeclareHealthSubcommand {
        &self.cmd
    }

    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            DeclareHealthSubcommand::Intake(cmd) => cmd.execute(client, format).await,
            DeclareHealthSubcommand::Effect(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum DeclareHealthSubcommand {
    /// Record structured substance intake without storing freeform notes.
    Intake(DeclareHealthIntakeCommand),
    /// Record a structured effect observation without storing freeform notes.
    Effect(DeclareHealthEffectCommand),
}

#[derive(Debug, Args)]
pub struct DeclareHealthIntakeCommand {
    /// Substance name as observed by the operator.
    #[arg(long)]
    substance: String,

    /// Numeric dose value.
    #[arg(long)]
    dose: Option<f64>,

    /// Dose unit, required when --dose is supplied.
    #[arg(long)]
    unit: Option<String>,

    /// Optional dose precision label, e.g. exact, approximate, range.
    #[arg(long)]
    precision: Option<String>,

    /// Intake route, e.g. oral, topical.
    #[arg(long)]
    route: Option<String>,

    /// Intake form, e.g. tablet, liquid.
    #[arg(long)]
    form: Option<String>,

    /// Observation time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    at: String,

    /// Timing quality: exact, approximate, date-only, unknown.
    #[arg(long, default_value = "exact", value_parser = parse_timing_quality)]
    timing_quality: HealthTimingQuality,

    /// Confidence from 0.0 to 1.0.
    #[arg(long)]
    confidence: Option<f64>,

    /// Freeform note. Accepted only as a redaction signal; not stored raw.
    #[arg(long)]
    note: Option<String>,
}

impl DeclareHealthIntakeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let substance = non_empty("health intake --substance", &self.substance)?;
        let dose = health_quantity(self.dose, self.unit.as_deref(), self.precision.clone())?;
        let response = client
            .health_intake_record(HealthIntakeRecordRequest {
                intake_id: None,
                substance,
                dose,
                route: self.route.clone(),
                form: self.form.clone(),
                occurred_at: parse_time_input(&self.at)?,
                timing_quality: self.timing_quality,
                confidence: self.confidence,
                note: self.note.clone(),
            })
            .await?;
        render_health_response(&response, format, "Health intake recorded")
    }
}

#[derive(Debug, Args)]
pub struct DeclareHealthEffectCommand {
    /// Effect or state observed.
    #[arg(long)]
    effect: String,

    /// Optional related intake UUID.
    #[arg(long)]
    related_intake_id: Option<sinex_primitives::Uuid>,

    /// Optional severity label.
    #[arg(long)]
    severity: Option<String>,

    /// Observation time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    at: String,

    /// Timing quality: exact, approximate, date-only, unknown.
    #[arg(long, default_value = "exact", value_parser = parse_timing_quality)]
    timing_quality: HealthTimingQuality,

    /// Confidence from 0.0 to 1.0.
    #[arg(long)]
    confidence: Option<f64>,

    /// Freeform note. Accepted only as a redaction signal; not stored raw.
    #[arg(long)]
    note: Option<String>,
}

impl DeclareHealthEffectCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let effect = non_empty("health effect --effect", &self.effect)?;
        let response = client
            .health_effect_record(HealthEffectRecordRequest {
                observation_id: None,
                related_intake_id: self.related_intake_id,
                effect,
                severity: self.severity.clone(),
                observed_at: parse_time_input(&self.at)?,
                timing_quality: self.timing_quality,
                confidence: self.confidence,
                note: self.note.clone(),
            })
            .await?;
        render_health_response(&response, format, "Health effect recorded")
    }
}

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl declare task --title 'Fix parser drift'
    sinexctl declare task --title 'Call accountant' --tag finance --due 2026-06-01
")]
pub struct DeclareTaskCommand {
    /// Task title.
    #[arg(long)]
    title: String,

    /// Longer task body or notes.
    #[arg(long)]
    body: Option<String>,

    /// Project identifier or slug.
    #[arg(long)]
    project_id: Option<String>,

    /// Tag to attach to the task. Can be repeated.
    #[arg(long = "tag")]
    tags: Vec<String>,

    /// Due time/date. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    due: Option<String>,

    /// Priority label.
    #[arg(long)]
    priority: Option<String>,
}

impl DeclareTaskCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let title = self.title.trim();
        if title.is_empty() {
            return Err(eyre!("task --title must not be empty"));
        }
        let due_at = self.due.as_deref().map(parse_time_input).transpose()?;
        let response = client
            .tasks_create(TaskCreateRequest {
                task_id: None,
                title: title.to_string(),
                body: self.body.clone(),
                external_refs: Vec::new(),
                project_id: self.project_id.clone(),
                tags: self.tags.clone(),
                due_at,
                priority: self.priority.clone(),
            })
            .await?;
        render_task_response(&response, format, "Task declared")
    }
}

pub(crate) fn render_task_response<T>(
    response: &TaskEventResponse<T>,
    format: OutputFormat,
    label: &str,
) -> Result<()>
where
    T: Serialize,
{
    match format {
        OutputFormat::Json | OutputFormat::Dot => {
            println!("{}", format_json(response)?);
        }
        OutputFormat::Yaml => {
            println!("{}", format_yaml(response)?);
        }
        OutputFormat::Table => {
            let task_id = response.state.task_id;
            let status = format!("{:?}", response.state.status).to_lowercase();
            let event_id = response
                .event
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("-");
            let material_id = response.material_id;
            println!("{label}");
            println!("  Task:     {task_id}");
            println!("  Status:   {status}");
            println!("  Event:    {event_id}");
            println!("  Material: {material_id}");
        }
    }
    Ok(())
}

fn render_health_response<T>(
    response: &HealthDeclarationResponse<T>,
    format: OutputFormat,
    label: &str,
) -> Result<()>
where
    T: Serialize,
{
    match format {
        OutputFormat::Json | OutputFormat::Dot => {
            println!("{}", format_json(response)?);
        }
        OutputFormat::Yaml => {
            println!("{}", format_yaml(response)?);
        }
        OutputFormat::Table => {
            let event_id = response
                .event
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("-");
            println!("{label}");
            println!("  Event:    {event_id}");
            println!("  Material: {}", response.material_id);
            println!("  Notes:    redacted by default");
        }
    }
    Ok(())
}

fn health_quantity(
    dose: Option<f64>,
    unit: Option<&str>,
    precision: Option<String>,
) -> Result<Option<HealthQuantity>> {
    let Some(value) = dose else {
        return Ok(None);
    };
    let unit = unit
        .map(str::trim)
        .filter(|unit| !unit.is_empty())
        .ok_or_else(|| eyre!("--unit is required when --dose is supplied"))?;
    Ok(Some(HealthQuantity {
        value,
        unit: unit.to_string(),
        precision,
    }))
}

fn parse_timing_quality(input: &str) -> std::result::Result<HealthTimingQuality, String> {
    match input {
        "exact" => Ok(HealthTimingQuality::Exact),
        "approximate" => Ok(HealthTimingQuality::Approximate),
        "date-only" | "date_only" => Ok(HealthTimingQuality::DateOnly),
        "unknown" => Ok(HealthTimingQuality::Unknown),
        other => Err(format!(
            "unknown timing quality `{other}`; valid values: exact,approximate,date-only,unknown"
        )),
    }
}

fn non_empty(label: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(eyre!("{label} must not be empty"));
    }
    Ok(trimmed.to_string())
}

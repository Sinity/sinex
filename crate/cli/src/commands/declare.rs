//! `sinexctl declare` — manual canonical declarations.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde_json::json;

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
            DeclareSubcommand::Task(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum DeclareSubcommand {
    /// Manually declare a canonical task.
    Task(DeclareTaskCommand),
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
        let params = json!({
            "title": title,
            "body": self.body,
            "project_id": self.project_id,
            "tags": self.tags,
            "due_at": due_at,
            "priority": self.priority,
        });
        let response = client.call_raw_rpc("tasks.create", params).await?;
        render_task_response(&response, format, "Task declared")
    }
}

pub(crate) fn render_task_response(
    response: &serde_json::Value,
    format: OutputFormat,
    label: &str,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => {
            println!("{}", format_json(response)?);
        }
        OutputFormat::Yaml => {
            println!("{}", format_yaml(response)?);
        }
        OutputFormat::Table => {
            let task_id = response["state"]["task_id"].as_str().unwrap_or("-");
            let status = response["state"]["status"].as_str().unwrap_or("-");
            let event_id = response["event"]["id"].as_str().unwrap_or("-");
            let material_id = response["material_id"].as_str().unwrap_or("-");
            println!("{label}");
            println!("  Task:     {task_id}");
            println!("  Status:   {status}");
            println!("  Event:    {event_id}");
            println!("  Material: {material_id}");
        }
    }
    Ok(())
}

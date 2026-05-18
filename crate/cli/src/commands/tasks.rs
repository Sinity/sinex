//! `sinexctl tasks` — task lifecycle and projection commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::tasks::{TaskCompleteRequest, TaskStateGetRequest, TaskStateResponse};

use crate::client::GatewayClient;
use crate::commands::declare::render_task_response;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::validation::parse_time_input;

#[derive(Debug, Args)]
pub struct TasksCommand {
    #[command(subcommand)]
    cmd: TasksSubcommand,
}

impl TasksCommand {
    #[must_use]
    pub fn subcommand(&self) -> &TasksSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            TasksSubcommand::Complete(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::State(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum TasksSubcommand {
    /// Mark a task completed.
    Complete(TaskCompleteCommand),
    /// Rebuild and show current task state.
    State(TaskStateCommand),
}

#[derive(Debug, Args)]
pub struct TaskCompleteCommand {
    /// Task UUID.
    task_id: String,

    /// Completion reason or note.
    #[arg(long)]
    reason: Option<String>,

    /// Completion time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    completed_at: Option<String>,
}

impl TaskCompleteCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let completed_at = self
            .completed_at
            .as_deref()
            .map(parse_time_input)
            .transpose()?;
        let task_id = parse_task_id(&self.task_id)?;
        let response = client
            .tasks_complete(TaskCompleteRequest {
                task_id,
                completed_at,
                reason: self.reason.clone(),
                external_version: None,
            })
            .await?;
        render_task_response(&response, format, "Task completed")
    }
}

#[derive(Debug, Args)]
pub struct TaskStateCommand {
    /// Task UUID.
    task_id: String,
}

impl TaskStateCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let task_id = parse_task_id(&self.task_id)?;
        let response = client
            .tasks_state_get(TaskStateGetRequest { task_id })
            .await?;
        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&response)?);
            }
            OutputFormat::Table => {
                if response.state.is_none() {
                    println!("No task state for {}.", self.task_id);
                } else if let Some(state) = response.state.as_ref() {
                    render_task_state_table(&response, state);
                }
            }
        }
        Ok(())
    }
}

fn parse_task_id(raw: &str) -> Result<Uuid> {
    raw.parse::<Uuid>()
        .map_err(|error| eyre!("invalid task UUID `{raw}`: {error}"))
}

fn render_task_state_table(
    response: &TaskStateResponse,
    state: &sinex_primitives::task_domain::TaskState,
) {
    println!("Task {}", state.task_id);
    println!("  Status: {}", format!("{:?}", state.status).to_lowercase());
    println!("  Title:  {}", state.title);
    println!("  Event:  {}", state.last_event_id);
    println!("  Events: {}", response.event_count);
}

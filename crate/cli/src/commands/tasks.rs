//! `sinexctl tasks` — task lifecycle and projection commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::tasks::{
    TaskCompleteRequest, TaskListRequest, TaskListResponse, TaskStateGetRequest, TaskStateResponse,
};
use sinex_primitives::task_domain::TaskStatus;

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
            TasksSubcommand::List(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::State(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum TasksSubcommand {
    /// Mark a task completed.
    Complete(TaskCompleteCommand),
    /// List current task states.
    List(TaskListCommand),
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
pub struct TaskListCommand {
    /// Filter by task lifecycle status.
    #[arg(long)]
    status: Option<String>,

    /// Filter by project id.
    #[arg(long)]
    project_id: Option<String>,

    /// Filter by tag.
    #[arg(long)]
    tag: Option<String>,

    /// Maximum number of task states to return.
    #[arg(long)]
    limit: Option<u32>,
}

impl TaskListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .tasks_list(TaskListRequest {
                status: self.status.as_deref().map(parse_task_status).transpose()?,
                project_id: self.project_id.clone(),
                tag: self.tag.clone(),
                limit: self.limit,
            })
            .await?;
        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&response)?);
            }
            OutputFormat::Table => render_task_list_table(&response),
        }
        Ok(())
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

fn parse_task_status(raw: &str) -> Result<TaskStatus> {
    raw.parse::<TaskStatus>()
        .map_err(|error| eyre!("invalid task status `{raw}`: {error}"))
}

fn render_task_state_table(
    response: &TaskStateResponse,
    state: &sinex_primitives::task_domain::TaskState,
) {
    println!("Task {}", state.task_id);
    println!("  Status: {}", state.status);
    println!("  Title:  {}", state.title);
    println!("  Event:  {}", state.last_event_id);
    println!("  Events: {}", response.event_count);
}

fn render_task_list_table(response: &TaskListResponse) {
    if response.tasks.is_empty() {
        println!("No task states found.");
        return;
    }

    println!(
        "{:<36}  {:<10}  {:<28}  {:<16}  Title",
        "Task ID", "Status", "Updated", "Tags"
    );
    for state in &response.tasks {
        let tags = if state.tags.is_empty() {
            "-".to_string()
        } else {
            state.tags.join(",")
        };
        println!(
            "{:<36}  {:<10}  {:<28}  {:<16}  {}",
            state.task_id, state.status, state.updated_at, tags, state.title
        );
    }
    if response.total > response.tasks.len() {
        println!(
            "Showing {} of {} task states (event_count={}, limit={}).",
            response.tasks.len(),
            response.total,
            response.event_count,
            response.limit
        );
    }
}

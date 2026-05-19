//! `sinexctl tasks` — task lifecycle and projection commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinex_primitives::rpc::tasks::{
    TaskCancelRequest, TaskCompleteRequest, TaskListRequest, TaskListResponse, TaskStateGetRequest,
    TaskStateResponse, TaskStatusSetRequest, TaskUpdateRequest,
};
use sinex_primitives::task_domain::{TaskFieldUpdate, TaskStatus};

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
            TasksSubcommand::Cancel(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::Complete(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::List(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::State(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::Status(cmd) => cmd.execute(client, format).await,
            TasksSubcommand::Update(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum TasksSubcommand {
    /// Mark a task cancelled.
    Cancel(TaskCancelCommand),
    /// Mark a task completed.
    Complete(TaskCompleteCommand),
    /// List current task states.
    List(TaskListCommand),
    /// Rebuild and show current task state.
    State(TaskStateCommand),
    /// Set a non-terminal task status.
    Status(TaskStatusCommand),
    /// Update task metadata.
    Update(TaskUpdateCommand),
}

#[derive(Debug, Args)]
pub struct TaskCancelCommand {
    /// Task UUID.
    task_id: String,

    /// Cancellation reason or note.
    #[arg(long)]
    reason: Option<String>,

    /// Cancellation time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    cancelled_at: Option<String>,
}

impl TaskCancelCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let cancelled_at = self
            .cancelled_at
            .as_deref()
            .map(parse_time_input)
            .transpose()?;
        let task_id = parse_task_id(&self.task_id)?;
        let response = client
            .tasks_cancel(TaskCancelRequest {
                task_id,
                cancelled_at,
                reason: self.reason.clone(),
                external_version: None,
            })
            .await?;
        render_task_response(&response, format, "Task cancelled")
    }
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
pub struct TaskUpdateCommand {
    /// Task UUID.
    task_id: String,

    /// Replace the task title.
    #[arg(long)]
    title: Option<String>,

    /// Replace the task body or notes.
    #[arg(long)]
    body: Option<String>,

    /// Clear the task body.
    #[arg(long)]
    clear_body: bool,

    /// Replace the project identifier.
    #[arg(long)]
    project_id: Option<String>,

    /// Clear the project identifier.
    #[arg(long)]
    clear_project_id: bool,

    /// Replace the full tag set. Can be repeated.
    #[arg(long = "tag")]
    tags: Vec<String>,

    /// Replace the due time/date. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    due: Option<String>,

    /// Clear the due time/date.
    #[arg(long)]
    clear_due: bool,

    /// Replace the priority label.
    #[arg(long)]
    priority: Option<String>,

    /// Clear the priority label.
    #[arg(long)]
    clear_priority: bool,

    /// Update reason or note.
    #[arg(long)]
    reason: Option<String>,

    /// Update event time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    updated_at: Option<String>,
}

impl TaskUpdateCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let task_id = parse_task_id(&self.task_id)?;
        let updated_at = self
            .updated_at
            .as_deref()
            .map(parse_time_input)
            .transpose()?;
        let due_at = self.due.as_deref().map(parse_time_input).transpose()?;
        let response = client
            .tasks_update(TaskUpdateRequest {
                task_id,
                updated_at,
                title: self.title.clone(),
                body: build_field_update(self.body.clone(), self.clear_body, "body")?,
                project_id: build_field_update(
                    self.project_id.clone(),
                    self.clear_project_id,
                    "project-id",
                )?,
                tags: (!self.tags.is_empty()).then(|| self.tags.clone()),
                due_at: build_field_update(due_at, self.clear_due, "due")?,
                priority: build_field_update(
                    self.priority.clone(),
                    self.clear_priority,
                    "priority",
                )?,
                external_refs: None,
                reason: self.reason.clone(),
                external_version: None,
            })
            .await?;
        render_task_response(&response, format, "Task updated")
    }
}

#[derive(Debug, Args)]
pub struct TaskStatusCommand {
    /// Task UUID.
    task_id: String,

    /// New non-terminal status: open, started, blocked, or deferred.
    #[arg(long)]
    status: String,

    /// Status change reason or note.
    #[arg(long)]
    reason: Option<String>,

    /// Status change time. Accepts RFC3339, YYYY-MM-DD, or relative forms.
    #[arg(long)]
    changed_at: Option<String>,
}

impl TaskStatusCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let task_id = parse_task_id(&self.task_id)?;
        let status = parse_task_status(&self.status)?;
        let changed_at = self
            .changed_at
            .as_deref()
            .map(parse_time_input)
            .transpose()?;
        let response = client
            .tasks_status_set(TaskStatusSetRequest {
                task_id,
                status,
                changed_at,
                reason: self.reason.clone(),
                external_version: None,
            })
            .await?;
        render_task_response(&response, format, "Task status changed")
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

    /// Include tasks due at or after this time/date.
    #[arg(long)]
    due_from: Option<String>,

    /// Include tasks due at or before this time/date.
    #[arg(long)]
    due_until: Option<String>,

    /// Maximum number of task states to return.
    #[arg(long)]
    limit: Option<u32>,
}

impl TaskListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let due_from = self.due_from.as_deref().map(parse_time_input).transpose()?;
        let due_until = self
            .due_until
            .as_deref()
            .map(parse_time_input)
            .transpose()?;
        let response = client
            .tasks_list(TaskListRequest {
                status: self.status.as_deref().map(parse_task_status).transpose()?,
                project_id: self.project_id.clone(),
                tag: self.tag.clone(),
                due_from,
                due_until,
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

fn build_field_update<T>(
    value: Option<T>,
    clear: bool,
    field_name: &str,
) -> Result<Option<TaskFieldUpdate<T>>> {
    match (value, clear) {
        (Some(_), true) => Err(eyre!("cannot both set and clear task {field_name}")),
        (Some(value), false) => Ok(Some(TaskFieldUpdate::Set(value))),
        (None, true) => Ok(Some(TaskFieldUpdate::Clear)),
        (None, false) => Ok(None),
    }
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

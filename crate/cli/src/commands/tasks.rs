//! `sinexctl tasks` — task lifecycle and projection commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use serde_json::json;

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
        let params = json!({
            "task_id": self.task_id,
            "completed_at": completed_at,
            "reason": self.reason,
        });
        let response = client.call_raw_rpc("tasks.complete", params).await?;
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
        let response = client
            .call_raw_rpc("tasks.state.get", json!({ "task_id": self.task_id }))
            .await?;
        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", format_json(&response)?);
            }
            OutputFormat::Yaml => {
                println!("{}", format_yaml(&response)?);
            }
            OutputFormat::Table => {
                if response["state"].is_null() {
                    println!("No task state for {}.", self.task_id);
                } else {
                    println!(
                        "Task {}",
                        response["state"]["task_id"].as_str().unwrap_or("-")
                    );
                    println!(
                        "  Status: {}",
                        response["state"]["status"].as_str().unwrap_or("-")
                    );
                    println!(
                        "  Title:  {}",
                        response["state"]["title"].as_str().unwrap_or("-")
                    );
                    println!(
                        "  Event:  {}",
                        response["state"]["last_event_id"].as_str().unwrap_or("-")
                    );
                    println!(
                        "  Events: {}",
                        response["event_count"].as_u64().unwrap_or(0)
                    );
                }
            }
        }
        Ok(())
    }
}

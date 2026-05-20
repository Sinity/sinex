//! `sinexctl instructions` - typed local instruction admission commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use sinex_primitives::Uuid;
use sinex_primitives::events::payloads::ActuationStatus;
use sinex_primitives::rpc::instructions::{
    HyprlandWorkspaceSwitchRequest, HyprlandWorkspaceSwitchResponse,
};

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use crate::validation::parse_time_input;

#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    sinexctl instructions hyprland-workspace --workspace 4 --socket-path /run/user/1000/hypr/.../.socket.sock
    sinexctl instructions hyprland-workspace --workspace 4 --dry-run -f json
")]
pub struct InstructionsCommand {
    #[command(subcommand)]
    cmd: InstructionsSubcommand,
}

impl InstructionsCommand {
    #[must_use]
    pub fn subcommand(&self) -> &InstructionsSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            InstructionsSubcommand::HyprlandWorkspace(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum InstructionsSubcommand {
    /// Admit and optionally dispatch a Hyprland workspace-switch instruction.
    HyprlandWorkspace(HyprlandWorkspaceCommand),
}

#[derive(Debug, Args)]
pub struct HyprlandWorkspaceCommand {
    /// Desired Hyprland workspace id.
    #[arg(long, short = 'w')]
    workspace: i32,

    /// Optional instruction UUID. Omit to let the gateway allocate one.
    #[arg(long = "instruction-id")]
    instruction_id: Option<Uuid>,

    /// Optional deadline: relative duration, date, or RFC3339 timestamp.
    #[arg(long)]
    deadline: Option<String>,

    /// Record the instruction and actuation plan without writing to Hyprland.
    #[arg(long)]
    dry_run: bool,

    /// Hyprland command socket path used for live dispatch.
    #[arg(long = "socket-path")]
    socket_path: Option<String>,
}

impl HyprlandWorkspaceCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let deadline = self.deadline.as_deref().map(parse_time_input).transpose()?;
        let response = client
            .instructions_hyprland_workspace_switch(HyprlandWorkspaceSwitchRequest {
                instruction_id: self.instruction_id,
                desired_workspace_id: self.workspace,
                deadline,
                dry_run: self.dry_run,
                command_socket_path: self.socket_path.clone(),
            })
            .await?;
        render_hyprland_workspace_switch(&response, format)
    }
}

fn render_hyprland_workspace_switch(
    response: &HyprlandWorkspaceSwitchResponse,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            let instruction_event_id = response
                .instruction_event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            let attempt_event_id = response
                .attempt_event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            let current_workspace = response
                .current_workspace_id
                .map_or_else(|| "<unknown>".to_string(), |id| id.to_string());
            let command_response = response
                .command_socket_response
                .as_deref()
                .unwrap_or("<none>");
            let error = response.attempt.error.as_deref().unwrap_or("<none>");

            println!("Hyprland workspace instruction");
            println!("  Instruction: {}", response.instruction.instruction_id);
            println!(
                "  Desired:     {}",
                response.instruction.desired_workspace_id
            );
            println!("  Current:     {current_workspace}");
            println!(
                "  Observation: {}",
                if response.observation_ready {
                    "ready"
                } else {
                    "unavailable"
                }
            );
            println!(
                "  Attempt:     {}",
                format_actuation_status(response.attempt.status)
            );
            println!("  Material:    {}", response.material_id);
            println!("  Event:       {instruction_event_id}");
            println!("  Attempt evt: {attempt_event_id}");
            println!("  Socket:      {command_response}");
            println!("  Error:       {error}");
        }
    }
    Ok(())
}

fn format_actuation_status(status: ActuationStatus) -> &'static str {
    match status {
        ActuationStatus::Accepted => "accepted",
        ActuationStatus::Rejected => "rejected",
        ActuationStatus::DryRun => "dry_run",
        ActuationStatus::NoopAlreadySatisfied => "noop_already_satisfied",
        ActuationStatus::Attempted => "attempted",
        ActuationStatus::Failed => "failed",
        ActuationStatus::Unavailable => "unavailable",
    }
}

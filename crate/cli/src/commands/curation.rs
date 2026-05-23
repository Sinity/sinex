//! `sinexctl curation` — proposal and judgment operator commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::curation::{
    CurationFinalizeRequest, CurationFinalizeResponse, CurationListProposalsRequest,
    CurationRecordJudgmentRequest, CurationRecordJudgmentResponse,
};

use crate::client::GatewayClient;
use crate::commands::common::parse_serde_enum;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

#[derive(Debug, Args)]
pub struct CurationCommand {
    #[command(subcommand)]
    cmd: CurationSubcommand,
}

impl CurationCommand {
    #[must_use]
    pub fn subcommand(&self) -> &CurationSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            CurationSubcommand::Proposals(cmd) => cmd.execute(client, format).await,
            CurationSubcommand::Judge(cmd) => cmd.execute(client, format).await,
            CurationSubcommand::Finalize(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum CurationSubcommand {
    /// List pending curation proposals.
    Proposals(CurationProposalsCommand),
    /// Record an authority judgment over a proposal event.
    Judge(CurationJudgeCommand),
    /// Finalize an accepted or modified judgment.
    Finalize(CurationFinalizeCommand),
}

#[derive(Debug, Args)]
pub struct CurationProposalsCommand {
    /// Proposal status to list.
    #[arg(long, default_value = "pending")]
    status: String,

    /// Maximum proposals to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl CurationProposalsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .curation_proposals_list(CurationListProposalsRequest {
                status: self.status.clone(),
                limit: self.limit,
            })
            .await?;
        render_proposals(&response, format)
    }
}

#[derive(Debug, Args)]
pub struct CurationJudgeCommand {
    /// Proposal event UUID.
    proposal_event_id: String,

    /// Actor kind: user, operator, `deterministic_policy`, or `test_fixture`.
    #[arg(long, default_value = "operator")]
    actor_kind: String,

    /// Actor id. Defaults to the authenticated RPC actor at the gateway.
    #[arg(long)]
    actor_id: Option<String>,

    /// Judgment decision: accept, reject, modify, or defer.
    #[arg(long)]
    decision: String,

    /// Corrected canonical payload JSON for modify decisions.
    #[arg(long)]
    corrected_payload: Option<String>,

    /// Human/operator comment.
    #[arg(long)]
    comment: Option<String>,
}

impl CurationJudgeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let corrected_payload = self
            .corrected_payload
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| eyre!("invalid --corrected-payload JSON: {error}"))?;
        let response = client
            .curation_judgments_record(CurationRecordJudgmentRequest {
                proposal_event_id: self.proposal_event_id.clone(),
                actor_kind: parse_serde_enum("actor-kind", &self.actor_kind)?,
                actor_id: self.actor_id.clone(),
                decision: parse_serde_enum("decision", &self.decision)?,
                corrected_payload,
                comment: self.comment.clone(),
                authorization_context: None,
            })
            .await?;
        render_judgment(&response, format)
    }
}

#[derive(Debug, Args)]
pub struct CurationFinalizeCommand {
    /// Judgment event UUID.
    judgment_event_id: String,
}

impl CurationFinalizeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .curation_finalize(CurationFinalizeRequest {
                judgment_event_id: self.judgment_event_id.clone(),
            })
            .await?;
        render_finalization(&response, format)
    }
}

fn render_finalization(response: &CurationFinalizeResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            let event_id = response
                .event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            println!("Curation finalization recorded");
            println!("  Event:      {event_id}");
            println!("  Proposal:   {}", response.finalized.proposal_id);
            println!("  Judgment:   {}", response.finalized.judgment_id);
            println!(
                "  Output:     {}/{}",
                response.finalized.output_source, response.finalized.output_event_type
            );
        }
    }
    Ok(())
}

fn render_proposals(response: &EventQueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => match response {
            EventQueryResult::Events { events, .. } => {
                println!("Curation proposals: {}", events.len());
                for event in events {
                    let id = event
                        .event
                        .id
                        .as_ref()
                        .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
                    let kind = event
                        .event
                        .payload
                        .get("proposal_kind")
                        .and_then(|value| value.as_str())
                        .unwrap_or("<unknown-kind>");
                    let status = event
                        .event
                        .payload
                        .get("status")
                        .and_then(|value| value.as_str())
                        .unwrap_or("<unknown-status>");
                    println!("  {id}  {status:10}  {kind}");
                }
            }
            _ => println!("{}", format_json(response)?),
        },
    }
    Ok(())
}

fn render_judgment(response: &CurationRecordJudgmentResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            let event_id = response
                .event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            println!("Curation judgment recorded");
            println!("  Event:    {event_id}");
            println!("  Proposal: {}", response.judgment.proposal_id);
            println!(
                "  Decision: {}",
                format!("{:?}", response.judgment.decision).to_lowercase()
            );
            println!("  Actor:    {}", response.judgment.actor_id);
        }
    }
    Ok(())
}

//! `sinexctl curation` — proposal and judgment operator commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::Uuid;
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::curation::{
    CurationFinalizeRequest, CurationFinalizeResponse, CurationListDuplicateCandidatesRequest,
    CurationListDuplicateCandidatesResponse, CurationListProposalsRequest,
    CurationRecordDuplicateJudgmentRequest, CurationRecordDuplicateJudgmentResponse,
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
            CurationSubcommand::Duplicates(cmd) => cmd.execute(client, format).await,
            CurationSubcommand::Judge(cmd) => cmd.execute(client, format).await,
            CurationSubcommand::DuplicateJudge(cmd) => cmd.execute(client, format).await,
            CurationSubcommand::Finalize(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum CurationSubcommand {
    /// List pending curation proposals.
    Proposals(CurationProposalsCommand),
    /// List cross-material duplicate candidate clusters.
    Duplicates(CurationDuplicatesCommand),
    /// Record an authority judgment over a proposal event.
    Judge(CurationJudgeCommand),
    /// Record a duplicate-resolution judgment over candidate events.
    DuplicateJudge(CurationDuplicateJudgeCommand),
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
pub struct CurationDuplicatesCommand {
    /// Restrict candidates to one event source.
    #[arg(long)]
    source: Option<String>,

    /// Restrict candidates to one event type.
    #[arg(long)]
    event_type: Option<String>,

    /// Maximum clusters to return.
    #[arg(long, default_value = "100")]
    limit: i64,

    /// Maximum events shown per cluster.
    #[arg(long, default_value = "10")]
    events_per_cluster: i64,
}

impl CurationDuplicatesCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .curation_duplicate_candidates_list(CurationListDuplicateCandidatesRequest {
                source: self.source.clone(),
                event_type: self.event_type.clone(),
                limit: self.limit,
                events_per_cluster: self.events_per_cluster,
            })
            .await?;
        render_duplicate_candidates(&response, format)
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
pub struct CurationDuplicateJudgeCommand {
    /// Candidate event source shared by the duplicate cluster.
    #[arg(long)]
    source: String,

    /// Candidate event type shared by the duplicate cluster.
    #[arg(long)]
    event_type: String,

    /// Logical key shared by the duplicate cluster.
    #[arg(long)]
    equivalence_key: String,

    /// Candidate event UUID. Repeat for every event in the cluster.
    #[arg(long = "event-id", required = true)]
    event_ids: Vec<Uuid>,

    /// Duplicate action: merge, prefer, or ignore.
    #[arg(long)]
    action: String,

    /// Preferred event UUID. Required when action is `prefer`.
    #[arg(long)]
    preferred_event_id: Option<Uuid>,

    /// Actor kind: user, operator, `deterministic_policy`, or `test_fixture`.
    #[arg(long, default_value = "operator")]
    actor_kind: String,

    /// Actor id. Defaults to the authenticated RPC actor at the gateway.
    #[arg(long)]
    actor_id: Option<String>,

    /// Human/operator comment.
    #[arg(long)]
    comment: Option<String>,
}

impl CurationDuplicateJudgeCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .curation_duplicate_judgments_record(CurationRecordDuplicateJudgmentRequest {
                source: self.source.clone(),
                event_type: self.event_type.clone(),
                equivalence_key: self.equivalence_key.clone(),
                event_ids: self.event_ids.clone(),
                action: parse_serde_enum("action", &self.action)?,
                preferred_event_id: self.preferred_event_id,
                actor_kind: parse_serde_enum("actor-kind", &self.actor_kind)?,
                actor_id: self.actor_id.clone(),
                comment: self.comment.clone(),
            })
            .await?;
        render_duplicate_judgment(&response, format)
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

fn render_duplicate_candidates(
    response: &CurationListDuplicateCandidatesResponse,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            println!("Duplicate candidate clusters: {}", response.clusters.len());
            for cluster in &response.clusters {
                println!(
                    "  {}  events={} materials={}",
                    cluster.cluster_id, cluster.event_count, cluster.material_count
                );
                for event in &cluster.events {
                    println!(
                        "    {}  material={}  ts={}",
                        event.event_id, event.source_material_id, event.ts_orig
                    );
                }
            }
        }
    }
    Ok(())
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

fn render_duplicate_judgment(
    response: &CurationRecordDuplicateJudgmentResponse,
    format: OutputFormat,
) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            let proposal_event_id = response
                .proposal_event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            let judgment_event_id = response
                .judgment_event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            println!("Duplicate judgment recorded");
            println!("  Proposal event: {proposal_event_id}");
            println!("  Judgment event: {judgment_event_id}");
            println!(
                "  Cluster:        {}",
                response
                    .proposal
                    .target_ref
                    .as_deref()
                    .unwrap_or("<missing-cluster>")
            );
            println!(
                "  Decision:       {}",
                format!("{:?}", response.judgment.decision).to_lowercase()
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

//! `sinexctl semantic curation` — proposal and judgment operator commands.

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
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};

use crate::client::GatewayClient;
use crate::commands::common::parse_serde_enum;
use crate::fmt::{format_json, format_yaml, print_finite_envelope};
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
        render_proposals(&response, &self.status, self.limit, format)
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
        render_duplicate_candidates(
            &response,
            CurationDuplicateQueryEcho {
                source: self.source.as_deref(),
                event_type: self.event_type.as_deref(),
                limit: self.limit,
                events_per_cluster: self.events_per_cluster,
            },
            format,
        )
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
    query: CurationDuplicateQueryEcho<'_>,
    format: OutputFormat,
) -> Result<()> {
    let envelope = curation_duplicates_envelope(response.clone(), query);
    if print_finite_envelope(&envelope, format)? {
        return Ok(());
    }

    match format {
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
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Yaml | OutputFormat::Dot => {}
    }
    Ok(())
}

fn render_finalization(response: &CurationFinalizeResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
            println!("{}", format_json(response)?)
        }
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            let event_id = response
                .event
                .id
                .as_ref()
                .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
            println!("Curation finalization recorded");
            println!("  Event:      {event_id}");
            println!("  Operation:  {}", response.operation.id);
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
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
            println!("{}", format_json(response)?)
        }
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

fn render_proposals(
    response: &EventQueryResult,
    status: &str,
    limit: i64,
    format: OutputFormat,
) -> Result<()> {
    let envelope = curation_proposals_envelope(response.clone(), status, limit);
    if print_finite_envelope(&envelope, format)? {
        return Ok(());
    }

    match format {
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
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Yaml | OutputFormat::Dot => {}
    }
    Ok(())
}

fn render_judgment(response: &CurationRecordJudgmentResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
            println!("{}", format_json(response)?)
        }
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

#[derive(Clone, Copy)]
struct CurationDuplicateQueryEcho<'a> {
    source: Option<&'a str>,
    event_type: Option<&'a str>,
    limit: i64,
    events_per_cluster: i64,
}

fn curation_proposals_envelope(
    response: EventQueryResult,
    status: &str,
    limit: i64,
) -> ViewEnvelope<EventQueryResult> {
    let mut envelope = ViewEnvelope::new("sinexctl.semantic.curation.proposals", response)
        .with_query_echo(serde_json::json!({
            "status": status,
            "limit": limit,
        }));
    envelope.caveats = curation_proposal_caveats(&envelope.payload, status);
    envelope
}

fn curation_proposal_caveats(response: &EventQueryResult, status: &str) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    match response {
        EventQueryResult::Events {
            events,
            next_cursor,
            ..
        } => {
            if events.is_empty() {
                caveats.push(curation_caveat(
                    ReadinessCaveatId::SourceAbsent,
                    format!(
                        "curation proposal query returned no {status} proposals; this does not prove there are no curatable observations"
                    ),
                    SinexObjectKind::Proposal,
                    format!("curation.proposals.{status}"),
                    "sinexctl semantic curation proposals",
                    "curation.proposals.list",
                ));
            }
            if next_cursor.is_some() {
                caveats.push(curation_caveat(
                    ReadinessCaveatId::WindowPartial,
                    "curation proposal query returned a pagination cursor; this is a partial proposal window",
                    SinexObjectKind::Proposal,
                    format!("curation.proposals.{status}"),
                    "sinexctl semantic curation proposals",
                    "curation.proposals.list",
                ));
            }
        }
        _ => caveats.push(curation_caveat(
            ReadinessCaveatId::CoverageUnmeasurable,
            "curation proposals returned a non-event projection; proposal coverage cannot be interpreted as a proposal list",
            SinexObjectKind::Proposal,
            format!("curation.proposals.{status}"),
            "sinexctl semantic curation proposals",
            "curation.proposals.list",
        )),
    }
    caveats
}

fn curation_duplicates_envelope(
    response: CurationListDuplicateCandidatesResponse,
    query: CurationDuplicateQueryEcho<'_>,
) -> ViewEnvelope<CurationListDuplicateCandidatesResponse> {
    let mut envelope =
        ViewEnvelope::new("sinexctl.semantic.curation.duplicates", response).with_query_echo(
            serde_json::json!({
                "source": query.source,
                "event_type": query.event_type,
                "limit": query.limit,
                "events_per_cluster": query.events_per_cluster,
            }),
        );
    envelope.caveats = curation_duplicate_caveats(&envelope.payload, query);
    envelope
}

fn curation_duplicate_caveats(
    response: &CurationListDuplicateCandidatesResponse,
    query: CurationDuplicateQueryEcho<'_>,
) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    if response.clusters.is_empty() {
        caveats.push(curation_caveat(
            ReadinessCaveatId::SourceAbsent,
            "duplicate-candidate query returned no clusters; this only proves the bounded candidate projection is empty",
            SinexObjectKind::Projection,
            "curation.duplicate_candidates",
            "sinexctl semantic curation duplicates",
            "curation.duplicate_candidates.list",
        ));
    }
    if query.limit > 0 && response.clusters.len() as i64 >= query.limit {
        caveats.push(curation_caveat(
            ReadinessCaveatId::WindowPartial,
            "duplicate-candidate query reached its cluster limit; additional candidate clusters may exist",
            SinexObjectKind::Projection,
            "curation.duplicate_candidates",
            "sinexctl semantic curation duplicates",
            "curation.duplicate_candidates.list",
        ));
    }
    if response.clusters.iter().any(|cluster| {
        cluster.event_count > cluster.events.len() as i64
            || (query.events_per_cluster > 0 && cluster.events.len() as i64 >= query.events_per_cluster)
    }) {
        caveats.push(curation_caveat(
            ReadinessCaveatId::WindowPartial,
            "at least one duplicate cluster has more events than this bounded response includes",
            SinexObjectKind::Projection,
            "curation.duplicate_candidates.events",
            "sinexctl semantic curation duplicates",
            "curation.duplicate_candidates.list",
        ));
    }
    caveats
}

fn curation_caveat(
    id: ReadinessCaveatId,
    message: impl Into<String>,
    kind: SinexObjectKind,
    ref_id: impl Into<String>,
    command_hint: &'static str,
    rpc_method: &'static str,
) -> CaveatView {
    let ref_id = ref_id.into();
    CaveatView {
        id: id.as_str().to_string(),
        message: message.into(),
        ref_: Some(
            SinexObjectRef::new(kind, ref_id.clone())
                .with_label(ref_id)
                .with_command_hint(command_hint)
                .with_rpc_method(rpc_method),
        ),
    }
}

#[cfg(test)]
#[path = "curation_test.rs"]
mod tests;

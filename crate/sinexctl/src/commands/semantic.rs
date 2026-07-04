//! `sinexctl semantic` - semantic epoch and shadow-lane operator commands.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sinex_primitives::rpc::semantic::{
    SemanticEpochCreateRequest, SemanticEpochListRequest, SemanticEpochListResponse,
    SemanticLaneCreateRequest, SemanticLaneDiffRecordEntityRelationRequest,
    SemanticLaneDiffsListRequest, SemanticLaneDiffsListResponse, SemanticLaneDiscardRequest,
    SemanticLaneListRequest, SemanticLaneListResponse, SemanticLaneOutputsListRequest,
    SemanticLaneOutputsListResponse, SemanticLaneOutputsSeedCanonicalGraphRequest,
    SemanticLaneOutputsSeedEntityEventsRequest, SemanticLaneOutputsWriteRequest,
    SemanticLaneSetStatusRequest,
};
use sinex_primitives::{EntityRelationLaneOutputs, SemanticComponentVersion, SemanticScope, Uuid};
use sinex_primitives::views::{
    CaveatView, ReadinessCaveatId, SinexObjectKind, SinexObjectRef, ViewEnvelope,
};
use std::path::{Path, PathBuf};

use crate::client::GatewayClient;
use crate::commands::common::parse_serde_enum;
use crate::commands::curation::CurationCommand;
use crate::commands::llm::LlmCommand;
use crate::fmt::{format_json, format_yaml, print_finite_envelope};
use crate::model::OutputFormat;
use crate::validation::parse_time_input;

#[derive(Debug, Args)]
pub struct SemanticCommand {
    #[command(subcommand)]
    cmd: SemanticSubcommand,
}

impl SemanticCommand {
    #[must_use]
    pub fn subcommand(&self) -> &SemanticSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            SemanticSubcommand::Epoch(cmd) => cmd.execute(client, format).await,
            SemanticSubcommand::Lane(cmd) => cmd.execute(client, format).await,
            SemanticSubcommand::Curation(cmd) => cmd.execute(client, format).await,
            SemanticSubcommand::Llm(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum SemanticSubcommand {
    /// Semantic epoch registry operations.
    Epoch(SemanticEpochCommand),
    /// Shadow-lane registry and inspection operations.
    Lane(SemanticLaneCommand),
    /// Curation proposal and judgment commands.
    Curation(CurationCommand),
    /// LLM prompt, routing, and budget read surfaces.
    Llm(LlmCommand),
}

#[derive(Debug, Args)]
pub struct SemanticEpochCommand {
    #[command(subcommand)]
    cmd: SemanticEpochSubcommand,
}

impl SemanticEpochCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            SemanticEpochSubcommand::Create(cmd) => cmd.execute(client, format).await,
            SemanticEpochSubcommand::List(cmd) => cmd.execute(client, format).await,
        }
    }

    #[must_use]
    pub fn subcommand(&self) -> &SemanticEpochSubcommand {
        &self.cmd
    }
}

#[derive(Debug, Subcommand)]
pub enum SemanticEpochSubcommand {
    /// Create a semantic epoch record.
    Create(SemanticEpochCreateCommand),
    /// List recent semantic epochs.
    List(SemanticEpochListCommand),
}

#[derive(Debug, Args)]
pub struct SemanticEpochCreateCommand {
    /// Epoch name.
    #[arg(long)]
    name: String,

    /// Scope kind, e.g. `source_material`, `event_set`, `document_chunk_set`.
    #[arg(long)]
    scope_kind: String,

    /// Scope input id. Repeat to declare the ordered resolved input set.
    #[arg(long = "input-id", required = true)]
    input_ids: Vec<String>,

    /// Hash of the resolved ordered input set.
    #[arg(long)]
    input_set_hash: String,

    /// Semantic configuration hash.
    #[arg(long)]
    config_hash: String,

    /// Optional code reference for the epoch implementation.
    #[arg(long)]
    code_ref: Option<String>,

    /// Component versions JSON array.
    #[arg(long)]
    components_json: Option<String>,

    /// Optional prompt-set hash.
    #[arg(long)]
    prompt_set_hash: Option<String>,

    /// Optional model-config hash.
    #[arg(long)]
    model_config_hash: Option<String>,
}

impl SemanticEpochCreateCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let components = parse_json_opt::<Vec<SemanticComponentVersion>>(
            "components-json",
            self.components_json.as_deref(),
        )?
        .unwrap_or_default();
        let response = client
            .semantic_epoch_create(SemanticEpochCreateRequest {
                epoch_id: None,
                name: self.name.clone(),
                scope: scope(&self.scope_kind, &self.input_ids, &self.input_set_hash),
                code_ref: self.code_ref.clone(),
                config_hash: self.config_hash.clone(),
                components,
                prompt_set_hash: self.prompt_set_hash.clone(),
                model_config_hash: self.model_config_hash.clone(),
                created_by: None,
                operation_id: None,
                supersedes_epoch_id: None,
            })
            .await?;
        render_value("Semantic epoch created", &response.epoch, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticEpochListCommand {
    /// Maximum records to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl SemanticEpochListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_epochs_list(SemanticEpochListRequest { limit: self.limit })
            .await?;
        let envelope = semantic_epoch_list_envelope(response, self.limit);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        render_values("Semantic epochs", &envelope.payload.epochs, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneCommand {
    #[command(subcommand)]
    cmd: SemanticLaneSubcommand,
}

impl SemanticLaneCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            SemanticLaneSubcommand::Create(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::List(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::Status(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::Discard(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::Outputs(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::SeedCanonicalGraph(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::SeedEntityEvents(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::WriteOutputs(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::Diffs(cmd) => cmd.execute(client, format).await,
            SemanticLaneSubcommand::Compare(cmd) => cmd.execute(client, format).await,
        }
    }

    #[must_use]
    pub fn subcommand(&self) -> &SemanticLaneSubcommand {
        &self.cmd
    }
}

#[derive(Debug, Subcommand)]
pub enum SemanticLaneSubcommand {
    /// Create a semantic shadow lane.
    Create(SemanticLaneCreateCommand),
    /// List semantic lanes.
    List(SemanticLaneListCommand),
    /// Set lane lifecycle status.
    Status(SemanticLaneStatusCommand),
    /// Discard a lane without promotion.
    Discard(SemanticLaneDiscardCommand),
    /// List lane outputs.
    Outputs(SemanticLaneOutputsCommand),
    /// Seed lane outputs from the current canonical entity/relation graph.
    SeedCanonicalGraph(SemanticLaneSeedCanonicalGraphCommand),
    /// Seed lane outputs from entity.resolved/entity.related events in the lane scope.
    SeedEntityEvents(SemanticLaneSeedEntityEventsCommand),
    /// Write entity/relation outputs into a lane.
    WriteOutputs(SemanticLaneWriteOutputsCommand),
    /// List recorded lane diffs.
    Diffs(SemanticLaneDiffsCommand),
    /// Compare two entity/relation lanes and record a diff.
    Compare(SemanticLaneCompareCommand),
}

#[derive(Debug, Args)]
pub struct SemanticLaneCreateCommand {
    /// Lane name.
    #[arg(long)]
    name: String,

    /// Lane kind: canonical, shadow, or experiment.
    #[arg(long, default_value = "shadow")]
    kind: String,

    /// Candidate epoch UUID.
    #[arg(long)]
    candidate_epoch_id: Uuid,

    /// Optional base epoch UUID.
    #[arg(long)]
    base_epoch_id: Option<Uuid>,

    /// Scope kind.
    #[arg(long)]
    scope_kind: String,

    /// Scope input id. Repeat to declare the ordered resolved input set.
    #[arg(long = "input-id", required = true)]
    input_ids: Vec<String>,

    /// Hash of the resolved ordered input set.
    #[arg(long)]
    input_set_hash: String,

    /// Operator-facing lane purpose.
    #[arg(long)]
    purpose: String,

    /// Optional expiration timestamp.
    #[arg(long)]
    expires_at: Option<String>,
}

impl SemanticLaneCreateCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_create(SemanticLaneCreateRequest {
                lane_id: None,
                name: self.name.clone(),
                kind: parse_serde_enum("kind", &self.kind)?,
                base_epoch_id: self.base_epoch_id,
                candidate_epoch_id: self.candidate_epoch_id,
                scope: scope(&self.scope_kind, &self.input_ids, &self.input_set_hash),
                purpose: self.purpose.clone(),
                operation_id: None,
                expires_at: self
                    .expires_at
                    .as_deref()
                    .map(parse_time_input)
                    .transpose()?,
            })
            .await?;
        render_value("Semantic lane created", &response.lane, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneListCommand {
    /// Optional lane status filter.
    #[arg(long)]
    status: Option<String>,

    /// Maximum records to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl SemanticLaneListCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let status = self
            .status
            .as_deref()
            .map(|raw| parse_serde_enum("status", raw))
            .transpose()?;
        let response = client
            .semantic_lanes_list(SemanticLaneListRequest {
                status,
                limit: self.limit,
            })
            .await?;
        let envelope = semantic_lane_list_envelope(response, self.status.as_deref(), self.limit);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        render_values("Semantic lanes", &envelope.payload.lanes, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneStatusCommand {
    /// Lane UUID.
    lane_id: Uuid,

    /// New status.
    #[arg(long)]
    status: String,
}

impl SemanticLaneStatusCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_set_status(SemanticLaneSetStatusRequest {
                lane_id: self.lane_id,
                status: parse_serde_enum("status", &self.status)?,
                completed_at: None,
            })
            .await?;
        render_value("Semantic lane status updated", &response.lane, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneDiscardCommand {
    /// Lane UUID.
    lane_id: Uuid,
}

impl SemanticLaneDiscardCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_discard(SemanticLaneDiscardRequest {
                lane_id: self.lane_id,
            })
            .await?;
        render_value(
            "Semantic lane discarded",
            &serde_json::to_value(response)?,
            format,
        )
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneOutputsCommand {
    /// Lane UUID.
    lane_id: Uuid,

    /// Maximum records to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl SemanticLaneOutputsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_outputs_list(SemanticLaneOutputsListRequest {
                lane_id: self.lane_id,
                limit: self.limit,
            })
            .await?;
        let envelope = semantic_lane_outputs_envelope(response, self.limit);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        render_values("Semantic lane outputs", &envelope.payload.outputs, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneSeedCanonicalGraphCommand {
    /// Lane UUID.
    lane_id: Uuid,
}

impl SemanticLaneSeedCanonicalGraphCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_outputs_seed_canonical_graph(
                SemanticLaneOutputsSeedCanonicalGraphRequest {
                    lane_id: self.lane_id,
                },
            )
            .await?;
        render_value(
            "Semantic lane seeded from canonical graph",
            &serde_json::to_value(response)?,
            format,
        )
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneSeedEntityEventsCommand {
    /// Lane UUID.
    lane_id: Uuid,
}

impl SemanticLaneSeedEntityEventsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_outputs_seed_entity_events(SemanticLaneOutputsSeedEntityEventsRequest {
                lane_id: self.lane_id,
            })
            .await?;
        render_value(
            "Semantic lane seeded from entity events",
            &serde_json::to_value(response)?,
            format,
        )
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneWriteOutputsCommand {
    /// Lane UUID.
    lane_id: Uuid,

    /// Entity/relation outputs JSON file.
    #[arg(long, conflicts_with = "outputs_json")]
    outputs_file: Option<PathBuf>,

    /// Entity/relation outputs JSON document.
    #[arg(long, conflicts_with = "outputs_file")]
    outputs_json: Option<String>,
}

impl SemanticLaneWriteOutputsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let outputs = read_outputs(self.outputs_file.as_deref(), self.outputs_json.as_deref())?;
        let response = client
            .semantic_lane_outputs_write(SemanticLaneOutputsWriteRequest {
                lane_id: self.lane_id,
                outputs,
            })
            .await?;
        render_value(
            "Semantic lane outputs written",
            &serde_json::to_value(response)?,
            format,
        )
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneDiffsCommand {
    /// Lane UUID.
    lane_id: Uuid,

    /// Maximum records to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl SemanticLaneDiffsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_diffs_list(SemanticLaneDiffsListRequest {
                lane_id: self.lane_id,
                limit: self.limit,
            })
            .await?;
        let envelope = semantic_lane_diffs_envelope(response, self.limit);
        if print_finite_envelope(&envelope, format)? {
            return Ok(());
        }
        render_values("Semantic lane diffs", &envelope.payload.diffs, format)
    }
}

#[derive(Debug, Args)]
pub struct SemanticLaneCompareCommand {
    /// Baseline lane UUID.
    #[arg(long)]
    baseline_lane_id: Uuid,

    /// Candidate lane UUID.
    #[arg(long)]
    candidate_lane_id: Uuid,

    /// Maximum representative examples to keep in the diff report.
    #[arg(long, default_value = "20")]
    max_examples: usize,

    /// Leave candidate lane status unchanged instead of marking it compared.
    #[arg(long)]
    keep_status: bool,
}

impl SemanticLaneCompareCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .semantic_lane_diff_record_entity_relation(
                SemanticLaneDiffRecordEntityRelationRequest {
                    diff_id: None,
                    baseline_lane_id: self.baseline_lane_id,
                    candidate_lane_id: self.candidate_lane_id,
                    max_examples: self.max_examples,
                    mark_candidate_compared: !self.keep_status,
                },
            )
            .await?;
        render_value(
            "Semantic lane diff recorded",
            &serde_json::to_value(response)?,
            format,
        )
    }
}

fn scope(kind: &str, input_ids: &[String], input_set_hash: &str) -> SemanticScope {
    SemanticScope {
        kind: kind.to_string(),
        input_ids: input_ids.to_vec(),
        input_set_hash: input_set_hash.to_string(),
    }
}

fn parse_json_opt<T: DeserializeOwned>(name: &str, raw: Option<&str>) -> Result<Option<T>> {
    raw.map(|value| serde_json::from_str(value).map_err(|error| eyre!("invalid --{name}: {error}")))
        .transpose()
}

fn read_outputs(
    outputs_file: Option<&Path>,
    outputs_json: Option<&str>,
) -> Result<EntityRelationLaneOutputs> {
    let raw = match (outputs_file, outputs_json) {
        (Some(path), None) => std::fs::read_to_string(path)
            .map_err(|error| eyre!("failed to read outputs file `{}`: {error}", path.display()))?,
        (None, Some(raw)) => raw.to_string(),
        (None, None) => {
            return Err(eyre!(
                "provide --outputs-file or --outputs-json for lane outputs"
            ));
        }
        (Some(_), Some(_)) => {
            return Err(eyre!(
                "provide only one of --outputs-file or --outputs-json"
            ));
        }
    };
    serde_json::from_str(&raw).map_err(|error| eyre!("invalid lane outputs JSON: {error}"))
}

fn render_value(label: &str, value: &Value, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
            println!("{}", format_json(value)?)
        }
        OutputFormat::Yaml => println!("{}", format_yaml(value)?),
        OutputFormat::Table => {
            println!("{label}");
            print_value_row(value);
        }
    }
    Ok(())
}

fn render_values(label: &str, values: &[Value], format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => {
            println!("{}", format_json(values)?)
        }
        OutputFormat::Yaml => println!("{}", format_yaml(values)?),
        OutputFormat::Table => {
            println!("{label}: {}", values.len());
            for value in values {
                print_value_row(value);
            }
        }
    }
    Ok(())
}

fn print_value_row(value: &Value) {
    let id = value
        .get("id")
        .or_else(|| value.get("lane_id"))
        .and_then(Value::as_str)
        .unwrap_or("<no-id>");
    let name = value
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| value.get("output_key").and_then(Value::as_str))
        .unwrap_or("");
    let status = value
        .get("status")
        .and_then(Value::as_str)
        .or_else(|| value.get("kind").and_then(Value::as_str))
        .or_else(|| value.get("diff_kind").and_then(Value::as_str))
        .unwrap_or("");
    println!("  {id}  {status:12}  {name}");
}

fn semantic_epoch_list_envelope(
    response: SemanticEpochListResponse,
    limit: i64,
) -> ViewEnvelope<SemanticEpochListResponse> {
    let mut envelope =
        ViewEnvelope::new("sinexctl.semantic.epoch.list", response).with_query_echo(
            serde_json::json!({
                "limit": limit,
            }),
        );
    envelope.caveats = semantic_list_caveats(
        envelope.payload.epochs.len(),
        limit,
        SemanticListCaveatSpec {
            empty_message:
                "semantic epoch registry returned no epochs; derivation regime coverage is absent",
            partial_message:
                "semantic epoch registry reached its limit; additional epochs may exist",
            ref_kind: SinexObjectKind::Projection,
            ref_id: "semantic.epochs",
            command_hint: "sinexctl semantic epoch list",
            rpc_method: "semantic.epochs.list",
        },
    );
    envelope
}

fn semantic_lane_list_envelope(
    response: SemanticLaneListResponse,
    status: Option<&str>,
    limit: i64,
) -> ViewEnvelope<SemanticLaneListResponse> {
    let mut envelope =
        ViewEnvelope::new("sinexctl.semantic.lane.list", response).with_query_echo(
            serde_json::json!({
                "status": status,
                "limit": limit,
            }),
        );
    envelope.caveats = semantic_list_caveats(
        envelope.payload.lanes.len(),
        limit,
        SemanticListCaveatSpec {
            empty_message: "semantic lane registry returned no lanes for this bounded query",
            partial_message:
                "semantic lane registry reached its limit; additional lanes may exist",
            ref_kind: SinexObjectKind::SemanticLane,
            ref_id: status.unwrap_or("semantic.lanes"),
            command_hint: "sinexctl semantic lane list",
            rpc_method: "semantic.lanes.list",
        },
    );
    envelope
}

fn semantic_lane_outputs_envelope(
    response: SemanticLaneOutputsListResponse,
    limit: i64,
) -> ViewEnvelope<SemanticLaneOutputsListResponse> {
    let lane_id = response.lane_id.to_string();
    let mut envelope =
        ViewEnvelope::new("sinexctl.semantic.lane.outputs", response).with_query_echo(
            serde_json::json!({
                "lane_id": lane_id,
                "limit": limit,
            }),
        );
    envelope.caveats = semantic_list_caveats(
        envelope.payload.outputs.len(),
        limit,
        SemanticListCaveatSpec {
            empty_message:
                "semantic lane output query returned no outputs; this lane has no inspectable derived records in the bounded view",
            partial_message:
                "semantic lane output query reached its limit; additional lane outputs may exist",
            ref_kind: SinexObjectKind::SemanticLane,
            ref_id: &lane_id,
            command_hint: "sinexctl semantic lane outputs",
            rpc_method: "semantic.lane.outputs.list",
        },
    );
    envelope
}

fn semantic_lane_diffs_envelope(
    response: SemanticLaneDiffsListResponse,
    limit: i64,
) -> ViewEnvelope<SemanticLaneDiffsListResponse> {
    let lane_id = response.lane_id.to_string();
    let mut envelope =
        ViewEnvelope::new("sinexctl.semantic.lane.diffs", response).with_query_echo(
            serde_json::json!({
                "lane_id": lane_id,
                "limit": limit,
            }),
        );
    envelope.caveats = semantic_list_caveats(
        envelope.payload.diffs.len(),
        limit,
        SemanticListCaveatSpec {
            empty_message:
                "semantic lane diff query returned no recorded diffs; lane comparison evidence is absent",
            partial_message:
                "semantic lane diff query reached its limit; additional lane diffs may exist",
            ref_kind: SinexObjectKind::SemanticLane,
            ref_id: &lane_id,
            command_hint: "sinexctl semantic lane diffs",
            rpc_method: "semantic.lane.diffs.list",
        },
    );
    envelope
}

struct SemanticListCaveatSpec<'a> {
    empty_message: &'static str,
    partial_message: &'static str,
    ref_kind: SinexObjectKind,
    ref_id: &'a str,
    command_hint: &'static str,
    rpc_method: &'static str,
}

fn semantic_list_caveats(
    observed_len: usize,
    limit: i64,
    spec: SemanticListCaveatSpec<'_>,
) -> Vec<CaveatView> {
    let mut caveats = Vec::new();
    if observed_len == 0 {
        caveats.push(semantic_caveat(
            ReadinessCaveatId::SourceAbsent,
            spec.empty_message,
            spec.ref_kind.clone(),
            spec.ref_id,
            spec.command_hint,
            spec.rpc_method,
        ));
    }
    if limit > 0 && observed_len as i64 >= limit {
        caveats.push(semantic_caveat(
            ReadinessCaveatId::WindowPartial,
            spec.partial_message,
            spec.ref_kind,
            spec.ref_id,
            spec.command_hint,
            spec.rpc_method,
        ));
    }
    caveats
}

fn semantic_caveat(
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
#[path = "semantic_test.rs"]
mod tests;

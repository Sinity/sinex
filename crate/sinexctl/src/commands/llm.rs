//! `sinexctl llm` — prompt/router/budget read surfaces.

use clap::{Args, Subcommand};
use color_eyre::Result;
use color_eyre::eyre::eyre;
use sinex_primitives::query::EventQueryResult;
use sinex_primitives::rpc::llm::{
    LlmBudgetReportRequest, LlmBudgetReportResponse, LlmPromptsListRequest, LlmRouteExplainRequest,
    LlmRouteExplainResponse,
};

use crate::client::GatewayClient;
use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;

#[derive(Debug, Args)]
pub struct LlmCommand {
    #[command(subcommand)]
    cmd: LlmSubcommand,
}

impl LlmCommand {
    #[must_use]
    pub fn subcommand(&self) -> &LlmSubcommand {
        &self.cmd
    }

    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match &self.cmd {
            LlmSubcommand::Prompts(cmd) => cmd.execute(client, format).await,
            LlmSubcommand::RouteExplain(cmd) => cmd.execute(client, format).await,
            LlmSubcommand::BudgetReport(cmd) => cmd.execute(client, format).await,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum LlmSubcommand {
    /// List prompt-template registry events.
    Prompts(LlmPromptsCommand),
    /// Explain a deterministic routing decision from request/policy JSON.
    RouteExplain(LlmRouteExplainCommand),
    /// Summarize recent budget-ledger events.
    BudgetReport(LlmBudgetReportCommand),
}

#[derive(Debug, Args)]
pub struct LlmPromptsCommand {
    /// Optional prompt status filter.
    #[arg(long)]
    status: Option<String>,

    /// Maximum prompt registry events to return.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl LlmPromptsCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .llm_prompts_list(LlmPromptsListRequest {
                status: self.status.clone(),
                limit: self.limit,
            })
            .await?;
        render_prompts(&response, format)
    }
}

#[derive(Debug, Args)]
pub struct LlmRouteExplainCommand {
    /// JSON `ModelTaskRequest`.
    #[arg(long = "request-json")]
    request_json: String,

    /// JSON `RoutingPolicyRecord`.
    #[arg(long = "policy-json")]
    policy_json: String,
}

impl LlmRouteExplainCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let request = serde_json::from_str(&self.request_json)
            .map_err(|error| eyre!("invalid --request-json: {error}"))?;
        let policy = serde_json::from_str(&self.policy_json)
            .map_err(|error| eyre!("invalid --policy-json: {error}"))?;
        let response = client
            .llm_route_explain(LlmRouteExplainRequest { request, policy })
            .await?;
        render_route_explain(&response, format)
    }
}

#[derive(Debug, Args)]
pub struct LlmBudgetReportCommand {
    /// Maximum budget ledger rows to read before summarizing.
    #[arg(long, default_value = "100")]
    limit: i64,
}

impl LlmBudgetReportCommand {
    async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        let response = client
            .llm_budget_report(LlmBudgetReportRequest { limit: self.limit })
            .await?;
        render_budget_report(&response, format)
    }
}

fn render_prompts(response: &EventQueryResult, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => match response {
            EventQueryResult::Events { events, .. } => {
                println!("LLM prompt templates: {}", events.len());
                for event in events {
                    let id = event
                        .event
                        .id
                        .as_ref()
                        .map_or_else(|| "<missing-id>".to_string(), ToString::to_string);
                    let prompt_id = event
                        .event
                        .payload
                        .get("prompt_id")
                        .and_then(|value| value.as_str())
                        .unwrap_or("<unknown-prompt>");
                    let version = event
                        .event
                        .payload
                        .get("version")
                        .and_then(|value| value.as_str())
                        .unwrap_or("<unknown-version>");
                    let status = event
                        .event
                        .payload
                        .get("status")
                        .and_then(|value| value.as_str())
                        .unwrap_or("<unknown-status>");
                    println!("  {id}  {status:8}  {prompt_id}@{version}");
                }
            }
            _ => println!("{}", format_json(response)?),
        },
    }
    Ok(())
}

fn render_route_explain(response: &LlmRouteExplainResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            println!("LLM route decision");
            println!("  Policy:   {}", response.decision.policy_id);
            println!(
                "  Prompt:   {}@{}",
                response.decision.prompt_id, response.decision.prompt_version
            );
            println!(
                "  Route:    {}/{}",
                response.decision.provider, response.decision.model
            );
            println!("  Reason:   {}", response.decision.decision_reason);
        }
    }
    Ok(())
}

fn render_budget_report(response: &LlmBudgetReportResponse, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json | OutputFormat::Ndjson | OutputFormat::Dot => println!("{}", format_json(response)?),
        OutputFormat::Yaml => println!("{}", format_yaml(response)?),
        OutputFormat::Table => {
            println!("LLM budget report");
            println!("  Rows:              {}", response.total_rows);
            println!("  Success:           {}", response.success_count);
            println!("  Failure:           {}", response.failure_count);
            println!("  Rejected:          {}", response.rejected_count);
            println!("  Prompt tokens:     {}", response.prompt_tokens);
            println!("  Completion tokens: {}", response.completion_tokens);
            println!("  Cost microUSD:     {}", response.cost_estimate_microusd);
            println!("  Runtime ms:        {}", response.runtime_ms);
        }
    }
    Ok(())
}

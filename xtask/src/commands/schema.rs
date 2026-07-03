//! Schema verification commands.

use color_eyre::eyre::{Context, Result, eyre};
use serde_json::json;
use sinex_db::schema::backfill::{
    PARSED_EVENT_COUNT_BACKFILL_KEY, ParsedEventCountBackfillOptions, list_backfill_runs,
    run_parsed_event_count_backfill,
};
use sinex_db::schema::strict_diff::{StrictDrift, check_strict};
use sqlx::postgres::PgPoolOptions;

use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::infra::stack::StackConfig;
use crate::output::StructuredError;
use crate::preflight;

/// Schema verification command group.
#[derive(Debug, Clone, clap::Args)]
pub struct SchemaCommand {
    #[command(subcommand)]
    pub subcommand: SchemaSubcommand,
}

/// Schema verification subcommands.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum SchemaSubcommand {
    /// Detect strict schema drift that declarative apply does not reconcile
    StrictDiff {
        /// Database URL to inspect. Without this, prepares the checkout-local stack first.
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Inspect or run explicit schema data backfills
    Backfill {
        #[command(subcommand)]
        subcommand: SchemaBackfillSubcommand,
    },
}

/// Explicit schema data backfill operations.
#[derive(Debug, Clone, clap::Subcommand)]
pub enum SchemaBackfillSubcommand {
    /// Report registered schema backfills and their persisted run state
    Status {
        /// Database URL to inspect. Without this, prepares the checkout-local stack first.
        #[arg(long)]
        database_url: Option<String>,
    },
    /// Run or resume a named schema backfill
    Run {
        /// Backfill key to run
        key: String,
        /// Database URL to inspect. Without this, prepares the checkout-local stack first.
        #[arg(long)]
        database_url: Option<String>,
        /// Rows to scan before persisting progress
        #[arg(long, default_value_t = 50_000)]
        batch_size: i64,
        /// Acknowledge that writers are quiesced for this first implementation slice
        #[arg(long)]
        assume_quiescent: bool,
        /// Clear persisted progress and recompute from the current event horizon
        #[arg(long)]
        restart: bool,
    },
}

impl XtaskCommand for SchemaCommand {
    fn name(&self) -> &'static str {
        "schema"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            SchemaSubcommand::StrictDiff { database_url } => {
                execute_strict_diff(database_url.as_deref(), ctx).await
            }
            SchemaSubcommand::Backfill { subcommand } => {
                execute_backfill(subcommand, ctx).await
            }
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
            .with_history_access(HistoryAccessMode::None)
            .with_history_tracking(false)
    }
}

async fn execute_backfill(
    subcommand: &SchemaBackfillSubcommand,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    match subcommand {
        SchemaBackfillSubcommand::Status { database_url } => {
            execute_backfill_status(database_url.as_deref(), ctx).await
        }
        SchemaBackfillSubcommand::Run {
            key,
            database_url,
            batch_size,
            assume_quiescent,
            restart,
        } => {
            execute_backfill_run(
                key,
                database_url.as_deref(),
                *batch_size,
                *assume_quiescent,
                *restart,
                ctx,
            )
            .await
        }
    }
}

async fn execute_backfill_status(
    database_url: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("schema backfill status");

    let database_url = resolve_strict_diff_database_url(database_url, ctx)?;
    let pool = connect_schema_pool(&database_url, "schema backfill status").await?;
    let runs = list_backfill_runs(&pool)
        .await
        .map_err(|error| eyre!("{error}"))
        .with_context(|| "schema backfill status failed")?;

    Ok(CommandResult::success()
        .with_message(format!("{} schema backfill(s) registered", runs.len()))
        .with_data(json!({ "backfills": runs }))
        .with_duration(ctx.elapsed()))
}

async fn execute_backfill_run(
    key: &str,
    database_url: Option<&str>,
    batch_size: i64,
    assume_quiescent: bool,
    restart: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("schema backfill run");

    if key != PARSED_EVENT_COUNT_BACKFILL_KEY {
        return Ok(CommandResult::failure(
            StructuredError::new(
                "UNKNOWN_SCHEMA_BACKFILL",
                format!("unknown schema backfill `{key}`"),
            )
            .with_suggestion(format!(
                "Run `xtask schema backfill status --format json` and choose `{PARSED_EVENT_COUNT_BACKFILL_KEY}`."
            )),
        )
        .with_duration(ctx.elapsed()));
    }

    if !assume_quiescent {
        return Ok(CommandResult::failure(
            StructuredError::new(
                "SCHEMA_BACKFILL_REQUIRES_QUIESCENCE",
                format!("schema backfill `{key}` requires quiescent writers"),
            )
            .with_suggestion(format!(
                "Stop or quiesce event writers, then rerun `xtask schema backfill run {key} --assume-quiescent`."
            )),
        )
        .with_duration(ctx.elapsed()));
    }

    let database_url = resolve_strict_diff_database_url(database_url, ctx)?;
    let pool = connect_schema_pool(&database_url, "schema backfill run").await?;
    let status = run_parsed_event_count_backfill(
        &pool,
        ParsedEventCountBackfillOptions {
            batch_size,
            assume_quiescent,
            restart,
            stop_after_chunks: None,
        },
    )
    .await
    .map_err(|error| eyre!("{error}"))
    .with_context(|| format!("schema backfill `{key}` failed"))?;

    Ok(CommandResult::success()
        .with_message(format!(
            "schema backfill `{}` is {} ({})",
            status.backfill_key, status.status, status.phase
        ))
        .with_data(json!({ "backfill": status }))
        .with_duration(ctx.elapsed()))
}

async fn execute_strict_diff(
    database_url: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("schema strict-diff");

    let database_url = resolve_strict_diff_database_url(database_url, ctx)?;

    let drifts = run_strict_diff(&database_url).await?;
    let drift_count = drifts.len();
    let data = json!({
        "drift_count": drift_count,
        "drifts": drifts,
    });

    if drift_count == 0 {
        return Ok(CommandResult::success()
            .with_message("No strict schema drift detected")
            .with_data(data)
            .with_duration(ctx.elapsed()));
    }

    Ok(CommandResult::failure(
        StructuredError::new(
            "STRICT_SCHEMA_DRIFT",
            format!("strict schema drift detected: {drift_count} finding(s)"),
        )
        .with_suggestion("Run schema apply against the intended database, then rerun xtask schema strict-diff. If drift remains, update strict_diff declarations or add an explicit migration/fixup."),
    )
    .with_details(drifts.iter().take(10).map(ToString::to_string))
    .with_data(data)
    .with_duration(ctx.elapsed()))
}

fn resolve_strict_diff_database_url(
    database_url: Option<&str>,
    ctx: &CommandContext,
) -> Result<String> {
    if let Some(database_url) = database_url {
        return Ok(database_url.to_owned());
    }

    let stage = ctx.start_stage("preflight");
    let ready = preflight::ensure_ready(ctx);
    ctx.finish_stage(stage, ready.is_ok());
    ready?;

    Ok(StackConfig::for_current_checkout()
        .wrap_err("failed to resolve checkout-local database URL after preflight")?
        .database_url())
}

pub(crate) async fn run_strict_diff(database_url: &str) -> Result<Vec<StrictDrift>> {
    let pool = connect_schema_pool(database_url, "strict schema diff").await?;

    check_strict(&pool)
        .await
        .map_err(|error| eyre!("{error}"))
        .with_context(|| "strict schema diff failed")
}

async fn connect_schema_pool(database_url: &str, purpose: &str) -> Result<sqlx::PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .with_context(|| format!("failed to connect to database for {purpose}"))?;
    Ok(pool)
}

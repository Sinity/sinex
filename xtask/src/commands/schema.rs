//! Schema verification commands.

use color_eyre::eyre::{Context, Result, eyre};
use serde_json::json;
use sinex_schema::strict_diff::{StrictDrift, check_strict};
use sqlx::postgres::PgPoolOptions;
use std::env;

use crate::command::{
    CommandContext, CommandMetadata, CommandResult, HistoryAccessMode, XtaskCommand,
};
use crate::output::StructuredError;

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
        /// Database URL to inspect. Defaults to DATABASE_URL.
        #[arg(long)]
        database_url: Option<String>,
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
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
            .with_history_access(HistoryAccessMode::None)
            .with_history_tracking(false)
    }
}

async fn execute_strict_diff(
    database_url: Option<&str>,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    ctx.heading("schema strict-diff");

    let database_url = database_url
        .map(str::to_owned)
        .or_else(|| env::var("DATABASE_URL").ok())
        .ok_or_else(|| eyre!("schema strict-diff requires --database-url or DATABASE_URL"))?;

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

pub(crate) async fn run_strict_diff(database_url: &str) -> Result<Vec<StrictDrift>> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .with_context(|| "failed to connect to database for strict schema diff")?;

    check_strict(&pool)
        .await
        .map_err(|error| eyre!("{error}"))
        .with_context(|| "strict schema diff failed")
}

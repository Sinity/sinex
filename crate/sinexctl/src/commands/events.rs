use clap::Subcommand;

use crate::Result;
use crate::client::GatewayClient;
use crate::commands::{
    AnnotateCommand, ContextCommand, ErrorsCommand, ExplainCommand, QueryCommand, RecentCommand,
    RelationsCommand, TimelineCommand, TraceCommand, WatchCommand,
};
use crate::model::OutputFormat;

/// Event search, inspection, lineage, streaming, and annotation commands.
#[derive(Debug, Subcommand)]
pub enum EventsCommand {
    /// Query/search events.
    Query(QueryCommand),

    /// Show recent events.
    Recent(RecentCommand),

    /// Show recent errors only.
    Errors(ErrorsCommand),

    /// Watch events in real-time.
    Watch(WatchCommand),

    /// Evaluate event relations over live events.
    Relations(RelationsCommand),

    /// Trace event provenance chain.
    Trace(TraceCommand),

    /// Inspect a single event with immediate provenance context.
    Inspect(ExplainCommand),

    /// Explain a single event with immediate provenance context.
    Explain(ExplainCommand),

    /// List recent events as a timeline.
    Timeline(TimelineCommand),

    /// Build a session-resumption context pack from recent activity.
    Context(ContextCommand),

    /// Annotate an event with a typed note.
    Annotate(AnnotateCommand),
}

impl EventsCommand {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::Query(cmd) => cmd.execute(client, format).await,
            Self::Recent(cmd) => cmd.execute(client, format).await,
            Self::Errors(cmd) => cmd.execute(client, format).await,
            Self::Watch(cmd) => cmd.execute(client, format).await,
            Self::Relations(cmd) => cmd.execute(client, format).await,
            Self::Trace(cmd) => cmd.execute(client, format).await,
            Self::Inspect(cmd) | Self::Explain(cmd) => cmd.execute(client, format).await,
            Self::Timeline(cmd) => cmd.execute(client, format).await,
            Self::Context(cmd) => cmd.execute(client, format).await,
            Self::Annotate(cmd) => cmd.execute(client, format).await,
        }
    }

    #[must_use]
    pub fn command_path(&self) -> &'static str {
        match self {
            Self::Query(_) => "events query",
            Self::Recent(_) => "events recent",
            Self::Errors(_) => "events errors",
            Self::Watch(_) => "events watch",
            Self::Relations(cmd) => cmd.command_path_with_root("events relations"),
            Self::Trace(_) => "events trace",
            Self::Inspect(_) => "events inspect",
            Self::Explain(_) => "events explain",
            Self::Timeline(_) => "events timeline",
            Self::Context(_) => "events context",
            Self::Annotate(_) => "events annotate",
        }
    }
}

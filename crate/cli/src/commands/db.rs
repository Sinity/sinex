//! Direct database commands for testing and debugging.
//!
//! These commands bypass the gateway and connect directly to the database,
//! useful for testing, debugging, and when the gateway is unavailable.

use clap::Subcommand;
use color_eyre::Result;
use serde::Serialize;
use sinex_db::create_pool;
use sinex_db::DbPool;
use std::env;

use crate::fmt::CommandOutput;
use crate::model::OutputFormat;

/// Database subcommands for direct DB access
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Check database health
    sinexctl db health

    # Query recent events
    sinexctl db query --limit 10

    # Query by source
    sinexctl db query --source fs-ingestor --limit 20

    # Show event statistics
    sinexctl db stats

    # Show stats by event type
    sinexctl db stats --by-type
")]
pub enum DbCommands {
    /// Check database health and connectivity
    Health,

    /// Query events directly from database
    Query {
        /// Filter by source
        #[arg(long)]
        source: Option<String>,

        /// Filter by event type
        #[arg(long, name = "type")]
        event_type: Option<String>,

        /// Maximum number of events to return
        #[arg(short, long, default_value = "10")]
        limit: i64,

        /// Output format
        #[arg(short = 'f', long, default_value = "table")]
        format: OutputFormat,
    },

    /// Show event statistics
    Stats {
        /// Group by event type instead of source
        #[arg(long)]
        by_type: bool,

        /// Output format
        #[arg(short = 'f', long, default_value = "table")]
        format: OutputFormat,
    },
}

impl DbCommands {
    pub async fn execute(&self) -> Result<()> {
        // Get database URL from environment
        let database_url = env::var("DATABASE_URL").map_err(|_| {
            color_eyre::eyre::eyre!(
                "DATABASE_URL not set. Set it in your environment or use the gateway commands instead."
            )
        })?;

        // Create connection pool
        let pool = create_pool(&database_url)
            .await
            .map_err(|e| color_eyre::eyre::eyre!("Failed to connect to database: {}", e))?;

        match self {
            Self::Health => db_health(&pool).await,
            Self::Query {
                source,
                event_type,
                limit,
                format,
            } => {
                db_query(
                    &pool,
                    source.as_deref(),
                    event_type.as_deref(),
                    *limit,
                    *format,
                )
                .await
            }
            Self::Stats { by_type, format } => db_stats(&pool, *by_type, *format).await,
        }
    }
}

#[derive(Debug, Serialize)]
struct DbHealthResult {
    connected: bool,
    database_name: String,
    event_count: i64,
    source_count: i64,
    oldest_event: Option<String>,
    newest_event: Option<String>,
}

async fn db_health(pool: &DbPool) -> Result<()> {
    // Check connection
    let db_name: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;

    // Get event count
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await?;

    // Get unique source count
    let source_count: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT source) FROM core.events")
        .fetch_one(pool)
        .await?;

    // Get time range
    let oldest: Option<String> = sqlx::query_scalar(
        "SELECT to_char(MIN(ts_ingest), 'YYYY-MM-DD HH24:MI:SS') FROM core.events",
    )
    .fetch_one(pool)
    .await?;

    let newest: Option<String> = sqlx::query_scalar(
        "SELECT to_char(MAX(ts_ingest), 'YYYY-MM-DD HH24:MI:SS') FROM core.events",
    )
    .fetch_one(pool)
    .await?;

    let result = DbHealthResult {
        connected: true,
        database_name: db_name,
        event_count,
        source_count,
        oldest_event: oldest,
        newest_event: newest,
    };

    // Format output
    println!("Database Health: ✓ connected");
    println!();
    println!("  Database: {}", result.database_name);
    println!("  Events: {}", result.event_count);
    println!("  Sources: {}", result.source_count);
    if let Some(oldest) = &result.oldest_event {
        println!("  Oldest event: {oldest}");
    }
    if let Some(newest) = &result.newest_event {
        println!("  Newest event: {newest}");
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct EventRow {
    id: String,
    source: String,
    event_type: String,
    host: String,
    ts_ingest: String,
}

async fn db_query(
    pool: &DbPool,
    source: Option<&str>,
    event_type: Option<&str>,
    limit: i64,
    format: OutputFormat,
) -> Result<()> {
    // Build query with optional filters
    let mut query = String::from(
        "SELECT id::text, source, event_type, host, to_char(ts_ingest, 'YYYY-MM-DD HH24:MI:SS') as ts_ingest
         FROM core.events WHERE 1=1",
    );

    if source.is_some() {
        query.push_str(" AND source = $1");
    }
    if event_type.is_some() {
        if source.is_some() {
            query.push_str(" AND event_type = $2");
        } else {
            query.push_str(" AND event_type = $1");
        }
    }
    query.push_str(" ORDER BY ts_ingest DESC LIMIT ");
    query.push_str(&limit.to_string());

    // Execute query based on parameters
    let events: Vec<EventRow> = match (source, event_type) {
        (Some(s), Some(t)) => sqlx::query_as::<_, (String, String, String, String, String)>(&query)
            .bind(s)
            .bind(t)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, source, event_type, host, ts_ingest)| EventRow {
                id,
                source,
                event_type,
                host,
                ts_ingest,
            })
            .collect(),
        (Some(s), None) => sqlx::query_as::<_, (String, String, String, String, String)>(&query)
            .bind(s)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, source, event_type, host, ts_ingest)| EventRow {
                id,
                source,
                event_type,
                host,
                ts_ingest,
            })
            .collect(),
        (None, Some(t)) => sqlx::query_as::<_, (String, String, String, String, String)>(&query)
            .bind(t)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, source, event_type, host, ts_ingest)| EventRow {
                id,
                source,
                event_type,
                host,
                ts_ingest,
            })
            .collect(),
        (None, None) => sqlx::query_as::<_, (String, String, String, String, String)>(&query)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, source, event_type, host, ts_ingest)| EventRow {
                id,
                source,
                event_type,
                host,
                ts_ingest,
            })
            .collect(),
    };

    if events.is_empty() {
        println!("No events found");
        return Ok(());
    }

    CommandOutput::list(events, "No events found", format_events_table).display(&format)?;
    Ok(())
}

fn format_events_table(events: &[EventRow]) -> String {
    let mut output = String::new();
    output.push_str(&format!("Found {} events:\n\n", events.len()));

    // Simple table format
    output.push_str(&format!(
        "{:<26} {:<20} {:<25} {:<15} {}\n",
        "ID", "SOURCE", "TYPE", "HOST", "RECORDED"
    ));
    output.push_str(&"-".repeat(100));
    output.push('\n');

    for event in events {
        output.push_str(&format!(
            "{:<26} {:<20} {:<25} {:<15} {}\n",
            event.id, event.source, event.event_type, event.host, event.ts_ingest
        ));
    }

    output
}

#[derive(Debug, Serialize)]
struct StatRow {
    name: String,
    count: i64,
}

async fn db_stats(pool: &DbPool, by_type: bool, format: OutputFormat) -> Result<()> {
    let query = if by_type {
        "SELECT event_type as name, COUNT(*) as count FROM core.events GROUP BY event_type ORDER BY count DESC"
    } else {
        "SELECT source as name, COUNT(*) as count FROM core.events GROUP BY source ORDER BY count DESC"
    };

    let stats: Vec<StatRow> = sqlx::query_as::<_, (String, i64)>(query)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(name, count)| StatRow { name, count })
        .collect();

    // Get total
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core.events")
        .fetch_one(pool)
        .await?;

    let label = if by_type { "Event Types" } else { "Sources" };

    if stats.is_empty() {
        println!("No events found");
        return Ok(());
    }

    CommandOutput::single(
        (label, total, stats),
        |(label, total, stats): &(&str, i64, Vec<StatRow>)| {
            let mut output = String::new();
            output.push_str(&format!("{label} (total: {total} events):\n\n"));

            output.push_str(&format!("{:<40} {:>10}\n", "NAME", "COUNT"));
            output.push_str(&"-".repeat(52));
            output.push('\n');

            for stat in stats {
                output.push_str(&format!("{:<40} {:>10}\n", stat.name, stat.count));
            }

            output
        },
    )
    .display(&format)?;

    Ok(())
}

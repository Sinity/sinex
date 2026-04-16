use clap::Args;
use color_eyre::Result;
use console::style;

use crate::client::GatewayClient;
use sinex_primitives::query::{AggregationMode, EventQuery, EventQueryResult, GroupByField, SortDirection};

#[derive(Debug, Args)]
pub struct VerifyCommand;

impl VerifyCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        println!();
        println!("{}", style("Sinex Trustworthiness Verification").bold().cyan());
        println!("{}", style("═".repeat(50)).dim());
        println!();

        let mut pass = 0u32;
        let mut warn = 0u32;
        let mut fail = 0u32;

        // 1. Event count sanity
        let total_events = count_events(client).await?;
        if total_events > 0 {
            println!("{} Event store has {} events", style("✓").green(), total_events);
            pass += 1;
        } else {
            println!("{} Event store is empty", style("⚠").yellow());
            warn += 1;
        }

        // 2. Source diversity
        let sources = count_sources(client).await?;
        if sources >= 2 {
            println!("{} {} distinct sources active", style("✓").green(), sources);
            pass += 1;
        } else if sources == 1 {
            println!("{} Only 1 source active", style("⚠").yellow());
            warn += 1;
        } else {
            println!("{} No sources producing events", style("✗").red());
            fail += 1;
        }

        // 3. Derived events present (automata working)
        let derived = count_derived_events(client).await?;
        if derived > 0 {
            println!("{} {} derived events (automata producing output)", style("✓").green(), derived);
            pass += 1;
        } else {
            println!("{} No derived events — automata may not be processing", style("⚠").yellow());
            warn += 1;
        }

        // 4. Gateway health
        match client.health().await {
            Ok(health) => {
                if health.healthy {
                    println!("{} Gateway healthy (DB: ok, NATS: ok)", style("✓").green());
                    pass += 1;
                } else {
                    println!("{} Gateway degraded: {}", style("⚠").yellow(),
                        health.degradation_reasons.join(", "));
                    warn += 1;
                }
            }
            Err(e) => {
                println!("{} Gateway health check failed: {}", style("✗").red(), e);
                fail += 1;
            }
        }

        // 5. Recent activity (events in last hour)
        let recent = count_recent_events(client).await?;
        if recent > 0 {
            println!("{} {} events in the last hour (pipeline flowing)", style("✓").green(), recent);
            pass += 1;
        } else {
            println!("{} No events in the last hour — pipeline may be stalled", style("⚠").yellow());
            warn += 1;
        }

        // Summary
        println!();
        println!("{}", style("─".repeat(50)).dim());
        println!(
            "  {} passed  {} warnings  {} failed",
            style(pass).green().bold(),
            style(warn).yellow().bold(),
            style(fail).red().bold(),
        );

        if fail > 0 {
            println!();
            println!("{}", style("Verification FAILED — investigate failures above").red().bold());
            std::process::exit(1);
        } else if warn > 0 {
            println!();
            println!("{}", style("Verification passed with warnings").yellow());
        } else {
            println!();
            println!("{}", style("All checks passed ✓").green().bold());
        }

        Ok(())
    }
}

async fn count_events(client: &GatewayClient) -> Result<i64> {
    let query = EventQuery {
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::Count { count } => Ok(count),
        _ => Ok(0),
    }
}

async fn count_sources(client: &GatewayClient) -> Result<i64> {
    let query = EventQuery {
        aggregation: Some(AggregationMode::CountBy {
            field: GroupByField::Source,
            limit: 100,
        }),
        direction: SortDirection::Desc,
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::GroupedCounts { groups } => Ok(groups.len() as i64),
        _ => Ok(0),
    }
}

async fn count_derived_events(client: &GatewayClient) -> Result<i64> {
    let query = EventQuery {
        event_types: vec![sinex_primitives::EventType::new("command.canonical")?],
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::Count { count } => Ok(count),
        _ => Ok(0),
    }
}

async fn count_recent_events(client: &GatewayClient) -> Result<i64> {
    let now = sinex_primitives::temporal::Timestamp::now();
    let one_hour_ago = sinex_primitives::temporal::Timestamp::new(
        now.inner() - time::Duration::hours(1),
    );
    let time_range = sinex_primitives::query::TimeRange::new(Some(one_hour_ago), Some(now))?;

    let query = EventQuery {
        time_range: Some(time_range),
        aggregation: Some(AggregationMode::Count),
        ..Default::default()
    };
    match client.query_events(query).await? {
        EventQueryResult::Count { count } => Ok(count),
        _ => Ok(0),
    }
}

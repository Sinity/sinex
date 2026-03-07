use clap::Args;
use color_eyre::Result;
use console::style;
use futures::StreamExt;
use sinex_primitives::query::{
    EventQuery, EventQueryResult, PayloadFilter, SortDirection, SubscriptionFilter, TimeRange,
};
use sinex_primitives::temporal::{Duration, Timestamp};

use crate::client::{GatewayClient, gateway::SseClientMessage};

/// Quick system status check
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Check system status
    sinexctl status

    # Pipe to jq for scripting
    sinexctl status -f json | jq '.nodes.active'
")]
pub struct StatusCommand;

impl StatusCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        println!("{}", style("System Status").bold().cyan());
        println!("{}", style("═".repeat(50)).dim());

        // Gateway connectivity
        match client.version().await {
            Ok(version) => {
                println!("Gateway: {} v{}", style("●").green(), version);
            }
            Err(e) => {
                println!(
                    "Gateway: {} {}",
                    style("●").red(),
                    style(format!("unreachable - {e}")).red()
                );
                return Ok(());
            }
        }

        // Nodes
        match client.list_nodes(None).await {
            Ok(nodes) => {
                let total = nodes.len();
                // Consider healthy if has heartbeat
                let healthy = nodes.iter().filter(|n| n.last_heartbeat.is_some()).count();
                let unhealthy = total - healthy;

                let status_color = if healthy == total {
                    style("●").green()
                } else if healthy > 0 {
                    style("●").yellow()
                } else {
                    style("●").red()
                };

                println!(
                    "Nodes:   {} {}/{} healthy{}",
                    status_color,
                    healthy,
                    total,
                    if unhealthy > 0 {
                        format!(", {unhealthy} unhealthy")
                    } else {
                        String::new()
                    }
                );

                // List nodes if there are issues
                if healthy != total {
                    for node in &nodes {
                        let has_heartbeat = node.last_heartbeat.is_some();
                        let icon = if has_heartbeat {
                            style("  ✓").green()
                        } else {
                            style("  ✗").red()
                        };
                        let name = node.hostname.as_deref().unwrap_or(&node.instance_id);
                        println!("{} {} ({})", icon, name, node.node_type);
                    }
                }
            }
            Err(e) => {
                println!(
                    "Nodes:   {} {}",
                    style("●").red(),
                    style(format!("error - {e}")).red()
                );
            }
        }

        // DLQ
        match client.dlq_list().await {
            Ok(stats) => {
                let status = if stats.total_messages == 0 {
                    style("●").green()
                } else {
                    style("●").yellow()
                };
                println!(
                    "DLQ:     {} {} messages",
                    status,
                    if stats.total_messages == 0 {
                        "0 ✓".to_string()
                    } else {
                        format!("{} ⚠", stats.total_messages)
                    }
                );
            }
            Err(e) => {
                println!(
                    "DLQ:     {} {}",
                    style("●").red(),
                    style(format!("error - {e}")).red()
                );
            }
        }

        // Recent events (quick count)
        let query = EventQuery {
            sources: vec![],
            event_types: vec![],
            time_range: TimeRange::new(Some(Timestamp::now() - Duration::hours(1)), None).ok(),
            payload: None,
            limit: 1000,
            direction: SortDirection::Desc,
            ..Default::default()
        };
        match client.query_events(query).await {
            Ok(EventQueryResult::Events { events, .. }) => {
                println!(
                    "Events:  {} {} in last hour",
                    style("●").green(),
                    events.len()
                );
            }
            Ok(_) => {
                println!(
                    "Events:  {} {}",
                    style("●").red(),
                    style("unexpected result type").red()
                );
            }
            Err(e) => {
                println!(
                    "Events:  {} {}",
                    style("●").red(),
                    style(format!("error - {e}")).red()
                );
            }
        }

        Ok(())
    }
}

/// Show recent events
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Last 20 events
    sinexctl recent

    # Last 50 events
    sinexctl recent -n 50

    # Last 100 events from terminal
    sinexctl recent -n 100 --source terminal-ingestor
")]
pub struct RecentCommand {
    /// Number of events to show
    #[arg(short = 'n', long, default_value = "20")]
    limit: i32,

    /// Time window (default: last hour)
    #[arg(long, short = 's', default_value = "1h")]
    since: String,

    /// Filter by source
    #[arg(long)]
    source: Option<String>,
}

impl RecentCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let since = parse_duration(&self.since)?;
        let query = EventQuery {
            sources: self
                .source
                .clone()
                .map(|s| vec![s.into()])
                .unwrap_or_default(),
            event_types: vec![],
            time_range: TimeRange::new(Some(Timestamp::now() - since), None).ok(),
            payload: None,
            limit: i64::from(self.limit),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let events = match client.query_events(query).await? {
            EventQueryResult::Events { events, .. } => events,
            _ => vec![],
        };

        if events.is_empty() {
            println!("No events found in the last {}", self.since);
            return Ok(());
        }

        println!(
            "{} events (last {})",
            style(events.len()).bold(),
            self.since
        );
        println!("{}", style("─".repeat(80)).dim());

        for result_event in &events {
            let timestamp = result_event
                .event
                .ts_orig.map_or_else(|| "unknown".to_string(), |ts| {
                    ts.format(time::macros::format_description!(
                        "[hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                });
            let source = style(result_event.event.source.as_str()).cyan();
            let event_type = style(result_event.event.event_type.as_str()).yellow();
            let snippet = result_event.snippet.as_deref().unwrap_or("");
            let snippet_display = if snippet.len() > 60 {
                format!("{}...", &snippet[..57])
            } else {
                snippet.to_string()
            };

            println!(
                "{} [{}] {} - {}",
                style(timestamp).dim(),
                source,
                event_type,
                snippet_display
            );
        }

        Ok(())
    }
}

/// Show recent errors only
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Recent errors
    sinexctl errors

    # Last 100 errors
    sinexctl errors -n 100
")]
pub struct ErrorsCommand {
    /// Number of errors to show
    #[arg(short = 'n', long, default_value = "50")]
    limit: i32,

    /// Time window
    #[arg(long, short = 's', default_value = "24h")]
    since: String,
}

impl ErrorsCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let since = parse_duration(&self.since)?;

        // Search for error-related events
        let query = EventQuery {
            sources: vec![],
            event_types: vec![],
            time_range: TimeRange::new(Some(Timestamp::now() - since), None).ok(),
            payload: Some(PayloadFilter::TextSearch {
                text: "error OR failed OR exception OR panic".to_string(),
            }),
            limit: i64::from(self.limit),
            direction: SortDirection::Desc,
            ..Default::default()
        };

        let events = match client.query_events(query).await? {
            EventQueryResult::Events { events, .. } => events,
            _ => vec![],
        };

        if events.is_empty() {
            println!(
                "{} No errors found in the last {}",
                style("✓").green(),
                self.since
            );
            return Ok(());
        }

        println!(
            "{} {} errors (last {})",
            style("⚠").yellow(),
            style(events.len()).bold(),
            self.since
        );
        println!("{}", style("─".repeat(80)).dim());

        for result_event in &events {
            let timestamp = result_event
                .event
                .ts_orig.map_or_else(|| "unknown".to_string(), |ts| {
                    ts.format(time::macros::format_description!(
                        "[year]-[month]-[day] [hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                });
            let source = style(result_event.event.source.as_str()).cyan();
            let event_type = style(result_event.event.event_type.as_str()).red();
            let snippet = result_event.snippet.as_deref().unwrap_or("");
            let snippet_display = if snippet.len() > 60 {
                format!("{}...", &snippet[..57])
            } else {
                snippet.to_string()
            };

            println!(
                "{} [{}] {} - {}",
                style(timestamp).dim(),
                source,
                event_type,
                snippet_display
            );
        }

        Ok(())
    }
}

/// Watch events in real-time
#[derive(Debug, Args)]
#[command(after_help = "\
EXAMPLES:
    # Watch all events
    sinexctl watch

    # Watch events from terminal ingestor
    sinexctl watch --source terminal-ingestor

    # Watch process execution events
    sinexctl watch --event-type process_exec
")]
pub struct WatchCommand {
    /// Filter by source
    #[arg(long)]
    source: Option<String>,

    /// Filter by event type
    #[arg(long)]
    event_type: Option<String>,
}

impl WatchCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let filter = SubscriptionFilter {
            sources: self
                .source
                .clone()
                .map(|s| vec![s.into()])
                .unwrap_or_default(),
            event_types: self
                .event_type
                .clone()
                .map(|t| vec![t.into()])
                .unwrap_or_default(),
            ..Default::default()
        };

        println!(
            "{}",
            style("Connecting to event stream... (Ctrl+C to stop)").dim()
        );

        let mut stream = client.subscribe_events(filter).await?;

        println!("{}", style("─".repeat(80)).dim());

        while let Some(result) = stream.next().await {
            match result {
                Ok(SseClientMessage::Event { event }) => {
                    let timestamp = event
                        .ts_orig.map_or_else(|| "unknown".to_string(), |ts| {
                            ts.format(time::macros::format_description!(
                                "[hour]:[minute]:[second]"
                            ))
                            .unwrap_or_else(|_| "invalid".to_string())
                        });
                    let source = style(event.source.as_str()).cyan();
                    let event_type = style(event.event_type.as_str()).yellow();

                    // Show a short payload summary
                    let summary = event
                        .payload
                        .as_object()
                        .and_then(|obj| {
                            obj.get("path")
                                .or(obj.get("command"))
                                .or(obj.get("title"))
                                .and_then(|v| v.as_str())
                        })
                        .unwrap_or("");
                    let summary_display = if summary.len() > 60 {
                        format!("{}...", &summary[..57])
                    } else {
                        summary.to_string()
                    };

                    println!(
                        "{} [{}] {} {}",
                        style(timestamp).dim(),
                        source,
                        event_type,
                        summary_display
                    );
                }
                Ok(SseClientMessage::Gap { dropped, .. }) => {
                    eprintln!(
                        "{}",
                        style(format!("⚠ {dropped} events dropped (slow consumer)")).yellow()
                    );
                }
                Ok(SseClientMessage::Heartbeat) => {
                    // Silent keepalive
                }
                Err(e) => {
                    eprintln!("{}", style(format!("Stream error: {e}")).red());
                    break;
                }
            }
        }

        println!("{}", style("Event stream ended.").dim());
        Ok(())
    }
}

/// Parse duration string like "1h", "2d", "30m"
fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(color_eyre::eyre::eyre!("Duration cannot be empty"));
    }

    let mut num_str = String::new();
    let mut unit = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
        } else {
            unit.push(ch);
        }
    }

    let num: i64 = num_str
        .parse()
        .map_err(|_| color_eyre::eyre::eyre!("Invalid duration number"))?;

    match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => Ok(Duration::seconds(num)),
        "m" | "min" | "minute" | "minutes" => Ok(Duration::minutes(num)),
        "h" | "hr" | "hour" | "hours" => Ok(Duration::hours(num)),
        "d" | "day" | "days" => Ok(Duration::days(num)),
        "w" | "week" | "weeks" => Ok(Duration::weeks(num)),
        _ => Err(color_eyre::eyre::eyre!("Unknown duration unit: {}", unit)),
    }
}

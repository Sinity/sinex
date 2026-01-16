use chrono::{Duration, Utc};
use clap::Args;
use color_eyre::Result;
use console::style;
use std::collections::HashSet;

use crate::client::GatewayClient;
use crate::model::nodes::NodeStatus;
use crate::model::search::SearchQuery;

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
                    style(format!("unreachable - {}", e)).red()
                );
                return Ok(());
            }
        }

        // Nodes
        match client.list_nodes(None).await {
            Ok(nodes) => {
                let total = nodes.len();
                let active = nodes
                    .iter()
                    .filter(|n| matches!(n.status, NodeStatus::Active))
                    .count();
                let draining = nodes
                    .iter()
                    .filter(|n| matches!(n.status, NodeStatus::Draining))
                    .count();
                let inactive = total - active - draining;

                let status_color = if active == total {
                    style("●").green()
                } else if active > 0 {
                    style("●").yellow()
                } else {
                    style("●").red()
                };

                println!(
                    "Nodes:   {} {}/{} active{}{}",
                    status_color,
                    active,
                    total,
                    if draining > 0 {
                        format!(", {} draining", draining)
                    } else {
                        String::new()
                    },
                    if inactive > 0 {
                        format!(", {} inactive", inactive)
                    } else {
                        String::new()
                    }
                );

                // List nodes if there are issues
                if active != total {
                    for node in &nodes {
                        let icon = match node.status {
                            NodeStatus::Active => style("  ✓").green(),
                            NodeStatus::Draining => style("  ◐").yellow(),
                            _ => style("  ✗").red(),
                        };
                        println!("{} {} ({})", icon, node.name, node.status);
                    }
                }
            }
            Err(e) => {
                println!(
                    "Nodes:   {} {}",
                    style("●").red(),
                    style(format!("error - {}", e)).red()
                );
            }
        }

        // DLQ
        match client.dlq_list().await {
            Ok(queues) => {
                let total_messages: u64 = queues.iter().map(|q| q.message_count).sum();
                let status = if total_messages == 0 {
                    style("●").green()
                } else {
                    style("●").yellow()
                };
                println!(
                    "DLQ:     {} {} messages",
                    status,
                    if total_messages == 0 {
                        "0 ✓".to_string()
                    } else {
                        format!("{} ⚠", total_messages)
                    }
                );

                if total_messages > 0 {
                    for q in &queues {
                        if q.message_count > 0 {
                            println!(
                                "  {} {} messages in {}",
                                style("⚠").yellow(),
                                q.message_count,
                                q.subject
                            );
                        }
                    }
                }
            }
            Err(e) => {
                println!(
                    "DLQ:     {} {}",
                    style("●").red(),
                    style(format!("error - {}", e)).red()
                );
            }
        }

        // Recent events (quick count)
        let query = SearchQuery {
            text: None,
            sources: vec![],
            event_types: vec![],
            start_time: Some(Utc::now() - Duration::hours(1)),
            end_time: None,
            limit: 1000,
            offset: 0,
        };
        match client.search_events(query).await {
            Ok(events) => {
                println!(
                    "Events:  {} {} in last hour",
                    style("●").green(),
                    events.len()
                );
            }
            Err(e) => {
                println!(
                    "Events:  {} {}",
                    style("●").red(),
                    style(format!("error - {}", e)).red()
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
    sinexctl recent -n 100 --source terminal-satellite
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
        let query = SearchQuery {
            text: None,
            sources: self.source.clone().map(|s| vec![s]).unwrap_or_default(),
            event_types: vec![],
            start_time: Some(Utc::now() - since),
            end_time: None,
            limit: self.limit,
            offset: 0,
        };

        let events = client.search_events(query).await?;

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

        for event in &events {
            let timestamp = event.timestamp.format("%H:%M:%S");
            let source = style(&event.source).cyan();
            let event_type = style(&event.event_type).yellow();
            let snippet = if event.snippet.len() > 60 {
                format!("{}...", &event.snippet[..57])
            } else {
                event.snippet.clone()
            };

            println!(
                "{} [{}] {} - {}",
                style(timestamp).dim(),
                source,
                event_type,
                snippet
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
        let query = SearchQuery {
            text: Some("error OR failed OR exception OR panic".to_string()),
            sources: vec![],
            event_types: vec![],
            start_time: Some(Utc::now() - since),
            end_time: None,
            limit: self.limit,
            offset: 0,
        };

        let events = client.search_events(query).await?;

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

        for event in &events {
            let timestamp = event.timestamp.format("%Y-%m-%d %H:%M:%S");
            let source = style(&event.source).cyan();
            let event_type = style(&event.event_type).red();
            let snippet = if event.snippet.len() > 60 {
                format!("{}...", &event.snippet[..57])
            } else {
                event.snippet.clone()
            };

            println!(
                "{} [{}] {} - {}",
                style(timestamp).dim(),
                source,
                event_type,
                snippet
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

    # Watch events from terminal satellite
    sinexctl watch --source terminal-satellite

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

    /// Poll interval in seconds
    #[arg(long, default_value = "2")]
    interval: u64,
}

impl WatchCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        let mut seen_ids: HashSet<String> = HashSet::new();
        let mut last_check = Utc::now() - Duration::minutes(1);

        println!("{}", style("Watching for events... (Ctrl+C to stop)").dim());
        println!("{}", style("─".repeat(80)).dim());

        loop {
            let query = SearchQuery {
                text: None,
                sources: self.source.clone().map(|s| vec![s]).unwrap_or_default(),
                event_types: self.event_type.clone().map(|t| vec![t]).unwrap_or_default(),
                start_time: Some(last_check),
                end_time: None,
                limit: 100,
                offset: 0,
            };

            match client.search_events(query).await {
                Ok(events) => {
                    for event in events {
                        if seen_ids.insert(event.event_id.to_string()) {
                            let timestamp = event.timestamp.format("%H:%M:%S");
                            let source = style(&event.source).cyan();
                            let event_type = style(&event.event_type).yellow();
                            let snippet = if event.snippet.len() > 60 {
                                format!("{}...", &event.snippet[..57])
                            } else {
                                event.snippet.clone()
                            };

                            println!(
                                "{} [{}] {} - {}",
                                style(timestamp).dim(),
                                source,
                                event_type,
                                snippet
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!("{}", style(format!("Error fetching events: {}", e)).red());
                }
            }

            last_check = Utc::now();
            tokio::time::sleep(std::time::Duration::from_secs(self.interval)).await;
        }
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

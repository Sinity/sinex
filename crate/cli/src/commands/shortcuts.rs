use crate::fmt::{format_json, format_yaml};
use crate::model::OutputFormat;
use clap::Args;
use color_eyre::Result;
use console::style;
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::query::{
    EventQuery, EventQueryResult, PayloadFilter, SortDirection, SubscriptionFilter, TimeRange,
};
use sinex_primitives::temporal::Timestamp;
use crate::parse::parse_duration;
use sinex_primitives::{RuntimeTargetDescriptor, RuntimeTargetKind};

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
    pub async fn execute(
        &self,
        client: &GatewayClient,
        runtime_target: Option<&RuntimeTargetDescriptor>,
        format: OutputFormat,
    ) -> Result<()> {

        use sinex_primitives::{RuntimeStatusSnapshot, RuntimeStatusSignal, RuntimeStatusSignalStatus, RuntimeStatusWarning};
        
        let target = runtime_target.cloned().unwrap_or_else(|| RuntimeTargetDescriptor {
            name: "unknown".to_string(),
            kind: RuntimeTargetKind::Unknown,
            ..Default::default()
        });
        
        let mut signals = Vec::new();
        let mut warnings = Vec::new();
        
        // Gateway connectivity
        let gateway_signal = match client.version().await {
            Ok(version) => RuntimeStatusSignal {
                name: "gateway".to_string(),
                status: RuntimeStatusSignalStatus::Healthy,
                source: "gateway version probe".to_string(),
                message: Some(format!("v{version}")),
            },
            Err(e) => {
                warnings.push(RuntimeStatusWarning {
                    source: "gateway".to_string(),
                    message: format!("unreachable: {e}"),
                });
                RuntimeStatusSignal {
                    name: "gateway".to_string(),
                    status: RuntimeStatusSignalStatus::Unhealthy,
                    source: "gateway version probe".to_string(),
                    message: Some(e.to_string()),
                }
            }
        };
        signals.push(gateway_signal);
        
        // Nodes
        match client.list_nodes(None).await {
            Ok(nodes) => {
                let total = nodes.len();
                let healthy = nodes.iter().filter(|n| n.last_heartbeat.is_some()).count();
                let status = if healthy == total {
                    RuntimeStatusSignalStatus::Healthy
                } else if healthy > 0 {
                    RuntimeStatusSignalStatus::Degraded
                } else {
                    RuntimeStatusSignalStatus::Unhealthy
                };
                signals.push(RuntimeStatusSignal {
                    name: "nodes".to_string(),
                    status,
                    source: "gateway nodes probe".to_string(),
                    message: Some(format!("{healthy}/{total} healthy")),
                });
            }
            Err(e) => {
                warnings.push(RuntimeStatusWarning {
                    source: "nodes".to_string(),
                    message: format!("error: {e}"),
                });
                signals.push(RuntimeStatusSignal {
                    name: "nodes".to_string(),
                    status: RuntimeStatusSignalStatus::Unknown,
                    source: "gateway nodes probe".to_string(),
                    message: Some(e.to_string()),
                });
            }
        }
        
        // DLQ
        match client.dlq_list().await {
            Ok(stats) => {
                let status = if stats.total_messages == 0 {
                    RuntimeStatusSignalStatus::Healthy
                } else {
                    RuntimeStatusSignalStatus::Degraded
                };
                signals.push(RuntimeStatusSignal {
                    name: "dlq".to_string(),
                    status,
                    source: "gateway dlq probe".to_string(),
                    message: Some(format!("{} messages", stats.total_messages)),
                });
            }
            Err(e) => {
                warnings.push(RuntimeStatusWarning {
                    source: "dlq".to_string(),
                    message: format!("error: {e}"),
                });
                signals.push(RuntimeStatusSignal {
                    name: "dlq".to_string(),
                    status: RuntimeStatusSignalStatus::Unknown,
                    source: "gateway dlq probe".to_string(),
                    message: Some(e.to_string()),
                });
            }
        }
        
        let snapshot = RuntimeStatusSnapshot {
            target,
            signals,
            warnings,
        };
        
        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                println!("{}", serde_json::to_string_pretty(&snapshot)?);
            }
            OutputFormat::Yaml => {
                println!("{}", serde_yml::to_string(&snapshot)?);
            }
            OutputFormat::Table => {
                println!("{}", style("System Status").bold().cyan());
                println!("{}", style("═".repeat(50)).dim());
                
                println!(
                    "Target:  {} {}",
                    style("●").cyan(),
                    style(format!(
                        "{} ({})",
                        snapshot.target.name,
                        runtime_target_kind_label(&snapshot.target.kind)
                    ))
                    .cyan()
                );
                if let Some(source) = &snapshot.target.source {
                    println!("         {}", style(format!("source: {source}")).dim());
                }
                if let Some(path) = &snapshot.target.source_path {
                    println!(
                        "         {}",
                        style(format!("descriptor: {}", path.display())).dim()
                    );
                }
                
                for signal in &snapshot.signals {
                    let color = match signal.status {
                        RuntimeStatusSignalStatus::Healthy => style("●").green(),
                        RuntimeStatusSignalStatus::Degraded => style("●").yellow(),
                        RuntimeStatusSignalStatus::Unhealthy => style("●").red(),
                        RuntimeStatusSignalStatus::Unknown => style("●").dim(),
                        RuntimeStatusSignalStatus::Skipped => style("●").dim(),
                        RuntimeStatusSignalStatus::Stale => style("●").yellow(),
                    };
                    
                    let name = format!("{:width$}", signal.name, width=8);
                    let message = signal.message.as_deref().unwrap_or("");
                    println!("{}: {} {}", name, color, message);
                }
                
                for warning in &snapshot.warnings {
                    println!("Warning [{}]: {}", warning.source, warning.message);
                }
            }
        }
        
        Ok(())
    }
}

fn runtime_target_kind_label(kind: &RuntimeTargetKind) -> &'static str {
    match kind {
        RuntimeTargetKind::Unknown => "unknown",
        RuntimeTargetKind::DevCheckout => "dev_checkout",
        RuntimeTargetKind::DeployedHost => "deployed_host",
        RuntimeTargetKind::Vm => "vm",
        RuntimeTargetKind::Test => "test",
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
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
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

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_json(&payload)?);
                return Ok(());
            }
            OutputFormat::Yaml => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_yaml(&payload)?);
                return Ok(());
            }
            OutputFormat::Table => {}
        }

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
            let timestamp = result_event.event.ts_orig.map_or_else(
                || "unknown".to_string(),
                |ts| {
                    ts.format(time::macros::format_description!(
                        "[hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                },
            );
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
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
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

        match format {
            OutputFormat::Json | OutputFormat::Dot => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_json(&payload)?);
                return Ok(());
            }
            OutputFormat::Yaml => {
                let payload = json!({
                    "since": self.since,
                    "count": events.len(),
                    "events": events,
                });
                println!("{}", format_yaml(&payload)?);
                return Ok(());
            }
            OutputFormat::Table => {}
        }

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
            let timestamp = result_event.event.ts_orig.map_or_else(
                || "unknown".to_string(),
                |ts| {
                    ts.format(time::macros::format_description!(
                        "[year]-[month]-[day] [hour]:[minute]:[second]"
                    ))
                    .unwrap_or_else(|_| "invalid".to_string())
                },
            );
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
    /// `--format json` emits one newline-delimited JSON object per stream
    /// message (`{"kind":"event"|"gap"|"error",...}`). `--format yaml` emits
    /// each message as a YAML document separated by `---`.
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
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

        let table_mode = matches!(format, OutputFormat::Table);

        if table_mode {
            println!(
                "{}",
                style("Connecting to event stream... (Ctrl+C to stop)").dim()
            );
        }

        let mut stream = client.subscribe_events(filter).await?;

        if table_mode {
            println!("{}", style("─".repeat(80)).dim());
        }

        while let Some(result) = stream.next().await {
            match result {
                Ok(SseClientMessage::Event { event }) => match format {
                    OutputFormat::Json | OutputFormat::Dot => {
                        let line = json!({ "kind": "event", "event": event });
                        println!("{}", serde_json::to_string(&line)?);
                    }
                    OutputFormat::Yaml => {
                        let doc = json!({ "kind": "event", "event": event });
                        println!("---");
                        print!("{}", format_yaml(&doc)?);
                    }
                    OutputFormat::Table => {
                        let timestamp = event.ts_orig.map_or_else(
                            || "unknown".to_string(),
                            |ts| {
                                ts.format(time::macros::format_description!(
                                    "[hour]:[minute]:[second]"
                                ))
                                .unwrap_or_else(|_| "invalid".to_string())
                            },
                        );
                        let source = style(event.source.as_str()).cyan();
                        let event_type = style(event.event_type.as_str()).yellow();

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
                },
                Ok(SseClientMessage::Gap { dropped, .. }) => match format {
                    OutputFormat::Json | OutputFormat::Dot => {
                        let line = json!({ "kind": "gap", "dropped": dropped });
                        println!("{}", serde_json::to_string(&line)?);
                    }
                    OutputFormat::Yaml => {
                        let doc = json!({ "kind": "gap", "dropped": dropped });
                        println!("---");
                        print!("{}", format_yaml(&doc)?);
                    }
                    OutputFormat::Table => {
                        eprintln!(
                            "{}",
                            style(format!("⚠ {dropped} events dropped (slow consumer)")).yellow()
                        );
                    }
                },
                Ok(SseClientMessage::Heartbeat) => {
                    // Silent keepalive in all formats.
                }
                Ok(SseClientMessage::Error { code, message }) => {
                    match format {
                        OutputFormat::Json | OutputFormat::Dot => {
                            let line =
                                json!({ "kind": "error", "code": code, "message": message });
                            println!("{}", serde_json::to_string(&line)?);
                        }
                        OutputFormat::Yaml => {
                            let doc =
                                json!({ "kind": "error", "code": code, "message": message });
                            println!("---");
                            print!("{}", format_yaml(&doc)?);
                        }
                        OutputFormat::Table => {
                            eprintln!(
                                "{}",
                                style(format!("Stream error [{code}]: {message}")).red()
                            );
                        }
                    }
                    break;
                }
                Err(e) => {
                    match format {
                        OutputFormat::Json | OutputFormat::Dot => {
                            let line = json!({ "kind": "error", "message": e.to_string() });
                            println!("{}", serde_json::to_string(&line)?);
                        }
                        OutputFormat::Yaml => {
                            let doc = json!({ "kind": "error", "message": e.to_string() });
                            println!("---");
                            print!("{}", format_yaml(&doc)?);
                        }
                        OutputFormat::Table => {
                            eprintln!("{}", style(format!("Stream error: {e}")).red());
                        }
                    }
                    break;
                }
            }
        }

        if table_mode {
            println!("{}", style("Event stream ended.").dim());
        }
        Ok(())
    }
}


//! CLI Commands for Metrics
//!
//! This module provides command-line interface commands for interacting with the metrics system.

use chrono::{Duration, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use crate::export::{export_json, export_prometheus};
use crate::storage::{MetricsError, MetricsStorage};

/// CLI command enumeration
#[derive(Debug, Clone)]
pub enum MetricsCommand {
    /// Export current metrics in Prometheus format
    ExportPrometheus,
    /// Export current metrics in JSON format
    ExportJson,
    /// Query stored metrics from database
    Query {
        metric_name: Option<String>,
        namespace: Option<String>,
        subsystem: Option<String>,
        hours_back: Option<i64>,
        limit: Option<i64>,
    },
    /// Show metrics aggregation
    Aggregate {
        metric_name: String,
        namespace: Option<String>,
        subsystem: Option<String>,
        hours_back: Option<i64>,
    },
    /// Initialize database schema
    InitSchema,
    /// Cleanup old metrics
    Cleanup { days_old: i64 },
    /// Show metrics statistics
    Stats,
}

/// CLI executor for metrics commands
pub struct MetricsCli {
    storage: Option<MetricsStorage>,
}

impl MetricsCli {
    pub fn new() -> Self {
        Self { storage: None }
    }

    pub fn with_storage(pool: PgPool) -> Self {
        Self {
            storage: Some(MetricsStorage::new(pool)),
        }
    }

    /// Execute a metrics command
    pub async fn execute(&self, command: MetricsCommand) -> Result<String, MetricsError> {
        match command {
            MetricsCommand::ExportPrometheus => Ok(export_prometheus()),
            MetricsCommand::ExportJson => {
                let json = export_json();
                Ok(serde_json::to_string_pretty(&json)?)
            }
            MetricsCommand::Query {
                metric_name,
                namespace,
                subsystem,
                hours_back,
                limit,
            } => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    MetricsError::Configuration("Database not configured for CLI".to_string())
                })?;

                let start_time = hours_back.map(|hours| Utc::now() - Duration::hours(hours));

                let entries = storage
                    .query_metrics(
                        metric_name.as_deref(),
                        namespace.as_deref(),
                        subsystem.as_deref(),
                        start_time,
                        None,
                        limit,
                    )
                    .await?;

                Ok(serde_json::to_string_pretty(&entries)?)
            }
            MetricsCommand::Aggregate {
                metric_name,
                namespace,
                subsystem,
                hours_back,
            } => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    MetricsError::Configuration("Database not configured for CLI".to_string())
                })?;

                let start_time = hours_back.map(|hours| Utc::now() - Duration::hours(hours));

                let aggregation = storage
                    .get_metrics_aggregation(
                        &metric_name,
                        namespace.as_deref(),
                        subsystem.as_deref(),
                        start_time,
                        None,
                    )
                    .await?;

                Ok(serde_json::to_string_pretty(&aggregation)?)
            }
            MetricsCommand::InitSchema => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    MetricsError::Configuration("Database not configured for CLI".to_string())
                })?;

                storage.init_schema().await?;
                Ok("Database schema initialized successfully".to_string())
            }
            MetricsCommand::Cleanup { days_old } => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    MetricsError::Configuration("Database not configured for CLI".to_string())
                })?;

                let cutoff_time = Utc::now() - Duration::days(days_old);
                let deleted_count = storage.cleanup_old_metrics(cutoff_time).await?;

                Ok(format!("Cleaned up {} old metrics entries", deleted_count))
            }
            MetricsCommand::Stats => {
                if let Some(storage) = &self.storage {
                    // Get some basic statistics from the database
                    let recent_entries = storage
                        .query_metrics(
                            None,
                            None,
                            None,
                            Some(Utc::now() - Duration::hours(24)),
                            None,
                            Some(100),
                        )
                        .await?;

                    let mut stats = HashMap::new();
                    stats.insert("recent_entries_24h".to_string(), recent_entries.len());

                    // Group by metric type
                    let mut type_counts = HashMap::new();
                    for entry in &recent_entries {
                        *type_counts.entry(entry.metric_type.clone()).or_insert(0) += 1;
                    }

                    Ok(format!(
                        "Metrics Statistics:\n{}\n\nType breakdown:\n{}",
                        serde_json::to_string_pretty(&stats)?,
                        serde_json::to_string_pretty(&type_counts)?
                    ))
                } else {
                    // Just show in-memory metrics stats
                    let prometheus = export_prometheus();
                    let line_count = prometheus.lines().count();
                    Ok(format!(
                        "In-memory metrics: {} lines in Prometheus export",
                        line_count
                    ))
                }
            }
        }
    }
}

impl Default for MetricsCli {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse command line arguments into a MetricsCommand
pub fn parse_metrics_command(args: &[String]) -> Result<MetricsCommand, MetricsError> {
    if args.is_empty() {
        return Err(MetricsError::Configuration(
            "No command provided".to_string(),
        ));
    }

    match args[0].as_str() {
        "export-prometheus" => Ok(MetricsCommand::ExportPrometheus),
        "export-json" => Ok(MetricsCommand::ExportJson),
        "query" => {
            let mut metric_name = None;
            let mut namespace = None;
            let mut subsystem = None;
            let mut hours_back = None;
            let mut limit = None;

            let mut i = 1;
            while i < args.len() {
                match args[i].as_str() {
                    "--metric" | "-m" => {
                        if i + 1 < args.len() {
                            metric_name = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--metric requires a value".to_string(),
                            ));
                        }
                    }
                    "--namespace" | "-n" => {
                        if i + 1 < args.len() {
                            namespace = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--namespace requires a value".to_string(),
                            ));
                        }
                    }
                    "--subsystem" | "-s" => {
                        if i + 1 < args.len() {
                            subsystem = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--subsystem requires a value".to_string(),
                            ));
                        }
                    }
                    "--hours" | "-h" => {
                        if i + 1 < args.len() {
                            hours_back = Some(args[i + 1].parse().map_err(|_| {
                                MetricsError::Configuration("Invalid hours value".to_string())
                            })?);
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--hours requires a value".to_string(),
                            ));
                        }
                    }
                    "--limit" | "-l" => {
                        if i + 1 < args.len() {
                            limit = Some(args[i + 1].parse().map_err(|_| {
                                MetricsError::Configuration("Invalid limit value".to_string())
                            })?);
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--limit requires a value".to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(MetricsError::Configuration(format!(
                            "Unknown query option: {}",
                            args[i]
                        )));
                    }
                }
            }

            Ok(MetricsCommand::Query {
                metric_name,
                namespace,
                subsystem,
                hours_back,
                limit,
            })
        }
        "aggregate" => {
            if args.len() < 2 {
                return Err(MetricsError::Configuration(
                    "aggregate requires a metric name".to_string(),
                ));
            }

            let metric_name = args[1].clone();
            let mut namespace = None;
            let mut subsystem = None;
            let mut hours_back = None;

            let mut i = 2;
            while i < args.len() {
                match args[i].as_str() {
                    "--namespace" | "-n" => {
                        if i + 1 < args.len() {
                            namespace = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--namespace requires a value".to_string(),
                            ));
                        }
                    }
                    "--subsystem" | "-s" => {
                        if i + 1 < args.len() {
                            subsystem = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--subsystem requires a value".to_string(),
                            ));
                        }
                    }
                    "--hours" | "-h" => {
                        if i + 1 < args.len() {
                            hours_back = Some(args[i + 1].parse().map_err(|_| {
                                MetricsError::Configuration("Invalid hours value".to_string())
                            })?);
                            i += 2;
                        } else {
                            return Err(MetricsError::Configuration(
                                "--hours requires a value".to_string(),
                            ));
                        }
                    }
                    _ => {
                        return Err(MetricsError::Configuration(format!(
                            "Unknown aggregate option: {}",
                            args[i]
                        )));
                    }
                }
            }

            Ok(MetricsCommand::Aggregate {
                metric_name,
                namespace,
                subsystem,
                hours_back,
            })
        }
        "init-schema" => Ok(MetricsCommand::InitSchema),
        "cleanup" => {
            if args.len() < 2 {
                return Err(MetricsError::Configuration(
                    "cleanup requires number of days".to_string(),
                ));
            }

            let days_old = args[1]
                .parse()
                .map_err(|_| MetricsError::Configuration("Invalid days value".to_string()))?;

            Ok(MetricsCommand::Cleanup { days_old })
        }
        "stats" => Ok(MetricsCommand::Stats),
        _ => Err(MetricsError::Configuration(format!(
            "Unknown command: {}",
            args[0]
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_export_commands() {
        let args = vec!["export-prometheus".to_string()];
        let cmd = parse_metrics_command(&args).unwrap();
        assert!(matches!(cmd, MetricsCommand::ExportPrometheus));

        let args = vec!["export-json".to_string()];
        let cmd = parse_metrics_command(&args).unwrap();
        assert!(matches!(cmd, MetricsCommand::ExportJson));
    }

    #[test]
    fn test_parse_query_command() {
        let args = vec![
            "query".to_string(),
            "--metric".to_string(),
            "test_counter".to_string(),
            "--hours".to_string(),
            "24".to_string(),
        ];
        let cmd = parse_metrics_command(&args).unwrap();
        if let MetricsCommand::Query {
            metric_name,
            hours_back,
            ..
        } = cmd
        {
            assert_eq!(metric_name, Some("test_counter".to_string()));
            assert_eq!(hours_back, Some(24));
        } else {
            panic!("Expected Query command");
        }
    }

    #[test]
    fn test_parse_aggregate_command() {
        let args = vec![
            "aggregate".to_string(),
            "cpu_usage".to_string(),
            "--namespace".to_string(),
            "sinex".to_string(),
        ];
        let cmd = parse_metrics_command(&args).unwrap();
        if let MetricsCommand::Aggregate {
            metric_name,
            namespace,
            ..
        } = cmd
        {
            assert_eq!(metric_name, "cpu_usage");
            assert_eq!(namespace, Some("sinex".to_string()));
        } else {
            panic!("Expected Aggregate command");
        }
    }
}

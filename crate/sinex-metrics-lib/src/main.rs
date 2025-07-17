//! Sinex Metrics CLI
//!
//! Command-line interface for managing and querying Sinex metrics.

use clap::{Arg, Command};
use sinex_metrics_lib::MetricsCli;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let app = Command::new("sinex-metrics")
        .version("0.4.2")
        .about("Sinex Metrics Management CLI")
        .subcommand(
            Command::new("export-prometheus")
                .about("Export current in-memory metrics in Prometheus format"),
        )
        .subcommand(
            Command::new("export-json").about("Export current in-memory metrics in JSON format"),
        )
        .subcommand(
            Command::new("query")
                .about("Query stored metrics from database")
                .arg(
                    Arg::new("metric")
                        .long("metric")
                        .short('m')
                        .value_name("METRIC_NAME")
                        .help("Specific metric name to query"),
                )
                .arg(
                    Arg::new("namespace")
                        .long("namespace")
                        .short('n')
                        .value_name("NAMESPACE")
                        .help("Filter by namespace (default: sinex)"),
                )
                .arg(
                    Arg::new("subsystem")
                        .long("subsystem")
                        .short('s')
                        .value_name("SUBSYSTEM")
                        .help("Filter by subsystem"),
                )
                .arg(
                    Arg::new("hours")
                        .long("hours")
                        .short('h')
                        .value_name("HOURS")
                        .help("Number of hours back to query")
                        .value_parser(clap::value_parser!(i64)),
                )
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .short('l')
                        .value_name("LIMIT")
                        .help("Maximum number of results")
                        .value_parser(clap::value_parser!(i64)),
                ),
        )
        .subcommand(
            Command::new("aggregate")
                .about("Show aggregated metrics (sum, avg, min, max)")
                .arg(
                    Arg::new("metric")
                        .value_name("METRIC_NAME")
                        .help("Metric name to aggregate")
                        .required(true),
                )
                .arg(
                    Arg::new("namespace")
                        .long("namespace")
                        .short('n')
                        .value_name("NAMESPACE")
                        .help("Filter by namespace"),
                )
                .arg(
                    Arg::new("subsystem")
                        .long("subsystem")
                        .short('s')
                        .value_name("SUBSYSTEM")
                        .help("Filter by subsystem"),
                )
                .arg(
                    Arg::new("hours")
                        .long("hours")
                        .short('h')
                        .value_name("HOURS")
                        .help("Number of hours back to aggregate")
                        .value_parser(clap::value_parser!(i64)),
                ),
        )
        .subcommand(
            Command::new("init-schema").about("Initialize the database schema for metrics storage"),
        )
        .subcommand(
            Command::new("cleanup")
                .about("Remove old metrics data")
                .arg(
                    Arg::new("days")
                        .value_name("DAYS")
                        .help("Remove data older than this many days")
                        .required(true)
                        .value_parser(clap::value_parser!(i64)),
                ),
        )
        .subcommand(Command::new("stats").about("Show metrics statistics and summary"));

    let matches = app.get_matches();

    // Create CLI instance, with database connection if available
    let cli = if let Ok(database_url) = env::var("DATABASE_URL") {
        let pool = sqlx::PgPool::connect(&database_url).await?;
        MetricsCli::with_storage(pool)
    } else {
        eprintln!("Warning: DATABASE_URL not set, database operations will not be available");
        MetricsCli::new()
    };

    // Convert clap matches to our command format
    let result = match matches.subcommand() {
        Some(("export-prometheus", _)) => {
            cli.execute(sinex_metrics_lib::MetricsCommand::ExportPrometheus)
                .await
        }
        Some(("export-json", _)) => {
            cli.execute(sinex_metrics_lib::MetricsCommand::ExportJson)
                .await
        }
        Some(("query", sub_matches)) => {
            let command = sinex_metrics_lib::MetricsCommand::Query {
                metric_name: sub_matches.get_one::<String>("metric").cloned(),
                namespace: sub_matches.get_one::<String>("namespace").cloned(),
                subsystem: sub_matches.get_one::<String>("subsystem").cloned(),
                hours_back: sub_matches.get_one::<i64>("hours").copied(),
                limit: sub_matches.get_one::<i64>("limit").copied(),
            };
            cli.execute(command).await
        }
        Some(("aggregate", sub_matches)) => {
            let metric_name = sub_matches.get_one::<String>("metric").unwrap().clone();
            let command = sinex_metrics_lib::MetricsCommand::Aggregate {
                metric_name,
                namespace: sub_matches.get_one::<String>("namespace").cloned(),
                subsystem: sub_matches.get_one::<String>("subsystem").cloned(),
                hours_back: sub_matches.get_one::<i64>("hours").copied(),
            };
            cli.execute(command).await
        }
        Some(("init-schema", _)) => {
            cli.execute(sinex_metrics_lib::MetricsCommand::InitSchema)
                .await
        }
        Some(("cleanup", sub_matches)) => {
            let days_old = *sub_matches.get_one::<i64>("days").unwrap();
            let command = sinex_metrics_lib::MetricsCommand::Cleanup { days_old };
            cli.execute(command).await
        }
        Some(("stats", _)) => cli.execute(sinex_metrics_lib::MetricsCommand::Stats).await,
        _ => {
            eprintln!("No subcommand specified. Use --help for usage information.");
            std::process::exit(1);
        }
    };

    match result {
        Ok(output) => {
            println!("{}", output);
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}

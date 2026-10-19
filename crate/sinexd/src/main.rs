//! `sinexd`: one context built in `main`; admission and the writer task; the
//! commit-log reader; the confirmed-stream publisher; the NATS ingress
//! consumer; the automaton driver; replay; the API (architecture §7.4).
mod analysis;
mod api;
mod backfill;
mod catalog;
mod commits;
mod context;
mod crawl;
mod daemon;
mod derive;
mod ingress;
mod keys;
mod library;
mod media_text;
mod operate;
mod publisher;
mod replay;
mod runner;
mod writer;

#[cfg(test)]
mod testdb;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "sinexd", about = "The Sinex daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the daemon.
    Run {
        #[arg(long)]
        database: String,
        /// Content store directory.
        #[arg(long)]
        store: PathBuf,
        /// NATS server URL, with the daemon's credentials when required.
        #[arg(long)]
        nats: String,
        /// Subject root on the NATS server.
        #[arg(long, default_value = "sinex")]
        subjects: String,
        #[arg(long, default_value = "localhost")]
        host: String,
        /// The API's Unix socket; default `$XDG_RUNTIME_DIR/sinex/sinexd.sock`,
        /// else `/run/sinex/sinexd.sock`.
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Also serve the API on this loopback address, e.g. 127.0.0.1:7171.
        #[arg(long)]
        listen: Option<std::net::SocketAddr>,
        /// Directory holding `read.token`, `agent.token` and `operator.token`.
        #[arg(long)]
        tokens: PathBuf,
        /// The zone clock times and days are read in; default the system's.
        #[arg(long)]
        zone: Option<String>,
        /// When a logical day starts.
        #[arg(long, default_value = "04:00")]
        day_start: String,
        /// The sources to read continuously (the NixOS module writes
        /// `/etc/sinex/sources.json`); none when unset.
        #[arg(long)]
        sources: Option<PathBuf>,
    },
    /// Publish every committed event not yet on the confirmed stream, then
    /// stop and report the rate. For first publication and measurement; the
    /// running daemon does the same continuously.
    Publish {
        #[arg(long)]
        database: String,
        #[arg(long)]
        nats: String,
        #[arg(long, default_value = "sinex")]
        subjects: String,
    },
    /// Capture a live system source into its spool (the daemon runs the same
    /// capture as a task).
    Capture {
        #[command(subcommand)]
        source: Capture,
    },
    /// Administration that needs the database but not the running daemon.
    Admin {
        #[command(subcommand)]
        command: Admin,
    },
    /// Read sources once to the end, through the writer, and report counts and
    /// rates. For first imports and measurement.
    Backfill {
        #[arg(long)]
        database: String,
        /// Content store directory.
        #[arg(long)]
        store: PathBuf,
        /// A source as `kind=path`; repeatable. The kinds are every
        /// registered source's (`sinexd admin catalog` lists them): exports
        /// and directories (a file, or a directory of them), databases, and
        /// live sources' spools.
        #[arg(long = "source")]
        sources: Vec<String>,
        /// The zone sources with local times were written in (IANA name);
        /// the system zone when unset. Recorded with each binding.
        #[arg(long)]
        zone: Option<String>,
        /// A source option as `key=value` (a clipboard spool's
        /// `selection=primary`); repeatable.
        #[arg(long = "option")]
        options: Vec<String>,
        #[arg(long, default_value = "localhost")]
        host: String,
        /// One JSON object: totals per source and peak memory.
        #[arg(long)]
        json: bool,
    },
    /// Run every derivation until nothing is dirty, through the writer, and
    /// report counts and rates. The daemon's driver, run to completion,
    /// including the maintained metric values.
    Derive {
        #[arg(long)]
        database: String,
        /// Content store directory, for derivations that read stored bytes
        /// (screen text); without it they count their inputs as failed.
        #[arg(long)]
        store: Option<PathBuf>,
        #[arg(long, default_value = "localhost")]
        host: String,
        /// One JSON object: counts per derivation and peak memory.
        #[arg(long)]
        json: bool,
    },
    /// The analysis layer: metric panels, recipes, results and briefs
    /// (architecture §6.5).
    Analysis {
        #[command(subcommand)]
        command: AnalysisCommand,
    },
}

#[derive(clap::Args)]
struct AnalysisAt {
    #[arg(long)]
    database: String,
    /// The zone logical days are read in.
    #[arg(long, default_value = "Europe/Warsaw")]
    zone: String,
    /// When a logical day starts.
    #[arg(long, default_value = "04:00")]
    day_start: String,
}

#[derive(Subcommand)]
enum AnalysisCommand {
    /// The shipped metrics, methods and recipes.
    Catalog,
    /// Metric cells per logical day, each with its observation state.
    Panel {
        #[command(flatten)]
        at: AnalysisAt,
        /// First logical day (YYYY-MM-DD).
        #[arg(long)]
        from: String,
        /// Last logical day, included.
        #[arg(long)]
        to: String,
        /// Only these metrics; repeat for more. Default: all.
        #[arg(long = "metric")]
        metrics: Vec<String>,
        /// Break down by this dimension (host, app, repository, unit).
        #[arg(long)]
        by: Option<String>,
        #[arg(long)]
        json: bool,
        /// Also store the cells as the maintained metric values.
        #[arg(long)]
        keep: bool,
    },
    /// Run a recipe over a range of logical days and store its result.
    Run {
        #[command(flatten)]
        at: AnalysisAt,
        recipe: analysis::Recipe,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        /// Record the run as the standing question's newest answer, which
        /// supersedes the previous one.
        #[arg(long)]
        standing: bool,
        /// Admit partly observed days covering at least this fraction.
        #[arg(long)]
        min_observed_fraction: Option<f64>,
        #[arg(long)]
        json: bool,
    },
    /// A stored result: its status, readsets, tables and findings.
    Show {
        #[command(flatten)]
        at: AnalysisAt,
        result: uuid::Uuid,
        #[arg(long)]
        json: bool,
    },
    /// The newest stored results.
    Results {
        #[command(flatten)]
        at: AnalysisAt,
        #[arg(long, default_value_t = 20)]
        limit: i64,
    },
    /// Why a metric's cell for a day is missing, partial or observed.
    WhyNot {
        #[command(flatten)]
        at: AnalysisAt,
        metric: String,
        day: String,
    },
    /// The brief for one logical day: data quality, the day, departures,
    /// associations, the machine.
    Brief {
        #[command(flatten)]
        at: AnalysisAt,
        /// Default: today.
        #[arg(long)]
        day: Option<String>,
        /// Bytes of findings; whole findings are dropped to fit.
        #[arg(long, default_value_t = 4000)]
        budget: usize,
        #[arg(long)]
        json: bool,
    },
    /// Recompute the maintained metric values of dirty and recent days.
    Maintain {
        #[command(flatten)]
        at: AnalysisAt,
        #[arg(long, default_value_t = 2)]
        recent_days: i64,
    },
}

#[derive(Subcommand)]
enum Capture {
    /// The systemd journal through `journalctl -o json`, resuming after the
    /// last spooled cursor.
    Journald {
        #[arg(long)]
        spool: PathBuf,
        /// On a first capture, start here (a `journalctl --since` value, such
        /// as `-1d`); the whole journal otherwise.
        #[arg(long)]
        since: Option<String>,
        /// Keep following new entries.
        #[arg(long)]
        follow: bool,
    },
}

#[derive(Subcommand)]
enum Admin {
    /// Create the schema in an empty database, or confirm it is current.
    Schema {
        #[arg(long)]
        database: String,
    },
    /// Print every source and derivation as JSON, for the NixOS module's
    /// generated options (`nix/catalog.json`).
    Catalog,
    /// Sequence gaps in the Sinnix capture lake's lanes: numbers between the
    /// first and last seen with no record. Facts about Sinnix's capture, not
    /// errors here.
    SequenceGaps {
        #[arg(long)]
        database: String,
    },
    /// Print the NATS server `authorization` block: the daemon user with full
    /// access and one user per producer limited to its own subjects. Passwords
    /// are read from files, so they never pass through the Nix store.
    NatsAuthorization {
        #[arg(long, default_value = "sinex")]
        subjects: String,
        #[arg(long, default_value = "sinexd")]
        daemon_user: String,
        #[arg(long)]
        daemon_password_file: PathBuf,
        /// `name=password-file`; repeatable.
        #[arg(long = "producer")]
        producers: Vec<String>,
    },
    /// Any command of the command table (`replay`, `hide`, `excise`,
    /// `recompute`, `library-add`, `crawl`, …), run locally against the
    /// database and content store without the daemon, with the operator's
    /// access: `sinexd admin <command> [arguments] --database URL --store DIR`.
    /// It is the same code the daemon's API runs.
    #[command(external_subcommand)]
    Table(Vec<String>),
}

async fn admin(command: Admin) -> Result<(), String> {
    match command {
        Admin::Table(args) => admin_table(args).await,
        Admin::Schema { database } => sinex_schema::apply_url(&database)
            .await
            .map(|applied| println!("{applied}"))
            .map_err(|e| e.to_string()),
        Admin::Catalog => {
            print!("{}", catalog::render());
            Ok(())
        }
        Admin::SequenceGaps { database } => sequence_gaps(&database).await,
        Admin::NatsAuthorization {
            subjects,
            daemon_user,
            daemon_password_file,
            producers,
        } => {
            let secret = |path: &std::path::Path| {
                std::fs::read_to_string(path)
                    .map(|s| s.trim_end().to_owned())
                    .map_err(|e| format!("{}: {e}", path.display()))
            };
            let daemon_password = secret(&daemon_password_file)?;
            let mut users = Vec::new();
            for producer in producers {
                let (name, file) = producer
                    .split_once('=')
                    .ok_or_else(|| format!("--producer {producer}: expected name=password-file"))?;
                if !sinex_protocol::nats::valid_producer(name) {
                    return Err(format!("--producer {name}: not a valid producer name"));
                }
                users.push((name.to_owned(), secret(std::path::Path::new(file))?));
            }
            let users: Vec<(&str, &str)> = users
                .iter()
                .map(|(n, p)| (n.as_str(), p.as_str()))
                .collect();
            println!(
                "{}",
                sinex_protocol::Subjects::new(subjects)
                    .authorization((&daemon_user, &daemon_password), &users)
            );
            Ok(())
        }
    }
}

/// Run one command-table command locally: the daemon's API, built over a
/// local writer, called with the operator's access.
async fn admin_table(args: Vec<String>) -> Result<(), String> {
    use clap::{Arg, value_parser};
    let name = args.first().ok_or("admin needs a command")?;
    let spec = sinex_protocol::command(name).ok_or_else(|| {
        let names: Vec<&str> = sinex_protocol::COMMANDS.iter().map(|c| c.name).collect();
        format!("no command `{name}`; commands: {}", names.join(", "))
    })?;
    let matches = sinex_protocol::cli::subcommand(spec)
        .arg(Arg::new("database").long("database").required(true))
        .arg(
            Arg::new("store")
                .long("store")
                .required(true)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(Arg::new("host").long("host").default_value("localhost"))
        .arg(Arg::new("zone").long("zone"))
        .arg(
            Arg::new("day-start")
                .long("day-start")
                .default_value("04:00"),
        )
        .try_get_matches_from(&args)
        .map_err(|e| e.to_string())?;
    let request =
        sinex_protocol::Request::from_args(name, sinex_protocol::cli::arguments(spec, &matches))?;
    let database: &String = matches.get_one("database").expect("required");
    let store: &PathBuf = matches.get_one("store").expect("required");
    let host: &String = matches.get_one("host").expect("defaulted");
    let zone = match matches.get_one::<String>("zone") {
        Some(name) => jiff::tz::TimeZone::get(name).map_err(|e| format!("zone {name}: {e}"))?,
        None => jiff::tz::TimeZone::system(),
    };
    let day_start: &String = matches.get_one("day-start").expect("defaulted");
    let day_start = sinex_model::query::time::clock(day_start)
        .ok_or_else(|| format!("day start `{day_start}` is not a clock time like 04:00"))?;
    let ctx = context::Context::open(database, store).await?;
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let local = api::Api {
        reads,
        writer: Some(ctx.writer.clone()),
        blobs: ctx.blobs.clone(),
        zone,
        day_start,
        agent_grants: Vec::new(),
        unlock: Default::default(),
        host: host.clone(),
    };
    let reply = local.call(request, sinex_protocol::Access::Operator).await;
    drop(local);
    ctx.close().await?;
    match reply {
        sinex_protocol::Reply::Ok { result } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).expect("JSON values print")
            );
            Ok(())
        }
        sinex_protocol::Reply::Refused { reason, .. } => Err(reason),
    }
}

/// A writer and a derivation driver for a one-shot command, with the content
/// store when `store` names one.
async fn local_driver(
    database: &str,
    host: &str,
    store: Option<&std::path::Path>,
) -> Result<(derive::Driver, tokio::task::JoinHandle<()>), String> {
    use sqlx::Connection;
    sinex_schema::apply_url(database)
        .await
        .map_err(|e| e.to_string())?;
    let writer_conn = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let (handle, queues) = writer::channels();
    let task = tokio::spawn(writer::run(
        database.to_owned(),
        writer_conn,
        queues,
        std::sync::Arc::new(keys::Registered::new(sinex_automata::keyed())),
    ));
    let read = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let mut driver = derive::Driver::start(
        read,
        handle,
        sinex_automata::keyed(),
        derive::per_event(),
        host,
    )
    .await?;
    if let Some(store) = store {
        driver =
            driver.with_blobs(sinex_store::blobs::Blobs::open(store).map_err(|e| e.to_string())?);
    }
    Ok((driver, task))
}

async fn run_derive(
    database: String,
    store: Option<PathBuf>,
    host: String,
    json: bool,
) -> Result<(), String> {
    let (driver, writer_task) = local_driver(&database, &host, store.as_deref()).await?;
    let mut driver = driver.with_maintained(vec![Box::new(analysis::MetricValues::new(
        analysis::Settings::operator_default(),
        2,
        std::time::Duration::ZERO,
    ))]);
    let started = std::time::Instant::now();
    driver.settle().await?;
    let wall = started.elapsed();
    let outputs: usize = driver.counts.values().map(|c| c.outputs).sum();
    let inputs: usize = driver.counts.values().map(|c| c.inputs).sum();
    let rate = outputs as f64 / wall.as_secs_f64().max(1e-9);
    if json {
        let per: serde_json::Map<String, serde_json::Value> = driver
            .counts
            .iter()
            .map(|(name, c)| (name.to_string(), c.to_json()))
            .collect();
        println!(
            "{}",
            serde_json::json!({"derivations": per, "wall_s": wall.as_secs_f64(),
                "inputs": inputs, "outputs": outputs, "outputs_per_s": rate,
                "peak_rss_mb": peak_rss_mb()})
        );
    } else {
        for (name, counts) in &driver.counts {
            println!("{name:18} {counts}");
        }
        println!(
            "wall {:.2}s | inputs read {inputs} | outputs {outputs} | {rate:.0} outputs/s",
            wall.as_secs_f64(),
        );
    }
    driver.end_runs().await?;
    drop(driver);
    writer_task.await.map_err(|e| e.to_string())
}

/// This process's peak resident memory (VmHWM), in MB.
pub(crate) fn peak_rss_mb() -> Option<f64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches("kB").trim().parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
}

async fn publish(database: &str, nats: &str, subjects: &str) -> Result<(), String> {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let client = sinex_sources::nats::connect(nats).await?;
    let js = publisher::context(client);
    let report = publisher::drain(&mut conn, &js, &sinex_protocol::Subjects::new(subjects)).await?;
    println!("{report}");
    Ok(())
}

/// The socket the daemon serves and `sinexctl` calls by default.
fn default_socket() -> PathBuf {
    std::env::var_os("XDG_RUNTIME_DIR")
        .map(|d| PathBuf::from(d).join("sinex/sinexd.sock"))
        .unwrap_or_else(|| PathBuf::from("/run/sinex/sinexd.sock"))
}

fn api_options(
    socket: Option<PathBuf>,
    listen: Option<std::net::SocketAddr>,
    tokens: PathBuf,
    zone: Option<String>,
    day_start: &str,
) -> Result<daemon::ApiOptions, String> {
    let zone = match zone {
        Some(name) => jiff::tz::TimeZone::get(&name).map_err(|e| format!("zone {name}: {e}"))?,
        None => jiff::tz::TimeZone::system(),
    };
    let day_start = sinex_model::query::time::clock(day_start)
        .ok_or_else(|| format!("day start `{day_start}` is not a clock time like 04:00"))?;
    Ok(daemon::ApiOptions {
        socket: socket.unwrap_or_else(default_socket),
        listen,
        tokens,
        zone,
        day_start,
    })
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Run {
            database,
            store,
            nats,
            subjects,
            host,
            socket,
            listen,
            tokens,
            zone,
            day_start,
            sources,
        } => match api_options(socket, listen, tokens, zone, &day_start) {
            Ok(api) => {
                daemon::run(daemon::Options {
                    database,
                    store,
                    nats,
                    subjects,
                    host,
                    api,
                    sources,
                })
                .await
            }
            Err(e) => Err(e),
        },
        Command::Publish {
            database,
            nats,
            subjects,
        } => publish(&database, &nats, &subjects).await,
        Command::Capture {
            source:
                Capture::Journald {
                    spool,
                    since,
                    follow,
                },
        } => {
            let start = match &since {
                Some(s) => sinex_sources::sources::journald::Start::Since(s),
                None => sinex_sources::sources::journald::Start::Beginning,
            };
            sinex_sources::sources::journald::capture(&spool, follow, start)
                .map(|n| println!("captured {n} journal entries"))
                .map_err(|e| e.to_string())
        }
        Command::Admin { command } => admin(command).await,
        Command::Backfill {
            database,
            store,
            sources,
            zone,
            options,
            host,
            json,
        } => {
            backfill::run(backfill::Options {
                database,
                store,
                sources,
                zone,
                options,
                host,
                json,
            })
            .await
        }
        Command::Derive {
            database,
            store,
            host,
            json,
        } => run_derive(database, store, host, json).await,
        Command::Analysis { command } => run_analysis(command).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sinexd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn analysis_open(
    at: &AnalysisAt,
) -> Result<(sqlx::PgConnection, analysis::Settings), String> {
    use sqlx::Connection;
    let settings = analysis::Settings::new(&at.zone, &at.day_start)?;
    let conn = sqlx::PgConnection::connect(&at.database)
        .await
        .map_err(|e| e.to_string())?;
    Ok((conn, settings))
}

fn metric_defs(ids: &[String]) -> Result<Vec<&'static sinex_analysis::MetricDef>, String> {
    if ids.is_empty() {
        return Ok(sinex_analysis::catalog::METRICS.iter().collect());
    }
    ids.iter()
        .map(|id| {
            sinex_analysis::catalog::metric(id).ok_or_else(|| {
                let known: Vec<&str> = sinex_analysis::catalog::METRICS
                    .iter()
                    .map(|m| m.id)
                    .collect();
                format!("no metric {id}; known: {}", known.join(", "))
            })
        })
        .collect()
}

fn print_json(v: &impl serde::Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(v).map_err(|e| e.to_string())?
    );
    Ok(())
}

async fn run_analysis(command: AnalysisCommand) -> Result<(), String> {
    match command {
        AnalysisCommand::Catalog => {
            println!("metrics:");
            for m in sinex_analysis::catalog::METRICS {
                println!(
                    "  {:<26} v{} {:<15} {} [{}]",
                    m.id,
                    m.version,
                    m.relation,
                    m.description,
                    m.opportunity.sources().join(", ")
                );
            }
            println!("methods:");
            for (id, version, summary) in sinex_analysis::methods::catalog() {
                println!("  {id:<20} v{version} {summary}");
            }
            println!("recipes: correlations, trends, deviations");
            Ok(())
        }
        AnalysisCommand::Panel {
            at,
            from,
            to,
            metrics,
            by,
            json,
            keep,
        } => {
            let (mut conn, settings) = analysis_open(&at).await?;
            let defs = metric_defs(&metrics)?;
            let (from, to) = (analysis::parse_day(&from)?, analysis::parse_day(&to)?);
            let as_of = sinex_store::analysis::head(&mut conn)
                .await
                .map_err(|e| e.to_string())?;
            let sources: Vec<&str> = {
                let mut s: Vec<&str> = defs
                    .iter()
                    .flat_map(|d| d.opportunity.sources().iter().copied())
                    .collect();
                s.sort_unstable();
                s.dedup();
                s
            };
            let started = std::time::Instant::now();
            let opp = analysis::opportunity(&mut conn, &sources, as_of).await?;
            let start = settings.civil.day_start(from).map_err(|e| e.to_string())?;
            let end = settings.civil.day_end(to).map_err(|e| e.to_string())?;
            let range = sinex_analysis::Span::new(
                i64::try_from(start.unix_ns() / 1000).unwrap_or(0),
                i64::try_from(end.unix_ns() / 1000).unwrap_or(0),
            );
            let p = analysis::panel(&mut conn, &settings, &defs, range, as_of, &opp).await?;
            if keep {
                analysis::keep(&mut conn, &settings, &p, as_of).await?;
            }
            if json {
                print_json(
                    &serde_json::json!({"as_of": as_of, "day_setting": settings.label, "opportunity": opp, "cells": p.cells}),
                )
            } else {
                print!("{}", analysis::table(&p, &defs, by.as_deref()));
                eprintln!(
                    "as of commit {as_of}; {} cells in {:.2}s{}",
                    p.cells.len(),
                    started.elapsed().as_secs_f64(),
                    if keep { "; stored" } else { "" }
                );
                Ok(())
            }
        }
        AnalysisCommand::Run {
            at,
            recipe,
            from,
            to,
            standing,
            min_observed_fraction,
            json,
        } => {
            let (mut conn, settings) = analysis_open(&at).await?;
            let eligibility = sinex_analysis::Eligibility {
                min_observed_fraction,
                admit_recorded: false,
            };
            let started = std::time::Instant::now();
            let run = analysis::run(
                &mut conn,
                &settings,
                recipe,
                analysis::parse_day(&from)?,
                analysis::parse_day(&to)?,
                eligibility,
                standing,
            )
            .await?;
            if json {
                return print_json(&serde_json::json!({
                    "result": run.result.id, "state": run.result.state, "reused": run.reused,
                    "findings": run.findings, "tables": run.tables,
                }));
            }
            println!(
                "result {} ({}{}) in {:.2}s",
                run.result.id,
                run.result.state,
                if run.reused {
                    ", an identical request's stored answer"
                } else {
                    ""
                },
                started.elapsed().as_secs_f64()
            );
            if run.findings.is_empty() {
                println!("no findings");
            }
            for f in &run.findings {
                println!("- {}", f.markdown());
            }
            Ok(())
        }
        AnalysisCommand::Show { at, result, json } => {
            let (mut conn, _) = analysis_open(&at).await?;
            let view = sinex_store::analysis::result(&mut conn, result)
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("no result {result}"))?;
            if json {
                return print_json(&serde_json::json!({
                    "id": view.id, "recipe": view.recipe, "request": view.request, "as_of": view.as_of,
                    "status": view.status, "reason": view.reason, "state": view.state,
                    "state_reason": view.state_reason, "readsets": view.readsets, "tables": view.tables,
                    "diagnostics": view.diagnostics, "findings": view.findings,
                }));
            }
            println!(
                "{} {} as of commit {}: {} ({}{})",
                view.id,
                view.recipe,
                view.as_of,
                view.status,
                view.state,
                view.state_reason
                    .map(|r| format!(": {r}"))
                    .unwrap_or_default()
            );
            let readsets = view.readsets.as_array().cloned().unwrap_or_default();
            for r in &readsets {
                println!(
                    "  read {} rows, digest {}",
                    r["rows"],
                    r["digest"].as_str().unwrap_or_default()
                );
            }
            for f in view.findings.iter().filter_map(analysis::stored_finding) {
                println!("- {}", f.markdown());
            }
            Ok(())
        }
        AnalysisCommand::Results { at, limit } => {
            let (mut conn, _) = analysis_open(&at).await?;
            for (id, recipe, status, state, at) in
                sinex_store::analysis::recent_results(&mut conn, None, limit)
                    .await
                    .map_err(|e| e.to_string())?
            {
                println!("{id} {at} {recipe:<14} {status:<14} {state}");
            }
            Ok(())
        }
        AnalysisCommand::WhyNot { at, metric, day } => {
            let (mut conn, settings) = analysis_open(&at).await?;
            let def = metric_defs(&[metric])?[0];
            print!(
                "{}",
                analysis::why_not(&mut conn, &settings, def, analysis::parse_day(&day)?).await?
            );
            Ok(())
        }
        AnalysisCommand::Brief {
            at,
            day,
            budget,
            json,
        } => {
            let (mut conn, settings) = analysis_open(&at).await?;
            let day = match day {
                Some(d) => analysis::parse_day(&d)?,
                None => settings
                    .civil
                    .day(sinex_model::Timestamp::now())
                    .map_err(|e| e.to_string())?,
            };
            let p = analysis::brief(&mut conn, &settings, day, budget).await?;
            if json {
                print_json(&p)
            } else {
                print!("{}", p.markdown);
                Ok(())
            }
        }
        AnalysisCommand::Maintain { at, recent_days } => {
            let (mut conn, settings) = analysis_open(&at).await?;
            let started = std::time::Instant::now();
            let n = analysis::maintain(&mut conn, &settings, recent_days).await?;
            println!(
                "{n} day(s) recomputed in {:.2}s",
                started.elapsed().as_secs_f64()
            );
            Ok(())
        }
    }
}

async fn sequence_gaps(database: &str) -> Result<(), String> {
    use sqlx::Connection;
    let mut c = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let gaps = sqlx::query!(
        r#"SELECT payload->>'lane' AS "lane!", payload->>'host' AS "host!",
                  count(DISTINCT (payload->>'seq')::bigint) AS "seen!",
                  min((payload->>'seq')::bigint) AS "first!",
                  max((payload->>'seq')::bigint) AS "last!"
           FROM core.events WHERE event_type = 'capture.observed'
           GROUP BY 1, 2 ORDER BY 1, 2"#
    )
    .fetch_all(&mut c)
    .await
    .map_err(|e| e.to_string())?;
    for g in gaps {
        println!(
            "{}@{}: {} seen in {}..={}, {} numbers without a record",
            g.lane,
            g.host,
            g.seen,
            g.first,
            g.last,
            (g.last - g.first + 1) - g.seen
        );
    }
    Ok(())
}

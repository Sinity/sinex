//! `sinexd`: one context built in `main`; admission and the writer task; the
//! commit-log reader; the confirmed-stream publisher; the NATS ingress
//! consumer; the derivation runner; replay; the API (architecture §7.4).
mod admin;
mod analysis;
mod api;
mod backfill;
mod binding_reader;
mod catalog;
mod commit_reader;
mod crawl;
mod daemon;
mod derivation_runner;
mod dirty_keys;
mod excision;
mod ingress;
mod library;
mod one_shot;
mod publisher;
mod replay;
mod retained;
mod row_retirement;
mod screen_text;
mod sealed_access;
mod source_runner;
mod sqlite_images;
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
        command: admin::Admin,
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
        /// Explicitly unlock retained sealed archive bytes for this backfill only.
        #[arg(long)]
        identity_file: Option<PathBuf>,
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

/// A writer and a derivation driver for a one-shot command, with the content
/// store when `store` names one.
async fn local_driver(
    database: &str,
    host: &str,
    store: Option<&std::path::Path>,
) -> Result<
    (
        derivation_runner::DerivationRunner,
        tokio::task::JoinHandle<()>,
    ),
    String,
> {
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
        std::sync::Arc::new(dirty_keys::Registered::new(sinex_derivations::keyed())),
    ));
    let read = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let mut driver = derivation_runner::DerivationRunner::start(
        read,
        handle,
        sinex_derivations::keyed(),
        derivation_runner::per_event(),
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
        Command::Admin { command } => admin::run(command).await,
        Command::Backfill {
            database,
            store,
            sources,
            zone,
            options,
            host,
            json,
            identity_file,
        } => {
            backfill::run(backfill::Options {
                database,
                store,
                sources,
                zone,
                options,
                host,
                json,
                identity_file,
            })
            .await
        }
        Command::Derive {
            database,
            store,
            host,
            json,
        } => run_derive(database, store, host, json).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sinexd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

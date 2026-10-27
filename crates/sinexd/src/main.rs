//! `sinexd`: one context built in `main`; admission and the writer task; the
//! commit-log reader; the confirmed-stream publisher; the NATS ingress
//! consumer; the derivation runner; replay; the API.
mod admin;
mod analysis;
mod api;
mod backfill;
mod binding_reader;
mod binding_runner;
mod catalog;
mod commit_reader;
mod crawl;
mod daemon;
mod derivation_runner;
mod diagnostics;
mod dirty_keys;
mod excision;
mod ingress;
mod intake;
mod library;
mod publisher;
mod reader_context;
mod replay;
mod screen_text;
mod sealing;
mod stored;
mod writer;

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
    /// Run the daemon. Create the schema first with `sinexd admin schema`.
    Run {
        #[arg(long)]
        database: String,
        /// Content store directory.
        #[arg(long)]
        store: PathBuf,
        /// NATS server URL, with the daemon's user when the server requires
        /// one (`nats://sinexd@127.0.0.1:4222`).
        #[arg(long)]
        nats: String,
        /// The NATS user's password, read from this file.
        #[arg(long)]
        nats_password_file: Option<PathBuf>,
        /// Subject root on the NATS server.
        #[arg(long, default_value = "sinex")]
        subject_root: String,
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
    /// Publish every committed event not yet on the confirmed stream, then stop
    /// and report the rate. For a first run and measurement; the running daemon
    /// does the same continuously.
    Publish {
        #[arg(long)]
        database: String,
        #[arg(long)]
        nats: String,
        #[arg(long)]
        nats_password_file: Option<PathBuf>,
        #[arg(long, default_value = "sinex")]
        subject_root: String,
    },
    /// Administration that needs the database but not the running daemon.
    Admin {
        #[command(subcommand)]
        command: admin::Admin,
    },
    /// Read sources once to the end, through the writer, and report counts and
    /// rates. For first imports and measurement. Create the schema first
    /// with `sinexd admin schema`.
    Backfill {
        #[arg(long)]
        database: String,
        /// Content store directory.
        #[arg(long)]
        store: PathBuf,
        /// An input as `format=path`; repeatable. The formats are listed by
        /// `sinexd admin catalog`: exports and directories (a file, or a
        /// directory of them), databases, and live sources' spools.
        #[arg(long = "read")]
        reads: Vec<String>,
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
        /// Unlock stored sealed archive bytes for this backfill only.
        #[arg(long)]
        identity_file: Option<PathBuf>,
    },
    /// Run every derivation until nothing is dirty, through the writer, and
    /// report counts and rates. The daemon's runner, run to completion.
    /// Create the schema first with `sinexd admin schema`.
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

/// A writer and a derivation runner for a one-shot command, with the content
/// store when `store` names one.
async fn local_runner(
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
    let mut writer_conn = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    writer::claim(&mut writer_conn).await?;
    let blobs = store
        .map(sinex_store::blobs::Blobs::open)
        .transpose()
        .map_err(|e| e.to_string())?;
    let (handle, queues) = writer::channels();
    let task = tokio::spawn(writer::run(
        database.to_owned(),
        writer_conn,
        queues,
        std::sync::Arc::new(dirty_keys::Registered::new(sinex_derivations::keyed())),
        blobs.clone(),
    ));
    let read = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| e.to_string())?;
    let mut runner = derivation_runner::DerivationRunner::start(
        read,
        handle,
        sinex_derivations::keyed(),
        derivation_runner::per_event(),
        host,
    )
    .await?;
    if let Some(blobs) = blobs {
        runner = runner.with_blobs(blobs);
    }
    Ok((runner, task))
}

async fn run_derive(
    database: String,
    store: Option<PathBuf>,
    host: String,
    json: bool,
) -> Result<(), String> {
    let (mut runner, writer_task) = local_runner(&database, &host, store.as_deref()).await?;
    let started = std::time::Instant::now();
    runner.catch_up().await?;
    let wall = started.elapsed();
    let outputs: usize = runner.counts.values().map(|c| c.outputs).sum();
    let inputs: usize = runner.counts.values().map(|c| c.inputs).sum();
    let rate = outputs as f64 / wall.as_secs_f64().max(1e-9);
    if json {
        let per: serde_json::Map<String, serde_json::Value> = runner
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
        for (name, counts) in &runner.counts {
            println!("{name:18} {counts}");
        }
        println!(
            "wall {:.2}s | inputs read {inputs} | outputs {outputs} | {rate:.0} outputs/s",
            wall.as_secs_f64(),
        );
    }
    runner.end_runs().await?;
    drop(runner);
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

async fn publish(
    database: &str,
    nats: &str,
    nats_password_file: Option<&std::path::Path>,
    subject_root: &str,
) -> Result<(), String> {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let client = sinex_source_core::nats::connect(nats, nats_password_file).await?;
    let js = publisher::context(client);
    let report =
        publisher::publish_all(&mut conn, &js, &sinex_protocol::Subjects::new(subject_root))
            .await?;
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
            nats_password_file,
            subject_root,
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
                    nats_password_file,
                    subject_root,
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
            nats_password_file,
            subject_root,
        } => {
            publish(
                &database,
                &nats,
                nats_password_file.as_deref(),
                &subject_root,
            )
            .await
        }
        Command::Admin { command } => admin::run(command).await,
        Command::Backfill {
            database,
            store,
            reads,
            zone,
            options,
            host,
            json,
            identity_file,
        } => {
            backfill::run(backfill::Options {
                database,
                store,
                reads,
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

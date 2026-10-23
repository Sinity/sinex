//! The running daemon (architecture §7.4): tasks under one supervisor in one
//! tokio runtime, built once here from one configuration. Every task gets the
//! connections and handles it needs from this function; none opens its own
//! configuration. A task that ends with an error stops the daemon, so the
//! service manager restarts it whole.
//!
//! Built: the writer (§4.3), the derivation runner for keyed and per-event
//! derivations (§6.2), the commit-log reader for read models (§4.3), the
//! confirmed-stream publisher (§4.3) and the NATS ingress with its have
//! responder (§4.3). Every commit wakes the driver, the reader and the
//! publisher, and the API (§7.2) serves reads from its own pool and writes
//! through the writer. The source tasks (§5) and signals (§7.2) are added
//! here by their lanes.
use crate::api::{self, Api};
use crate::commit_reader::{self, Consumer, ReadModelRunner};
use crate::dirty_keys::Registered;
use crate::{derivation_runner, ingress, publisher, source_runner, writer};
use sinex_protocol::Subjects;
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

/// How often the derivation runner looks for work when no commit woke it:
/// keys whose outputs wait on evidence outside their window settle with time.
const RUNNER_IDLE: Duration = Duration::from_secs(30);

/// The trailing days of metric values are recomputed at least this often
/// (new opportunity, such as time passing for a live source, changes cells
/// without a commit); dirty days are recomputed whenever the driver settles.
const METRICS_IDLE: Duration = Duration::from_secs(600);
/// How often the binding runner reads every configured binding again.
const BINDINGS_EVERY: Duration = Duration::from_secs(5);

pub struct Options {
    pub database: String,
    pub store: PathBuf,
    /// The NATS server, with the daemon's credentials in the URL when the
    /// server requires them.
    pub nats: String,
    /// Subject root: `sinex` unless several deployments share a server.
    pub subjects: String,
    pub host: String,
    pub api: ApiOptions,
    /// The sources configuration (`sources.json`) the binding runner reads.
    pub sources: Option<PathBuf>,
}

pub struct ApiOptions {
    pub socket: PathBuf,
    pub listen: Option<std::net::SocketAddr>,
    /// Directory holding `read.token`, `agent.token` and `operator.token`,
    /// and optionally `agent.grants` (classes the agent token may read).
    pub tokens: PathBuf,
    pub zone: jiff::tz::TimeZone,
    pub day_start: jiff::civil::Time,
}

async fn connect(url: &str) -> Result<PgConnection, String> {
    PgConnection::connect(url)
        .await
        .map_err(|e| format!("database: {e}"))
}

pub async fn run(o: Options) -> Result<(), String> {
    let subjects = Subjects::new(o.subjects.clone());
    let blobs = Blobs::open(&o.store).map_err(|e| e.to_string())?;
    let tokens =
        api::Tokens::load(&o.api.tokens).map_err(|e| format!("{}: {e}", o.api.tokens.display()))?;
    let client = sinex_sources::nats::connect(&o.nats).await?;
    let js = publisher::context(client.clone());

    let (stop_tx, stop) = watch::channel(false);
    let (commits_tx, commits) = watch::channel(0u64);
    let (handle, queues) = writer::channels();
    let mut tasks: JoinSet<(&'static str, Result<(), String>)> = JoinSet::new();

    let mut writer_conn = connect(&o.database).await?;
    writer::claim(&mut writer_conn).await?;
    // Blob files an excision committed to deleting but a crash left behind.
    let (deleted, left) = sinex_store::excision::finish_deletions(&mut writer_conn, &blobs)
        .await
        .map_err(|e| format!("finishing blob deletions: {e}"))?;
    if !deleted.is_empty() || !left.is_empty() {
        eprintln!(
            "sinexd: finished {} blob deletions left by a stop; {} files could not be deleted",
            deleted.len(),
            left.len()
        );
    }
    let url = o.database.clone();
    tasks.spawn(async move {
        writer::run_notifying(
            url,
            writer_conn,
            queues,
            Arc::new(Registered::new(sinex_derivations::keyed())),
            commits_tx,
        )
        .await;
        ("writer", Ok(()))
    });

    // Every derivation runs in the driver: events from keyed and per-event
    // derivations (screen text included), then the maintained metric values.
    let driver = derivation_runner::DerivationRunner::start(
        connect(&o.database).await?,
        handle.clone(),
        sinex_derivations::keyed(),
        derivation_runner::per_event(),
        &o.host,
    )
    .await?
    .with_blobs(blobs.clone())
    .with_maintained(vec![Box::new(crate::analysis::MetricValues::new(
        crate::analysis::Settings::operator_default(),
        2,
        METRICS_IDLE,
    ))]);
    let wake = Arc::new(Notify::new());
    let (w, mut c) = (wake.clone(), commits.clone());
    tasks.spawn(async move {
        tokio::select! {
            result = driver.run(w.clone(), RUNNER_IDLE) => ("derivation runner", result),
            // Every commit wakes the driver.
            () = async move {
                while c.changed().await.is_ok() {
                    w.notify_one();
                }
            } => ("derivation runner wake", Ok(())),
        }
    });

    let mut consumers: Vec<Box<dyn Consumer>> = Vec::new();
    for model in commit_reader::read_models() {
        consumers.push(Box::new(ReadModelRunner(model)));
    }
    let reader_conn = connect(&o.database).await?;
    let (c, s) = (commits.clone(), stop.clone());
    tasks.spawn(async move {
        (
            "commit-log reader",
            commit_reader::run(reader_conn, consumers, c, s).await,
        )
    });

    let publisher_conn = connect(&o.database).await?;
    let (c, s, sj) = (commits.clone(), stop.clone(), subjects.clone());
    tasks.spawn(async move {
        (
            "publisher",
            publisher::run(publisher_conn, js, sj, c, s).await,
        )
    });

    // Readers never use the writer's connection.
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&o.database)
        .await
        .map_err(|e| format!("read pool: {e}"))?;
    let reader_api = Api {
        reads: reads.clone(),
        writer: Some(handle.clone()),
        blobs: blobs.clone(),
        zone: o.api.zone.clone(),
        day_start: o.api.day_start,
        agent_grants: api::agent_grants(&o.api.tokens)
            .map_err(|e| format!("{}: {e}", o.api.tokens.display()))?,
        unlock: Default::default(),
        host: o.host.clone(),
    };
    let config = api::Config {
        socket: o.api.socket.clone(),
        listen: o.api.listen,
        tokens,
    };
    let (c, mut s) = (commits.clone(), stop.clone());
    eprintln!("sinexd: API on {}", o.api.socket.display());
    tasks.spawn(async move {
        (
            "API",
            api::serve(config, reader_api, c, async move {
                let _ = s.wait_for(|s| *s).await;
            })
            .await,
        )
    });

    // The configured sources, read continuously by the binding runner.
    if let Some(file) = &o.sources {
        let planned = source_runner::plan(&source_runner::Config::load(file)?, &o.host)?;
        let (read_conn, b, w, host, s) = (
            connect(&o.database).await?,
            blobs.clone(),
            handle.clone(),
            o.host.clone(),
            stop.clone(),
        );
        tasks.spawn(async move {
            (
                "binding runner",
                source_runner::run(read_conn, b, w, host, planned, BINDINGS_EVERY, s).await,
            )
        });
    }

    let ingest = ingress::Ingest::new(
        subjects,
        handle.clone(),
        connect(&o.database).await?,
        reads,
        blobs,
        o.host.clone(),
    )?;
    let s = stop.clone();
    tasks.spawn(async move { ("ingress", ingress::run(client, ingest, s).await) });
    // The writer stops when every handle is gone; the tasks above hold theirs.
    drop(handle);

    let outcome = tokio::select! {
        _ = shutdown_signal() => Ok(()),
        Some(done) = tasks.join_next() => match done {
            Ok((name, Ok(()))) => Err(format!("{name} stopped")),
            Ok((name, Err(error))) => Err(format!("{name}: {error}")),
            Err(error) => Err(format!("a task panicked: {error}")),
        },
    };
    let _ = stop_tx.send(true);
    // Give tasks a moment to finish their current step, then stop the rest.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    tasks.abort_all();
    outcome
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

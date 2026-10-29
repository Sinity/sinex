//! The running daemon: tasks under one supervisor in one tokio runtime, built
//! once here from one configuration. Every task gets the connections and
//! handles it needs from this function; none opens its own configuration. A
//! task that ends with an error stops the daemon, so the service manager
//! restarts it whole.
//!
//! Built: the writer, the derivation runner for keyed and per-event
//! derivations, the commit-log reader for read models, the confirmed-stream
//! publisher, the NATS ingress with its have responder, and the telemetry
//! sampler, whose spool the binding runner reads with the configured sources. Every commit wakes
//! the runner, the reader and the publisher, and the API serves reads from its
//! own pool and writes through the writer. The source tasks and signals are
//! added here by their lanes.
use crate::api::{self, Api};
use crate::commit_reader::{self, Consumer, ReadModelRunner};
use crate::dirty_keys::Registered;
use crate::{binding_runner, derivation_runner, ingress, publisher, telemetry, writer};
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

/// How often the binding runner reads every configured binding again.
const BINDINGS_EVERY: Duration = Duration::from_secs(5);

pub(crate) struct Options {
    pub database: String,
    pub store: PathBuf,
    /// The NATS server, with the daemon's user in the URL when the server
    /// requires one.
    pub nats: String,
    /// That user's password.
    pub nats_password_file: Option<PathBuf>,
    pub producer_grants: ingress::fetch::ProducerGrants,
    /// Subject root: `sinex` unless several deployments share a server.
    pub subject_root: String,
    pub host: String,
    pub api: ApiOptions,
    /// The sources configuration (`sources.json`) the binding runner reads.
    pub sources: Option<PathBuf>,
    /// Where the daemon spools its telemetry samples.
    pub telemetry_spool: PathBuf,
}

pub(crate) struct ApiOptions {
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

pub(crate) async fn run(o: Options) -> Result<(), String> {
    let subjects = Subjects::new(o.subject_root.clone());
    let blobs = Blobs::open(&o.store).map_err(|e| e.to_string())?;
    let tokens =
        api::Tokens::load(&o.api.tokens).map_err(|e| format!("{}: {e}", o.api.tokens.display()))?;
    let client = sinex_source_core::nats::connect(&o.nats, o.nats_password_file.as_deref()).await?;
    let js = publisher::context(client.clone());

    let (stop_tx, stop) = watch::channel(false);
    let (commits_tx, commits) = watch::channel(0u64);
    let (read_models_tx, read_models) = watch::channel(());
    let (handle, queues) = writer::channels();
    let mut tasks: JoinSet<(&'static str, Result<(), String>)> = JoinSet::new();

    let writer_conn = connect(&o.database).await?;
    let mut writer_conn = writer::claim(writer_conn).await?;
    blobs
        .recover(&mut writer_conn)
        .await
        .map_err(|error| error.to_string())?;
    // Finish durable excision punches before sources resume.
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
    // Remove committed extents whose admission left no catalog row.
    let without_rows = sinex_store::blobs::delete_unreferenced_extents(&mut writer_conn, &blobs)
        .await
        .map_err(|e| format!("deleting extents without a row: {e}"))?;
    if !without_rows.is_empty() {
        eprintln!(
            "sinexd: deleted {} unreferenced byte extents",
            without_rows.len()
        );
    }
    sinex_store::registry::write_sources(&mut writer_conn, &crate::catalog::registry_sources())
        .await
        .map_err(|e| format!("writing the source registry: {e}"))?;
    sinex_store::source_declarations::register(
        &mut writer_conn,
        &crate::catalog::source_declarations(),
    )
    .await
    .map_err(|error| format!("registering native sources: {error}"))?;
    for grant in o
        .producer_grants
        .values()
        .filter_map(|grant| grant.write.as_ref())
    {
        sinex_store::source_declarations::register(&mut writer_conn, &grant.source_declarations)
            .await
            .map_err(|error| format!("registering configured source catalog: {error}"))?;
    }
    // The configured derivations run in the runner, keyed and per-event
    // (screen text included).
    let mut sources = match &o.sources {
        Some(file) => binding_runner::Config::load(file)?,
        None => binding_runner::Config::default(),
    };
    // The daemon reads its own telemetry spool, whatever is configured.
    let telemetry_spool = o.telemetry_spool.clone();
    // Discovery must see the configured spool before its first planning turn.
    std::fs::create_dir_all(&telemetry_spool)
        .map_err(|error| format!("telemetry spool: {error}"))?;
    sources.sources.insert(
        sinex_sources::sources::telemetry::SPEC.name.to_owned(),
        binding_runner::SourceConfig {
            paths: vec![telemetry_spool.display().to_string()],
            ..Default::default()
        },
    );
    let configuration = sources.derivation_configuration();
    let (keyed, per_event) = (
        crate::analysis::keyed_with(&configuration),
        derivation_runner::per_event_with(&configuration)?,
    );
    if let Some(name) = sources.derivations.keys().find(|name| {
        !keyed.iter().any(|d| d.name() == name.as_str())
            && !per_event.iter().any(|d| d.name() == name.as_str())
    }) {
        return Err(format!("derivation {name}: no such derivation"));
    }
    let keyed: Vec<_> = keyed
        .into_iter()
        .filter(|d| sources.runs(d.name()))
        .collect();
    let per_event: Vec<_> = per_event
        .into_iter()
        .filter(|d| d.name() == sinex_derivations::tags::TagApplier::NAME || sources.runs(d.name()))
        .collect();
    for (name, _) in sources.derivations.iter().filter(|(_, runs)| !**runs) {
        eprintln!("sinexd: derivation {name} is turned off by the configuration");
    }
    let time_settings =
        sinex_model::time_settings::TimeSettings::from_parts(o.api.zone.clone(), o.api.day_start);
    let writer_derivations = crate::analysis::keyed_with(&configuration)
        .into_iter()
        .filter(|d| sources.runs(d.name()))
        .collect();
    let writer_settings = time_settings.clone();
    let url = o.database.clone();
    let writer_store = blobs.clone();
    tasks.spawn(async move {
        writer::run_notifying(
            url,
            writer_conn,
            queues,
            Arc::new(Registered::new(writer_derivations, writer_settings)),
            Some(writer_store),
            commits_tx,
        )
        .await;
        ("writer", Ok(()))
    });

    let runner = derivation_runner::DerivationRunner::start(
        connect(&o.database).await?,
        handle.clone(),
        keyed,
        per_event,
        &o.host,
        time_settings,
    )
    .await?
    .with_blobs(blobs.clone());
    let wake = Arc::new(Notify::new());
    let (w, mut c, s) = (wake.clone(), commits.clone(), stop.clone());
    tasks.spawn(async move {
        tokio::select! {
            result = runner.run(w.clone(), RUNNER_IDLE, s) => ("derivation runner", result),
            // Every commit wakes the runner.
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
            commit_reader::run(reader_conn, consumers, c, read_models_tx, s).await,
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
        source_config: Arc::new(sources.clone()),
        reads: reads.clone(),
        writer: Some(handle.clone()),
        blobs: blobs.clone(),
        zone: o.api.zone.clone(),
        day_start: o.api.day_start,
        derivations: configuration,
        agent_grants: api::agent_grants(&o.api.tokens)
            .map_err(|e| format!("{}: {e}", o.api.tokens.display()))?,
        unlock: Default::default(),
        host: o.host.clone(),
        library_roots: if sources
            .exclude
            .get("library.scan")
            .is_some_and(Option::is_some)
        {
            Vec::new()
        } else {
            sources
                .sources
                .get("library.scan")
                .into_iter()
                .flat_map(|source| source.paths.iter().map(std::path::PathBuf::from))
                .chain(
                    sources
                        .discovery
                        .input_roots
                        .get("library")
                        .into_iter()
                        .flatten()
                        .cloned(),
                )
                .collect()
        },
    };
    let public_context = crate::public_context::Provider::new(
        reader_api
            .context()
            .clock()
            .map_err(|error| error.to_string())?
            .day
            .wire(),
        o.host.clone(),
        reader_api.source_config.signals.as_ref(),
    )?;
    if let Some(settings_identity) = public_context.settings_identity.clone() {
        let (signals_api, signals_client, signals_subjects, signals_grants, c, s) = (
            reader_api.clone(),
            client.clone(),
            subjects.clone(),
            Arc::new(o.producer_grants.clone()),
            commits.clone(),
            stop.clone(),
        );
        tasks.spawn(async move {
            (
                "signals",
                crate::signals::run(
                    signals_api,
                    signals_client,
                    signals_subjects,
                    signals_grants,
                    settings_identity,
                    c,
                    s,
                )
                .await,
            )
        });
    }
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
            api::serve(config, reader_api, c, read_models, async move {
                let _ = s.wait_for(|s| *s).await;
            })
            .await,
        )
    });

    // The daemon's own state, sampled into its telemetry spool.
    let (sample_conn, sample_database, s) = (
        connect(&o.database).await?,
        o.database.clone(),
        stop.clone(),
    );
    tasks.spawn(async move {
        (
            "telemetry",
            telemetry::run((sample_conn, sample_database), telemetry_spool, s).await,
        )
    });

    // The configured sources and the telemetry spool, read continuously by
    // the binding runner. A configuration naming an unknown source or format
    // stops the daemon here; the runner plans it again as files appear.
    binding_runner::plan(&sources, &o.host)?;
    let (read_conn, database, b, w, host, s) = (
        connect(&o.database).await?,
        o.database.clone(),
        blobs.clone(),
        handle.clone(),
        o.host.clone(),
        stop.clone(),
    );
    tasks.spawn(async move {
        (
            "binding runner",
            binding_runner::run(
                (read_conn, database),
                b,
                w,
                host,
                sources,
                BINDINGS_EVERY,
                s,
            )
            .await,
        )
    });

    let producer_grants = Arc::new(o.producer_grants);
    let requests = ingress::Requests {
        public_context,
        client: client.clone(),
        consumer_admin: std::sync::Arc::new(tokio::sync::Mutex::new(())),
        read_grants: producer_grants.clone(),
        subjects: subjects.clone(),
        writer: handle.clone(),
        reads,
        blobs: blobs.clone(),
    };
    let ingest = ingress::Ingest::new(
        subjects,
        handle.clone(),
        connect(&o.database).await?,
        blobs.clone(),
        o.host.clone(),
        producer_grants,
    )?;
    let s = stop.clone();
    tasks.spawn(async move { ("ingress", ingress::run(client, ingest, requests, s).await) });
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
    let drained = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_ok();
    tasks.abort_all();
    if drained && let Err(error) = forget_admitted(&o.database, &blobs).await {
        eprintln!("sinexd: forgetting admitted blob names at shutdown: {error}");
    }

    outcome
}

async fn forget_admitted(database: &str, blobs: &Blobs) -> Result<(), String> {
    let mut conn = writer::claim(connect(database).await?).await?;
    let result = sinex_store::blobs::commit_pending_extents(&mut conn, blobs)
        .await
        .map_err(|e| e.to_string());
    // Closing the session releases its writer claim even when cleanup fails.
    let closed = conn.close().await.map_err(|e| e.to_string());
    result.and(closed)
}

async fn shutdown_signal() {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(test)]
mod tests;

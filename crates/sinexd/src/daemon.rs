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
use crate::{Error, Result};
use crate::{binding_runner, derivation_runner, ingress, publisher, telemetry, writer};
use sinex_protocol::Subjects;
use sinex_store::Blobs;
use sqlx::{Connection, PgConnection};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, watch};
use tokio::task::{AbortHandle, Id, JoinSet};

/// How often the derivation runner looks for work when no commit woke it:
/// keys whose outputs wait on readings outside their window are decided as time passes.
const RUNNER_IDLE: Duration = Duration::from_secs(30);

/// How often the binding runner reads every configured binding again.
const BINDINGS_EVERY: Duration = Duration::from_secs(5);

// Leave time within the ten-second stop bound for cancelled task joins,
// runtime teardown and the service manager to reap the process.
const STOP_GRACE: Duration = Duration::from_secs(7);
const ABORT_GRACE: Duration = Duration::from_secs(1);
pub(crate) const RUNTIME_STOP_GRACE: Duration = Duration::from_secs(1);

#[derive(Default, serde::Serialize)]
struct ShutdownSummary {
    requested: usize,
    joined: usize,
    failed: usize,
    cancelled: usize,
    panicked: usize,
    abort_requested: usize,
    writer_joined: bool,
    tasks: Vec<StoppedTask>,
}

#[derive(serde::Serialize)]
struct StoppedTask {
    name: &'static str,
    outcome: StopOutcome,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum StopOutcome {
    Complete,
    PublicationDeferred,
    Failed,
    Cancelled,
    Panicked,
    JoinError,
    StillRunning,
}

#[derive(Default)]
struct Tasks {
    running: JoinSet<Result<StopOutcome>>,
    names: HashMap<Id, &'static str>,
    shutdown: ShutdownSummary,
}

impl Tasks {
    fn spawn<F>(&mut self, name: &'static str, task: F) -> AbortHandle
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        self.spawn_with_outcome(
            name,
            async move { task.await.map(|()| StopOutcome::Complete) },
        )
    }

    fn spawn_with_outcome<F>(&mut self, name: &'static str, task: F) -> AbortHandle
    where
        F: Future<Output = Result<StopOutcome>> + Send + 'static,
    {
        let task = self.running.spawn(task);
        self.names.insert(task.id(), name);
        self.shutdown.requested += 1;
        task
    }

    async fn join_next(&mut self) -> Option<Result<&'static str>> {
        let (id, outcome, error) = match self.running.join_next_with_id().await? {
            Ok((id, Ok(outcome))) => (id, outcome, None),
            Ok((id, Err(error))) => {
                self.shutdown.failed += 1;
                (
                    id,
                    StopOutcome::Failed,
                    Some(Error::internal(error.to_string())),
                )
            }
            Err(error) => {
                let outcome = if error.is_cancelled() {
                    self.shutdown.cancelled += 1;
                    StopOutcome::Cancelled
                } else if error.is_panic() {
                    self.shutdown.panicked += 1;
                    StopOutcome::Panicked
                } else {
                    self.shutdown.failed += 1;
                    StopOutcome::JoinError
                };
                (
                    error.id(),
                    outcome,
                    Some(Error::internal(error.to_string())),
                )
            }
        };
        let name = self.names.remove(&id).expect("supervised task has a name");
        self.shutdown.joined += 1;
        self.shutdown.writer_joined |= name == "writer" && matches!(outcome, StopOutcome::Complete);
        self.shutdown.tasks.push(StoppedTask { name, outcome });
        Some(match error {
            Some(error) => Err(error.within(name)),
            None => Ok(name),
        })
    }

    async fn join_until(&mut self, grace: Duration) {
        let _ =
            tokio::time::timeout(grace, async { while self.join_next().await.is_some() {} }).await;
    }

    async fn finish(mut self, grace: Duration) -> ShutdownSummary {
        self.join_until(grace).await;
        self.shutdown.abort_requested = self.running.len();
        self.running.abort_all();
        self.join_until(ABORT_GRACE).await;
        let mut remaining: Vec<_> = self.names.into_values().collect();
        remaining.sort_unstable();
        self.shutdown
            .tasks
            .extend(remaining.into_iter().map(|name| StoppedTask {
                name,
                outcome: StopOutcome::StillRunning,
            }));
        self.shutdown
    }
}

pub(crate) struct Options {
    pub class_recipient: Option<Arc<sinex_sealing::Recipient>>,
    pub library_cache: PathBuf,
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

pub(crate) async fn connect(url: &str, task: &str) -> Result<PgConnection> {
    let options = url
        .parse::<sqlx::postgres::PgConnectOptions>()?
        .application_name(task);
    PgConnection::connect_with(&options)
        .await
        .map_err(|e| Error::from(e).within("database"))
}

pub(crate) async fn run(o: Options) -> Result<()> {
    let blobs = Blobs::open(&o.store)?;
    let (stop_tx, stop) = watch::channel(false);
    let mut tasks = Tasks::default();
    // Supervise startup too: it can already own a writer transaction before
    // the API and the complete task set are ready.
    let outcome = tokio::select! {
        biased;
        _ = shutdown_signal() => Ok(()),
        result = serve(o, blobs, stop, &mut tasks) => result,
    };
    stop_tx.send_replace(true);
    let shutdown = tasks.finish(STOP_GRACE).await;
    eprintln!(
        "sinexd: shutdown {}",
        serde_json::to_string(&shutdown).expect("shutdown result serializes")
    );
    outcome
}

async fn serve(
    o: Options,
    blobs: Blobs,
    stop: watch::Receiver<bool>,
    tasks: &mut Tasks,
) -> Result<()> {
    let subjects = Subjects::new(o.subject_root.clone());
    let tokens = api::Tokens::load(&o.api.tokens).map_err(|e| Error::file(&o.api.tokens, e))?;
    let client = sinex_source_core::nats::connect(&o.nats, o.nats_password_file.as_deref())
        .await
        .map_err(Error::Nats)?;
    let js = publisher::context(client.clone());

    let (commits_tx, commits) = watch::channel(0u64);
    let (read_models_tx, read_models) = watch::channel(());
    let (mut handle, queues) = writer::channels();
    handle.class_recipient = o.class_recipient.clone();
    let writer_conn = connect(&o.database, "sinex.writer").await?;
    let mut writer_conn = writer::claim(writer_conn).await?;
    crate::backup::policy::recover(&mut writer_conn).await?;
    blobs.recover(&mut writer_conn).await?;
    // Finish removing bytes a tombstone freed before sources resume.
    let (removed, left) = sinex_store::lifecycle::remove_unreferenced(&mut writer_conn, &blobs)
        .await
        .map_err(|e| Error::from(e).within("removing tombstoned bytes"))?;
    if !removed.is_empty() || !left.is_empty() {
        eprintln!(
            "sinexd: removed {} tombstoned byte ranges left by a stop; {} could not be removed",
            removed.len(),
            left.len()
        );
    }
    // No operation runs while the writer starts; a stopped one resumes later.
    sinex_store::lifecycle::release_held_keys(&mut writer_conn)
        .await
        .map_err(|e| Error::from(e).within("releasing held keys"))?;
    // Remove committed extents whose admission left no catalog row.
    let without_rows = sinex_store::blobs::delete_unreferenced_extents(&mut writer_conn, &blobs)
        .await
        .map_err(|e| Error::from(e).within("deleting extents without a row"))?;
    if !without_rows.is_empty() {
        eprintln!(
            "sinexd: deleted {} unreferenced byte extents",
            without_rows.len()
        );
    }
    crate::catalog::install_built_in_sources(&mut writer_conn).await?;
    for grant in o
        .producer_grants
        .values()
        .filter_map(|grant| grant.write.as_ref())
    {
        sinex_store::source_declarations::register(&mut writer_conn, &grant.source_declarations)
            .await
            .map_err(|error| {
                Error::internal(format!("registering configured source catalog: {error}"))
            })?;
    }
    // The configured derivations run in the runner, keyed and per-event
    // (screen text included).
    let mut sources = match &o.sources {
        Some(file) => binding_runner::Config::load(file)?,
        None => binding_runner::Config::default(),
    };
    let exclusions = sources
        .exclude
        .iter()
        .filter_map(|(name, exclusion)| {
            exclusion
                .as_ref()
                .map(|exclusion| (name.clone(), exclusion.reason.clone()))
        })
        .collect();
    sinex_store::registry::write_exclusions(&mut writer_conn, &exclusions).await?;
    // The daemon reads its own telemetry spool, whatever is configured.
    let telemetry_spool = o.telemetry_spool.clone();
    // Discovery must see the configured spool before its first planning turn.
    std::fs::create_dir_all(&telemetry_spool)
        .map_err(|error| Error::from(error).within("telemetry spool"))?;
    sources
        .discovery
        .inputs
        .entry(sinex_sources::sources::telemetry::FORMAT.name.to_owned())
        .or_default()
        .push(telemetry_spool.clone());
    let configuration = sources.derivation_configuration();
    // Fusion runs only the families with a written definition; an invalid
    // declaration refuses to start, naming the family and the reason.
    sinex_derivations::fusion::declaration::check().map_err(Error::bad_value)?;
    let catalog = derivation_runner::Catalog::new(&configuration)?;
    let (keyed, per_event) = (&catalog.keyed, &catalog.per_event);
    let built_names = crate::catalog::derivation_names();
    if let Some(name) = sources
        .derivations
        .keys()
        .find(|name| !built_names.contains(name))
    {
        return Err(Error::not_found(format!(
            "derivation {name}: no such derivation"
        )));
    }
    let keyed: Vec<_> = keyed
        .iter()
        .filter(|d| sources.runs(d.name()) == Some(true))
        .cloned()
        .collect();
    let per_event: Vec<_> = per_event
        .iter()
        .filter(|d| sources.runs(d.name()) == Some(true))
        .cloned()
        .collect();
    let sampled_derivations = per_event
        .iter()
        .map(|d| (d.name(), d.inputs_types()))
        .collect();
    for (name, _) in sources.derivations.iter().filter(|(_, runs)| !**runs) {
        eprintln!("sinexd: derivation {name} is turned off by the configuration");
    }
    let time_settings =
        sinex_model::time_settings::TimeSettings::from_parts(o.api.zone.clone(), o.api.day_start);
    // Admission marks the keys of every keyed derivation the build has,
    // running or not. A derivation turned off is paused: its marks wait, and
    // it computes them when it runs again.
    let writer_derivations = crate::analysis::keyed_with(&configuration);
    let writer_settings = time_settings.clone();
    let url = o.database.clone();
    let writer_store = blobs.clone();
    tasks.spawn("writer", async move {
        writer::run_notifying(
            url,
            writer_conn,
            queues,
            Arc::new(Registered::new(writer_derivations, writer_settings)),
            Some(writer_store),
            commits_tx,
        )
        .await;
        Ok(())
    });

    let runner = derivation_runner::DerivationRunner::start(
        connect(&o.database, "sinex.derivations").await?,
        handle.clone(),
        keyed,
        per_event,
        &o.host,
        time_settings,
        crate::build_identity::current()?,
    )
    .await?
    .with_blobs(blobs.clone());
    let wake = Arc::new(Notify::new());
    let (w, mut c, s) = (wake.clone(), commits.clone(), stop.clone());
    tasks.spawn("derivation runner", async move {
        tokio::select! {
            result = runner.run(w.clone(), RUNNER_IDLE, s) => result,
            // Every commit wakes the runner.
            () = async move {
                while c.changed().await.is_ok() {
                    w.notify_one();
                }
            } => Ok(()),
        }
    });

    let mut consumers: Vec<Box<dyn Consumer>> = Vec::new();
    for model in commit_reader::read_models() {
        consumers.push(Box::new(ReadModelRunner(model)));
    }
    let reader_conn = connect(&o.database, "sinex.read_models").await?;
    let (c, s) = (commits.clone(), stop.clone());
    tasks.spawn(
        "commit-log reader",
        commit_reader::run(reader_conn, consumers, c, read_models_tx, s),
    );

    let telemetry_js = js.clone();
    let confirmed = subjects.confirmed_stream();
    let publisher_conn = connect(&o.database, "sinex.publisher").await?;
    let (c, s, sj) = (commits.clone(), stop.clone(), subjects.clone());
    tasks.spawn_with_outcome("publisher", async move {
        let in_flight = publisher::run(publisher_conn, js, sj, c, s).await?;
        Ok(if in_flight {
            StopOutcome::PublicationDeferred
        } else {
            StopOutcome::Complete
        })
    });

    // Readers never use the writer's connection.
    let reads = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect_with(
            o.database
                .parse::<sqlx::postgres::PgConnectOptions>()?
                .application_name("sinex.api"),
        )
        .await
        .map_err(|e| Error::from(e).within("read pool"))?;
    let source_status = crate::binding_runner::SourceStatus::default();
    let reader_api = Api {
        backup_copy: Default::default(),
        backup_producers: Some((
            client.clone(),
            subjects.clone(),
            o.producer_grants
                .iter()
                .filter(|(_, grant)| grant.write.is_some())
                .map(|(name, _)| name.clone())
                .collect(),
        )),
        source_config: Arc::new(sources.clone()),
        source_status: source_status.clone(),
        running_stages: Default::default(),
        sql_cursors: Default::default(),
        stop: stop.clone(),
        reads: reads.clone(),
        writer: Some(handle.clone()),
        blobs: blobs.clone(),
        zone: o.api.zone.clone(),
        day_start: o.api.day_start,
        derivations: catalog,
        agent_grants: api::agent_grants(&o.api.tokens)
            .map_err(|e| Error::file(&o.api.tokens, e))?,
        unlock: Default::default(),
        host: o.host.clone(),
        library_cache: o.library_cache.clone(),
        library_roots: if sources
            .exclude
            .get("library.scan")
            .is_some_and(Option::is_some)
        {
            Vec::new()
        } else {
            sources
                .discovery
                .input_roots
                .get("library")
                .into_iter()
                .chain(
                    sources
                        .discovery
                        .inputs
                        .get(sinex_sources::sources::library::FORMAT.name),
                )
                .flatten()
                .cloned()
                .collect()
        },
    };
    let public_context = crate::public_context::Provider::new(
        reader_api.context().clock()?.day.wire(),
        o.host.clone(),
        reader_api.source_config.signals.as_ref(),
    )?;
    if let Some(settings_identity) = public_context.settings_identity.clone() {
        let (signals_api, c, s) = (reader_api.clone(), commits.clone(), stop.clone());
        tasks.spawn(
            "signals",
            crate::signals::run(signals_api, settings_identity, c, s),
        );
    }
    let config = api::Config {
        socket: o.api.socket.clone(),
        listen: o.api.listen,
        tokens,
    };
    let (c, mut s) = (commits.clone(), stop.clone());
    let (producer_requests_ready, ready) = tokio::sync::oneshot::channel();
    tasks.spawn("API", async move {
        match ready.await {
            Ok(()) => {
                eprintln!("sinexd: API on {}", config.socket.display());
                api::serve(config, reader_api, c, read_models, async move {
                    let _ = s.wait_for(|s| *s).await;
                })
                .await
            }
            Err(_) => Err(Error::internal(
                "producer request subscriptions did not become ready",
            )),
        }
    });

    // The daemon's own state, sampled into its telemetry spool.
    let (sample_conn, sample_database, s) = (
        connect(&o.database, "sinex.telemetry").await?,
        o.database.clone(),
        stop.clone(),
    );
    tasks.spawn(
        "telemetry",
        telemetry::run(
            (sample_conn, sample_database),
            telemetry_spool,
            telemetry_js,
            confirmed,
            sampled_derivations,
            s,
        ),
    );

    // The configured sources and the telemetry spool, read continuously by
    // the binding runner. A configuration naming an unknown source or format
    // stops the daemon here; the runner plans it again as files appear.
    binding_runner::plan(&sources, &o.host)?;
    let (read_conn, database, b, w, host, s) = (
        connect(&o.database, "sinex.bindings").await?,
        o.database.clone(),
        blobs.clone(),
        handle.clone(),
        o.host.clone(),
        stop.clone(),
    );
    tasks.spawn(
        "binding runner",
        binding_runner::run(
            (read_conn, database),
            b,
            w,
            host,
            sources,
            BINDINGS_EVERY,
            source_status,
            s,
        ),
    );

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
        connect(&o.database, "sinex.ingress").await?,
        blobs.clone(),
        o.host.clone(),
        producer_grants,
    )?;
    let s = stop.clone();
    tasks.spawn(
        "ingress",
        ingress::run(client, ingest, requests, s, producer_requests_ready),
    );
    // The writer stops when every handle is gone; the tasks above hold theirs.
    drop(handle);

    match tasks.join_next().await {
        Some(Ok(name)) => Err(Error::internal(format!("{name} stopped"))),
        Some(Err(error)) => Err(error),
        None => Err(Error::internal("daemon has no running tasks")),
    }
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

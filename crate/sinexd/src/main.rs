//! `sinexd`: one context built in `main`; admission and the writer task; the
//! commit-log reader; the confirmed-stream publisher; the NATS ingress
//! consumer; the automaton driver; replay; the API (architecture §7.4).
mod api;
mod backfill;
mod commits;
mod context;
mod daemon;
mod derive;
mod ingress;
mod keys;
mod operate;
mod publisher;
mod replay;
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
        #[arg(long)]
        atuin: Option<PathBuf>,
        #[arg(long)]
        zsh: Option<PathBuf>,
        /// A Hyprland capture spool directory.
        #[arg(long)]
        hyprland_spool: Option<PathBuf>,
        /// A journald capture spool directory.
        #[arg(long)]
        journald_spool: Option<PathBuf>,
        /// A file-watch capture spool directory.
        #[arg(long)]
        file_watch_spool: Option<PathBuf>,
        /// ActivityWatch's `aw-server-rust/sqlite.db`.
        #[arg(long)]
        activitywatch: Option<PathBuf>,
        /// A local repository whose commits to read; repeatable.
        #[arg(long = "repository")]
        repositories: Vec<PathBuf>,
        #[arg(long, default_value = "localhost")]
        host: String,
    },
    /// Run every derivation until nothing is dirty, through the writer, and
    /// report counts and rates. The daemon's driver, run to completion.
    Derive {
        #[arg(long)]
        database: String,
        #[arg(long, default_value = "localhost")]
        host: String,
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
    /// Read stored materials again with the current parsers and rules
    /// (architecture §5.5).
    Replay {
        #[command(flatten)]
        at: Store,
        /// A material to re-read; repeat for more.
        #[arg(long = "material")]
        materials: Vec<uuid::Uuid>,
        /// Every material of a binding (`atuin@host`).
        #[arg(long)]
        binding: Option<String>,
        /// Only readings of this event type; repeat for more.
        #[arg(long = "event-type")]
        event_types: Vec<String>,
        /// Only readings at or after this time (RFC 3339).
        #[arg(long, value_parser = timestamp)]
        from: Option<sinex_model::Timestamp>,
        /// Only readings before this time (RFC 3339).
        #[arg(long, value_parser = timestamp)]
        until: Option<sinex_model::Timestamp>,
        /// Show what the replay would change and keep nothing.
        #[arg(long)]
        compare: bool,
        /// Continue an interrupted replay, skipping the chains it finished.
        #[arg(long, conflicts_with_all = ["materials", "binding", "event_types", "from", "until"])]
        resume: Option<uuid::Uuid>,
        /// List every change, not only the first few of each kind.
        #[arg(long)]
        all: bool,
    },
    /// Take a record's readings out of views, counts and re-imports.
    Hide {
        #[command(flatten)]
        at: Store,
        #[arg(long)]
        event: uuid::Uuid,
    },
    /// Put a hidden record back.
    Unhide {
        #[command(flatten)]
        at: Store,
        #[arg(long)]
        event: uuid::Uuid,
    },
    /// Permanently delete a record (or a whole material) and its bytes.
    Excise {
        #[command(flatten)]
        at: Store,
        #[arg(
            long,
            required_unless_present = "material",
            conflicts_with = "material"
        )]
        event: Option<uuid::Uuid>,
        #[arg(long)]
        material: Option<uuid::Uuid>,
        /// Report exactly what would be removed, and remove nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Recompute a keyed derivation's outputs. With `--compare`, compute into
    /// the comparison lane and show old against new without writing anything.
    Recompute {
        #[arg(long)]
        database: String,
        derivation: String,
        #[arg(long)]
        compare: bool,
        /// Only keys touched by inputs at or after this time (RFC 3339).
        #[arg(long, value_parser = timestamp)]
        from: Option<sinex_model::Timestamp>,
        /// Only keys touched by inputs before this time (RFC 3339).
        #[arg(long, value_parser = timestamp)]
        until: Option<sinex_model::Timestamp>,
        /// Examples of each kind of difference to show.
        #[arg(long, default_value_t = 5)]
        examples: usize,
        #[arg(long, default_value = "localhost")]
        host: String,
    },
}

#[derive(clap::Args)]
struct Store {
    #[arg(long)]
    database: String,
    /// Content store directory.
    #[arg(long)]
    store: PathBuf,
}

fn timestamp(s: &str) -> Result<sinex_model::Timestamp, String> {
    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
        .map(sinex_model::Timestamp)
        .map_err(|e| format!("not an RFC 3339 time: {e}"))
}

/// Who an admin command acts as, for the operation log.
const ACTOR: &str = "operator";

async fn hide(at: Store, event: sinex_model::EventId, undo: bool) -> Result<(), String> {
    let ctx = context::Context::open(&at.database, &at.store).await?;
    let result = operate::hide(&ctx, event, undo, ACTOR).await;
    ctx.close().await?;
    result?;
    println!("{} {event}", if undo { "unhidden" } else { "hidden" });
    Ok(())
}

async fn admin(command: Admin) -> Result<(), String> {
    use sinex_model::{EventId, MaterialId, OperationId};
    use sinex_store::excision::Target;
    match command {
        Admin::Schema { database } => sinex_schema::apply_url(&database)
            .await
            .map(|applied| println!("{applied}"))
            .map_err(|e| e.to_string()),
        Admin::Replay {
            at,
            materials,
            binding,
            event_types,
            from,
            until,
            compare,
            resume,
            all,
        } => {
            let mut ctx = context::Context::open(&at.database, &at.store).await?;
            let report = replay::run(
                &mut ctx,
                replay::Options {
                    scope: replay::Scope {
                        materials,
                        binding,
                        event_types: (!event_types.is_empty()).then_some(event_types),
                        from,
                        until,
                    },
                    compare,
                    resume: resume.map(OperationId),
                    actor: ACTOR.into(),
                },
            )
            .await;
            ctx.close().await?;
            let report = report?;
            if compare {
                println!("compare only: nothing was changed");
            }
            println!("{report}");
            print!("{}", replay::describe(&report.diff, all));
            Ok(())
        }
        Admin::Hide { at, event } => hide(at, EventId(event), false).await,
        Admin::Unhide { at, event } => hide(at, EventId(event), true).await,
        Admin::Excise {
            at,
            event,
            material,
            dry_run,
        } => {
            let target = match (event, material) {
                (Some(e), _) => Target::Event(EventId(e)),
                (None, Some(m)) => Target::Material(MaterialId(m)),
                (None, None) => unreachable!("clap requires one"),
            };
            let mut ctx = context::Context::open(&at.database, &at.store).await?;
            let result = if dry_run {
                operate::plan(&mut ctx, target)
                    .await
                    .map(|plan| format!("dry run: {}", operate::describe(&plan)))
            } else {
                operate::excise(&mut ctx, target, ACTOR).await
            };
            ctx.close().await?;
            println!("{}", result?);
            Ok(())
        }
        Admin::Recompute {
            database,
            derivation,
            compare,
            from,
            until,
            examples,
            host,
        } => {
            let range = (from.is_some() || until.is_some()).then(|| {
                (
                    from.unwrap_or(sinex_model::Timestamp(time::OffsetDateTime::UNIX_EPOCH)),
                    until.unwrap_or_else(sinex_model::Timestamp::now),
                )
            });
            let (mut driver, writer_task) = local_driver(&database, &host).await?;
            let result = if compare {
                driver
                    .compare(&derivation, range, examples)
                    .await
                    .map(|cmp| cmp.to_string())
            } else {
                driver
                    .recompute_all(&derivation, range)
                    .await
                    .map(|n| format!("{derivation}: {n} keys marked for recompute\n"))
            };
            driver.end_runs().await?;
            drop(driver);
            writer_task.await.map_err(|e| e.to_string())?;
            print!("{}", result?);
            Ok(())
        }
    }
}

/// A writer and a derivation driver for a one-shot command.
async fn local_driver(
    database: &str,
    host: &str,
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
    let driver = derive::Driver::start(
        read,
        handle,
        sinex_automata::keyed(),
        sinex_automata::per_event(),
        host,
    )
    .await?;
    Ok((driver, task))
}

async fn run_derive(database: String, host: String) -> Result<(), String> {
    let (mut driver, writer_task) = local_driver(&database, &host).await?;
    let started = std::time::Instant::now();
    driver.settle().await?;
    let wall = started.elapsed();
    for (name, counts) in &driver.counts {
        println!("{name:18} {counts}");
    }
    let outputs: usize = driver.counts.values().map(|c| c.outputs).sum();
    let inputs: usize = driver.counts.values().map(|c| c.inputs).sum();
    println!(
        "wall {:.2}s | inputs read {inputs} | outputs {outputs} | {:.0} outputs/s",
        wall.as_secs_f64(),
        outputs as f64 / wall.as_secs_f64().max(1e-9)
    );
    driver.end_runs().await?;
    drop(driver);
    writer_task.await.map_err(|e| e.to_string())
}

async fn publish(database: &str, nats: &str, subjects: &str) -> Result<(), String> {
    use sqlx::Connection;
    let mut conn = sqlx::PgConnection::connect(database)
        .await
        .map_err(|e| format!("database: {e}"))?;
    let client = async_nats::connect(nats)
        .await
        .map_err(|e| format!("NATS at {nats}: {e}"))?;
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
        } => match api_options(socket, listen, tokens, zone, &day_start) {
            Ok(api) => {
                daemon::run(daemon::Options {
                    database,
                    store,
                    nats,
                    subjects,
                    host,
                    api,
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
            atuin,
            zsh,
            hyprland_spool,
            journald_spool,
            file_watch_spool,
            activitywatch,
            repositories,
            host,
        } => {
            backfill::run(backfill::Options {
                database,
                store,
                atuin,
                zsh,
                hyprland_spool,
                journald_spool,
                file_watch_spool,
                activitywatch,
                repositories,
                host,
            })
            .await
        }
        Command::Derive { database, host } => run_derive(database, host).await,
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sinexd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}

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
#[cfg(test)]
mod library_tests;
mod media_text;
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
        /// An export or directory source as `kind=path`: chromium (a browser's
        /// user-data directory), takeout-chrome, spotify, raindrop (a `.csv`
        /// or `.jsonl` export), mbox (a file or a directory of files), notes,
        /// maildir (a directory), weechat (a log file or directory), takeout
        /// (a Google Takeout archive or a directory of them: My Activity,
        /// YouTube history, Keep, Tasks, Play Store, Purchases, Calendar),
        /// calendar (an `.ics` file, or archives holding them), sinnix-lake
        /// (the capture lake's root, `/realm`).
        #[arg(long = "source")]
        sources: Vec<String>,
        /// The zone WeeChat logs and floating calendar times were written in
        /// (IANA name); the system zone when unset.
        #[arg(long)]
        zone: Option<String>,
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
    /// Print every source and derivation as JSON, for the NixOS module's
    /// generated options (`nix/catalog.json`).
    Catalog,
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
    /// Comparisons kept by `recompute --compare`, newest first.
    Comparisons {
        #[arg(long)]
        database: String,
        /// Only comparisons of this derivation.
        derivation: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: i64,
    },
    /// Run a media-library command without the daemon (architecture §6.6):
    /// `library-add PATH`, `library-item ID`, `library-tag ID TAG`, and the
    /// rest of `sinexctl`'s `library-*` commands. Positional arguments go in
    /// the command's order; options as `name=value`.
    Library {
        #[command(flatten)]
        at: Store,
        #[arg(long, default_value = "localhost")]
        host: String,
        command: String,
        args: Vec<String>,
    },
    /// Fetch pages from stored browser history whose host is on the allow
    /// list and that were not fetched yet (`page.fetched`).
    Crawl {
        #[command(flatten)]
        at: Store,
        #[arg(long, default_value = "localhost")]
        host: String,
        /// A domain whose pages may be fetched (and its subdomains); repeatable.
        /// Nothing is fetched without one.
        #[arg(long = "allow")]
        allow: Vec<String>,
        /// Fetch these URLs instead of the stored history; repeatable.
        #[arg(long = "url")]
        urls: Vec<String>,
        #[arg(long)]
        limit: Option<usize>,
        /// Seconds between two requests to the same host.
        #[arg(long, default_value_t = 2)]
        pause: u64,
    },
    /// Read text from screen segments not yet read (`screen.text`) with the
    /// local OCR engine.
    MediaText {
        #[command(flatten)]
        at: Store,
        #[arg(long, default_value = "localhost")]
        host: String,
        /// Read at most this many segments.
        #[arg(long)]
        limit: Option<u64>,
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
        Admin::Catalog => {
            print!("{}", catalog::render());
            Ok(())
        }
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
        Admin::Crawl {
            at,
            host,
            allow,
            urls,
            limit,
            pause,
        } => {
            let mut ctx = context::Context::open(&at.database, &at.store).await?;
            let t = crawl::run(
                &mut ctx,
                &host,
                allow,
                urls,
                limit,
                std::time::Duration::from_secs(pause),
            )
            .await?;
            println!("crawl  {t}");
            Ok(())
        }
        Admin::MediaText { at, host, limit } => {
            let mut ctx = context::Context::open(&at.database, &at.store).await?;
            let t = media_text::run(&mut ctx.aux, &ctx.blobs, &ctx.writer, &host, limit).await?;
            println!(
                "screen text: {} segments read, {} failed, {:.1}s",
                t.read, t.failed, t.seconds
            );
            Ok(())
        }
        Admin::Library {
            at,
            host,
            command,
            args,
        } => {
            let spec = sinex_protocol::command(&command)
                .filter(|c| c.name.starts_with("library-"))
                .ok_or_else(|| format!("no library command `{command}`"))?;
            let mut map = serde_json::Map::new();
            let mut positional = spec.args.iter().filter(|a| a.positional);
            for arg in args {
                let (name, value) = match arg.split_once('=') {
                    Some((n, v)) if spec.args.iter().any(|a| a.name == n) => {
                        (n.to_owned(), v.to_owned())
                    }
                    _ => {
                        let a = positional.next().ok_or_else(|| {
                            format!("`{command}` takes no more positional arguments: {arg}")
                        })?;
                        (a.name.to_owned(), arg)
                    }
                };
                let kind = spec.args.iter().find(|a| a.name == name).map(|a| a.kind);
                let value = match kind {
                    Some(sinex_protocol::ArgKind::Bool) => serde_json::Value::Bool(value == "true"),
                    _ => serde_json::Value::String(value),
                };
                map.insert(name, value);
            }
            let request = sinex_protocol::Request::from_args(&command, map)?;
            let mut ctx = context::Context::open(&at.database, &at.store).await?;
            let writer = ctx.writer.clone();
            let blobs = ctx.blobs.clone();
            let result =
                library::dispatch(&mut ctx.aux, Some(&writer), &blobs, &host, request, ACTOR).await;
            drop(writer);
            ctx.close().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&result?).expect("JSON values print")
            );
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
        Admin::Comparisons {
            database,
            derivation,
            limit,
        } => {
            use sqlx::Connection;
            let mut conn = sqlx::PgConnection::connect(&database)
                .await
                .map_err(|e| e.to_string())?;
            let kept = sinex_store::derived::comparisons(&mut conn, derivation.as_deref(), limit)
                .await
                .map_err(|e| e.to_string())?;
            let show = |t: sinex_model::Timestamp| {
                t.0.format(&time::format_description::well_known::Rfc3339)
                    .unwrap_or_default()
            };
            for c in kept {
                let range = match c.range {
                    Some((from, until)) => format!(
                        " | inputs {} .. {}",
                        from.map_or("start".into(), show),
                        until.map_or("now".into(), show)
                    ),
                    None => String::new(),
                };
                println!(
                    "{} {} v{} at {}{range}: keys {} | unchanged {} | added {} | changed {} | removed {}",
                    c.id,
                    c.derivation,
                    c.version,
                    show(c.made_at),
                    c.keys,
                    c.unchanged,
                    c.added,
                    c.changed,
                    c.removed
                );
            }
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
            sources,
            zone,
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
                sources,
                zone,
                host,
            })
            .await
        }
        Command::Derive { database, host } => run_derive(database, host).await,
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

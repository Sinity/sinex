//! `sinex-session`: sources that need the operator's graphical session
//! (Hyprland socket and plugin, session D-Bus, clipboard, accessibility, MPRIS,
//! PipeWire audio, screen capture; home-directory sources). An outside
//! producer of `sinexd` over the NATS ingress (architecture §7.3).
//!
//! Capture and publishing are separate steps with the spool between them:
//! capture writes what arrives to disk first, so nothing is lost while the
//! daemon or the bus is away, and publishing drains the spool through the
//! same drivers and parsers `sinexd` uses.
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(name = "sinex-session", about = "Session-bound Sinex sources")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Capture one live source into its spool until it ends (or for a while).
    Capture {
        #[command(subcommand)]
        source: Live,
    },
    /// Capture the session's sources and produce them to `sinexd` over the
    /// NATS ingress, until stopped.
    Run(Run),
}

#[derive(clap::Args)]
struct Run {
    /// The NATS server, with this producer's credentials in the URL when the
    /// server requires them.
    #[arg(long)]
    nats: String,
    /// Subject root (`sinex` unless several deployments share a server).
    #[arg(long, default_value = "sinex")]
    subjects: String,
    /// This producer's name, as its credentials allow.
    #[arg(long)]
    producer: String,
    /// The content store `sinexd` uses on this host.
    #[arg(long)]
    store: PathBuf,
    /// Where spools and positions live.
    #[arg(long)]
    state: PathBuf,
    #[arg(long, default_value = "localhost")]
    host: String,
    /// Capture Hyprland's event socket.
    #[arg(long)]
    hyprland: bool,
    /// A directory whose file changes to capture; repeatable.
    #[arg(long = "watch")]
    watch: Vec<PathBuf>,
    /// Seconds between productions of what was captured.
    #[arg(long, default_value_t = 2)]
    every: u64,
    /// Stop after this many seconds.
    #[arg(long)]
    seconds: Option<u64>,
}

#[derive(Subcommand)]
enum Live {
    /// Hyprland's event socket.
    Hyprland {
        #[arg(long)]
        spool: PathBuf,
        /// Stop after this many seconds; capture until the socket closes otherwise.
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// Changes to files under the given directories.
    Files {
        #[arg(long)]
        spool: PathBuf,
        /// A directory to watch; repeatable.
        #[arg(long = "root", required = true)]
        roots: Vec<PathBuf>,
        /// A directory name not to watch; repeatable. Defaults to `.git`,
        /// `target`, `node_modules`, `.direnv`, `__pycache__`.
        #[arg(long = "skip")]
        skip: Vec<String>,
        /// Stop after this many seconds; watch until stopped otherwise.
        #[arg(long)]
        seconds: Option<u64>,
    },
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run(run) => {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            match runtime.block_on(produce(run)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("sinex-session: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Capture {
            source:
                Live::Files {
                    spool,
                    roots,
                    skip,
                    seconds,
                },
        } => {
            use sinex_sources::sources::file_watch;
            let skip = if skip.is_empty() {
                file_watch::SKIP.iter().map(|s| (*s).to_owned()).collect()
            } else {
                skip
            };
            match file_watch::capture(
                &file_watch::Roots { roots, skip },
                &spool,
                seconds.map(Duration::from_secs),
            ) {
                Ok(n) => {
                    println!("captured {n} changes");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("sinex-session: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Capture {
            source: Live::Hyprland { spool, seconds },
        } => {
            let Some(socket) = sinex_sources::sources::hyprland::socket_path() else {
                eprintln!(
                    "sinex-session: no Hyprland instance in this environment (HYPRLAND_INSTANCE_SIGNATURE or XDG_RUNTIME_DIR unset)"
                );
                return ExitCode::FAILURE;
            };
            let capture = std::thread::spawn(move || {
                sinex_sources::sources::hyprland::capture(&socket, &spool)
            });
            if let Some(seconds) = seconds {
                std::thread::sleep(Duration::from_secs(seconds));
                // Everything captured is already written to the spool file;
                // leaving the reader mid-wait loses nothing.
                println!("captured for {seconds}s");
                return ExitCode::SUCCESS;
            }
            match capture.join() {
                Ok(Ok(lines)) => {
                    println!("socket closed after {lines} lines");
                    ExitCode::SUCCESS
                }
                Ok(Err(e)) => {
                    eprintln!("sinex-session: {e}");
                    ExitCode::FAILURE
                }
                Err(_) => ExitCode::FAILURE,
            }
        }
    }
}

/// A captured source and where it is produced from.
struct Binding {
    source: Box<dyn sinex_sources::Source>,
    name: String,
    driver: sinex_sources::drivers::live_stream::LiveStream,
}

/// Capture threads write spools; this loop produces what they wrote.
async fn produce(run: Run) -> Result<(), String> {
    use sinex_sources::drivers::producer::{Position, Producer, Store};
    use sinex_sources::sources::{file_watch, hyprland};

    let spools = run.state.join("spool");
    let positions = run.state.join("positions");
    let store = Store::open(&run.store).map_err(|e| format!("content store: {e}"))?;
    let mut producer = Producer::connect(
        &run.nats,
        sinex_protocol::Subjects::new(run.subjects.clone()),
        &run.producer,
    )
    .await
    .map_err(|e| e.to_string())?;

    let mut bindings = Vec::new();
    if run.hyprland {
        let socket = hyprland::socket_path().ok_or("no Hyprland instance in this environment")?;
        let spool = spools.join("hyprland");
        let s = spool.clone();
        std::thread::spawn(move || {
            if let Err(e) = hyprland::capture(&socket, &s) {
                eprintln!("sinex-session: Hyprland capture: {e}");
            }
        });
        bindings.push(Binding {
            source: Box::new(hyprland::source()),
            name: format!("hyprland@{}", run.host),
            driver: hyprland::driver(spool, run.host.clone()),
        });
    }
    if !run.watch.is_empty() {
        let spool = spools.join("files");
        let (s, roots) = (spool.clone(), run.watch.clone());
        std::thread::spawn(move || {
            let roots = file_watch::Roots {
                roots,
                skip: file_watch::SKIP.iter().map(|s| (*s).to_owned()).collect(),
            };
            if let Err(e) = file_watch::capture(&roots, &s, None) {
                eprintln!("sinex-session: file watch: {e}");
            }
        });
        bindings.push(Binding {
            source: Box::new(file_watch::source()),
            name: format!("file-watch@{}", run.host),
            driver: file_watch::driver(spool, run.host.clone()),
        });
    }
    if bindings.is_empty() {
        return Err("nothing to capture: pass --hyprland and/or --watch".into());
    }

    let deadline = run
        .seconds
        .map(|s| std::time::Instant::now() + Duration::from_secs(s));
    let private = sinex_protocol::private_mode::PrivateMode::default();
    loop {
        // Private mode: capture nothing until it is off again.
        if private.on() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        for b in &bindings {
            // Drain everything captured so far, one material at a time.
            loop {
                let position = producer
                    .position(&b.name, &positions)
                    .await
                    .map_err(|e| e.to_string())?;
                let acquired = b.driver.poll(&position.cursor).map_err(|e| e.to_string())?;
                let had_material = acquired.material.is_some();
                let sent = producer
                    .produce(b.source.as_ref(), &b.name, acquired, &store, &positions)
                    .await
                    .map_err(|e| e.to_string())?;
                if had_material {
                    println!(
                        "{}: {} events in {} frames, {} admitted, {} unread",
                        b.name, sent.events, sent.frames, sent.admitted, sent.unread
                    );
                }
                let committed = Position::load(&positions, &b.name).map_err(|e| e.to_string())?;
                b.driver
                    .remove_passed(&committed.cursor)
                    .map_err(|e| e.to_string())?;
                if !had_material {
                    break;
                }
            }
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(run.every)).await;
    }
}

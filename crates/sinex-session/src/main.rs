//! `sinex-session`: live sources that need the operator's graphical session
//! (the Hyprland socket, notifications and MPRIS on the session D-Bus, the
//! clipboard, the editor and terminal integration socket, file changes,
//! screen stills and PipeWire audio). An outside producer of `sinexd` over the
//! NATS ingress; files the operator's programs keep (shell histories,
//! databases, notes) are read by `sinexd`.
//!
//! Capture and publishing are separate steps with the spool between them:
//! capture writes what arrives to disk first, so nothing is lost while the
//! daemon or the bus is away, and publishing drains the spool through the
//! same drivers and parsers `sinexd` uses.
use clap::{Parser, Subcommand};
use sinex_sources::live;
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
    Capture(Live),
    /// Capture the session's sources and produce them to `sinexd` over the
    /// NATS ingress, until stopped.
    Run(Run),
    /// Print one clipboard frame for the content on standard input (run by
    /// `wl-paste --watch`).
    #[command(hide = true)]
    ClipboardFrame {
        #[arg(long, default_value = "clipboard")]
        selection: String,
    },
}

#[derive(clap::Args)]
struct Run {
    /// The NATS server, with this producer's user in the URL when the server
    /// requires one (`nats://NAME@HOST:PORT`).
    #[arg(long)]
    nats: String,
    /// That user's password, read from this file.
    #[arg(long)]
    nats_password_file: Option<PathBuf>,
    /// Subject root (`sinex` unless several deployments share a server).
    #[arg(long, default_value = "sinex")]
    subject_root: String,
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
    /// Host inputs, settings and source exclusions.
    #[arg(long)]
    sources: Option<PathBuf>,
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

#[derive(clap::Args)]
struct Live {
    /// The live format (`hyprland`, `notifications`, `players`,
    /// `clipboard`, `producers`, `media`, `file-watch`).
    format: String,
    #[arg(long)]
    spool: PathBuf,
    /// A source option as `key=value` (`selection=primary`, `kind=screen`,
    /// `every=60`, `roots=/home/me`); repeatable.
    #[arg(long = "option")]
    options: Vec<String>,
    /// Stop after this many seconds; capture until the source ends otherwise.
    #[arg(long)]
    seconds: Option<u64>,
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Run(run) => {
            let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
            match runtime.block_on(produce(run)) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(e),
            }
        }
        Command::ClipboardFrame { selection } => {
            match sinex_sources::sources::clipboard::print_frame(&selection) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(e.to_string()),
            }
        }
        Command::Capture(live) => match tokio::runtime::Runtime::new()
            .expect("tokio runtime")
            .block_on(capture(live))
        {
            Ok(n) => {
                println!("captured {n}");
                ExitCode::SUCCESS
            }
            Err(e) => fail(e),
        },
    }
}

fn fail(e: String) -> ExitCode {
    eprintln!("sinex-session: {e}");
    ExitCode::FAILURE
}

fn secs(s: Option<u64>) -> Option<Duration> {
    s.map(Duration::from_secs)
}

async fn capture(live: Live) -> Result<u64, String> {
    let shutdown = sinex_source_core::capture::shutdown_signal().map_err(|e| e.to_string())?;
    let stop = std::sync::Arc::new(sinex_source_core::capture::CaptureStop::new(secs(
        live.seconds,
    )));
    let task_stop = stop.clone();
    let mut task = tokio::task::spawn_blocking(move || capture_source(live, &task_stop));
    let result = tokio::select! {
        result = &mut task => result.map_err(|e| e.to_string())?,
        _ = shutdown => {
            stop.stop();
            task.await.map_err(|e| e.to_string())?
        }
    };
    stop.stop();
    result
}

fn capture_source(
    live: Live,
    stop: &sinex_source_core::capture::CaptureStop,
) -> Result<u64, String> {
    let (format, binding) = live::binding(
        &live.format,
        &live.spool,
        "localhost",
        live::options(&live.options)?,
    )?;
    (format.capture.expect("live formats capture"))(&binding, stop)
}

fn requested(
    run: &Run,
) -> Result<
    Vec<(
        &'static sinex_source_core::InputFormat,
        sinex_source_core::Binding,
    )>,
    String,
> {
    use sinex_sources::discovery::{self, Configuration, DiscoveryContext, Owner};
    let configuration = run
        .sources
        .as_deref()
        .map(Configuration::load)
        .transpose()?
        .unwrap_or_default();
    let mut context = DiscoveryContext::for_process(&run.host, run.state.join("spool"));
    context.sources = configuration.sources;
    context.exclude = configuration.exclude;
    context.watch = run.watch.clone();
    context.watch.extend(configuration.discovery.watch);
    discovery::captures(&context, Owner::Session)
}

/// Capture threads write spools; the shared loop produces what they wrote.
async fn produce(run: Run) -> Result<(), String> {
    use sinex_source_core::drivers::producer::{Producer, Store};

    requested(&run)?;
    let store = Store::open(&run.store).map_err(|e| format!("content store: {e}"))?;
    let mut producer = Producer::connect(
        &run.nats,
        run.nats_password_file.as_deref(),
        sinex_protocol::Subjects::new(run.subject_root.clone()),
        &run.producer,
    )
    .await
    .map_err(|e| e.to_string())?;
    let until = run
        .seconds
        .map(|s| std::time::Instant::now() + Duration::from_secs(s));
    live::run(
        &mut producer,
        &store,
        &run.state.join("positions"),
        || requested(&run),
        Duration::from_secs(run.every),
        until,
    )
    .await
}

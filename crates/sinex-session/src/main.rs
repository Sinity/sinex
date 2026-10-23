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
use serde_json::Value;
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
    /// Capture desktop notifications.
    #[arg(long)]
    notifications: bool,
    /// Capture media players (MPRIS).
    #[arg(long)]
    players: bool,
    /// Capture the clipboard and the primary selection.
    #[arg(long)]
    clipboard: bool,
    /// Listen on the producer socket for the editor and terminal integrations.
    #[arg(long)]
    producers: bool,
    /// Take a still of every output this often, in seconds.
    #[arg(long)]
    screen_every: Option<u64>,
    /// Record the default audio source in segments of this many seconds.
    #[arg(long)]
    audio_segment: Option<u64>,
    /// Seconds between productions of what was captured.
    #[arg(long, default_value_t = 2)]
    every: u64,
    /// Stop after this many seconds.
    #[arg(long)]
    seconds: Option<u64>,
}

#[derive(clap::Args)]
struct Live {
    /// The kind of live source (`hyprland`, `notifications`, `players`,
    /// `clipboard`, `producers`, `media`, `file-watch`).
    kind: String,
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
    let shutdown = sinex_sources::capture::shutdown_signal().map_err(|e| e.to_string())?;
    let control = std::sync::Arc::new(sinex_sources::capture::Control::new(secs(live.seconds)));
    let stop = control.clone();
    let mut task = tokio::task::spawn_blocking(move || capture_source(live, &stop));
    let result = tokio::select! {
        result = &mut task => result.map_err(|e| e.to_string())?,
        _ = shutdown => {
            control.stop();
            task.await.map_err(|e| e.to_string())?
        }
    };
    control.stop();
    result
}

fn capture_source(live: Live, control: &sinex_sources::capture::Control) -> Result<u64, String> {
    let (kind, binding) = live::binding(
        &live.kind,
        &live.spool,
        "localhost",
        live::options(&live.options)?,
    )?;
    (kind.capture.expect("live kinds capture"))(&binding, control)
}

/// The live sources `run` asks for, as kinds and their options.
fn requested(run: &Run) -> Vec<(&'static str, &'static str, Value)> {
    let mut out: Vec<(&'static str, &'static str, Value)> = Vec::new();
    if run.hyprland {
        out.push(("hyprland", "hyprland", Value::Null));
    }
    if !run.watch.is_empty() {
        let roots: Vec<Value> = run
            .watch
            .iter()
            .map(|p| p.display().to_string().into())
            .collect();
        out.push(("file-watch", "files", serde_json::json!({ "roots": roots })));
    }
    if run.notifications {
        out.push(("notifications", "notifications", Value::Null));
    }
    if run.players {
        out.push(("players", "players", Value::Null));
    }
    if run.clipboard {
        out.push((
            "clipboard",
            "clipboard",
            serde_json::json!({"selection": "clipboard"}),
        ));
        out.push((
            "clipboard",
            "primary",
            serde_json::json!({"selection": "primary"}),
        ));
    }
    if run.producers {
        out.push(("producers", "producers", Value::Null));
    }
    if let Some(every) = run.screen_every {
        out.push((
            "media",
            "screen",
            serde_json::json!({"kind": "screen", "every": every}),
        ));
    }
    if let Some(segment) = run.audio_segment {
        out.push((
            "media",
            "audio",
            serde_json::json!({"kind": "audio", "segment": segment}),
        ));
    }
    out
}

/// Capture threads write spools; the shared loop produces what they wrote.
async fn produce(run: Run) -> Result<(), String> {
    use sinex_sources::drivers::producer::{Producer, Store};

    let spools = run.state.join("spool");
    let store = Store::open(&run.store).map_err(|e| format!("content store: {e}"))?;
    let mut producer = Producer::connect(
        &run.nats,
        sinex_protocol::Subjects::new(run.subjects.clone()),
        &run.producer,
    )
    .await
    .map_err(|e| e.to_string())?;
    let bindings = requested(&run)
        .into_iter()
        .map(|(kind, spool, options)| live::binding(kind, &spools.join(spool), &run.host, options))
        .collect::<Result<Vec<_>, _>>()?;
    if bindings.is_empty() {
        return Err("nothing to capture: pass at least one source flag".into());
    }
    let until = run
        .seconds
        .map(|s| std::time::Instant::now() + Duration::from_secs(s));
    live::run(
        &mut producer,
        &store,
        &run.state.join("positions"),
        bindings,
        Duration::from_secs(run.every),
        until,
    )
    .await
}

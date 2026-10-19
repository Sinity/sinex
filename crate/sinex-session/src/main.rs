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
    /// Desktop notifications on the session bus.
    Notifications {
        #[arg(long)]
        spool: PathBuf,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// Media players (MPRIS) on the session bus.
    Players {
        #[arg(long)]
        spool: PathBuf,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// The clipboard (or, with `--primary`, the primary selection).
    Clipboard {
        #[arg(long)]
        spool: PathBuf,
        #[arg(long)]
        primary: bool,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// Lines the editor and terminal integrations write to the producer socket.
    Producers {
        #[arg(long)]
        spool: PathBuf,
        /// The socket; `$SINEX_SESSION_SOCKET` or `$XDG_RUNTIME_DIR/sinex/session.sock` by default.
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// Stills of every output at an interval.
    Screen {
        #[arg(long)]
        spool: PathBuf,
        /// Seconds between stills.
        #[arg(long, default_value_t = 60)]
        every: u64,
        #[arg(long)]
        seconds: Option<u64>,
    },
    /// The default audio source, in segments.
    Audio {
        #[arg(long)]
        spool: PathBuf,
        /// Seconds per segment.
        #[arg(long, default_value_t = 300)]
        segment: u64,
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
                Err(e) => fail(e),
            }
        }
        Command::ClipboardFrame { selection } => {
            match sinex_sources::sources::clipboard::print_frame(&selection) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => fail(e.to_string()),
            }
        }
        Command::Capture { source } => match capture(source) {
            Ok(message) => {
                println!("{message}");
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

/// The command `wl-paste --watch` runs: this binary's frame printer.
fn clipboard_helper(selection: &str) -> Vec<std::ffi::OsString> {
    let exe = std::env::current_exe().unwrap_or_else(|_| "sinex-session".into());
    vec![exe.into(), "clipboard-frame".into(), "--selection".into(), selection.into()]
}

fn capture(source: Live) -> Result<String, String> {
    use sinex_sources::sources::{clipboard, file_watch, media_capture, mpris, notifications, session_socket};
    let e = |e: std::io::Error| e.to_string();
    Ok(match source {
        Live::Files { spool, roots, skip, seconds } => {
            let skip = if skip.is_empty() {
                file_watch::SKIP.iter().map(|s| (*s).to_owned()).collect()
            } else {
                skip
            };
            let n = file_watch::capture(&file_watch::Roots { roots, skip }, &spool, secs(seconds)).map_err(|e| e.to_string())?;
            format!("captured {n} changes")
        }
        Live::Notifications { spool, seconds } => {
            format!("captured {} messages", notifications::capture(&spool, secs(seconds)).map_err(e)?)
        }
        Live::Players { spool, seconds } => {
            format!("captured {} lines", mpris::capture(&spool, secs(seconds)).map_err(e)?)
        }
        Live::Clipboard { spool, primary, seconds } => {
            let selection = if primary { "primary" } else { "clipboard" };
            let n = clipboard::capture(selection, &clipboard_helper(selection), &spool, secs(seconds)).map_err(e)?;
            format!("captured {n} {selection} changes")
        }
        Live::Producers { spool, socket, seconds } => {
            let socket = socket.or_else(session_socket::default_path).ok_or("no producer socket path (XDG_RUNTIME_DIR unset)")?;
            format!("captured {} lines", session_socket::capture(&socket, &spool, secs(seconds)).map_err(e)?)
        }
        Live::Screen { spool, every, seconds } => {
            let n = media_capture::capture_screen(&spool, Duration::from_secs(every), secs(seconds)).map_err(e)?;
            format!("captured {n} stills")
        }
        Live::Audio { spool, segment, seconds } => {
            let n = media_capture::capture_audio(&spool, Duration::from_secs(segment), secs(seconds)).map_err(e)?;
            format!("captured {n} segments")
        }
        Live::Hyprland { spool, seconds } => {
            let socket = sinex_sources::sources::hyprland::socket_path()
                .ok_or("no Hyprland instance in this environment (HYPRLAND_INSTANCE_SIGNATURE or XDG_RUNTIME_DIR unset)")?;
            let capture = std::thread::spawn(move || sinex_sources::sources::hyprland::capture(&socket, &spool));
            if let Some(seconds) = seconds {
                std::thread::sleep(Duration::from_secs(seconds));
                // Everything captured is already written to the spool file;
                // leaving the reader mid-wait loses nothing.
                return Ok(format!("captured for {seconds}s"));
            }
            match capture.join() {
                Ok(Ok(lines)) => format!("socket closed after {lines} lines"),
                Ok(Err(err)) => return Err(err.to_string()),
                Err(_) => return Err("capture thread panicked".into()),
            }
        }
    })
}

/// How a binding's spool is read: line streams or whole media segments.
enum Spool {
    Live(sinex_sources::drivers::live_stream::LiveStream),
    Media(sinex_sources::drivers::media::Media),
}

impl Spool {
    fn poll(&self, cursor: &serde_json::Value) -> Result<sinex_sources::drivers::Acquired, String> {
        match self {
            Spool::Live(d) => d.poll(cursor).map_err(|e| e.to_string()),
            Spool::Media(d) => d.poll(cursor).map_err(|e| e.to_string()),
        }
    }

    fn remove_passed(&self, committed: &serde_json::Value) -> Result<usize, String> {
        match self {
            Spool::Live(d) => d.remove_passed(committed),
            Spool::Media(d) => d.remove_passed(committed),
        }
        .map_err(|e| e.to_string())
    }
}

/// A captured source and where it is produced from.
struct Binding {
    source: Box<dyn sinex_sources::Source>,
    name: String,
    driver: Spool,
}

/// Start `capture` on its own thread; a capture that ends is reported, and
/// production of what it already spooled goes on.
fn spawn(name: &'static str, capture: impl FnOnce() -> Result<u64, String> + Send + 'static) {
    std::thread::spawn(move || {
        if let Err(e) = capture() {
            eprintln!("sinex-session: {name} capture: {e}");
        }
    });
}

/// Capture threads write spools; this loop produces what they wrote.
async fn produce(run: Run) -> Result<(), String> {
    use sinex_sources::drivers::producer::{Position, Producer, Store};
    use sinex_sources::sources::{
        clipboard, file_watch, hyprland, media_capture, mpris, notifications, session_socket,
    };

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

    let host = run.host.clone();
    let mut bindings = Vec::new();
    if run.hyprland {
        let socket = hyprland::socket_path().ok_or("no Hyprland instance in this environment")?;
        let spool = spools.join("hyprland");
        let s = spool.clone();
        spawn("Hyprland", move || hyprland::capture(&socket, &s).map_err(|e| e.to_string()));
        bindings.push(Binding {
            source: Box::new(hyprland::source()),
            name: format!("hyprland@{host}"),
            driver: Spool::Live(hyprland::driver(spool, host.clone())),
        });
    }
    if !run.watch.is_empty() {
        let spool = spools.join("files");
        let (s, roots) = (spool.clone(), run.watch.clone());
        spawn("file watch", move || {
            let roots = file_watch::Roots {
                roots,
                skip: file_watch::SKIP.iter().map(|s| (*s).to_owned()).collect(),
            };
            file_watch::capture(&roots, &s, None).map_err(|e| e.to_string())
        });
        bindings.push(Binding {
            source: Box::new(file_watch::source()),
            name: format!("file-watch@{host}"),
            driver: Spool::Live(file_watch::driver(spool, host.clone())),
        });
    }
    if run.notifications {
        let spool = spools.join("notifications");
        let s = spool.clone();
        spawn("notifications", move || notifications::capture(&s, None).map_err(|e| e.to_string()));
        bindings.push(Binding {
            source: Box::new(notifications::source()),
            name: format!("notifications@{host}"),
            driver: Spool::Live(notifications::driver(spool, host.clone())),
        });
    }
    if run.players {
        let spool = spools.join("players");
        let s = spool.clone();
        spawn("players", move || mpris::capture(&s, None).map_err(|e| e.to_string()));
        bindings.push(Binding {
            source: Box::new(mpris::source()),
            name: format!("players@{host}"),
            driver: Spool::Live(mpris::driver(spool, host.clone())),
        });
    }
    if run.clipboard {
        for selection in ["clipboard", "primary"] {
            let spool = spools.join(selection);
            let s = spool.clone();
            spawn("clipboard", move || {
                clipboard::capture(selection, &clipboard_helper(selection), &s, None).map_err(|e| e.to_string())
            });
            bindings.push(Binding {
                source: Box::new(clipboard::source()),
                name: format!("{selection}@{host}"),
                driver: Spool::Live(clipboard::driver(spool, host.clone(), selection)),
            });
        }
    }
    if run.producers {
        let socket = session_socket::default_path().ok_or("no producer socket path (XDG_RUNTIME_DIR unset)")?;
        let spool = spools.join("producers");
        let s = spool.clone();
        spawn("producer socket", move || session_socket::capture(&socket, &s, None).map_err(|e| e.to_string()));
        bindings.push(Binding {
            source: Box::new(session_socket::source()),
            name: format!("producers@{host}"),
            driver: Spool::Live(session_socket::driver(spool, host.clone())),
        });
    }
    if let Some(every) = run.screen_every {
        let spool = spools.join("screen");
        let s = spool.clone();
        spawn("screen", move || {
            media_capture::capture_screen(&s, Duration::from_secs(every), None).map_err(|e| e.to_string())
        });
        bindings.push(Binding {
            source: Box::new(media_capture::source()),
            name: format!("screen@{host}"),
            driver: Spool::Media(media_capture::driver(spool, host.clone(), "screen")),
        });
    }
    if let Some(segment) = run.audio_segment {
        let spool = spools.join("audio");
        let s = spool.clone();
        spawn("audio", move || {
            media_capture::capture_audio(&s, Duration::from_secs(segment), None).map_err(|e| e.to_string())
        });
        bindings.push(Binding {
            source: Box::new(media_capture::source()),
            name: format!("audio@{host}"),
            driver: Spool::Media(media_capture::driver(spool, host.clone(), "audio")),
        });
    }
    if bindings.is_empty() {
        return Err("nothing to capture: pass at least one source flag".into());
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
                let acquired = b.driver.poll(&position.cursor)?;
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
                b.driver.remove_passed(&committed.cursor)?;
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

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
}

fn main() -> ExitCode {
    match Cli::parse().command {
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

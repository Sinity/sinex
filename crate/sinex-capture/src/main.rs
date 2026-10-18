//! `sinex-capture`: the capture-only node for satellite hosts (`rkv.7`,
//! `1c5`; architecture §7.3). It runs the same drivers and sources as
//! `sinexd`, keeps a local spool, and produces to the one database's `sinexd`
//! over the NATS ingress with its own credentials. It has no database.
//!
//! The ingress registers a material only once its bytes are in `sinexd`'s
//! content store. A node that shares that store (a mount) works today; moving
//! the bytes themselves over the link is the part of the producer protocol
//! still to build, and the node says so when the store is missing.
use clap::Parser;
use sinex_sources::Source;
use sinex_sources::drivers::live_stream::LiveStream;
use sinex_sources::drivers::producer::{Position, Producer, Store};
use sinex_sources::sources::{file_watch, journald};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "sinex-capture",
    about = "Capture-only Sinex node for a satellite host"
)]
struct Cli {
    /// The NATS server, with this node's credentials in the URL.
    #[arg(long)]
    nats: String,
    #[arg(long, default_value = "sinex")]
    subjects: String,
    /// This node's producer name, as its credentials allow.
    #[arg(long)]
    producer: String,
    /// `sinexd`'s content store, as mounted on this host.
    #[arg(long)]
    store: PathBuf,
    /// Where spools and positions live.
    #[arg(long)]
    state: PathBuf,
    /// This host's name, recorded with every input.
    #[arg(long)]
    host: String,
    /// Capture the systemd journal.
    #[arg(long)]
    journald: bool,
    /// A directory whose file changes to capture; repeatable.
    #[arg(long = "watch")]
    watch: Vec<PathBuf>,
    #[arg(long, default_value_t = 2)]
    every: u64,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    if !cli.store.is_dir() {
        eprintln!(
            "sinex-capture: {} is not sinexd's content store; this node needs the store mounted (sending material bytes over the link is not built yet)",
            cli.store.display()
        );
        return ExitCode::FAILURE;
    }
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sinex-capture: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    let spools = cli.state.join("spool");
    let positions = cli.state.join("positions");
    let store = Store::open(&cli.store).map_err(|e| format!("content store: {e}"))?;
    let mut producer = Producer::connect(
        &cli.nats,
        sinex_protocol::Subjects::new(cli.subjects.clone()),
        &cli.producer,
    )
    .await
    .map_err(|e| e.to_string())?;

    let mut bindings: Vec<(Box<dyn Source>, String, LiveStream)> = Vec::new();
    if cli.journald {
        let spool = spools.join("journald");
        let s = spool.clone();
        std::thread::spawn(move || {
            if let Err(e) = journald::capture(&s, true, journald::Start::Beginning) {
                eprintln!("sinex-capture: journald: {e}");
            }
        });
        bindings.push((
            Box::new(journald::source()),
            format!("journald@{}", cli.host),
            journald::driver(spool, cli.host.clone()),
        ));
    }
    if !cli.watch.is_empty() {
        let spool = spools.join("files");
        let (s, roots) = (spool.clone(), cli.watch.clone());
        std::thread::spawn(move || {
            let roots = file_watch::Roots {
                roots,
                skip: file_watch::SKIP.iter().map(|s| (*s).to_owned()).collect(),
            };
            if let Err(e) = file_watch::capture(&roots, &s, None) {
                eprintln!("sinex-capture: file watch: {e}");
            }
        });
        bindings.push((
            Box::new(file_watch::source()),
            format!("file-watch@{}", cli.host),
            file_watch::driver(spool, cli.host.clone()),
        ));
    }
    if bindings.is_empty() {
        return Err("nothing to capture: pass --journald and/or --watch".into());
    }
    loop {
        for (source, binding, driver) in &bindings {
            loop {
                let position = Position::load(&positions, binding).map_err(|e| e.to_string())?;
                let acquired = driver.poll(&position.cursor).map_err(|e| e.to_string())?;
                let had_material = acquired.material.is_some();
                producer
                    .produce(source.as_ref(), binding, acquired, &store, &positions)
                    .await
                    .map_err(|e| e.to_string())?;
                let committed = Position::load(&positions, binding).map_err(|e| e.to_string())?;
                driver
                    .remove_passed(&committed.cursor)
                    .map_err(|e| e.to_string())?;
                if !had_material {
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(cli.every)).await;
    }
}

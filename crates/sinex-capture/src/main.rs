//! `sinex-capture`: the capture-only node for satellite hosts. It runs the same
//! drivers and sources as `sinexd`, keeps a local spool, and produces to the
//! one database's `sinexd` over the NATS ingress with its own credentials. It
//! has no database.
//!
//! The ingress registers a material only once its bytes are in `sinexd`'s
//! content store. A node that mounts that store places them there directly;
//! with `--upload`, the node keeps its own store and sends each material's
//! bytes over the link in chunk frames first.
use clap::Parser;
use sinex_source_core::drivers::producer::Store;
use sinex_sources::live;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

#[derive(Parser)]
#[command(
    name = "sinex-capture",
    about = "Capture-only Sinex node for a satellite host"
)]
struct Cli {
    /// The NATS server, with this node's user in the URL
    /// (`nats://NAME@HOST:PORT`).
    #[arg(long)]
    nats: String,
    /// That user's password, read from this file.
    #[arg(long)]
    nats_password_file: Option<PathBuf>,
    #[arg(long, default_value = "sinex")]
    subject_root: String,
    /// This node's producer name, as its credentials allow.
    #[arg(long)]
    producer: String,
    /// `sinexd`'s content store as mounted on this host, or with `--upload`
    /// this node's own store.
    #[arg(long)]
    store: PathBuf,
    /// The store is this node's own: send material bytes over the link.
    #[arg(long)]
    upload: bool,
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
    if !cli.upload && !cli.store.is_dir() {
        eprintln!(
            "sinex-capture: {} is not sinexd's content store; mount it, or pass --upload to keep a local store and send the bytes over the link",
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
    let connection = live::Connection {
        url: cli.nats.clone(),
        password_file: cli.nats_password_file.clone(),
        subjects: sinex_protocol::Subjects::new(cli.subject_root.clone()),
        producer: cli.producer.clone(),
        upload: cli.upload,
    };
    let mut requested: Vec<(&str, &str, serde_json::Value)> = Vec::new();
    if cli.journald {
        requested.push(("journald", "journald", serde_json::json!({"follow": true})));
    }
    if !cli.watch.is_empty() {
        let roots: Vec<String> = cli.watch.iter().map(|p| p.display().to_string()).collect();
        requested.push(("file-watch", "files", serde_json::json!({ "roots": roots })));
    }
    if requested.is_empty() {
        return Err("nothing to capture: pass --journald and/or --watch".into());
    }
    let bindings = requested
        .into_iter()
        .map(|(format, spool, options)| {
            live::binding(format, &spools.join(spool), &cli.host, options)
        })
        .collect::<Result<Vec<_>, _>>()?;
    live::run(
        connection,
        &store,
        &positions,
        bindings,
        Duration::from_secs(cli.every),
        None,
    )
    .await
}

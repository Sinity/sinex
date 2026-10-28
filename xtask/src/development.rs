//! Checkout-only daemon and explicitly selected session capture.
use crate::{Result, database, target_dir};
use devkit::{
    Run,
    pg::{Schema, Server},
    process::{Process, development_lock, source_hash_excluding},
};
use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::PathBuf,
    process::Command,
    time::{Duration, Instant},
};

#[derive(Debug, clap::Args)]
pub struct Development {
    #[arg(long)]
    pub watch: bool,
    /// Private directory for content, tokens, and development output.
    #[arg(long)]
    pub output: PathBuf,
    /// Explicit daemon source bindings; no bindings when omitted.
    #[arg(long)]
    pub sources: Option<PathBuf>,
    /// Explicit session captures. Nothing is captured when omitted.
    #[arg(long, value_parser = ["hyprland", "notifications", "players", "clipboard", "integrations"])]
    pub session_source: Vec<String>,
    /// Stop after a bounded observation, useful for smoke runs.
    #[arg(long)]
    pub seconds: Option<u64>,
}

fn ready(socket: &std::path::Path) -> bool {
    let Ok(mut stream) = UnixStream::connect(socket) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
    if stream
        .write_all(b"GET /v1/commands HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.1 200")
}

pub fn execute(run: &mut Run, options: &Development) -> Result {
    if options.seconds == Some(0) {
        return Err("seconds must be positive".into());
    }
    let _development_lock = development_lock(&run.root)?;
    fs::create_dir(&options.output).map_err(|e| format!("development output must be new: {e}"))?;
    fs::set_permissions(&options.output, fs::Permissions::from_mode(0o700))
        .map_err(|e| e.to_string())?;
    let output = fs::canonicalize(&options.output).map_err(|e| e.to_string())?;
    let mut prepared = None;
    run.step("services", |run| {
        // Always start an owned cluster. A configured host service is never selected.
        let server = Server::start(&database::cluster_options())?;
        server.ensure_roles(database::ROLES)?;
        let (hash, apply) = database::schema(&run.root)?;
        let databases = server.ensure_schema(&Schema {
            project: "sinex",
            hash,
            bootstrap: &apply,
        })?;
        server.clone_database(&databases.template, "development")?;
        let nats = devkit::nats::Server::start()?;
        prepared = Some((server, databases, nats));
        Ok(())
    })?;
    let (server, databases, nats) = prepared.ok_or("services not started")?;
    let env = database::env(&server, &databases);
    let tokens = output.join("tokens");
    fs::create_dir(&tokens).map_err(|e| e.to_string())?;
    let mut random_source = fs::File::open("/dev/urandom").map_err(|e| e.to_string())?;
    for name in ["read", "agent", "operator"] {
        let mut random = [0u8; 32];
        random_source
            .read_exact(&mut random)
            .map_err(|e| e.to_string())?;
        let token = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = tokens.join(format!("{name}.token"));
        fs::write(&path, token).map_err(|e| e.to_string())?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).map_err(|e| e.to_string())?;
    }
    // The owned cluster's short socket directory avoids Unix path length limits.
    let socket = server.socket().join("sinexd.sock");
    let store = output.join("store");
    let build = |run: &mut Run| -> Result {
        let mut args = vec!["build", "--locked", "-p", "sinexd"];
        if !options.session_source.is_empty() {
            args.extend(["-p", "sinex-session"]);
        }
        run.cargo_with("build", &args, &env)?;
        Ok(())
    };
    let start = |run: &mut Run| -> Result<Vec<Process>> {
        let mut command = Command::new(target_dir().join("debug/sinexd"));
        daemon_arguments(
            &mut command,
            &server.url("development", Some("sinex_writer")),
            &store,
            nats.url(),
            &tokens,
            &socket,
            options.sources.as_deref(),
        );
        let mut children = vec![Process::start(&mut command)?];
        run.step("readiness", |_| {
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                if let Some(status) = children[0].poll()? {
                    return Err(format!("daemon exited before readiness: {status}").into());
                }
                if ready(&socket) {
                    println!("development API ready: {}", socket.display());
                    break;
                }
                if Instant::now() >= deadline {
                    return Err("daemon API readiness timed out".into());
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(())
        })?;
        if !options.session_source.is_empty() {
            let mut command = Command::new(target_dir().join("debug/sinex-session"));
            session_arguments(
                &mut command,
                nats.url(),
                &store,
                &output.join("session"),
                &options.session_source,
            );
            command.env("SINEX_SESSION_SOCKET", server.socket().join("session.sock"));
            children.push(Process::start(&mut command)?);
        }
        fs::write(output.join("runtime.json"),serde_json::to_vec_pretty(&serde_json::json!({"socket":socket,"postgres_socket":server.socket(),"session_socket":server.socket().join("session.sock"),"nats":nats.url(),"processes":children.iter().map(Process::id).collect::<Vec<_>>()})).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
        Ok(children)
    };
    let root = run.root.clone();
    let changes = || -> Result<blake3::Hash> {
        let mut hash = blake3::Hasher::new();
        hash.update(source_hash_excluding(&root, std::slice::from_ref(&output))?.as_bytes());
        if let Some(sources) = &options.sources {
            hash.update(
                &fs::read(sources)
                    .map_err(|e| format!("source bindings {}: {e}", sources.display()))?,
            );
        }
        Ok(hash.finalize())
    };
    let mut observed = changes()?;
    build(run)?;
    let mut children = start(run)?;
    let started = Instant::now();
    loop {
        for child in &mut children {
            if let Some(status) = child.poll()? {
                return Err(format!("development child exited: {status}"));
            }
        }
        if options
            .seconds
            .is_some_and(|s| started.elapsed() >= Duration::from_secs(s))
        {
            return Ok(());
        }
        if options.watch {
            let changed = changes()?;
            if changed != observed {
                let edited = Instant::now();
                std::thread::sleep(Duration::from_millis(200));
                observed = changes()?;
                match build(run) {
                    Ok(()) => {
                        for child in &mut children {
                            child.stop()?;
                        }
                        children.clear();
                        children = start(run)?;
                        println!(
                            "change detected to ready: {:.3}s",
                            edited.elapsed().as_secs_f64()
                        );
                    }
                    Err(error) => {
                        eprintln!("rebuild failed; running process has previous build: {error}");
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn daemon_arguments(
    command: &mut Command,
    database: &str,
    store: &std::path::Path,
    nats: &str,
    tokens: &std::path::Path,
    socket: &std::path::Path,
    sources: Option<&std::path::Path>,
) {
    command
        .args(["run", "--database", database])
        .arg("--store")
        .arg(store)
        .args(["--nats", nats])
        .arg("--tokens")
        .arg(tokens)
        .arg("--socket")
        .arg(socket)
        .args(["--host", "development"]);
    if let Some(sources) = sources {
        command.arg("--sources").arg(sources);
    }
}

fn session_arguments(
    command: &mut Command,
    nats: &str,
    store: &std::path::Path,
    state: &std::path::Path,
    sources: &[String],
) {
    command
        .args([
            "run",
            "--nats",
            nats,
            "--producer",
            "development-session",
            "--host",
            "development",
        ])
        .arg("--store")
        .arg(store)
        .arg("--state")
        .arg(state);
    for source in sources {
        command.arg(format!("--{source}"));
    }
}

#[cfg(test)]
mod tests;

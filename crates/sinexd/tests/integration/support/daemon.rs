use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub(crate) struct Process {
    child: Child,
    log: PathBuf,
}

impl Process {
    pub(crate) fn start(command: &mut Command, log: PathBuf) -> Self {
        Self {
            child: command
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
            log,
        }
    }

    pub(crate) fn id(&self) -> u32 {
        self.child.id()
    }

    pub(crate) fn running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "process stopped: {}",
            self.log()
        );
    }

    pub(crate) fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap()
    }

    pub(crate) async fn stop(mut self) {
        let started = Instant::now();
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = started + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                eprintln!("SIGTERM to reaped exit: {:?}", started.elapsed());
                assert!(
                    status.success(),
                    "process stopped unsuccessfully: {}",
                    self.log()
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "process did not stop: {}",
                self.log()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
            self.child.wait().unwrap();
        }
    }
}

pub(crate) fn run(binary: &Path, database: &str, store: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(binary);
    command.args(args.get(..2).unwrap_or(args));
    if args.first() == Some(&"admin")
        && !args.contains(&"--deadline")
        && args
            .get(1)
            .is_some_and(|name| sinex_protocol::command(name).is_some())
    {
        command.args(["--deadline", "none"]);
    }
    command
        .args(args.get(2..).unwrap_or_default())
        .args(["--database", database, "--store"])
        .arg(store);
    command
}

pub(crate) fn finish(command: &mut Command) -> String {
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "command failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

pub(crate) fn query(binary: &Path, database: &str, store: &Path, text: &str) -> Value {
    serde_json::from_str(&finish(
        run(binary, database, store, &["admin", "query"]).args(["--page-size", "1000", text]),
    ))
    .unwrap()
}

/// One request to the daemon's API socket as the bearer of `token`: its
/// result, or a panic naming the failure.
pub(crate) fn call(socket: &Path, token: &str, mut request: Value) -> Value {
    use std::io::{Read, Write};
    // These fixtures assert delivered facts, not the operation's latency.
    request
        .as_object_mut()
        .unwrap()
        .entry("deadline")
        .or_insert(Value::Null);
    let body = serde_json::to_vec(&request).unwrap();
    let mut stream = std::os::unix::net::UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(stream, "POST /v1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
    stream.write_all(&body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let split = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .unwrap();
    let headers = std::str::from_utf8(&response[..split]).unwrap();
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    let reply: sinex_protocol::Reply = serde_json::from_slice(&response[split + 4..]).unwrap();
    match reply {
        sinex_protocol::Reply::Ok { result } => result,
        sinex_protocol::Reply::Failed { failure }
        | sinex_protocol::Reply::StoppedEarly { failure, .. } => panic!("{request}: {failure:?}"),
    }
}

pub(crate) fn derivations() -> serde_json::Map<String, Value> {
    let catalog: Value =
        serde_json::from_str(include_str!("../../../../../nix/catalog.json")).unwrap();
    catalog["derivations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|derivation| {
            let name = derivation["name"].as_str().unwrap();
            (name.to_owned(), serde_json::json!(name != "tag.applier"))
        })
        .collect()
}

pub(crate) fn sources(path: &Path, allowed: &[&str]) {
    let excluded: serde_json::Map<String, Value> = sinex_sources::sources::specs()
        .iter()
        .filter(|spec| !allowed.contains(&spec.name))
        .map(|spec| {
            (
                spec.name.to_owned(),
                serde_json::json!({"reason": "fixture selects synthetic inputs only"}),
            )
        })
        .collect();
    std::fs::write(
        path,
        serde_json::to_vec(&serde_json::json!({"exclude": excluded, "derivations": derivations()}))
            .unwrap(),
    )
    .unwrap();
}

pub(crate) fn daemon(
    binary: &Path,
    database: &str,
    store: &Path,
    root: &Path,
    nats: &devkit::nats::TestNats,
) -> Process {
    let tokens = root.join("tokens");
    std::fs::create_dir_all(&tokens).unwrap();
    let config = root.join("sources.json");
    sources(&config, &["sinex.telemetry"]);
    Process::start(
        run(
            binary,
            database,
            store,
            &[
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
                nats.prefix(),
                "--host",
                "fixture",
            ],
        )
        .arg("--tokens")
        .arg(tokens)
        .arg("--sources")
        .arg(config)
        .arg("--socket")
        .arg(root.join("api.sock")),
        root.join("daemon.log"),
    )
}

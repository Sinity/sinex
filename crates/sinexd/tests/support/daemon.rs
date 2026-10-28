use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct Process {
    child: Child,
    log: PathBuf,
}

impl Process {
    pub fn start(command: &mut Command, log: PathBuf) -> Self {
        Self {
            child: command
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
            log,
        }
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn running(&mut self) {
        assert!(
            self.child.try_wait().unwrap().is_none(),
            "process stopped: {}",
            self.log()
        );
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap()
    }

    pub async fn stop(mut self) {
        assert!(
            Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
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

pub fn run(binary: &Path, database: &str, store: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(binary);
    command
        .args(args)
        .args(["--database", database, "--store"])
        .arg(store);
    command
}

pub fn finish(command: &mut Command) -> String {
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "command failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}

pub fn query(binary: &Path, database: &str, store: &Path, text: &str) -> Value {
    serde_json::from_str(&finish(
        run(binary, database, store, &["admin", "query"]).args(["--page", "1000", text]),
    ))
    .unwrap()
}

pub fn sources(path: &Path, allowed: &[&str]) {
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
        serde_json::to_vec(&serde_json::json!({"exclude": excluded})).unwrap(),
    )
    .unwrap();
}

pub fn daemon(
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

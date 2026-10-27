use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Session {
    child: Option<Child>,
}

impl Session {
    fn finish(mut self) -> Output {
        let until = Instant::now() + Duration::from_secs(5);
        loop {
            if self.child.as_mut().unwrap().try_wait().unwrap().is_some() {
                return self.child.take().unwrap().wait_with_output().unwrap();
            }
            assert!(Instant::now() < until, "session did not stop promptly");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = Command::new("sh")
                .args(["-c", "kill -TERM $1", "fixture", &child.id().to_string()])
                .status();
            let until = Instant::now() + Duration::from_secs(1);
            while child.try_wait().ok().flatten().is_none() && Instant::now() < until {
                std::thread::sleep(Duration::from_millis(10));
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    pids: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_owned();
        let bin = root.join("bin");
        std::fs::create_dir(&bin).unwrap();
        let shell = Command::new("sh")
            .args(["-c", "command -v sh"])
            .output()
            .unwrap();
        let shell = String::from_utf8(shell.stdout).unwrap();
        for (name, body) in [
            (
                "wl-paste",
                r#"printf '%s\n' "$$" >> "$FAKE_PIDS"
if [ "$FAKE_EMIT" = 1 ]; then printf '%s\n' '{"selection":"clipboard","types":[],"secret":false,"text":"synthetic capture"}'; fi
(while :; do printf x >> "$FAKE_HEARTBEAT"; sleep 0.01; done) &
printf '%s\n' "$!" >> "$FAKE_DESCENDANTS"
while [ ! -s "$FAKE_HEARTBEAT" ]; do sleep 0.01; done
if [ -n "$FAKE_DELAYED_EMIT" ]; then
    while [ ! -e "$FAKE_DELAYED_EMIT" ]; do sleep 0.01; done
    printf '%s\n' '{"selection":"clipboard","types":[],"secret":false,"text":"synthetic capture"}'
fi
exec sleep 30
"#,
            ),
            (
                "pw-record",
                r#"printf '%s\n' "$$" >> "$FAKE_PIDS"
for file do :; done
printf 'synthetic audio bytes' > "$file"
(while :; do printf x >> "$FAKE_HEARTBEAT"; sleep 0.01; done) &
printf '%s\n' "$!" >> "$FAKE_DESCENDANTS"
while [ ! -s "$FAKE_HEARTBEAT" ]; do sleep 0.01; done
exec sleep 30
"#,
            ),
            ("ffmpeg", "exit 1\n"),
        ] {
            let path = bin.join(name);
            std::fs::write(&path, format!("#!{}\n{body}", shell.trim())).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let pids = root.join("pids");
        Self {
            _dir: dir,
            root,
            pids,
        }
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sinex-session"));
        let mut path = vec![self.root.join("bin")];
        path.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        command
            .env("PATH", std::env::join_paths(path).unwrap())
            .env("FAKE_PIDS", &self.pids)
            .env("FAKE_DESCENDANTS", self.root.join("descendants"))
            .env("FAKE_HEARTBEAT", self.root.join("heartbeat"))
            .env("SINEX_STATE_DIR", self.root.join("private-state"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command
    }

    fn run(&self, nats: &devkit::nats::TestNats) -> Command {
        let mut command = self.command();
        command
            .args([
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
                nats.prefix(),
                "--producer",
                "fixture",
                "--clipboard",
                "--every",
                "1",
            ])
            .arg("--state")
            .arg(self.root.join("state"))
            .arg("--store")
            .arg(self.root.join("store"));
        command
    }

    fn pids(&self) -> Vec<u32> {
        std::fs::read_to_string(&self.pids)
            .unwrap_or_default()
            .lines()
            .map(|s| s.parse().unwrap())
            .collect()
    }

    fn started(&self, count: usize) {
        let until = Instant::now() + Duration::from_secs(5);
        while self.pids().len() < count
            || self.descendants().len() < count
            || !self.root.join("heartbeat").exists()
        {
            assert!(Instant::now() < until, "fake capture child did not start");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn descendants(&self) -> Vec<u32> {
        std::fs::read_to_string(self.root.join("descendants"))
            .unwrap_or_default()
            .lines()
            .map(|s| s.parse().unwrap())
            .collect()
    }

    fn all_reaped(&self) {
        let pids = self.pids();
        let descendants = self.descendants();
        assert!(!pids.is_empty(), "at least one real child was required");
        assert!(
            !descendants.is_empty(),
            "the child had an actual descendant"
        );
        for pid in pids.into_iter().chain(descendants) {
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "capture child {pid} outlived sinex-session"
            );
        }
        let before = std::fs::read(self.root.join("heartbeat")).unwrap();
        assert!(!before.is_empty());
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            std::fs::read(self.root.join("heartbeat")).unwrap(),
            before,
            "capture wrote after sinex-session completed"
        );
    }
}

#[devkit::test]
fn capture_deadline_reaps_clipboard_and_audio_children() {
    for kind in ["clipboard", "media"] {
        let fixture = Fixture::new();
        let mut command = fixture.command();
        command
            .args(["capture", kind, "--seconds", "1"])
            .arg("--spool")
            .arg(fixture.root.join("spool"));
        if kind == "media" {
            command.args(["--option", "kind=audio", "--option", "segment=30"]);
        }
        let output = Session {
            child: Some(command.spawn().unwrap()),
        }
        .finish();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture.all_reaped();
    }
}

#[devkit::test]
fn run_deadline_reaps_both_clipboard_selections() {
    let nats = devkit::nats::test_nats().unwrap();
    let fixture = Fixture::new();
    let output = Session {
        child: Some(fixture.run(&nats).args(["--seconds", "1"]).spawn().unwrap()),
    }
    .finish();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture.pids().len(), 2);
    fixture.all_reaped();
}

#[devkit::test]
fn run_signals_join_capture_threads_and_reap_children() {
    for signal in ["INT", "TERM"] {
        let nats = devkit::nats::test_nats().unwrap();
        let fixture = Fixture::new();
        let session = Session {
            child: Some(fixture.run(&nats).spawn().unwrap()),
        };
        fixture.started(2);
        assert!(
            Command::new("sh")
                .args([
                    "-c",
                    "kill -$1 $2",
                    "fixture",
                    signal,
                    &session.child.as_ref().unwrap().id().to_string()
                ])
                .status()
                .unwrap()
                .success()
        );
        let output = session.finish();
        assert!(
            output.status.success(),
            "{signal}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        fixture.all_reaped();
    }
}

#[devkit::test]
fn a_producer_error_stops_already_running_capture_children() {
    let nats = devkit::nats::test_nats().unwrap();
    let fixture = Fixture::new();
    // No ingress stream exists: publishing the captured synthetic line fails.
    let output = Session {
        child: Some(fixture.run(&nats).env("FAKE_EMIT", "1").spawn().unwrap()),
    }
    .finish();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no stream found for given subject"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture.all_reaped();
}

#[devkit::test]
fn a_first_record_after_saved_empty_polls_reaches_transport_and_reaps_children() {
    let nats = devkit::nats::test_nats().unwrap();
    let fixture = Fixture::new();
    let release = fixture.root.join("emit");
    let session = Session {
        child: Some(
            fixture
                .run(&nats)
                .env("FAKE_DELAYED_EMIT", &release)
                .spawn()
                .unwrap(),
        ),
    };
    fixture.started(2);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let positions = std::fs::read_dir(fixture.root.join("state/positions"))
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter_map(|entry| std::fs::read(entry.path()).ok())
            .filter_map(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .filter(|position| position["cursor"]["inner"]["offset"] == 0)
            .count();
        if positions == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both empty spool polls were not saved"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    std::fs::write(release, b"emit").unwrap();
    let output = session.finish();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no stream found for given subject"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    fixture.all_reaped();
}

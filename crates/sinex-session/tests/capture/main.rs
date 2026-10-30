#[path = "../../../sinex-source-core/tests/support/hello.rs"]
mod hello;
use std::io::BufRead;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Session {
    child: Option<Child>,
}

impl Session {
    fn signal(&self, signal: &str) {
        assert!(
            Command::new("sh")
                .args([
                    "-c",
                    "kill -$1 $2",
                    "fixture",
                    signal,
                    &self.child.as_ref().unwrap().id().to_string(),
                ])
                .status()
                .unwrap()
                .success()
        );
    }

    fn stop(self) -> Output {
        self.signal("TERM");
        self.finish()
    }

    fn publishing_failed(&mut self, count: usize) -> String {
        let mut reader =
            std::io::BufReader::new(self.child.as_mut().unwrap().stderr.as_mut().unwrap());
        let mut log = String::new();
        let mut failed = 0;
        loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "session ended before publishing failed: {log}"
            );
            log.push_str(&line);
            if line.contains("no stream found for given subject") {
                failed += 1;
                if failed == count {
                    return log;
                }
            }
        }
    }

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
        let runtime = root.join("run");
        std::fs::create_dir(&runtime).unwrap();
        std::fs::write(runtime.join("wayland-test"), b"").unwrap();
        let excluded: serde_json::Map<String, serde_json::Value> = sinex_sources::sources::specs()
            .iter()
            .filter(|spec| spec.name != "desktop.clipboard")
            .map(|spec| {
                (
                    spec.name.to_owned(),
                    serde_json::json!({"reason":"fixture captures only the clipboard"}),
                )
            })
            .collect();
        std::fs::write(
            root.join("sources.json"),
            serde_json::to_vec(&serde_json::json!({"exclude":excluded})).unwrap(),
        )
        .unwrap();
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
            .env("XDG_RUNTIME_DIR", self.root.join("run"))
            .env("WAYLAND_DISPLAY", "wayland-test")
            .env_remove("HYPRLAND_INSTANCE_SIGNATURE")
            .env_remove("DBUS_SESSION_BUS_ADDRESS")
            .env("FAKE_PIDS", &self.pids)
            .env("FAKE_DESCENDANTS", self.root.join("descendants"))
            .env("FAKE_HEARTBEAT", self.root.join("heartbeat"))
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
                "--every",
                "1",
            ])
            .arg("--sources")
            .arg(self.root.join("sources.json"))
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

    fn keeps_capturing(&self, session: &mut Session) {
        let before = std::fs::metadata(self.root.join("heartbeat"))
            .unwrap()
            .len();
        let until = Instant::now() + Duration::from_secs(5);
        while std::fs::metadata(self.root.join("heartbeat"))
            .unwrap()
            .len()
            == before
        {
            assert!(
                session
                    .child
                    .as_mut()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none(),
                "publishing failure ended the session"
            );
            assert!(
                Instant::now() < until,
                "capture stopped after publishing failed"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            session
                .child
                .as_mut()
                .unwrap()
                .try_wait()
                .unwrap()
                .is_none()
        );
        for pid in self.pids().into_iter().chain(self.descendants()) {
            assert!(
                Path::new(&format!("/proc/{pid}")).exists(),
                "capture child {pid} stopped after publishing failed"
            );
        }
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
    let _runtime = tokio::runtime::Runtime::new().unwrap();
    let _hello = _runtime.block_on(hello::hello(
        &nats,
        &sinex_protocol::Subjects::new(nats.prefix()),
        "fixture",
    ));
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
        let _runtime = tokio::runtime::Runtime::new().unwrap();
        let _hello = _runtime.block_on(hello::hello(
            &nats,
            &sinex_protocol::Subjects::new(nats.prefix()),
            "fixture",
        ));
        let fixture = Fixture::new();
        let session = Session {
            child: Some(fixture.run(&nats).spawn().unwrap()),
        };
        fixture.started(2);
        session.signal(signal);
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
fn publishing_errors_leave_capture_children_running_until_stopped() {
    let nats = devkit::nats::test_nats().unwrap();
    let _runtime = tokio::runtime::Runtime::new().unwrap();
    let _hello = _runtime.block_on(hello::hello(
        &nats,
        &sinex_protocol::Subjects::new(nats.prefix()),
        "fixture",
    ));
    let fixture = Fixture::new();
    // No ingress stream exists: publishing the captured synthetic line fails.
    let mut session = Session {
        child: Some(fixture.run(&nats).env("FAKE_EMIT", "1").spawn().unwrap()),
    };
    fixture.started(2);
    let mut log = session.publishing_failed(2);
    fixture.keeps_capturing(&mut session);
    let output = session.stop();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "{log}");
    assert_eq!(log.matches("no stream found for given subject").count(), 2);
    fixture.all_reaped();
}

#[devkit::test]
fn a_first_record_after_saved_empty_polls_reaches_transport_while_capture_continues() {
    let nats = devkit::nats::test_nats().unwrap();
    let _runtime = tokio::runtime::Runtime::new().unwrap();
    let _hello = _runtime.block_on(hello::hello(
        &nats,
        &sinex_protocol::Subjects::new(nats.prefix()),
        "fixture",
    ));
    let fixture = Fixture::new();
    let release = fixture.root.join("emit");
    let mut session = Session {
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
            .filter_map(|entry| {
                let path = entry.path();
                let binding = path.file_stem()?.to_str()?;
                sinex_source_core::drivers::producer::Position::load(path.parent()?, binding).ok()
            })
            .filter(|position| position.cursor["inner"]["offset"] == 0)
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
    let mut log = session.publishing_failed(2);
    fixture.keeps_capturing(&mut session);
    let output = session.stop();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "{log}");
    assert_eq!(log.matches("no stream found for given subject").count(), 2);
    fixture.all_reaped();
}

#[devkit::test]
fn drain_without_spools_exits_without_starting_session_captures() {
    let nats = devkit::nats::test_nats().unwrap();
    let _runtime = tokio::runtime::Runtime::new().unwrap();
    let _hello = _runtime.block_on(hello::hello(
        &nats,
        &sinex_protocol::Subjects::new(nats.prefix()),
        "fixture",
    ));
    let fixture = Fixture::new();
    let output = Session {
        child: Some(fixture.run(&nats).arg("--drain").spawn().unwrap()),
    }
    .finish();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixture.pids().is_empty());
    assert!(!fixture.root.join("state/spool").exists());
}

#[devkit::test]
fn drain_reads_a_stopped_spool_without_session_capabilities_and_keeps_unacknowledged_bytes() {
    use sinex_source_core::drivers::live_stream::{Rotation, SpoolWriter};
    let nats = devkit::nats::test_nats().unwrap();
    let _runtime = tokio::runtime::Runtime::new().unwrap();
    let _hello = _runtime.block_on(hello::hello(
        &nats,
        &sinex_protocol::Subjects::new(nats.prefix()),
        "fixture",
    ));
    let fixture = Fixture::new();
    let spool = fixture.root.join("state/spool/clipboard");
    let mut writer = SpoolWriter::open(&spool, Rotation::default()).unwrap();
    writer
        .append(
            sinex_model::Timestamp::now(),
            br#"{"selection":"clipboard","types":[],"secret":false,"text":"synthetic drain"}"#,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);
    let before: Vec<_> = std::fs::read_dir(&spool)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    let output = Session {
        child: Some(
            fixture
                .run(&nats)
                .arg("--drain")
                .env_remove("WAYLAND_DISPLAY")
                .spawn()
                .unwrap(),
        ),
    }
    .finish();
    // There is no ingress stream, so finished data must remain available to retry.
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no stream found for given subject"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(fixture.pids().is_empty());
    for (path, bytes) in before {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}

/// Only the capture command receives the signal. It must stop and reap the
/// journal reader in its separate process group before reporting success.
#[devkit::test]
async fn capture_stops_its_child_when_only_the_command_receives_sigterm() {
    use std::os::unix::{fs::PermissionsExt, process::CommandExt};
    let dir = tempfile::tempdir().unwrap();
    let journalctl = dir.path().join("journalctl");
    let child_pid_file = dir.path().join("child.pid");
    std::fs::write(
        &journalctl,
        "#!/bin/sh\necho $$ > \"$TEST_CHILD_PID\"\nexec sleep 300\n",
    )
    .unwrap();
    std::fs::set_permissions(&journalctl, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = format!(
        "{}:{}",
        dir.path().display(),
        std::env::var("PATH").unwrap()
    );
    let mut daemon = Session {
        child: Some(
            Command::new(env!("CARGO_BIN_EXE_sinex-session"))
                .args(["capture", "journald", "--spool"])
                .arg(dir.path().join("spool"))
                .args(["--option", "follow=true"])
                .env("PATH", path)
                .env("TEST_CHILD_PID", &child_pid_file)
                .process_group(0)
                .stdout(Stdio::null())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap(),
        ),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    let child = loop {
        assert!(
            daemon.child.as_mut().unwrap().try_wait().unwrap().is_none(),
            "capture stopped before its reader started"
        );
        if let Ok(pid) = std::fs::read_to_string(&child_pid_file)
            && let Ok(pid) = pid.trim().parse::<u32>()
        {
            break pid;
        }
        assert!(Instant::now() < deadline, "journal reader never started");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(
        Command::new("kill")
            .args(["-TERM", &daemon.child.as_mut().unwrap().id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    loop {
        if let Some(status) = daemon.child.as_mut().unwrap().try_wait().unwrap() {
            assert!(status.success(), "capture exited with {status}");
            break;
        }
        assert!(Instant::now() < deadline, "capture did not stop");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !Path::new(&format!("/proc/{child}")).exists(),
        "journal reader was not reaped"
    );
}

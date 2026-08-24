#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Behavior coverage for the explicit AgentCTL lease handoff used by the
//! pre-push hook. These tests execute the real shell functions and fake only
//! AgentCTL, PostgreSQL, NATS, and the selected command.

use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Clone, Copy)]
enum NatsMode {
    Ready,
    Foreign,
    Absent,
}

#[derive(Clone, Copy)]
enum ExecutionPath {
    SelectedBinary,
    NixFallback,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

fn executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write test executable");
    let mut permissions = fs::metadata(path)
        .expect("stat test executable")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make test executable runnable");
}

fn lease_json(root: &Path, lease_id: &str, postgres_port: u16, nats_port: u16) -> String {
    lease_json_with_state(
        root,
        lease_id,
        postgres_port,
        nats_port,
        "running",
        false,
        "active",
        "active",
        "running",
    )
}

fn lease_json_with_state(
    root: &Path,
    lease_id: &str,
    postgres_port: u16,
    nats_port: u16,
    phase: &str,
    terminal: bool,
    lease_state: &str,
    active_state: &str,
    sub_state: &str,
) -> String {
    json!({
        "payload": { "value": {
            "job_id": lease_id,
            "project_id": "sinex",
            "operation": "dev_services",
            "checkout": { "project_id": "sinex", "path": root },
            "state": {
                "phase": phase,
                "terminal": terminal,
                "systemd": { "ActiveState": active_state, "SubState": sub_state }
            },
            "lease": {
                "id": lease_id,
                "host": "127.0.0.1",
                "readiness": "project-command",
                "lifetime": "job",
                "state": lease_state,
                "ports": [
                    { "name": "postgres", "environment": "SINEX_DEV_POSTGRES_PORT", "port": postgres_port },
                    { "name": "nats", "environment": "SINEX_DEV_NATS_PORT", "port": nats_port }
                ]
            }
        }}
    })
    .to_string()
}

fn mutate_lease(lease: &str, mutate: impl FnOnce(&mut serde_json::Value)) -> String {
    let mut value: serde_json::Value = serde_json::from_str(lease).expect("valid lease fixture");
    mutate(&mut value);
    value.to_string()
}

fn free_nats_port() -> u16 {
    (44308..=44435)
        .find(|port| TcpListener::bind(("127.0.0.1", *port)).is_ok())
        .expect("find a free AgentCTL NATS port")
}

fn run_checkout_env(
    fixture: &TempDir,
    lease_id: &str,
    lease: Option<&str>,
    nats_port: u16,
    nats_mode: NatsMode,
    lease_after: Option<&str>,
    execution_path: ExecutionPath,
) -> Output {
    let bin = fixture.path().join("bin");
    let pg_bin = fixture.path().join("pg-bin");
    fs::create_dir_all(&bin).expect("create fake bin");
    fs::create_dir_all(&pg_bin).expect("create fake postgres bin");
    executable(&pg_bin.join("postgres"), "#!/usr/bin/env bash\nexit 0\n");
    executable(
        &bin.join("agentctl"),
        "#!/usr/bin/env bash\nif [[ \"$1 $2 $3\" != \"job get $FAKE_AGENTCTL_EXPECTED_ID\" ]]; then exit 2; fi\nif [[ \"${FAKE_AGENTCTL_STATUS:-0}\" != 0 ]]; then exit \"$FAKE_AGENTCTL_STATUS\"; fi\ncall_number=$(wc -l < \"$FAKE_AGENTCTL_CALLS\")\ncall_number=$((call_number + 1))\nprintf '%s\\n' \"$call_number\" >> \"$FAKE_AGENTCTL_CALLS\"\nif [[ -n \"${FAKE_AGENTCTL_AFTER:-}\" && \"$call_number\" -ge \"${FAKE_AGENTCTL_AFTER_CALL:-3}\" ]]; then cat \"$FAKE_AGENTCTL_AFTER\"; else cat \"$FAKE_AGENTCTL_JSON\"; fi\n",
    );
    executable(
        &bin.join("pg_isready"),
        "#!/usr/bin/env bash\nhost= port=\nwhile (($#)); do case \"$1\" in -h) host=$2; shift 2;; -p) port=$2; shift 2;; *) shift;; esac; done\nprintf '%s\\t%s\\n' \"$host\" \"$port\" > \"$FAKE_PG_CAPTURE\"\nexit 0\n",
    );
    executable(
        &bin.join("nix"),
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$FAKE_NIX_CAPTURE\"\nenv | sort > \"$FAKE_XTASK_CAPTURE\"\n",
    );
    let xtask = fixture.path().join("xtask");
    executable(
        &xtask,
        "#!/usr/bin/env bash\nenv | sort > \"$FAKE_XTASK_CAPTURE\"\n",
    );

    let lease_file = fixture.path().join("lease.json");
    fs::write(&lease_file, lease.unwrap_or_default()).expect("write fake lease response");
    let lease_after_file = fixture.path().join("lease-after.json");
    if let Some(lease_after) = lease_after {
        fs::write(&lease_after_file, lease_after).expect("write fake final lease response");
    }
    let pg_capture = fixture.path().join("pg-capture");
    let xtask_capture = fixture.path().join("xtask-capture");
    let nix_capture = fixture.path().join("nix-capture");
    let agentctl_calls = fixture.path().join("agentctl-calls");
    fs::write(&agentctl_calls, "").expect("create AgentCTL call log");
    let rc = fixture.path().join("devshell.rc");
    fs::write(&rc, "# test devshell\n").expect("write fake devshell rc");

    let listener = (!matches!(nats_mode, NatsMode::Absent))
        .then(|| TcpListener::bind(("127.0.0.1", nats_port)).expect("bind fake NATS"));
    let listener_thread = listener.map(|listener| {
        listener
            .set_nonblocking(true)
            .expect("set fake NATS nonblocking");
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        if matches!(nats_mode, NatsMode::Ready) {
                            stream
                                .write_all(b"INFO {\"server_id\":\"fixture\"}\r\n")
                                .expect("write NATS INFO");
                            stream
                                .set_read_timeout(Some(Duration::from_secs(1)))
                                .expect("set NATS read timeout");
                            let mut command = Vec::new();
                            let mut buffer = [0_u8; 256];
                            while !command
                                .windows(b"\r\nPING\r\n".len())
                                .any(|window| window == b"\r\nPING\r\n")
                            {
                                let read = stream.read(&mut buffer).expect("read NATS command");
                                if read == 0 {
                                    break;
                                }
                                command.extend_from_slice(&buffer[..read]);
                            }
                            let command = String::from_utf8_lossy(&command);
                            assert!(
                                command.contains("CONNECT {"),
                                "missing NATS CONNECT: {command}"
                            );
                            assert!(
                                command.contains("\r\nPING\r\n"),
                                "missing NATS PING: {command}"
                            );
                            stream
                                .write_all(b"+OK\r\nPONG\r\n")
                                .expect("write NATS PONG");
                        } else {
                            stream
                                .write_all(b"HTTP/1.1 200 OK\r\n\r\n")
                                .expect("write foreign listener response");
                        }
                        return;
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::yield_now();
                    }
                    Err(error) => panic!("accept fake NATS connection: {error}"),
                }
            }
        })
    });

    let hook = repo_root().join(".githooks/pre-push");
    let command = r#"
source "$1"
REPO_ROOT="$2"
SINEX_PG_BIN="$3"
FAKE_DEVSHELL_RC="$4"
_sinex_pre_push_devshell_rc() { printf '%s\n' "$FAKE_DEVSHELL_RC"; }
case "$6" in
  selected) _sinex_pre_push_selected_xtask "test selected binary" "$5" test ;;
  fallback) _sinex_pre_push_clean_env nix develop "$REPO_ROOT" --command xtask test ;;
  *) echo "unknown execution path" >&2; exit 2 ;;
esac
"#;
    let mut env = HashMap::new();
    env.insert(
        "PATH",
        format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
    );
    env.insert("SINEX_PRE_PUSH_AGENTCTL_LEASE_ID", lease_id.to_string());
    env.insert("FAKE_AGENTCTL_EXPECTED_ID", lease_id.to_string());
    env.insert("FAKE_AGENTCTL_JSON", lease_file.display().to_string());
    env.insert("FAKE_AGENTCTL_CALLS", agentctl_calls.display().to_string());
    env.insert("FAKE_PG_CAPTURE", pg_capture.display().to_string());
    env.insert("FAKE_XTASK_CAPTURE", xtask_capture.display().to_string());
    env.insert("FAKE_NIX_CAPTURE", nix_capture.display().to_string());
    if lease_after.is_some() {
        env.insert(
            "FAKE_AGENTCTL_AFTER",
            lease_after_file.display().to_string(),
        );
        env.insert("FAKE_AGENTCTL_AFTER_CALL", "3".to_string());
    }
    let mut process = Command::new("bash");
    process
        .arg("-c")
        .arg(command)
        .arg("pre-push-lease-test")
        .arg(&hook)
        .arg(repo_root())
        .arg(&pg_bin)
        .arg(&rc)
        .arg(&xtask)
        .arg(match execution_path {
            ExecutionPath::SelectedBinary => "selected",
            ExecutionPath::NixFallback => "fallback",
        })
        .envs(env);
    let output = process.output().expect("execute pre-push lease helper");
    if let Some(thread) = listener_thread {
        thread.join().expect("join fake NATS listener");
    }
    output
}

fn assert_three_identity_reads(fixture: &TempDir) {
    assert_eq!(
        fs::read_to_string(fixture.path().join("agentctl-calls"))
            .expect("read AgentCTL call log")
            .lines()
            .count(),
        3,
        "execution must perform initial, endpoint, and final identity reads"
    );
}

#[test]
fn active_lease_propagates_exact_ports_and_postgres_socket() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "11111111-1111-4111-8111-111111111111";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Ready,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(
        output.status.success(),
        "pre-push helper failed: {output:?}"
    );

    let captured_text =
        fs::read_to_string(fixture.path().join("xtask-capture")).expect("read xtask environment");
    let captured: HashMap<_, _> = captured_text
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    assert_eq!(captured["SINEX_DEV_POSTGRES_PORT"], "45559");
    assert_eq!(captured["SINEX_DEV_NATS_PORT"], nats_port.to_string());
    assert_eq!(captured["PGPORT"], "45559");
    assert!(!captured.contains_key("SINEX_PRE_PUSH_AGENTCTL_LEASE_ID"));
    let pg_host = captured["PGHOST"];
    assert_eq!(
        captured["PGHOST"],
        format!("{}/run", captured["SINEX_DEV_STATE_DIR"])
    );
    assert_eq!(
        captured["DATABASE_URL"],
        format!("postgresql:///sinex_dev?host={pg_host}&port=45559")
    );
    assert_eq!(
        fs::read_to_string(fixture.path().join("pg-capture")).expect("read postgres probe"),
        format!("{pg_host}\t45559\n")
    );
    assert_three_identity_reads(&fixture);
}

#[test]
fn stale_lease_is_rejected_before_xtask_runs() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "22222222-2222-4222-8222-222222222222";
    let output = run_checkout_env(
        &fixture,
        lease_id,
        None,
        44308,
        NatsMode::Absent,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(
        !output.status.success(),
        "stale lease unexpectedly passed: {output:?}"
    );
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("job lookup failed or the lease is stale")
    );
}

#[test]
fn unreachable_active_lease_is_rejected_before_xtask_runs() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "33333333-3333-4333-8333-333333333333";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Absent,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(
        !output.status.success(),
        "unreachable lease unexpectedly passed: {output:?}"
    );
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NATS protocol readiness failed"));
}

#[test]
fn foreign_tcp_listener_is_rejected_before_xtask_runs() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "44444444-4444-4444-8444-444444444444";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Foreign,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NATS protocol readiness failed"));
}

#[test]
fn malformed_duplicate_ports_are_rejected() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "55555555-5555-4555-8555-555555555555";
    let nats_port = free_nats_port();
    let valid = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let duplicate = mutate_lease(&valid, |value| {
        let ports = value["payload"]["value"]["lease"]["ports"]
            .as_array_mut()
            .expect("ports array");
        let first = ports[0].clone();
        ports.push(first);
    });
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&duplicate),
        nats_port,
        NatsMode::Absent,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not the active dev_services lease"));
}

#[test]
fn malformed_port_value_is_rejected() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "59595959-5959-4959-8959-595959595959";
    let nats_port = free_nats_port();
    let valid = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let malformed = mutate_lease(&valid, |value| {
        value["payload"]["value"]["lease"]["ports"][0]["port"] = json!("45559");
    });
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&malformed),
        nats_port,
        NatsMode::Absent,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("not the active dev_services lease"));
}

#[test]
fn wrong_operation_and_checkout_are_rejected() {
    for (index, mutate) in [
        (0, |value: &mut serde_json::Value| {
            value["payload"]["value"]["operation"] = json!("check_default");
        }),
        (1, |value: &mut serde_json::Value| {
            value["payload"]["value"]["checkout"]["path"] = json!("/foreign/checkout");
        }),
    ] {
        let fixture = tempfile::tempdir().expect("create fixture directory");
        let lease_id = format!("66666666-6666-4666-8666-66666666666{index}");
        let lease = mutate_lease(&lease_json(&repo_root(), &lease_id, 45559, 44308), mutate);
        let output = run_checkout_env(
            &fixture,
            &lease_id,
            Some(&lease),
            44308,
            NatsMode::Absent,
            None,
            ExecutionPath::SelectedBinary,
        );
        assert!(!output.status.success());
        assert!(!fixture.path().join("xtask-capture").exists());
    }
}

#[test]
fn cancellation_during_final_validation_is_rejected() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "77777777-7777-4777-8777-777777777777";
    let nats_port = free_nats_port();
    let active = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let cancelled = lease_json_with_state(
        &repo_root(),
        lease_id,
        45559,
        nats_port,
        "cancelled",
        true,
        "released",
        "inactive",
        "dead",
    );
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&active),
        nats_port,
        NatsMode::Ready,
        Some(&cancelled),
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("job or lease coordinates changed"));
}

#[test]
fn fallback_execution_path_revalidates_before_launch() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "88888888-8888-4888-8888-888888888888";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Ready,
        None,
        ExecutionPath::NixFallback,
    );
    assert!(output.status.success(), "fallback failed: {output:?}");
    assert!(fixture.path().join("nix-capture").exists());
    assert_three_identity_reads(&fixture);
}

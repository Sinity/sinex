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
enum PostgresMode {
    Ready,
    Foreign,
    Malformed,
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
    postgres_mode: PostgresMode,
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
        "#!/usr/bin/env bash\nif [[ \"$1 $2 $3\" != \"job get $FAKE_AGENTCTL_EXPECTED_ID\" ]]; then exit 2; fi\nif [[ \"${FAKE_AGENTCTL_STATUS:-0}\" != 0 ]]; then exit \"$FAKE_AGENTCTL_STATUS\"; fi\ncall_number=$(wc -l < \"$FAKE_AGENTCTL_CALLS\")\ncall_number=$((call_number + 1))\nprintf '%s\\n' \"$call_number\" >> \"$FAKE_AGENTCTL_CALLS\"\nresponse=\"$FAKE_AGENTCTL_JSON\"\nif [[ -n \"${FAKE_AGENTCTL_AFTER:-}\" && \"$call_number\" -ge \"${FAKE_AGENTCTL_AFTER_CALL:-3}\" ]]; then response=\"$FAKE_AGENTCTL_AFTER\"; fi\nif [[ -n \"${FAKE_AGENTCTL_SWITCH_MARKER:-}\" && -f \"$FAKE_AGENTCTL_SWITCH_MARKER\" ]]; then response=\"$FAKE_AGENTCTL_AFTER\"; fi\ncat \"$response\"\nif [[ -n \"${FAKE_AGENTCTL_SWITCH_AFTER_CALL:-}\" && \"$call_number\" == \"$FAKE_AGENTCTL_SWITCH_AFTER_CALL\" ]]; then : > \"$FAKE_AGENTCTL_SWITCH_MARKER\"; fi\n",
    );
    executable(
        &bin.join("pg_isready"),
        "#!/usr/bin/env bash\nhost= port=\nwhile (($#)); do case \"$1\" in -h) host=$2; shift 2;; -p) port=$2; shift 2;; *) shift;; esac; done\nprintf '%s\\t%s\\n' \"$host\" \"$port\" > \"$FAKE_PG_CAPTURE\"\nexit 0\n",
    );
    executable(
        &bin.join("psql"),
        "#!/usr/bin/env bash\nport=\nwhile (($#)); do case \"$1\" in -p) port=$2; shift 2;; *) shift;; esac; done\ncase \"$FAKE_PSQL_MODE\" in ready) printf 'sinex_dev|%s\\n' \"$port\";; foreign) printf 'other_db|%s\\n' \"$port\";; malformed) printf 'not-a-postgres-identity\\n';; absent) exit 1;; esac\n",
    );
    executable(
        &bin.join("nix"),
        "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$FAKE_NIX_CAPTURE\"\nif [[ \"$1\" == develop ]]; then\n  while (($#)) && [[ \"$1\" != --command ]]; do shift; done\n  [[ \"$1\" == --command ]] || exit 2\n  shift\n  exec \"$@\"\nfi\nenv | sort > \"$FAKE_XTASK_CAPTURE\"\n",
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
    let agentctl_switch_marker = fixture.path().join("agentctl-switch");
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
            let expected_connections = if matches!(nats_mode, NatsMode::Ready) {
                2
            } else {
                1
            };
            let mut connections = 0;
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        connections += 1;
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
                        if connections >= expected_connections {
                            return;
                        }
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
    env.insert(
        "FAKE_PSQL_MODE",
        match postgres_mode {
            PostgresMode::Ready => "ready",
            PostgresMode::Foreign => "foreign",
            PostgresMode::Malformed => "malformed",
            PostgresMode::Absent => "absent",
        }
        .to_string(),
    );
    env.insert("FAKE_XTASK_CAPTURE", xtask_capture.display().to_string());
    env.insert("FAKE_NIX_CAPTURE", nix_capture.display().to_string());
    if lease_after.is_some() {
        env.insert(
            "FAKE_AGENTCTL_AFTER",
            lease_after_file.display().to_string(),
        );
        env.insert("FAKE_AGENTCTL_AFTER_CALL", "3".to_string());
    }
    if lease_id == "77777777-7777-4777-8777-777777777777" {
        env.insert(
            "FAKE_AGENTCTL_SWITCH_MARKER",
            agentctl_switch_marker.display().to_string(),
        );
        env.insert("FAKE_AGENTCTL_SWITCH_AFTER_CALL", "3".to_string());
        env.insert("FAKE_AGENTCTL_AFTER_CALL", "999".to_string());
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

fn assert_five_identity_reads(fixture: &TempDir) {
    assert_eq!(
        fs::read_to_string(fixture.path().join("agentctl-calls"))
            .expect("read AgentCTL call log")
            .lines()
            .count(),
        5,
        "execution must perform identity reads before and after the command"
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
        PostgresMode::Ready,
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
    assert_five_identity_reads(&fixture);
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
        PostgresMode::Absent,
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
        PostgresMode::Ready,
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
        PostgresMode::Ready,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NATS protocol readiness failed"));
}

#[test]
fn foreign_postgres_database_is_rejected_by_protocol_identity() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "49494949-4949-4949-8494-949494949494";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Absent,
        PostgresMode::Foreign,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("PostgreSQL protocol identity"));
}

#[test]
fn malformed_postgres_protocol_identity_is_rejected() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "48484848-4848-4848-8484-848484848484";
    let nats_port = free_nats_port();
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some(&lease),
        nats_port,
        NatsMode::Absent,
        PostgresMode::Malformed,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("PostgreSQL protocol identity"));
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
        PostgresMode::Ready,
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
        PostgresMode::Ready,
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
            PostgresMode::Ready,
            None,
            ExecutionPath::SelectedBinary,
        );
        assert!(!output.status.success());
        assert!(!fixture.path().join("xtask-capture").exists());
    }
}

#[test]
fn malformed_state_project_and_lease_identities_are_rejected() {
    let cases: [(&str, fn(&mut serde_json::Value)); 3] = [
        ("state", |value: &mut serde_json::Value| {
            value["payload"]["value"]["state"]["phase"] = json!("not-a-phase");
        }),
        ("project", |value: &mut serde_json::Value| {
            value["payload"]["value"]["project_id"] = json!("foreign-project");
        }),
        ("lease", |value: &mut serde_json::Value| {
            value["payload"]["value"]["lease"]["id"] =
                json!("99999999-9999-4999-8999-999999999999");
        }),
    ];
    for (index, (identity, mutate)) in cases.into_iter().enumerate() {
        let fixture = tempfile::tempdir().expect("create fixture directory");
        let lease_id = format!("9{index}999999-9999-4999-8999-99999999999{index}");
        let lease = mutate_lease(&lease_json(&repo_root(), &lease_id, 45559, 44308), mutate);
        let output = run_checkout_env(
            &fixture,
            &lease_id,
            Some(&lease),
            44308,
            NatsMode::Absent,
            PostgresMode::Ready,
            None,
            ExecutionPath::SelectedBinary,
        );
        assert!(
            !output.status.success(),
            "malformed {identity} identity passed"
        );
        assert!(!fixture.path().join("xtask-capture").exists());
    }
}

#[test]
fn malformed_agentctl_response_is_rejected() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "90909090-9090-4990-8990-909090909090";
    let output = run_checkout_env(
        &fixture,
        lease_id,
        Some("not-json"),
        44308,
        NatsMode::Absent,
        PostgresMode::Absent,
        None,
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
}

#[test]
fn cancellation_during_final_validation_is_rejected_before_launch() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "79797979-7979-4797-8797-979797979797";
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
        PostgresMode::Ready,
        Some(&cancelled),
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("changed before execution"));
}

#[test]
fn cancellation_between_final_identity_read_and_launch_cannot_pass() {
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
        PostgresMode::Ready,
        Some(&cancelled),
        ExecutionPath::SelectedBinary,
    );
    assert!(!output.status.success());
    assert!(fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("changed during execution"));
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
        PostgresMode::Ready,
        None,
        ExecutionPath::NixFallback,
    );
    assert!(output.status.success(), "fallback failed: {output:?}");
    assert!(fixture.path().join("nix-capture").exists());
    let captured: HashMap<_, _> = fs::read_to_string(fixture.path().join("xtask-capture"))
        .expect("read fallback xtask environment")
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect();
    assert!(!captured.contains_key("SINEX_PRE_PUSH_AGENTCTL_LEASE_ID"));
    assert!(!captured.contains_key("SINEX_PRE_PUSH_AGENTCTL_PG_RUN_DIR"));
    assert_five_identity_reads(&fixture);
}

#[derive(Clone, Copy)]
enum SelectorRoute {
    ActiveBinary,
    PathBinary,
    CachedBinary,
    CheckoutWrapper,
}

fn run_selector_route(route: SelectorRoute) -> String {
    let fixture = tempfile::tempdir().expect("create selector fixture directory");
    let active = fixture.path().join("active");
    let path = fixture.path().join("path");
    let cache = fixture.path().join("cache");
    let wrapper = fixture.path().join(".direnv/bin");
    for directory in [&active, &path, &cache, &wrapper] {
        fs::create_dir_all(directory).expect("create selector directory");
    }
    for candidate in [
        active.join("xtask"),
        path.join("xtask"),
        cache.join("xtask"),
        wrapper.join("xtask"),
    ] {
        executable(&candidate, "#!/usr/bin/env bash\nexit 0\n");
    }
    let capture = fixture.path().join("selector-capture");
    let route_name = match route {
        SelectorRoute::ActiveBinary => "active",
        SelectorRoute::PathBinary => "path",
        SelectorRoute::CachedBinary => "cache",
        SelectorRoute::CheckoutWrapper => "wrapper",
    };
    let command = r#"
source "$1"
REPO_ROOT="$2"
ROUTE="$3"
CAPTURE="$4"
PATH="$5:$PATH"
PATH_CANDIDATE="$6"
CACHE_CANDIDATE="$7"
WRAPPER_CANDIDATE="$8"
_sinex_pre_push_branch_changes_xtask_build_inputs() { return 1; }
_sinex_pre_push_xtask_binary_is_usable_for_branch() { return 0; }
_sinex_pre_push_xtask_env_matches_checkout() { [[ "$ROUTE" == active ]]; }
_sinex_pre_push_checkout_xtask_binary() { return 1; }
_sinex_pre_push_path_xtask_binary() { [[ "$ROUTE" == path ]] && printf '%s\n' "$PATH_CANDIDATE"; }
_sinex_pre_push_cached_xtask_binary() { [[ "$ROUTE" == cache ]] && printf '%s\n' "$CACHE_CANDIDATE"; }
_sinex_pre_push_checkout_xtask_wrapper() { [[ "$ROUTE" == wrapper ]] && printf '%s\n' "$WRAPPER_CANDIDATE"; }
_sinex_pre_push_selected_xtask() { printf 'selected|%s|%s\n' "$1" "$2" > "$CAPTURE"; }
_sinex_pre_push_checkout_env() { printf 'wrapper|%s\n' "$1" > "$CAPTURE"; }
_sinex_pre_push_run_xtask check
"#;
    let output = Command::new("bash")
        .arg("-c")
        .arg(command)
        .arg("selector-test")
        .arg(repo_root())
        .arg(route_name)
        .arg(&capture)
        .arg(&active)
        .arg(&path.join("xtask"))
        .arg(&cache.join("xtask"))
        .arg(&wrapper.join("xtask"))
        .env(
            "SINEX_DEV_ROOT",
            if matches!(route, SelectorRoute::ActiveBinary) {
                repo_root()
            } else {
                PathBuf::from("/foreign/checkout")
            },
        )
        .output()
        .expect("run selector route");
    assert!(
        output.status.success(),
        "selector route failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    fs::read_to_string(capture).expect("read selector capture")
}

#[test]
fn selector_routes_choose_each_independent_candidate() {
    let active = run_selector_route(SelectorRoute::ActiveBinary);
    assert!(
        active.starts_with("selected|active devshell xtask|"),
        "{active}"
    );
    assert!(active.ends_with("/active/xtask\n"), "{active}");

    let path = run_selector_route(SelectorRoute::PathBinary);
    assert!(
        path.starts_with("selected|read-only PATH xtask fallback|"),
        "{path}"
    );
    assert!(path.ends_with("/path/xtask\n"), "{path}");

    let cache = run_selector_route(SelectorRoute::CachedBinary);
    assert!(
        cache.starts_with("selected|read-only cached xtask fallback|"),
        "{cache}"
    );
    assert!(cache.ends_with("/cache/xtask\n"), "{cache}");

    let wrapper = run_selector_route(SelectorRoute::CheckoutWrapper);
    assert!(wrapper.starts_with("wrapper|"), "{wrapper}");
    assert!(wrapper.ends_with("/.direnv/bin/xtask\n"), "{wrapper}");
}

#[test]
fn dev_services_cache_identity_excludes_allocated_ports() {
    let descriptor: toml::Value =
        toml::from_str(include_str!("../../../.agentctl/project.toml")).expect("parse descriptor");
    let dev_services = &descriptor["operations"]["dev_services"];
    assert_eq!(dev_services["cache"].as_str(), Some("tree+environment"));
    assert_eq!(
        descriptor["workspace"]["provider"].as_str(),
        Some("git-worktree")
    );
    assert_eq!(
        descriptor["workspace"]["verification_operations"]
            .as_array()
            .expect("verification operations")
            .iter()
            .map(|value| value.as_str().expect("operation name"))
            .collect::<Vec<_>>(),
        vec!["check_default"]
    );
    let inherited = descriptor["environment"]["inherit"]
        .as_array()
        .expect("inherited environment");
    for allocated in ["SINEX_DEV_POSTGRES_PORT", "SINEX_DEV_NATS_PORT"] {
        assert!(
            !inherited
                .iter()
                .any(|value| value.as_str() == Some(allocated)),
            "allocated service output {allocated} must not enter cache identity"
        );
    }
}

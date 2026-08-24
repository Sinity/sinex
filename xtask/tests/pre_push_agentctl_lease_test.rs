#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Behavior coverage for the explicit AgentCTL lease handoff used by the
//! pre-push hook. These tests execute the real shell functions and fake only
//! the external AgentCTL, PostgreSQL probe, and selected xtask command.

use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

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
    json!({
        "payload": { "value": {
            "job_id": lease_id,
            "project_id": "sinex",
            "operation": "dev_services",
            "checkout": { "project_id": "sinex", "path": root },
            "state": {
                "phase": "running",
                "terminal": false,
                "systemd": { "ActiveState": "active", "SubState": "running" }
            },
            "lease": {
                "id": lease_id,
                "host": "127.0.0.1",
                "readiness": "project-command",
                "lifetime": "job",
                "state": "active",
                "ports": [
                    { "name": "postgres", "environment": "SINEX_DEV_POSTGRES_PORT", "port": postgres_port },
                    { "name": "nats", "environment": "SINEX_DEV_NATS_PORT", "port": nats_port }
                ]
            }
        }}
    })
    .to_string()
}

fn run_checkout_env(
    fixture: &TempDir,
    lease_id: &str,
    lease: Option<&str>,
    nats_port: u16,
    listen_for_nats: bool,
) -> Output {
    let bin = fixture.path().join("bin");
    let pg_bin = fixture.path().join("pg-bin");
    fs::create_dir_all(&bin).expect("create fake bin");
    fs::create_dir_all(&pg_bin).expect("create fake postgres bin");
    executable(&pg_bin.join("postgres"), "#!/usr/bin/env bash\nexit 0\n");
    executable(
        &bin.join("agentctl"),
        "#!/usr/bin/env bash\nif [[ \"$1 $2 $3\" != \"job get $FAKE_AGENTCTL_EXPECTED_ID\" ]]; then exit 2; fi\nif [[ \"${FAKE_AGENTCTL_STATUS:-0}\" != 0 ]]; then exit \"$FAKE_AGENTCTL_STATUS\"; fi\nif [[ ! -s \"$FAKE_AGENTCTL_JSON\" ]]; then exit 1; fi\ncat \"$FAKE_AGENTCTL_JSON\"\n",
    );
    executable(
        &bin.join("pg_isready"),
        "#!/usr/bin/env bash\nhost= port=\nwhile (($#)); do case \"$1\" in -h) host=$2; shift 2;; -p) port=$2; shift 2;; *) shift;; esac; done\nprintf '%s\\t%s\\n' \"$host\" \"$port\" > \"$FAKE_PG_CAPTURE\"\nexit 0\n",
    );
    let xtask = fixture.path().join("xtask");
    executable(
        &xtask,
        "#!/usr/bin/env bash\nenv | sort > \"$FAKE_XTASK_CAPTURE\"\n",
    );
    let lease_file = fixture.path().join("lease.json");
    fs::write(&lease_file, lease.unwrap_or_default()).expect("write fake lease response");
    let pg_capture = fixture.path().join("pg-capture");
    let xtask_capture = fixture.path().join("xtask-capture");
    let rc = fixture.path().join("devshell.rc");
    fs::write(&rc, "# test devshell\n").expect("write fake devshell rc");

    let listener = listen_for_nats
        .then(|| TcpListener::bind(("127.0.0.1", nats_port)).expect("bind fake NATS"));
    let listener_thread = listener.map(|listener| {
        listener
            .set_nonblocking(true)
            .expect("set fake NATS nonblocking");
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while Instant::now() < deadline {
                if listener.accept().is_ok() {
                    return;
                }
                thread::yield_now();
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
_sinex_pre_push_checkout_env "$5"
"#;
    let mut env = HashMap::new();
    env.insert(
        "PATH",
        format!("{}:{}", bin.display(), std::env::var("PATH").unwrap()),
    );
    env.insert("SINEX_PRE_PUSH_AGENTCTL_LEASE_ID", lease_id.to_string());
    env.insert("FAKE_AGENTCTL_EXPECTED_ID", lease_id.to_string());
    env.insert("FAKE_AGENTCTL_JSON", lease_file.display().to_string());
    env.insert("FAKE_PG_CAPTURE", pg_capture.display().to_string());
    env.insert("FAKE_XTASK_CAPTURE", xtask_capture.display().to_string());
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
        .envs(env);
    let output = process.output().expect("execute pre-push lease helper");
    if let Some(thread) = listener_thread {
        thread.join().expect("join fake NATS listener");
    }
    output
}

#[test]
fn active_lease_propagates_exact_ports_and_postgres_socket() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "11111111-1111-4111-8111-111111111111";
    let nats_port = (44308..=44435)
        .find(|port| TcpListener::bind(("127.0.0.1", *port)).is_ok())
        .expect("find a free AgentCTL NATS port");
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(&fixture, lease_id, Some(&lease), nats_port, true);
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
}

#[test]
fn stale_lease_is_rejected_before_xtask_runs() {
    let fixture = tempfile::tempdir().expect("create fixture directory");
    let lease_id = "22222222-2222-4222-8222-222222222222";
    let output = run_checkout_env(&fixture, lease_id, None, 44308, false);
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
    let nats_port = (44308..=44435)
        .find(|port| TcpListener::bind(("127.0.0.1", *port)).is_ok())
        .expect("find a free AgentCTL NATS port");
    let lease = lease_json(&repo_root(), lease_id, 45559, nats_port);
    let output = run_checkout_env(&fixture, lease_id, Some(&lease), nats_port, false);
    assert!(
        !output.status.success(),
        "unreachable lease unexpectedly passed: {output:?}"
    );
    assert!(!fixture.path().join("xtask-capture").exists());
    assert!(String::from_utf8_lossy(&output.stderr).contains("NATS is not reachable"));
}

//! Regression coverage for sinex-1ul: dev Postgres binds a per-checkout unix
//! socket (not TCP 5432) and must fail fast + clean up a stale pidfile left
//! behind by a killed/crashed postmaster, rather than hanging the schema-apply
//! bootstrap forever waiting on a dead process's socket.
//!
//! This test extracts `_sinex_cargo_cleanup_stale_postgres_pid` verbatim from
//! the live `flake.nix` devshell hook and exercises it under `bash` against a
//! throwaway directory, so a regression in the stale-pidfile-cleanup logic
//! (malformed pid, dead pid, unrelated live pid, or a genuinely live
//! postmaster) fails this test without any change here.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

const FUNCTION_NAME: &str = "_sinex_cargo_cleanup_stale_postgres_pid";

fn extract_function() -> String {
    let flake_nix = std::fs::read_to_string(repo_root().join("flake.nix"))
        .expect("flake.nix must be readable from the repo root");

    let mut out = String::new();
    let mut active = false;
    for line in flake_nix.lines() {
        if !active && line == format!("                {FUNCTION_NAME}() {{") {
            active = true;
        }
        if active {
            out.push_str(&line.replace("''${", "${"));
            out.push('\n');
            if line == "                }" {
                active = false;
            }
        }
    }
    assert!(
        out.contains(&format!("{FUNCTION_NAME}()")),
        "flake.nix no longer defines {FUNCTION_NAME} at the expected fixed \
         indentation -- this test's extraction must be updated alongside any \
         refactor of the dev-Postgres bootstrap functions"
    );
    out
}

struct Fixture {
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("pgdata")).unwrap();
        std::fs::create_dir_all(dir.path().join("pgrun")).unwrap();
        Self { dir }
    }

    fn pgdata(&self) -> PathBuf {
        self.dir.path().join("pgdata")
    }

    fn pgrun(&self) -> PathBuf {
        self.dir.path().join("pgrun")
    }

    fn pid_file(&self) -> PathBuf {
        self.pgdata().join("postmaster.pid")
    }

    fn socket_path(&self) -> PathBuf {
        self.pgrun().join(".s.PGSQL.65432")
    }

    fn run(&self) -> (bool, String, String) {
        let functions = extract_function();
        let mut script = functions;
        script.push_str(&format!("{FUNCTION_NAME}\n"));
        let output = Command::new("bash")
            .arg("-c")
            .arg(script)
            .env("pgdata", self.pgdata())
            .env("pgrun", self.pgrun())
            .env("pgport", "65432")
            .output()
            .expect("bash must be on PATH to run this test");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    }
}

#[test]
fn no_pid_file_is_a_silent_noop() {
    let fx = Fixture::new();
    let (ok, _out, err) = fx.run();
    assert!(ok, "must succeed when there is no pidfile at all");
    assert!(err.is_empty(), "must not warn when there is nothing to clean up: {err}");
}

#[test]
fn malformed_pid_file_is_removed() {
    let fx = Fixture::new();
    std::fs::write(fx.pid_file(), "not-a-pid\nrest of file\n").unwrap();
    std::fs::write(fx.socket_path(), "").unwrap();

    let (ok, _out, err) = fx.run();
    assert!(ok);
    assert!(
        err.contains("malformed"),
        "must warn about a malformed pidfile, got stderr: {err}"
    );
    assert!(!fx.pid_file().exists(), "malformed pidfile must be removed");
    assert!(!fx.socket_path().exists(), "stale socket must be removed alongside the pidfile");
}

#[test]
fn dead_pid_file_is_removed() {
    let fx = Fixture::new();
    // PID 1 always exists on a real system but is never OUR postmaster, and
    // PID 0 / a very large unused PID reliably fails kill -0. Use a
    // definitely-dead high PID instead of guessing at kernel pid_max.
    std::fs::write(fx.pid_file(), "999999999\n").unwrap();
    std::fs::write(fx.socket_path(), "").unwrap();

    let (ok, _out, err) = fx.run();
    assert!(ok);
    assert!(
        err.contains("stale") && err.contains("dead PID"),
        "must warn about a stale pidfile for a dead PID, got stderr: {err}"
    );
    assert!(!fx.pid_file().exists(), "dead-PID pidfile must be removed");
}

#[test]
fn live_but_unrelated_pid_is_removed() {
    let fx = Fixture::new();
    // Our own test process is alive but its /proc/<pid>/cmdline will not
    // contain this fixture's throwaway pgdata path, so it must be treated
    // as "unrelated live PID reusing this number" and cleaned up.
    let our_pid = std::process::id();
    std::fs::write(fx.pid_file(), format!("{our_pid}\n")).unwrap();
    std::fs::write(fx.socket_path(), "").unwrap();

    let (ok, _out, err) = fx.run();
    assert!(ok);
    assert!(
        err.contains("unrelated live PID"),
        "must warn about an unrelated live PID reusing the number, got stderr: {err}"
    );
    assert!(!fx.pid_file().exists(), "unrelated-live-PID pidfile must be removed");
}

#[test]
fn live_matching_postmaster_is_left_alone() {
    let fx = Fixture::new();
    // Spawn a real child whose cmdline we control: pass the fixture's pgdata
    // path as an argv element so `_sinex_cargo_cleanup_stale_postgres_pid`'s
    // cmdline-contains-pgdata check finds a match, exactly like a real
    // postmaster invoked with `-D "$pgdata"`.
    // `sh -c 'sleep 30' <pgdata_arg>` sets $0 to pgdata_arg for the script,
    // so the *sh* process's own /proc/<pid>/cmdline contains the pgdata path
    // (as a real postmaster's cmdline would via `-D "$pgdata"`), while the
    // sleep itself runs as sh's child, keeping the parent alive for the test.
    let pgdata_arg = fx.pgdata().to_string_lossy().to_string();
    let mut child = Command::new("sh")
        .arg("-c")
        .arg("sleep 30")
        .arg(&pgdata_arg)
        .spawn()
        .expect("failed to spawn a live process to mimic a real postmaster");
    let pid = child.id();
    std::fs::write(fx.pid_file(), format!("{pid}\n")).unwrap();
    std::fs::write(fx.socket_path(), "marker").unwrap();

    let (ok, _out, err) = fx.run();
    let _ = child.kill();
    let _ = child.wait();

    assert!(ok);
    assert!(
        err.is_empty(),
        "must not warn or remove anything for a genuinely live, matching postmaster: {err}"
    );
    assert!(
        fx.pid_file().exists(),
        "pidfile for a live matching postmaster must be preserved"
    );
    assert!(
        fx.socket_path().exists(),
        "socket for a live matching postmaster must be preserved"
    );
}

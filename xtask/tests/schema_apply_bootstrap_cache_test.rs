//! Regression coverage for sinex-guw: the devshell `cargo`/`xtask` launcher
//! used to rebuild `schema-apply-bootstrap` through `nix build` on every
//! invocation, adding ~30-60s of serial latency even for quick reruns. The
//! fix caches the built binary path under `$SINEX_DEV_STATE_DIR`, keyed by a
//! content fingerprint over the source files that can affect the build
//! (`Cargo.toml`, `Cargo.lock`, `flake.nix`, `crate/sinex-schema`,
//! `crate/sinex-primitives`).
//!
//! These tests extract the real `_sinex_schema_apply_bootstrap_fingerprint`
//! and `_sinex_schema_apply_bootstrap_bin` shell functions verbatim out of
//! the repo's live `flake.nix` and execute them under `bash` against a
//! throwaway git fixture repo, so a regression in the caching logic (e.g.
//! widening/narrowing the fingerprinted path set, or dropping the
//! cache-hit short-circuit) makes these tests fail without any change here.

use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

const WANTED_FUNCTIONS: &[&str] = &[
    "_sinex_schema_apply_bootstrap_fingerprint",
    "_sinex_schema_apply_bootstrap_bin",
];

/// Extract the two caching functions verbatim from the live `flake.nix` at
/// their fixed 16-space devshell-hook indentation, un-escaping the Nix
/// `''${` multi-line-string escape back to `${`.
fn extract_flake_functions() -> String {
    let flake_nix = std::fs::read_to_string(repo_root().join("flake.nix"))
        .expect("flake.nix must be readable from the repo root");

    let mut out = String::new();
    let mut current: Option<&str> = None;
    for line in flake_nix.lines() {
        if current.is_none() {
            for name in WANTED_FUNCTIONS {
                if line == format!("                {name}() {{") {
                    current = Some(name);
                    break;
                }
            }
        }
        if let Some(name) = current {
            out.push_str(&line.replace("''${", "${"));
            out.push('\n');
            if line == "                }" {
                current = None;
            }
        }
    }

    for name in WANTED_FUNCTIONS {
        assert!(
            out.contains(&format!("{name}()")),
            "flake.nix no longer defines {name} at the expected fixed \
             indentation -- this test's extraction must be updated alongside \
             any refactor of the devshell caching functions"
        );
    }
    out
}

fn init_fixture_repo(dir: &Path) {
    let run = |args: &[&str]| {
        let status = Command::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git must be on PATH");
        assert!(status.success(), "git {args:?} failed");
    };
    run(&["init", "-q"]);
    run(&["config", "user.email", "test@example.invalid"]);
    run(&["config", "user.name", "test"]);

    std::fs::write(dir.join("Cargo.toml"), "[workspace]\nmembers = []\n").unwrap();
    std::fs::write(dir.join("flake.nix"), "{ }\n").unwrap();
    std::fs::create_dir_all(dir.join("crate/sinex-schema/src")).unwrap();
    std::fs::create_dir_all(dir.join("crate/sinex-primitives/src")).unwrap();
    std::fs::create_dir_all(dir.join("crate/sinexd/src")).unwrap();
    std::fs::write(
        dir.join("crate/sinex-schema/src/lib.rs"),
        "// schema v1\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("crate/sinex-primitives/src/lib.rs"),
        "// primitives v1\n",
    )
    .unwrap();
    std::fs::write(dir.join("crate/sinexd/src/lib.rs"), "// unrelated crate\n").unwrap();

    run(&["add", "-A"]);
    run(&["commit", "-q", "-m", "init"]);
}

fn run_fingerprint(functions: &str, root_dir: &Path) -> String {
    let mut script = functions.to_string();
    script.push_str("_sinex_schema_apply_bootstrap_fingerprint\n");

    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("root_dir", root_dir)
        .output()
        .expect("bash must be on PATH to run this test");
    assert!(
        output.status.success(),
        "fingerprint function exited non-zero: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[test]
fn fingerprint_is_deterministic_across_repeated_calls() {
    let functions = extract_flake_functions();
    let fixture = tempfile::tempdir().expect("tempdir");
    init_fixture_repo(fixture.path());

    let a = run_fingerprint(&functions, fixture.path());
    let b = run_fingerprint(&functions, fixture.path());
    assert_eq!(
        a, b,
        "fingerprint must be stable across repeated calls with no source changes"
    );
    assert!(!a.is_empty(), "fingerprint must not be empty");
}

#[test]
fn fingerprint_changes_when_a_watched_crate_changes() {
    let functions = extract_flake_functions();
    let fixture = tempfile::tempdir().expect("tempdir");
    init_fixture_repo(fixture.path());

    let before = run_fingerprint(&functions, fixture.path());

    std::fs::write(
        fixture.path().join("crate/sinex-schema/src/lib.rs"),
        "// schema v2 -- content changed\n",
    )
    .unwrap();

    let after = run_fingerprint(&functions, fixture.path());
    assert_ne!(
        before, after,
        "fingerprint must change when crate/sinex-schema content changes on disk, \
         even without a git commit (this is the cache-invalidation signal the \
         devshell wrapper relies on)"
    );
}

#[test]
fn fingerprint_ignores_changes_outside_the_watched_paths() {
    let functions = extract_flake_functions();
    let fixture = tempfile::tempdir().expect("tempdir");
    init_fixture_repo(fixture.path());

    let before = run_fingerprint(&functions, fixture.path());

    std::fs::write(
        fixture.path().join("crate/sinexd/src/lib.rs"),
        "// unrelated crate, edited\n",
    )
    .unwrap();

    let after = run_fingerprint(&functions, fixture.path());
    assert_eq!(
        before, after,
        "fingerprint must NOT change for edits outside Cargo.toml/Cargo.lock/flake.nix/\
         crate/sinex-schema/crate/sinex-primitives -- otherwise every unrelated commit \
         would force a schema-apply-bootstrap rebuild"
    );
}

/// Full cache-hit path through `_sinex_schema_apply_bootstrap_bin`: a
/// pre-populated fingerprint+path cache matching the current fingerprint
/// must be returned WITHOUT invoking `nix build` at all.
#[test]
fn bin_returns_cached_path_without_invoking_nix_when_fingerprint_matches() {
    let functions = extract_flake_functions();
    let fixture = tempfile::tempdir().expect("tempdir");
    init_fixture_repo(fixture.path());

    let dev_state_dir = fixture.path().join("dev-state");
    std::fs::create_dir_all(dev_state_dir.join("run/logs")).unwrap();

    let fingerprint = run_fingerprint(&functions, fixture.path());

    let fake_bin = fixture.path().join("fake-schema-apply-bootstrap");
    std::fs::write(&fake_bin, "#!/bin/sh\necho fake\n").unwrap();
    std::fs::set_permissions(
        &fake_bin,
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();

    std::fs::write(
        dev_state_dir.join("schema-apply-bootstrap.fingerprint"),
        &fingerprint,
    )
    .unwrap();
    std::fs::write(
        dev_state_dir.join("schema-apply-bootstrap.path"),
        fake_bin.to_str().unwrap(),
    )
    .unwrap();

    // A `nix` on PATH that would prove itself invoked by writing a marker
    // file -- the cache-hit path must never reach this.
    let bin_dir = fixture.path().join("stub-bin");
    std::fs::create_dir_all(&bin_dir).unwrap();
    let nix_invoked_marker = fixture.path().join("nix-was-invoked");
    std::fs::write(
        bin_dir.join("nix"),
        format!(
            "#!/bin/sh\ntouch {}\necho stub-nix-should-not-run >&2\nexit 1\n",
            nix_invoked_marker.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(
        bin_dir.join("nix"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();

    let mut script = functions.clone();
    script.push_str("_sinex_schema_apply_bootstrap_bin\n");

    let path_env = format!(
        "{}:{}",
        bin_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("root_dir", fixture.path())
        .env("SINEX_DEV_STATE_DIR", &dev_state_dir)
        .env("pglog", dev_state_dir.join("run/logs"))
        .env("bootstrap_log", dev_state_dir.join("run/logs/bootstrap.log"))
        .env("PATH", path_env)
        .output()
        .expect("bash must be on PATH to run this test");

    assert!(
        output.status.success(),
        "cache-hit path must exit 0, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        fake_bin.to_str().unwrap(),
        "must print the cached binary path on a fingerprint hit"
    );
    assert!(
        !nix_invoked_marker.exists(),
        "nix build must NOT run when the cache fingerprint matches -- this is \
         the exact rebuild-avoidance sinex-guw's fix provides"
    );
}

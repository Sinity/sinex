//! Regression coverage for sinex-sbm: two `xtask --bg` command frontends
//! launched concurrently must never both attempt the checkout-local SQLx
//! bootstrap (schema apply DDL) before one is queued behind the coordinated
//! background-job slot -- that race produced a real Postgres
//! duplicate-index/pg_class error on 2026-07-03.
//!
//! The fix (`_sinex_xtask_is_launcher_only_background_request`) makes a
//! bare, unqueued `xtask ... --bg` invocation (no `XTASK_BG_JOB_ID`/
//! `XTASK_BG_INVOCATION_ID` in the environment, `--bg` present without
//! `--fg`) skip the pre-exec SQLx bootstrap entirely via
//! `_sinex_xtask_requires_sqlx_database`, so the DDL only ever runs once
//! the *actual* background worker (which sets those env vars) is
//! dispatched by the coordinator. These tests extract the real functions
//! out of the repo's live `flake.nix`, so narrowing the launcher-only
//! detection back to nothing (recreating the sinex-sbm race) makes this
//! test fail without any change here.

use std::path::PathBuf;
use std::process::{Command, Stdio};

const WANTED_FUNCTIONS: &[&str] = &[
    "_sinex_xtask_command_name",
    "_sinex_xtask_is_help_request",
    "_sinex_xtask_is_launcher_only_background_request",
    "_sinex_xtask_requires_sqlx_database",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

/// Extract the named functions verbatim from `flake.nix`, stubbing out the
/// deeper predicates (`_sinex_xtask_is_dependency_bootstrap_subcommand`,
/// `_sinex_xtask_is_vm_test_subcommand`, `_sinex_xtask_is_no_compile_subcommand`,
/// `_sinex_xtask_changed_strict_has_no_rust_delta`,
/// `_sinex_xtask_is_read_only_subcommand`) that
/// `_sinex_xtask_requires_sqlx_database` delegates to for unrelated
/// early-outs -- this test targets the launcher-only-background-request
/// gate specifically, not those other predicates' own git/filesystem logic.
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
                assert!(
                    out.trim_end().ends_with('}'),
                    "extracted {name} did not terminate on a bare '}}' line"
                );
                current = None;
            }
        }
    }

    for name in WANTED_FUNCTIONS {
        assert!(
            out.contains(&format!("{name}()")),
            "flake.nix no longer defines {name} at the expected fixed \
             indentation -- this test's extraction must be updated \
             alongside any refactor of the devshell wrapper functions"
        );
    }

    // Deeper predicates default to "not this class" (return 1) so the test
    // isolates the launcher-only-background-request gate. Configurable via
    // env for callers that need a different stub result.
    out.push_str(
        r#"
_sinex_xtask_is_dependency_bootstrap_subcommand() { return "${STUB_DEP_RC:-1}"; }
_sinex_xtask_is_vm_test_subcommand() { return "${STUB_VM_RC:-1}"; }
_sinex_xtask_is_no_compile_subcommand() { return "${STUB_FIX_RC:-1}"; }
_sinex_xtask_changed_strict_has_no_rust_delta() { return "${STUB_CHECK_RC:-1}"; }
_sinex_xtask_is_read_only_subcommand() { return "${STUB_RO_RC:-1}"; }
"#,
    );
    out
}

/// Run `bash -c '<extracted functions>; env...; <fn> "$@"'` and return
/// whether it exited zero.
fn call_with_env(
    function_source: &str,
    function: &str,
    args: &[&str],
    env: &[(&str, &str)],
) -> bool {
    let mut script = function_source.to_string();
    script.push_str(function);
    script.push_str(" \"$@\"\n");

    let mut cmd = Command::new("bash");
    cmd.arg("-c").arg(script).arg("--").args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("bash must be on PATH to run this test");
    let status = child.wait().expect("bash did not exit");
    if !status.success() && !status.code().map(|c| c == 1).unwrap_or(false) {
        let mut buf = Vec::new();
        use std::io::Read;
        child.stderr.take().unwrap().read_to_end(&mut buf).ok();
        panic!(
            "unexpected bash exit {status:?} for {function} {args:?}: {}",
            String::from_utf8_lossy(&buf)
        );
    }
    status.success()
}

fn call(function_source: &str, function: &str, args: &[&str]) -> bool {
    call_with_env(function_source, function, args, &[])
}

/// The exact sinex-sbm regression: a bare launcher-side `xtask test --bg`
/// (no worker env vars set) must be recognized as launcher-only-background.
#[test]
fn test_bare_bg_without_worker_env_is_launcher_only() {
    let functions = extract_flake_functions();
    for &args in &[
        &["test", "--bg"][..],
        &["fix", "--bg"][..],
        &["build", "--bg"][..],
    ] {
        assert!(
            call(
                &functions,
                "_sinex_xtask_is_launcher_only_background_request",
                args
            ),
            "{args:?} with no XTASK_BG_JOB_ID/XTASK_BG_INVOCATION_ID must be \
             judged launcher-only -- this is exactly the unqueued frontend \
             invocation that raced on the pre-exec SQLx bootstrap in the \
             sinex-sbm incident"
        );
    }
}

/// Once the coordinator dispatches the actual background worker (which sets
/// XTASK_BG_JOB_ID / XTASK_BG_INVOCATION_ID), the same `--bg` flag must no
/// longer be treated as launcher-only -- the worker DOES need to run the
/// bootstrap (behind the coordinated slot, serialized by flock).
#[test]
fn test_bg_with_worker_env_is_not_launcher_only() {
    let functions = extract_flake_functions();
    assert!(
        !call_with_env(
            &functions,
            "_sinex_xtask_is_launcher_only_background_request",
            &["test", "--bg"],
            &[("XTASK_BG_JOB_ID", "job-123")],
        ),
        "with XTASK_BG_JOB_ID set, this is the dispatched worker itself, not \
         the launcher -- must not be treated as launcher-only"
    );
    assert!(
        !call_with_env(
            &functions,
            "_sinex_xtask_is_launcher_only_background_request",
            &["test", "--bg"],
            &[("XTASK_BG_INVOCATION_ID", "inv-456")],
        ),
        "with XTASK_BG_INVOCATION_ID set, this is the dispatched worker \
         itself -- must not be treated as launcher-only"
    );
}

/// `--bg` together with `--fg` (foreground-forced) must not be treated as
/// launcher-only-background, matching the flag-precedence semantics.
#[test]
fn test_bg_with_fg_is_not_launcher_only() {
    let functions = extract_flake_functions();
    assert!(
        !call(
            &functions,
            "_sinex_xtask_is_launcher_only_background_request",
            &["test", "--bg", "--fg"]
        ),
        "--bg alongside --fg must not be judged launcher-only-background"
    );
}

/// A plain foreground invocation (no --bg at all) is obviously not a
/// launcher-only background request.
#[test]
fn test_no_bg_flag_is_not_launcher_only() {
    let functions = extract_flake_functions();
    assert!(
        !call(
            &functions,
            "_sinex_xtask_is_launcher_only_background_request",
            &["test"]
        ),
        "a plain 'xtask test' with no --bg must not be launcher-only-background"
    );
}

/// The full regression path: `_sinex_xtask_requires_sqlx_database` must
/// return "not required" (skip the pre-exec DDL bootstrap) for a bare
/// launcher-only `--bg` invocation of a command that would otherwise
/// require it (test/check/build/deps/fix).
#[test]
fn test_requires_sqlx_database_skips_bootstrap_for_launcher_only_bg() {
    let functions = extract_flake_functions();
    for &cmd in &["test", "check", "build", "deps", "fix"] {
        assert!(
            !call(
                &functions,
                "_sinex_xtask_requires_sqlx_database",
                &[cmd, "--bg"]
            ),
            "'{cmd} --bg' from a bare launcher invocation must skip the \
             pre-exec SQLx bootstrap -- this is the sinex-sbm fix. If this \
             regresses, two concurrent 'xtask ... --bg' launches can both \
             attempt checkout-local schema apply DDL before either is \
             queued behind the coordinated slot, racing on Postgres \
             pg_class/index creation exactly as in the original incident."
        );
        // But the same command WITHOUT --bg (a genuine foreground/worker
        // invocation) must still require the bootstrap -- the fix must not
        // have over-widened into skipping DDL for ordinary invocations.
        assert!(
            call(&functions, "_sinex_xtask_requires_sqlx_database", &[cmd]),
            "'{cmd}' without --bg must still require the SQLx bootstrap -- \
             only the launcher-only-background case is exempt"
        );
    }
}

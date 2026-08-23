//! Regression coverage for sinex-ive (P0 false-pass): the checkout-local xtask
//! devshell wrapper in `flake.nix` must never let a stale/unbuildable binary
//! silently stand in for proof-producing commands (`test`, `build`, `schema`,
//! `deps`, `docs`, unqualified `check`/`fix`/`doctor`/`infra`/`run`).
//!
//! These tests extract the real shell functions out of the repo's live
//! `flake.nix` (not a hand-copied replica) and execute them under `bash`, so
//! widening the allowlist in `flake.nix` — the exact regression this bug was —
//! makes this test fail without any change here.

use std::path::PathBuf;
use std::process::{Command, Stdio};

/// Functions pulled verbatim out of `flake.nix`'s devshell hook, in dependency
/// order. `_sinex_xtask_is_observability_command` is the narrow allowlist used
/// when the checkout-local binary is stale and a rebuild just failed — this is
/// the exact gate sinex-ive tightened. `_sinex_xtask_can_use_existing_binary`
/// is the separate, deliberately broader "skip the staleness check entirely"
/// allowlist used only when the binary is *not* stale.
const WANTED_FUNCTIONS: &[&str] = &[
    "_sinex_xtask_command_name",
    "_sinex_xtask_is_help_request",
    "_sinex_xtask_is_observability_command",
    "_sinex_xtask_can_use_existing_binary",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

/// Extract the named functions from the live `flake.nix` by scanning for their
/// `name() {` .. `}` blocks at the devshell hook's fixed indentation, and
/// un-escape the Nix multi-line-string `''${` escape back to `${` so the
/// extracted body is valid POSIX shell. Stubs out the three deeper helper
/// functions that the `check`/`fix`/`doctor`/`infra`/`run` branches delegate
/// to (`_sinex_xtask_changed_strict_has_no_rust_delta`,
/// `_sinex_xtask_is_no_compile_subcommand`, `_sinex_xtask_is_read_only_subcommand`)
/// — those have their own git/filesystem dependencies out of scope for this
/// test, which targets the allowlist dispatch itself, not those nested
/// predicates.
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
             indentation — this test's extraction must be updated alongside \
             any refactor of the devshell wrapper functions"
        );
    }

    out.push_str(
        r#"
_sinex_xtask_changed_strict_has_no_rust_delta() { return "${STUB_CHECK_RC:-1}"; }
_sinex_xtask_is_no_compile_subcommand() { return "${STUB_FIX_RC:-1}"; }
_sinex_xtask_is_read_only_subcommand() { return "${STUB_RO_RC:-1}"; }
"#,
    );
    out
}

/// Run `bash -c '<extracted functions>; <fn> "$@"'` and return whether it
/// exited zero (the command name was judged safe by the real flake.nix logic).
fn call(function_source: &str, function: &str, args: &[&str]) -> bool {
    let mut script = function_source.to_string();
    script.push_str(function);
    script.push_str(" \"$@\"\n");

    let mut child = Command::new("bash")
        .arg("-c")
        .arg(script)
        .arg("--") // becomes $0
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("bash must be on PATH to run this test");
    let status = child.wait().expect("bash did not exit");
    if !status.success() {
        // consume stderr so failures are quiet unless the caller inspects it
        let mut buf = Vec::new();
        use std::io::Read;
        child.stderr.take().unwrap().read_to_end(&mut buf).ok();
        if !status.code().map(|c| c == 1).unwrap_or(false) {
            // exit code other than the plain "false" case: surface it, since
            // it likely means a shell syntax error in the extraction.
            panic!(
                "unexpected bash exit {status:?} for {function} {args:?}: {}",
                String::from_utf8_lossy(&buf)
            );
        }
    }
    status.success()
}

/// The exact regression: proof-producing commands must NOT be judged safe to
/// run against a stale/unbuildable binary, even though they ARE allowed to
/// skip the staleness check when the binary is fresh (`can_use_existing_binary`).
#[test]
fn test_proof_producing_commands_refuse_stale_binary_fallback() {
    let functions = extract_flake_functions();
    for &cmd in &["test", "build", "schema", "deps", "docs"] {
        assert!(
            !call(&functions, "_sinex_xtask_is_observability_command", &[cmd]),
            "_sinex_xtask_is_observability_command must refuse '{cmd}' — a \
             stale binary must not silently stand in for it (this is the \
             sinex-ive false-pass regression)"
        );
        assert!(
            call(&functions, "_sinex_xtask_can_use_existing_binary", &[cmd]),
            "_sinex_xtask_can_use_existing_binary must still allow '{cmd}' to \
             skip the staleness check when the binary is NOT stale"
        );
    }
}

/// Pure read-only telemetry commands are safe to run against a stale binary —
/// they don't claim to verify the current tree.
#[test]
fn test_pure_observability_commands_allow_stale_binary_fallback() {
    let functions = extract_flake_functions();
    for &cmd in &["history", "analytics", "snapshot"] {
        assert!(
            call(&functions, "_sinex_xtask_is_observability_command", &[cmd]),
            "_sinex_xtask_is_observability_command must allow '{cmd}' — it \
             cannot false-pass a stale-tree proof because it makes no \
             verification claim"
        );
    }
}

/// `check`/`fix`/`doctor`/`infra`/`run` delegate to a deeper read-only/
/// no-rust-delta predicate rather than being flatly allowed or refused — the
/// dispatch itself must route to that predicate for every one of them
/// (mutating any of these case arms into a fixed `return 0` would recreate
/// the sinex-ive class of bug for that command name).
#[test]
fn test_conditional_commands_delegate_to_their_predicate_not_a_fixed_allow() {
    let functions = extract_flake_functions();
    for &cmd in &["check", "fix", "doctor", "infra", "run"] {
        // Stub predicate returns 1 (unsafe) — the wrapper must propagate that,
        // not override it with an unconditional allow.
        let mut denying = functions.clone();
        denying.push_str("export STUB_CHECK_RC=1 STUB_FIX_RC=1 STUB_RO_RC=1\n");
        assert!(
            !call(&denying, "_sinex_xtask_is_observability_command", &[cmd]),
            "'{cmd}' must be refused when its delegated predicate says unsafe"
        );

        // Stub predicate returns 0 (safe) — the wrapper must honor that too.
        let mut allowing = functions.clone();
        allowing.push_str("export STUB_CHECK_RC=0 STUB_FIX_RC=0 STUB_RO_RC=0\n");
        assert!(
            call(&allowing, "_sinex_xtask_is_observability_command", &[cmd]),
            "'{cmd}' must be allowed when its delegated predicate says safe"
        );
    }
}

/// `-h`/`--help`/`--version`/`--list-commands`/no-args are always safe against
/// a stale binary regardless of any subcommand present.
#[test]
fn test_help_and_metadata_flags_allow_stale_binary_fallback() {
    let functions = extract_flake_functions();
    for args in [vec!["-h"], vec!["--help"], vec!["--version"], vec![]] {
        assert!(
            call(&functions, "_sinex_xtask_is_observability_command", &args),
            "help/metadata invocation {args:?} must be treated as observability-safe"
        );
    }
    // -h anywhere in the argv short-circuits via _sinex_xtask_is_help_request,
    // even ahead of a proof-producing subcommand.
    assert!(
        call(
            &functions,
            "_sinex_xtask_is_observability_command",
            &["test", "-h"]
        ),
        "'-h' anywhere in argv must short-circuit to observability-safe, even \
         alongside a proof-producing subcommand name"
    );
}

/// An unrecognized command name must fall to the strict `*)` refuse branch,
/// not silently pass through as safe.
#[test]
fn test_unknown_command_refuses_stale_binary_fallback() {
    let functions = extract_flake_functions();
    assert!(
        !call(
            &functions,
            "_sinex_xtask_is_observability_command",
            &["some-future-subcommand"]
        ),
        "unrecognized command names must default to refuse, not silently pass"
    );
}

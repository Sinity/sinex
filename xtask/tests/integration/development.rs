use std::process::Command;

#[test]
fn public_commands_describe_explicit_scope_and_budgets() {
    for command in ["vm", "run", "coverage"] {
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args([command, "--help"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn public_commands_reject_missing_scope_and_unknown_capture() {
    for args in [
        vec!["vm"],
        vec![
            "run",
            "--output",
            "unused",
            "--session-source",
            "everything",
        ],
    ] {
        assert!(
            !Command::new(env!("CARGO_BIN_EXE_xtask"))
                .args(args)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}

#[test]
fn test_command_describes_building_and_running_tests() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .args(["test", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    for description in ["--no-run", "cargo nextest run", "schema template"] {
        assert!(help.contains(description), "{help}");
    }
}

#[test]
fn cargo_entry_preserves_direct_arguments_targets_and_status() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let selector = bin.join("sinnix-cargo-target");
    std::fs::write(
        &selector,
        r#"#!/usr/bin/env bash
set -eu
printf '%s\0' "$@" >> "$PROBE_SELECTION"
[ "${PROBE_FAIL:-0}" = 0 ] || exit 7
printf '%s\n' "$PROBE_TARGET"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&selector, std::fs::Permissions::from_mode(0o755)).unwrap();
    let cargo = dir.path().join("cargo");
    std::fs::write(
        &cargo,
        r#"#!/usr/bin/env bash
set -eu
printf '%s\0' "${CARGO_TARGET_DIR:-}" "$@" > "$PROBE_OUTPUT"
exit 23
"#,
    )
    .unwrap();
    std::fs::set_permissions(&cargo, std::fs::Permissions::from_mode(0o755)).unwrap();
    let wrapper = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../nix/cargo-command.sh");
    let selected = dir.path().join("selected target");
    let output = dir.path().join("output");
    let selections = dir.path().join("selection");
    let path = std::env::join_paths(
        std::iter::once(bin).chain(std::env::split_paths(&std::env::var_os("PATH").unwrap())),
    )
    .unwrap();
    let args = [
        "metadata",
        "--locked",
        "--offline",
        "--no-deps",
        "--format-version",
        "1",
    ];
    for (automatic, elsewhere, failed) in [
        (false, false, false),
        (true, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let _ = std::fs::remove_file(&output);
        let _ = std::fs::remove_file(&selections);
        let mut command = Command::new("bash");
        command
            .arg(&wrapper)
            .arg(&cargo)
            .args(args)
            .env("PATH", &path)
            .env("CARGO_TARGET_DIR", &selected)
            .env(
                "PROBE_TARGET",
                if elsewhere {
                    dir.path().join("other target")
                } else {
                    selected.clone()
                },
            )
            .env("PROBE_OUTPUT", &output)
            .env("PROBE_SELECTION", &selections)
            .env("PROBE_FAIL", if failed { "1" } else { "0" })
            .env_remove("SINEX_CARGO_TARGET");
        if automatic {
            command.env("SINEX_CARGO_TARGET", &selected);
        }
        let result = command.output().unwrap();
        assert_eq!(
            result.status.code(),
            Some(if failed { 7 } else { 23 }),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        assert!(result.stdout.is_empty());
        assert!(!selected.exists());
        if failed {
            assert!(!output.exists());
            continue;
        }
        let mut expected = selected.as_os_str().as_encoded_bytes().to_vec();
        expected.push(0);
        for arg in args {
            expected.extend_from_slice(arg.as_bytes());
            expected.push(0);
        }
        assert_eq!(std::fs::read(&output).unwrap(), expected);
        if !automatic {
            assert!(!selections.exists());
        } else if elsewhere {
            assert_eq!(std::fs::read(&selections).unwrap(), b"--select-only\0");
        } else {
            let mut expected = b"--select-only\0--cargo\0".to_vec();
            for arg in args {
                expected.extend_from_slice(arg.as_bytes());
                expected.push(0);
            }
            assert_eq!(std::fs::read(&selections).unwrap(), expected);
        }
    }
}

#[test]
fn shell_target_setup_selects_once_without_populating_and_keeps_overrides() {
    use std::os::unix::fs::PermissionsExt;
    let bash = Command::new("bash")
        .args(["-c", "printf '%s' \"$BASH\""])
        .output()
        .unwrap();
    assert!(bash.status.success());
    let bash = String::from_utf8(bash.stdout).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("bin");
    std::fs::create_dir(&bin).unwrap();
    let selector = bin.join("sinnix-cargo-target");
    std::fs::write(&selector, format!("#!{bash}\nprintf '%s\\0' \"$@\" >> \"$PROBE_SELECTION\"\nprintf '%s\\n' \"$PROBE_TARGET\"\n")).unwrap();
    std::fs::set_permissions(&selector, std::fs::Permissions::from_mode(0o755)).unwrap();
    let setup =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../nix/cargo-environment.sh");
    let selected = dir.path().join("selected target");
    let calls = dir.path().join("selection");
    for (explicit, helper) in [(false, true), (true, true), (false, false)] {
        let _ = std::fs::remove_file(&calls);
        let inherited = dir.path().join("explicit target");
        let mut command = Command::new(&bash);
        command.args(["-c", "source \"$1\"; source \"$1\"; printf '%s\\0' \"$CARGO_TARGET_DIR\" \"${SINEX_CARGO_TARGET:-}\"", "fixture"]).arg(&setup)
            .current_dir(dir.path()).env("PATH", if helper { bin.as_os_str() } else { std::ffi::OsStr::new("") })
            .env("PROBE_SELECTION", &calls).env("PROBE_TARGET", &selected)
            .env_remove("CARGO_TARGET_DIR").env_remove("SINEX_CARGO_TARGET");
        if explicit {
            command.env("CARGO_TARGET_DIR", &inherited);
        }
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let target = if explicit {
            inherited
        } else if helper {
            selected.clone()
        } else {
            dir.path().join("target")
        };
        let mut expected = target.as_os_str().as_encoded_bytes().to_vec();
        expected.push(0);
        if !explicit && helper {
            expected.extend_from_slice(target.as_os_str().as_encoded_bytes());
        }
        expected.push(0);
        assert_eq!(result.stdout, expected);
        assert!(!target.exists());
        if !explicit && helper {
            assert_eq!(std::fs::read(&calls).unwrap(), b"--select-only\0");
        } else {
            assert!(!calls.exists());
        }
    }
}

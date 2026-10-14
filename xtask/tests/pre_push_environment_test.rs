#![allow(clippy::expect_used, clippy::unwrap_used)]

//! Current foreground-service endpoint and pre-push selector contracts.
//! Allocated AgentCTL lease APIs were retired in PR #2649.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

fn executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write synthetic executable");
    let mut permissions = fs::metadata(path).expect("stat executable").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make executable runnable");
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
        .arg(repo_root().join(".githooks/pre-push"))
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
fn dev_services_descriptor_uses_the_foreground_owner_contract() {
    let descriptor: toml::Value =
        toml::from_str(include_str!("../../.agentctl/project.toml")).expect("parse descriptor");
    let operations = descriptor["operations"].as_table().expect("operations");
    for (name, operation) in operations {
        for retired in [
            "service",
            "estimate_memory_bytes",
            "exclusive_keys",
            "scratch",
            "supersede",
        ] {
            assert!(
                operation.get(retired).is_none(),
                "{name} retained ignored key {retired}"
            );
        }
    }
    assert_eq!(
        operations["dev_services"]["cache"].as_str(),
        Some("tree+environment")
    );
    assert_eq!(operations["dev_services"]["pool"].as_str(), Some("service"));
    let quick = &operations["verify_quick"];
    assert_eq!(
        quick["exec"]
            .as_array()
            .expect("quick exec")
            .iter()
            .map(|v| v.as_str().expect("argument"))
            .collect::<Vec<_>>(),
        ["bash", "xtask/scripts/verify-quick.sh"]
    );
    assert_eq!(
        descriptor["workspace"]["verify"]["focused"].as_str(),
        Some("verify_quick")
    );
}

fn selected_port(contents: Option<&str>) -> String {
    let state = tempfile::tempdir().expect("temporary endpoint state");
    fs::create_dir_all(state.path().join("dev-state")).expect("create development state");
    if let Some(contents) = contents {
        fs::write(state.path().join("dev-state/nats.port"), contents).expect("persist endpoint");
    }
    let output = Command::new("bash")
        .args(["-c", r#"source "$1"; CACHE="$2"; _sinex_pre_push_cache_root() { printf '%s' "$CACHE"; }; _sinex_pre_push_checkout_hash() { printf '000000000000'; }; _sinex_pre_push_nats_port"#, "endpoint-test"])
        .arg(repo_root().join(".githooks/pre-push"))
        .arg(state.path())
        .output().expect("execute actual endpoint reader");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("endpoint is UTF-8")
}

#[test]
fn pre_push_uses_the_persisted_nats_endpoint() {
    assert_eq!(selected_port(Some("44309\n")), "44309");
}

#[test]
fn pre_push_uses_the_hash_default_without_a_valid_assignment() {
    for contents in [None, Some("invalid\n"), Some("0\n"), Some("65536\n")] {
        assert_eq!(selected_port(contents), "4222");
    }
}

#[test]
fn cached_direnv_environment_uses_the_persisted_nats_endpoint() {
    let state = tempfile::tempdir().expect("temporary endpoint state");
    let assignment = state.path().join("nats.port");
    fs::write(&assignment, "44309\n").expect("persist endpoint");
    let watched = state.path().join("watched");
    let script = r#"
STATE="$2"
WATCHED="$3"
use() {
    export SINEX_DEV_STATE_DIR="$STATE"
    export SINEX_DEV_NATS_PORT=44308
    export SINEX_NATS_URL=nats://127.0.0.1:44308
}
watch_file() { printf '%s' "$1" > "$WATCHED"; }
unset XDG_RUNTIME_DIR
source "$1"
printf '%s\n%s\n' "$SINEX_DEV_NATS_PORT" "$SINEX_NATS_URL"
"#;
    let output = Command::new("bash")
        .args(["-c", script, "cached-env-test"])
        .arg(repo_root().join(".envrc"))
        .arg(state.path())
        .arg(&watched)
        .output()
        .expect("read assignment after importing a cached environment");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 output"),
        "44309\nnats://127.0.0.1:44309\n"
    );
    assert_eq!(
        fs::read_to_string(watched).expect("watch registration"),
        assignment.to_str().expect("UTF-8 path")
    );
}

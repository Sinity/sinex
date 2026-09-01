use super::*;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::{
    EnvGuard, sinex_test,
    timing::{Timeouts, WaitHelpers},
};
use std::sync::{Arc, Barrier};
use std::thread;

fn test_context(background: bool) -> CommandContext {
    CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        background,
        None,
        "test",
    )
}

#[cfg(unix)]
async fn spawn_managed_persistent_child(
    name: &str,
) -> ::xtask::sandbox::TestResult<(Child, nix::unistd::Pid, i32)> {
    let mut command = tokio::process::Command::new("sh");
    configure_managed_child_tokio(&mut command);
    command
        .arg("-c")
        .arg("sleep 30 & descendant=$!; echo $descendant; wait $descendant")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let mut child = command.spawn()?;
    crate::process::register_tokio_child_process_group(&child, name);
    let leader_pid = child.id().expect("managed child exposes a PID") as i32;
    let stdout = child
        .stdout
        .take()
        .expect("managed child stdout should be piped");
    let mut lines = BufReader::new(stdout).lines();
    let descendant_pid = lines
        .next_line()
        .await?
        .expect("managed shell should print the persistent descendant PID")
        .parse::<i32>()?;

    let process_group = nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(leader_pid)))?;
    assert_eq!(
        process_group.as_raw(),
        leader_pid,
        "managed fixture must use the dedicated process-group configuration used in production"
    );
    assert_eq!(
        nix::unistd::getpgid(Some(nix::unistd::Pid::from_raw(descendant_pid)))?,
        process_group,
        "persistent descendant must join the managed child process group"
    );
    assert_eq!(
        unsafe { libc::kill(-process_group.as_raw(), 0) },
        0,
        "managed process group must exist before cleanup injection"
    );
    assert_eq!(
        unsafe { libc::kill(descendant_pid, 0) },
        0,
        "persistent descendant must exist before cleanup injection"
    );

    Ok((child, process_group, descendant_pid))
}

#[cfg(unix)]
async fn spawn_managed_completed_child(
    name: &str,
    script: &str,
) -> ::xtask::sandbox::TestResult<Child> {
    let mut command = tokio::process::Command::new("sh");
    configure_managed_child_tokio(&mut command);
    command
        .arg("-c")
        .arg(script)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    let child = command.spawn()?;
    crate::process::register_tokio_child_process_group(&child, name);
    Ok(child)
}

#[cfg(unix)]
async fn assert_managed_child_group_and_descendant_gone(
    process_group: nix::unistd::Pid,
    descendant_pid: i32,
) -> ::xtask::sandbox::TestResult<()> {
    WaitHelpers::wait_for_condition(
        move || async move {
            let group_gone = unsafe { libc::kill(-process_group.as_raw(), 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            let descendant_gone = unsafe { libc::kill(descendant_pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
            Ok::<_, std::io::Error>(group_gone && descendant_gone)
        },
        Timeouts::QUICK,
    )
    .await?;
    Ok(())
}

fn base_command(subcommand: RunSubcommand) -> RunCommand {
    RunCommand {
        subcommand,
        watch: false,
        release: false,
        dry_run: false,
        logs: false,
        metrics: false,
        dev_journal: false,
    }
}

#[sinex_test]
async fn test_run_metadata_has_no_outer_timeout() -> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::Core { instance_id: None });

    assert_eq!(
        command.metadata().timeout,
        None,
        "xtask run owns long-lived runtime processes and must not be killed by the generic command watchdog"
    );
    Ok(())
}

#[sinex_test]
#[sinex_test]
async fn test_binary_lookup() -> ::xtask::sandbox::TestResult<()> {
    // All binaries should be findable
    for (name, package, _, _) in BINARIES {
        let found = lookup_binary(name);
        assert!(found.is_some(), "Binary {name} not found");
        assert_eq!(found.unwrap().1, *package);
    }
    Ok(())
}

#[sinex_test]
async fn test_require_spawned_pid_accepts_present_pid() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(require_spawned_pid(Some(42), "sinexd")?, 42);
    Ok(())
}

#[sinex_test]
async fn test_require_spawned_pid_rejects_missing_pid() -> ::xtask::sandbox::TestResult<()> {
    let error = require_spawned_pid(None, "sinexd").expect_err("missing PID must fail honestly");
    let rendered = error.to_string();
    assert!(rendered.contains("sinexd"));
    assert!(rendered.contains("did not expose a PID"));
    Ok(())
}

#[sinex_test]
async fn test_runtime_cli_args_serve_supervisor_without_source() -> ::xtask::sandbox::TestResult<()>
{
    // Post-collapse: no source → empty args (sinexd defaults to `serve`).
    assert_eq!(
        runtime_cli_args("sinexd", "gateway-123", RuntimeTarget::Supervisor),
        Vec::<String>::new()
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_cli_args_dispatch_scan_source() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        runtime_cli_args(
            "sinexd",
            "terminal-source-123",
            RuntimeTarget::Source("terminal.zsh-history")
        ),
        vec![
            "scan-source-driver".to_string(),
            "--source".to_string(),
            "terminal.zsh-history".to_string(),
            "--service-name".to_string(),
            "terminal-source-123".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_core_runtime_env_disables_hosted_sources_and_automata()
-> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::Core { instance_id: None });
    let env = command.runtime_env_vars(RuntimeTarget::Supervisor);

    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_AUTOMATA_ENABLED" && value.is_empty()),
        "core runs should not start every automaton; use all-automatons or module targets: {env:?}"
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_SOURCE_BINDINGS_PATH" && value.is_empty()),
        "core runs should not host source bindings; use all-sources or source module targets: {env:?}"
    );
    assert!(
        !env.iter()
            .any(|(key, value)| key == "SINEX_EVENT_ENGINE_ENABLED" && value == "false"),
        "core is the event-engine/API leg and must keep the event engine enabled"
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_cli_args_automaton_uses_supervisor_selector_env()
-> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::RuntimeModule {
        name: "interval-lift".to_string(),
        instance_id: None,
    });

    assert_eq!(
        runtime_cli_args(
            "sinexd",
            "interval-lift-123",
            RuntimeTarget::Automaton("interval-lift")
        ),
        Vec::<String>::new(),
        "automata are selected through SINEX_AUTOMATA_ENABLED, not a source-driver argv"
    );

    let env = command.runtime_env_vars(RuntimeTarget::Automaton("interval-lift"));
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_AUTOMATA_ENABLED" && value == "interval-lift"),
        "single automaton runs must select exactly the requested automaton: {env:?}"
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_API_ENABLED" && value == "false"),
        "single automaton runs should not spend a gateway pool"
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_EVENT_ENGINE_ENABLED" && value == "false"),
        "single automaton runs must not start a duplicate event-engine pool"
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_SOURCE_BINDINGS_PATH" && value.is_empty()),
        "single automaton runs must not inherit hosted source bindings"
    );
    Ok(())
}

#[sinex_test]
async fn test_all_automata_env_runs_one_selected_supervisor() -> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::AllAutomatons { instance_id: None });
    let env = command.runtime_env_vars(RuntimeTarget::AllAutomata);

    assert!(command.runs_bundle());
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_AUTOMATA_ENABLED" && value == "all"),
        "all-automatons should be one supervisor with the all selector, not N full supervisors"
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_API_ENABLED" && value == "false")
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_EVENT_ENGINE_ENABLED" && value == "false")
    );
    assert!(
        env.iter()
            .any(|(key, value)| key == "SINEX_SOURCE_BINDINGS_PATH" && value.is_empty())
    );
    Ok(())
}

#[sinex_test]
async fn test_append_source_binding_args_for_scan_source_driver() -> ::xtask::sandbox::TestResult<()>
{
    let mut args = vec![
        "scan-source-driver".to_string(),
        "--source".to_string(),
        "terminal.zsh-history".to_string(),
    ];
    append_source_binding_args(
        &mut args,
        DevSourceBinding {
            source_id: "terminal.zsh-history".to_string(),
            instance_idx: 1,
            service_name: None,
            runtime_config: Some(serde_json::json!({
                "path": "/home/sinity/.zsh_history",
                "skip_empty": true
            })),
            extra_args: vec![
                "scan".to_string(),
                "--until".to_string(),
                "snapshot".to_string(),
            ],
            extra_env: HashMap::from([("SINEX_DEMO".to_string(), "1".to_string())]),
        },
    );

    assert_eq!(
        args,
        vec![
            "scan-source-driver".to_string(),
            "--source".to_string(),
            "terminal.zsh-history".to_string(),
            "--runtime-config".to_string(),
            r#"{"path":"/home/sinity/.zsh_history","skip_empty":true}"#.to_string(),
            "--extra-arg".to_string(),
            "scan".to_string(),
            "--extra-arg".to_string(),
            "--until".to_string(),
            "--extra-arg".to_string(),
            "snapshot".to_string(),
            "--extra-env".to_string(),
            "SINEX_DEMO=1".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_default_all_source_bindings_excludes_journald() -> ::xtask::sandbox::TestResult<()> {
    let manifest = DevSourceBindingsManifest {
        bindings: vec![
            DevSourceBinding {
                source_id: "terminal.atuin-history".to_string(),
                instance_idx: 1,
                service_name: None,
                runtime_config: None,
                extra_args: Vec::new(),
                extra_env: HashMap::new(),
            },
            DevSourceBinding {
                source_id: "system.journald".to_string(),
                instance_idx: 1,
                service_name: None,
                runtime_config: None,
                extra_args: Vec::new(),
                extra_env: HashMap::new(),
            },
        ],
    };

    let bindings = default_all_source_bindings_from_manifest(manifest, false);

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].source_id, "terminal.atuin-history");
    Ok(())
}

#[sinex_test]
async fn test_all_source_bindings_can_include_default_excluded_sources()
-> ::xtask::sandbox::TestResult<()> {
    let manifest = DevSourceBindingsManifest {
        bindings: vec![
            DevSourceBinding {
                source_id: "terminal.atuin-history".to_string(),
                instance_idx: 1,
                service_name: None,
                runtime_config: None,
                extra_args: Vec::new(),
                extra_env: HashMap::new(),
            },
            DevSourceBinding {
                source_id: "system.journald".to_string(),
                instance_idx: 1,
                service_name: None,
                runtime_config: None,
                extra_args: Vec::new(),
                extra_env: HashMap::new(),
            },
        ],
    };

    let bindings = default_all_source_bindings_from_manifest(manifest, true);

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].source_id, "terminal.atuin-history");
    assert_eq!(bindings[1].source_id, "system.journald");
    Ok(())
}

#[sinex_test]
async fn test_source_binding_runtime_args_uses_manifest_identity()
-> ::xtask::sandbox::TestResult<()> {
    let binding = DevSourceBinding {
        source_id: "git-commit-history".to_string(),
        instance_idx: 3,
        service_name: None,
        runtime_config: Some(serde_json::json!({"repo": "/realm/project/sinex"})),
        extra_args: Vec::new(),
        extra_env: HashMap::new(),
    };
    let service_name = default_source_binding_service_name(&binding);

    assert_eq!(service_name, "source-driver-git-commit-history-3");
    assert_eq!(
        source_binding_runtime_args(&binding, &service_name),
        vec![
            "scan-source-driver".to_string(),
            "--source".to_string(),
            "git-commit-history".to_string(),
            "--service-name".to_string(),
            "source-driver-git-commit-history-3".to_string(),
            "--instance-idx".to_string(),
            "3".to_string(),
            "--runtime-config".to_string(),
            r#"{"repo":"/realm/project/sinex"}"#.to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_runtime_module_resolution_distinguishes_binding_and_binary_names()
-> ::xtask::sandbox::TestResult<()> {
    let manifest = DevSourceBindingsManifest {
        bindings: vec![DevSourceBinding {
            source_id: "desktop.activitywatch".to_string(),
            instance_idx: 1,
            service_name: None,
            runtime_config: None,
            extra_args: Vec::new(),
            extra_env: HashMap::new(),
        }],
    };

    assert!(matches!(
        resolve_runtime_module("desktop.activitywatch", Some(&manifest)),
        Some(RuntimeModuleResolution::SourceBinding(binding))
            if binding.source_id == "desktop.activitywatch"
    ));
    assert!(matches!(
        resolve_runtime_module("desktop-source", Some(&manifest)),
        Some(RuntimeModuleResolution::Binary(_))
    ));
    Ok(())
}

#[sinex_test]
async fn test_source_binding_service_from_cmdline_args_requires_scan_driver()
-> ::xtask::sandbox::TestResult<()> {
    let args = vec![
        "/var/cache/sinex/target/debug/sinexd".to_string(),
        "scan-source-driver".to_string(),
        "--source".to_string(),
        "browser.history".to_string(),
        "--service-name".to_string(),
        "source-driver-browser.history-3".to_string(),
    ];

    assert_eq!(
        source_binding_service_from_cmdline_args(&args).as_deref(),
        Some("source-driver-browser.history-3")
    );

    let non_source_args = vec![
        "sinexd".to_string(),
        "serve".to_string(),
        "--service-name".to_string(),
        "source-driver-browser.history-3".to_string(),
    ];
    assert_eq!(
        source_binding_service_from_cmdline_args(&non_source_args),
        None
    );
    Ok(())
}

#[sinex_test]
async fn test_all_sources_subcommand_can_target_reconcile_service()
-> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::AllSources {
        instance_id: None,
        reconcile: true,
        service_name: Some("source-driver-browser.history-3".to_string()),
        include_default_excluded: false,
    });

    assert!(command.runs_bundle());
    assert!(!command.runs_single_binary());
    Ok(())
}

#[sinex_test(serial = true)]
async fn test_source_bindings_wait_for_later_success_before_completing()
-> ::xtask::sandbox::TestResult<()> {
    let mut children = HashMap::from([
        (
            "source-early".to_string(),
            spawn_managed_completed_child("source-early", "exit 0").await?,
        ),
        (
            "source-later".to_string(),
            spawn_managed_completed_child("source-later", "sleep 1; exit 0").await?,
        ),
    ]);
    let mut cancellation = ForegroundCancellation::install()?;
    let exit =
        wait_for_all_source_bindings(&mut children, &test_context(false), &mut cancellation).await;

    let ChildExit::AllSucceeded { completed } = exit else {
        bail!("an early successful source must not terminate a later source binding");
    };
    assert_eq!(completed.len(), 2, "both source bindings must complete");
    assert!(completed.contains(&"source-early".to_string()));
    assert!(completed.contains(&"source-later".to_string()));
    for child in children.values_mut() {
        assert!(
            child.try_wait()?.is_some_and(|status| status.success()),
            "each source binding must finish successfully instead of being terminated after an early sibling exit"
        );
    }
    assert!(
        foreground_bundle_terminal_result(
            SOURCE_BINDINGS_FOREGROUND_BUNDLE,
            ChildExit::AllSucceeded { completed },
            Vec::new(),
            serde_json::json!({}),
        )
        .errors
        .is_empty()
    );
    Ok(())
}

#[sinex_test(serial = true)]
async fn test_source_binding_child_failure_is_preserved() -> ::xtask::sandbox::TestResult<()> {
    let (sibling, process_group, descendant_pid) =
        spawn_managed_persistent_child("source-sibling").await?;
    let mut children = HashMap::from([
        (
            "source-failure".to_string(),
            spawn_managed_completed_child("source-failure", "exit 17").await?,
        ),
        ("source-sibling".to_string(), sibling),
    ]);
    let mut cancellation = ForegroundCancellation::install()?;
    let exit =
        wait_for_all_source_bindings(&mut children, &test_context(false), &mut cancellation).await;

    assert!(!exit.is_success());
    assert!(exit.trigger().contains("source-failure"));
    let shutdown_failures = stop_bundle_children(&mut children, exit.exited_name()).await;
    assert!(shutdown_failures.is_empty());
    assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;
    let result = foreground_bundle_terminal_result(
        SOURCE_BINDINGS_FOREGROUND_BUNDLE,
        exit,
        shutdown_failures,
        serde_json::json!({}),
    );
    assert_eq!(result.errors[0].code, "SOURCE_BINDING_EXITED");
    Ok(())
}

#[sinex_test]
async fn test_source_binding_wait_error_is_preserved() -> ::xtask::sandbox::TestResult<()> {
    let exit = ChildExit::WaitError {
        name: "source-wait-error".to_string(),
        error: std::io::Error::other("injected wait failure"),
    };

    assert!(!exit.is_success());
    assert_eq!(exit.exited_name(), None);
    assert!(exit.trigger().contains("injected wait failure"));
    let result = foreground_bundle_terminal_result(
        SOURCE_BINDINGS_FOREGROUND_BUNDLE,
        exit,
        Vec::new(),
        serde_json::json!({}),
    );
    assert_eq!(result.errors[0].code, "SOURCE_BINDING_WAIT_FAILED");
    Ok(())
}

#[sinex_test]
async fn test_bundle_child_failure_is_preserved_as_failed_result()
-> ::xtask::sandbox::TestResult<()> {
    let mut children = HashMap::from([(
        "core-child".to_string(),
        Command::new("sh").args(["-c", "exit 23"]).spawn()?,
    )]);
    let exit = wait_for_any_child_exit(&mut children, &test_context(false)).await;

    let result = foreground_bundle_terminal_result(
        GENERIC_FOREGROUND_BUNDLE,
        exit,
        Vec::new(),
        serde_json::json!({ "binaries": ["sinexd"] }),
    );
    assert!(result.is_failure(), "a nonzero child must fail run_core");
    assert_eq!(result.errors[0].code, "BUNDLE_EXITED");
    Ok(())
}

#[sinex_test]
async fn test_trigger_failure_and_all_cleanup_failures_are_preserved()
-> ::xtask::sandbox::TestResult<()> {
    let result = foreground_bundle_terminal_result(
        SOURCE_BINDINGS_FOREGROUND_BUNDLE,
        ChildExit::WaitError {
            name: "trigger-source".to_string(),
            error: std::io::Error::other("injected trigger wait failure"),
        },
        vec![
            "sibling-a: injected cleanup failure".to_string(),
            "sibling-b: another injected cleanup failure".to_string(),
        ],
        serde_json::json!({}),
    );

    assert!(result.is_failure());
    assert_eq!(result.errors.len(), 3, "trigger plus every cleanup failure");
    assert_eq!(result.errors[0].code, "SOURCE_BINDING_WAIT_FAILED");
    assert_eq!(result.errors[1].code, "SOURCE_BINDING_CLEANUP_FAILED");
    assert_eq!(result.errors[2].code, "SOURCE_BINDING_CLEANUP_FAILED");
    assert!(
        result.errors[0]
            .message
            .contains("injected trigger wait failure")
    );
    assert!(result.errors[1].message.contains("sibling-a"));
    assert!(result.errors[2].message.contains("sibling-b"));
    Ok(())
}

#[sinex_test]
async fn test_wait_error_still_terminates_source_child_group() -> ::xtask::sandbox::TestResult<()> {
    let mut children = HashMap::new();
    let mut managed_children = Vec::new();
    for name in ["source-wait-error", "source-sibling"] {
        let (child, process_group, descendant_pid) = spawn_managed_persistent_child(name).await?;
        managed_children.push((process_group, descendant_pid));
        children.insert(name.to_string(), child);
    }

    let exit = ChildExit::WaitError {
        name: "source-wait-error".to_string(),
        error: std::io::Error::other("injected wait failure"),
    };
    let failures = stop_bundle_children(&mut children, exit.exited_name()).await;

    assert!(failures.is_empty());
    for child in children.values_mut() {
        assert!(child.try_wait()?.is_some(), "source child was reaped");
    }
    for (process_group, descendant_pid) in managed_children {
        assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;
    }
    Ok(())
}

#[sinex_test(serial = true)]
async fn test_source_binding_cancellation_preserves_trigger_and_tears_down_siblings()
-> ::xtask::sandbox::TestResult<()> {
    let (sibling, process_group, descendant_pid) =
        spawn_managed_persistent_child("source-cancelled-sibling").await?;
    let mut children = HashMap::from([("source-cancelled-sibling".to_string(), sibling)]);
    let mut cancellation = ForegroundCancellation::install()?;
    let signal = tokio::spawn(async {
        tokio::task::yield_now().await;
        let result = unsafe { libc::raise(libc::SIGTERM) };
        assert_eq!(
            result, 0,
            "the test SIGTERM must reach the process receiver"
        );
    });
    let exit = tokio::time::timeout(
        std::time::Duration::from_secs(Timeouts::QUICK),
        wait_for_all_source_bindings(&mut children, &test_context(false), &mut cancellation),
    )
    .await
    .map_err(|_| color_eyre::eyre::eyre!("SIGTERM did not cancel the source-binding wait"))?;
    signal.await?;
    assert!(matches!(exit, ChildExit::Cancelled));
    let shutdown_failures = stop_bundle_children(&mut children, exit.exited_name()).await;

    assert!(shutdown_failures.is_empty());
    assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;
    let result = foreground_bundle_terminal_result(
        SOURCE_BINDINGS_FOREGROUND_BUNDLE,
        exit,
        shutdown_failures,
        serde_json::json!({}),
    );
    assert!(result.is_failure());
    assert_eq!(result.errors[0].code, "SOURCE_BINDING_CANCELLED");
    assert!(result.errors[0].message.contains("cancellation"));
    Ok(())
}

#[sinex_test]
async fn test_source_binding_setup_failure_terminates_started_groups()
-> ::xtask::sandbox::TestResult<()> {
    let (child, process_group, descendant_pid) =
        spawn_managed_persistent_child("started-before-setup-failure").await?;
    let mut children = HashMap::from([("started-before-setup-failure".to_string(), child)]);
    let error =
        fail_source_binding_setup::<()>(&mut children, eyre!("injected later spawn failure"))
            .await
            .expect_err("setup failure must be returned after child cleanup");

    assert!(format!("{error:#}").contains("injected later spawn failure"));
    assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;
    Ok(())
}

#[sinex_test]
async fn test_poll_error_attempts_process_group_termination_before_returning()
-> ::xtask::sandbox::TestResult<()> {
    let (mut child, process_group, descendant_pid) =
        spawn_managed_persistent_child("source-poll-error").await?;

    let error = stop_bundle_child_after_poll(
        "source-poll-error",
        &mut child,
        Some(std::io::Error::other("injected poll failure")),
    )
    .await
    .expect_err("the polling failure remains visible after cleanup");

    assert!(error.to_string().contains("injected poll failure"));
    assert!(child.try_wait()?.is_some(), "source child was reaped");
    assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;
    Ok(())
}

#[sinex_test]
async fn test_agentctl_run_all_sources_inherits_selected_manifest()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor: toml::Value = toml::from_str(include_str!("../../../.agentctl/project.toml"))?;
    let inherit = descriptor["environment"]["inherit"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("AgentCTL environment.inherit must be an array"))?;
    let inherited: Vec<&str> = inherit.iter().filter_map(toml::Value::as_str).collect();
    assert_eq!(
        inherited
            .iter()
            .filter(|value| **value == "SINEX_SOURCE_BINDINGS_PATH")
            .count(),
        1,
        "the manifest path is an explicit, single-variable AgentCTL inheritance contract"
    );
    assert!(
        !inherited.contains(&"SINEX_ARBITRARY_ENV_OVERLAY"),
        "run_all_sources must not admit an arbitrary environment overlay"
    );

    let manifest_dir = tempfile::tempdir()?;
    let manifest_path = manifest_dir.path().join("selected-bindings.json");
    std::fs::write(
        &manifest_path,
        r#"{"bindings":[{"source_id":"selected.source","service_name":"selected-service"}]}"#,
    )?;
    let _env = EnvGuard::set_single("SINEX_SOURCE_BINDINGS_PATH", &manifest_path);
    let manifest = load_dev_source_bindings_manifest().ok_or_else(|| {
        color_eyre::eyre::eyre!("selected source-binding manifest was not loaded")
    })?;

    assert_eq!(manifest.bindings.len(), 1);
    assert_eq!(manifest.bindings[0].source_id, "selected.source");
    assert_eq!(
        default_source_binding_service_name(&manifest.bindings[0]),
        "selected-service"
    );
    Ok(())
}

#[sinex_test]
async fn test_agentctl_runtime_operations_are_non_cacheable_with_eight_hour_leases()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor: toml::Value = toml::from_str(include_str!("../../../.agentctl/project.toml"))?;
    let operations = descriptor["operations"]
        .as_table()
        .ok_or_else(|| color_eyre::eyre::eyre!("AgentCTL operations must be a TOML table"))?;

    for operation_name in ["run_core", "run_all_automatons", "run_all_sources"] {
        let operation = operations
            .get(operation_name)
            .and_then(toml::Value::as_table)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("missing AgentCTL runtime operation {operation_name}")
            })?;
        assert_eq!(
            operation.get("cache").and_then(toml::Value::as_str),
            Some("none"),
            "{operation_name} must relaunch instead of reusing a terminal runtime result"
        );
        assert_eq!(
            operation
                .get("timeout_seconds")
                .and_then(toml::Value::as_integer),
            Some(28_800),
            "{operation_name} keeps the declared eight-hour foreground lease"
        );
    }

    for operation_name in ["run_core", "run_all_automatons"] {
        let parameters = operations[operation_name]["parameters"]
            .as_table()
            .ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "{operation_name} must declare the bounded instance-id parameter"
                )
            })?;
        assert_eq!(
            parameters.keys().map(String::as_str).collect::<Vec<_>>(),
            ["instance_id", "release"],
            "{operation_name} admits only its declared release and instance-id inputs"
        );
        let instance_id = &parameters["instance_id"];
        assert_eq!(
            instance_id.get("type").and_then(toml::Value::as_str),
            Some("string")
        );
        assert_eq!(
            instance_id.get("flag").and_then(toml::Value::as_str),
            Some("--instance-id")
        );
        assert_eq!(
            instance_id
                .get("max_length")
                .and_then(toml::Value::as_integer),
            Some(128)
        );
        assert_eq!(
            instance_id.get("grammar").and_then(toml::Value::as_str),
            Some("safe-token")
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_agentctl_dev_services_use_shared_checkout_service_lease_contract()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor: toml::Value = toml::from_str(include_str!("../../../.agentctl/project.toml"))?;
    let operations = descriptor["operations"]
        .as_table()
        .ok_or_else(|| eyre!("AgentCTL operations must be a TOML table"))?;
    let dev_services = operations
        .get("dev_services")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| eyre!("missing AgentCTL dev_services operation"))?;

    assert_eq!(
        dev_services["exec"]
            .as_array()
            .ok_or_else(|| eyre!("dev_services exec must be an argv array"))?
            .iter()
            .map(|value| value.as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        ["nix", "run", ".#xtask", "--", "infra", "lease-services"]
    );
    assert_eq!(
        dev_services.get("cache").and_then(toml::Value::as_str),
        Some("tree+environment"),
        "matching starts in one checkout must share the running service job and lease"
    );
    assert_eq!(
        dev_services.get("result").and_then(toml::Value::as_str),
        Some("exit")
    );
    assert_eq!(
        dev_services
            .get("timeout_seconds")
            .and_then(toml::Value::as_integer),
        Some(28_800),
        "the shared foreground service keeps its eight-hour lease"
    );
    assert!(
        dev_services.get("parameters").is_none(),
        "the service contract must not expose caller-controlled inputs"
    );
    assert!(
        dev_services.get("exclusive_keys").is_none(),
        "same-checkout sharing must not become global exclusivity"
    );

    let inherit = descriptor["environment"]["inherit"]
        .as_array()
        .ok_or_else(|| eyre!("AgentCTL environment.inherit must be an array"))?;
    for output in ["SINEX_DEV_POSTGRES_PORT", "SINEX_DEV_NATS_PORT"] {
        assert!(
            !inherit.iter().any(|value| value.as_str() == Some(output)),
            "allocated service output {output} must not affect cache identity"
        );
    }

    let service = dev_services
        .get("service")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| eyre!("dev_services must declare its service contract"))?;
    assert_eq!(
        service.get("readiness").and_then(toml::Value::as_str),
        Some("project-command")
    );
    assert_eq!(
        service.get("lifetime").and_then(toml::Value::as_str),
        Some("job")
    );

    let ports = service["ports"]
        .as_table()
        .ok_or_else(|| eyre!("dev_services service ports must be a TOML table"))?;
    for (slot, environment, lower, upper) in [
        (
            "postgres",
            "SINEX_DEV_POSTGRES_PORT",
            45_432_i64,
            45_559_i64,
        ),
        ("nats", "SINEX_DEV_NATS_PORT", 44_308_i64, 44_435_i64),
    ] {
        let port = ports
            .get(slot)
            .and_then(toml::Value::as_table)
            .ok_or_else(|| eyre!("missing dev_services {slot} port slot"))?;
        assert_eq!(
            port.get("environment").and_then(toml::Value::as_str),
            Some(environment)
        );
        let range = port
            .get("range")
            .and_then(toml::Value::as_array)
            .ok_or_else(|| eyre!("dev_services {slot} port range must be an array"))?;
        assert_eq!(
            range
                .iter()
                .map(|value| value.as_integer().unwrap_or_default())
                .collect::<Vec<_>>(),
            [lower, upper]
        );
    }

    Ok(())
}

#[test]
#[ignore = "read-only integration test; run when the live AgentCTL project registration has been refreshed from this checkout"]
fn test_live_agentctl_descriptor_exposes_service_cache_identity_without_starting_services() {
    let output = std::process::Command::new("agentctl")
        .args(["project", "operations", "sinex"])
        .output()
        .expect("AgentCTL must be installed for the opt-in live descriptor test");
    assert!(
        output.status.success(),
        "AgentCTL project operation inspection failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("AgentCTL operation inspection must return JSON");
    let operations = response["payload"]["value"]["operations"]
        .as_array()
        .expect("AgentCTL operation inspection must expose operations");
    let dev_services = operations
        .iter()
        .find(|operation| operation["name"] == "dev_services")
        .expect("live AgentCTL must expose dev_services");
    assert_eq!(
        dev_services["cache"], "tree+environment",
        "the running AgentCTL must load same-checkout coalescing semantics"
    );
    assert_eq!(dev_services["service"]["lifetime"], "job");
    assert_eq!(dev_services["service"]["readiness"], "project-command");
    assert_eq!(dev_services["parameters"], serde_json::json!([]));
}

#[test]
#[ignore = "service integration test; starts and then cancels the current checkout's dev_services lease"]
fn test_live_agentctl_coalesces_matching_workspace_jobs() {
    let operations = std::process::Command::new("agentctl")
        .args(["project", "operations", "sinex"])
        .output()
        .expect("inspect AgentCTL operations for the opt-in coalescing test");
    assert!(
        operations.status.success(),
        "operation inspection failed: {}",
        String::from_utf8_lossy(&operations.stderr)
    );
    let operations: serde_json::Value =
        serde_json::from_slice(&operations.stdout).expect("operation inspection must return JSON");
    let dev_services = operations["payload"]["value"]["operations"]
        .as_array()
        .expect("operation inspection must expose operations")
        .iter()
        .find(|operation| operation["name"] == "dev_services")
        .expect("live AgentCTL must expose dev_services");
    assert_eq!(dev_services["cache"], "tree+environment");
    assert_eq!(dev_services["parameters"], serde_json::json!([]));
    assert_eq!(dev_services["service"]["lifetime"], "job");

    let workspace_list = std::process::Command::new("agentctl")
        .args(["workspace", "list", "--project", "sinex"])
        .output()
        .expect("AgentCTL must be installed for the opt-in coalescing test");
    assert!(
        workspace_list.status.success(),
        "workspace inspection failed: {}",
        String::from_utf8_lossy(&workspace_list.stderr)
    );
    let response: serde_json::Value = serde_json::from_slice(&workspace_list.stdout)
        .expect("workspace inspection must return JSON");
    let checkout = crate::config::workspace_root()
        .to_string_lossy()
        .to_string();
    let workspace_id = response["payload"]["value"]["workspaces"]
        .as_array()
        .expect("workspace inspection must expose workspaces")
        .iter()
        .find(|workspace| workspace["path"] == checkout)
        .and_then(|workspace| workspace["workspace_id"].as_str())
        .expect("current checkout must have a registered workspace")
        .to_string();

    let gate = Arc::new(Barrier::new(2));
    let starts = (0..2)
        .map(|_| {
            let gate = Arc::clone(&gate);
            let workspace_id = workspace_id.clone();
            thread::spawn(move || {
                gate.wait();
                std::process::Command::new("agentctl")
                    .args([
                        "job",
                        "start",
                        "sinex",
                        "dev_services",
                        "--workspace",
                        &workspace_id,
                    ])
                    .output()
                    .expect("start dev_services through AgentCTL")
            })
        })
        .collect::<Vec<_>>();
    let outputs = starts
        .into_iter()
        .map(|thread| thread.join().expect("AgentCTL start thread must not panic"))
        .collect::<Vec<_>>();
    let job_ids = outputs
        .iter()
        .map(|output| {
            assert!(
                output.status.success(),
                "AgentCTL start failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let response: serde_json::Value =
                serde_json::from_slice(&output.stdout).expect("AgentCTL start must return JSON");
            response["payload"]["value"]["job_id"]
                .as_str()
                .expect("AgentCTL start must return a job ID")
                .to_string()
        })
        .collect::<Vec<_>>();
    let mut unique_job_ids = job_ids.clone();
    unique_job_ids.sort();
    unique_job_ids.dedup();
    for job_id in &unique_job_ids {
        let cancel = std::process::Command::new("agentctl")
            .args(["job", "cancel", job_id])
            .output()
            .expect("cancel started dev_services job");
        assert!(
            cancel.status.success(),
            "failed to cancel dev_services job {job_id}: {}",
            String::from_utf8_lossy(&cancel.stderr)
        );
    }
    let wait = std::process::Command::new("agentctl")
        .args(["job", "wait", &job_ids[0], "--timeout-seconds", "60"])
        .status()
        .expect("wait for cancelled dev_services job");
    assert!(
        wait.success(),
        "cancelled dev_services job did not become terminal"
    );
    assert_eq!(
        job_ids[0], job_ids[1],
        "matching same-workspace starts must coalesce to one AgentCTL job"
    );
}

#[sinex_test]
async fn test_agentctl_verification_operations_bind_only_typed_inputs()
-> ::xtask::sandbox::TestResult<()> {
    let descriptor: toml::Value = toml::from_str(include_str!("../../../.agentctl/project.toml"))?;
    let operations = descriptor["operations"]
        .as_table()
        .ok_or_else(|| color_eyre::eyre::eyre!("AgentCTL operations must be a TOML table"))?;

    let verify_plan = operations
        .get("verify_plan")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing AgentCTL verify_plan operation"))?;
    assert_eq!(
        verify_plan["exec"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("verify_plan exec must be an argv array"))?
            .iter()
            .map(|value| value.as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        ["xtask", "verify", "plan", "--check"]
    );
    assert_eq!(
        verify_plan.get("result").and_then(toml::Value::as_str),
        Some("exit")
    );
    assert_eq!(
        verify_plan.get("cache").and_then(toml::Value::as_str),
        Some("tree+environment")
    );
    assert_eq!(
        verify_plan
            .get("timeout_seconds")
            .and_then(toml::Value::as_integer),
        Some(1_800)
    );
    assert!(
        verify_plan.get("parameters").is_none(),
        "verify_plan exposes no caller-controlled argv, environment, or cwd input"
    );

    let verify_closure = operations
        .get("verify_closure")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| color_eyre::eyre::eyre!("missing AgentCTL verify_closure operation"))?;
    assert_eq!(
        verify_closure["exec"]
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("verify_closure exec must be an argv array"))?
            .iter()
            .map(|value| value.as_str().unwrap_or_default())
            .collect::<Vec<_>>(),
        ["xtask", "verify", "closure"]
    );
    assert_eq!(
        verify_closure.get("result").and_then(toml::Value::as_str),
        Some("exit")
    );
    assert_eq!(
        verify_closure.get("cache").and_then(toml::Value::as_str),
        Some("none")
    );
    assert_eq!(
        verify_closure
            .get("timeout_seconds")
            .and_then(toml::Value::as_integer),
        Some(1_800)
    );

    let parameters = verify_closure["parameters"]
        .as_table()
        .ok_or_else(|| color_eyre::eyre::eyre!("verify_closure parameters must be a table"))?;
    assert_eq!(
        parameters.keys().map(String::as_str).collect::<Vec<_>>(),
        ["bead_id", "dry_run", "json"],
        "verify_closure exposes only its typed positional id and boolean flags"
    );
    let bead_id = &parameters["bead_id"];
    assert_eq!(
        bead_id.get("type").and_then(toml::Value::as_str),
        Some("string")
    );
    assert_eq!(
        bead_id.get("position").and_then(toml::Value::as_integer),
        Some(1)
    );
    assert_eq!(
        bead_id.get("required").and_then(toml::Value::as_bool),
        Some(true)
    );
    assert_eq!(
        bead_id.get("max_length").and_then(toml::Value::as_integer),
        Some(128)
    );
    assert_eq!(
        bead_id.get("grammar").and_then(toml::Value::as_str),
        Some("safe-token")
    );
    for (name, flag) in [("json", "--json"), ("dry_run", "--dry-run")] {
        assert_eq!(
            parameters[name].get("type").and_then(toml::Value::as_str),
            Some("bool")
        );
        assert_eq!(
            parameters[name].get("flag").and_then(toml::Value::as_str),
            Some(flag)
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_build_cargo_run_args_target_sinexd() -> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::RuntimeModule {
        name: "terminal-source".to_string(),
        instance_id: None,
    });
    assert_eq!(
        command.build_cargo_run_args(
            "sinexd",
            "terminal-source-123",
            RuntimeTarget::Source("terminal.zsh-history")
        ),
        vec![
            "run".to_string(),
            "-p".to_string(),
            "sinexd".to_string(),
            "--".to_string(),
            "scan-source-driver".to_string(),
            "--source".to_string(),
            "terminal.zsh-history".to_string(),
            "--service-name".to_string(),
            "terminal-source-123".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_target_binary_path_uses_debug_and_release_profiles()
-> ::xtask::sandbox::TestResult<()> {
    let target_root = crate::orchestrator::get_target_dir(&crate::config::workspace_root());
    assert_eq!(
        target_binary_path(false, "sinexd"),
        target_root.join("debug/sinexd")
    );
    assert_eq!(
        target_binary_path(true, "sinexd"),
        target_root.join("release/sinexd")
    );
    Ok(())
}

#[sinex_test]
async fn test_local_runtime_coordinates_describe_current_checkout()
-> ::xtask::sandbox::TestResult<()> {
    let command = base_command(RunSubcommand::Core { instance_id: None });
    let coordinates = command.local_runtime_coordinates()?;
    let checkout = crate::config::workspace_root();

    assert_eq!(coordinates.mode, "dev-local-explicit");
    assert_eq!(coordinates.checkout_root, checkout.display().to_string());
    assert!(
        coordinates
            .database_url
            .starts_with("postgresql:///sinex_dev"),
        "database URL should point at the checkout-local dev database"
    );
    assert!(
        coordinates.nats_url.starts_with("nats://localhost:"),
        "NATS URL should point at the checkout-local dev broker"
    );
    assert!(coordinates.logs_dir.contains("dev-state"));
    Ok(())
}

#[sinex_test]
async fn test_source_bundle_contains_only_real_runtime_sources() -> ::xtask::sandbox::TestResult<()>
{
    assert_eq!(
        SOURCE_TARGETS,
        &[
            "fs-source",
            "terminal-source",
            "desktop-source",
            "system-source"
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_automaton_bundle_includes_non_suffix_automatons() -> ::xtask::sandbox::TestResult<()>
{
    assert_eq!(
        AUTOMATON_TARGETS,
        &[
            "analytics-automaton",
            "attention-stream",
            "interval-lift",
            "health-automaton",
            "session-detector",
            "hourly-summarizer",
            "daily-summarizer",
            "terminal-canonicalizer",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_list_run_targets_drops_ghosts_and_oneshot_scan_surface()
-> ::xtask::sandbox::TestResult<()> {
    let targets = list_run_targets();
    assert!(targets.contains(&"attention-stream".to_string()));
    assert!(targets.contains(&"interval-lift".to_string()));
    assert!(targets.contains(&"session-detector".to_string()));
    assert!(targets.contains(&"terminal-canonicalizer".to_string()));
    assert!(!targets.contains(&"document-ingestor".to_string()));
    assert!(!targets.contains(&"search-automaton".to_string()));
    assert!(!targets.contains(&"pkm-automaton".to_string()));
    assert!(!targets.contains(&"content-automaton".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_watch_rejects_bundle_targets() -> ::xtask::sandbox::TestResult<()> {
    let ctx = test_context(false);
    let mut command = base_command(RunSubcommand::Core { instance_id: None });
    command.watch = true;

    let err = command
        .validate_flag_compatibility(&ctx)
        .expect_err("bundle watch must be rejected");
    assert!(
        err.to_string()
            .contains("--watch only supports single local module targets")
    );
    Ok(())
}

#[sinex_test]
#[sinex_test]
async fn test_dev_journal_writes_durable_ndjson_entries() -> ::xtask::sandbox::TestResult<()> {
    // Verify that DevJournal writes queryable NDJSON entries that survive
    // the journal handle being dropped (process exit simulation). (#1140)
    let dir = tempfile::tempdir()?;
    let journal_path = dir.path().join("dev-journal.log");

    {
        let journal = DevJournal::new(&journal_path)?;
        journal.write_entry("sinexd", 12345, "sinexd started");
        journal.write_entry("sinexd", 12345, "listening on :8080");
    } // Journal dropped → writer task flushed and exited

    // Read back and verify entries survived.
    let content = std::fs::read_to_string(&journal_path)?;
    let entries: Vec<serde_json::Value> = content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    assert_eq!(entries.len(), 2, "both entries must be durable");
    for entry in &entries {
        assert_eq!(entry["_SYSTEMD_UNIT"], "sinexd.service");
        assert_eq!(entry["_PID"], "12345");
        assert_eq!(entry["SYSLOG_IDENTIFIER"], "sinexd");
        assert!(!entry["__REALTIME_TIMESTAMP"].as_str().unwrap().is_empty());
        assert!(!entry["_BOOT_ID"].as_str().unwrap().is_empty());
    }
    assert_eq!(entries[0]["MESSAGE"], "sinexd started");
    assert_eq!(entries[1]["MESSAGE"], "listening on :8080");

    Ok(())
}

#[sinex_test]
async fn test_dev_journal_rejects_watch_mode() -> ::xtask::sandbox::TestResult<()> {
    let ctx = test_context(false);
    let mut command = base_command(RunSubcommand::RuntimeModule {
        name: "sinexd".to_string(),
        instance_id: None,
    });
    command.watch = true;
    command.dev_journal = true;

    let err = command
        .validate_flag_compatibility(&ctx)
        .expect_err("watch+journal must be rejected");
    assert!(
        err.to_string()
            .contains("--logs and --dev-journal are incompatible with --watch")
    );
    Ok(())
}

#[sinex_test]
async fn test_unix_timestamp_helpers_reject_pre_epoch_clock() -> ::xtask::sandbox::TestResult<()> {
    let before_epoch = std::time::UNIX_EPOCH
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("pre-epoch timestamp");

    let secs_error =
        unix_timestamp_secs(before_epoch, "boot timestamp").expect_err("pre-epoch secs");
    assert!(
        format!("{secs_error:#}").contains("boot timestamp: system clock is before the unix epoch")
    );

    let micros_error =
        unix_timestamp_micros(before_epoch, "entry timestamp").expect_err("pre-epoch micros");
    assert!(
        format!("{micros_error:#}")
            .contains("entry timestamp: system clock is before the unix epoch")
    );

    Ok(())
}

#[sinex_test]
async fn test_metrics_reject_non_local_subcommands() -> ::xtask::sandbox::TestResult<()> {
    let ctx = test_context(false);
    let mut command = base_command(RunSubcommand::Tether {
        target: "prod".to_string(),
        filter: "events.>".to_string(),
        from_beginning: false,
        from_sequence: None,
    });
    command.metrics = true;

    let err = command
        .validate_flag_compatibility(&ctx)
        .expect_err("metrics on tether must be rejected");
    assert!(
        err.to_string()
            .contains("--metrics only supports local binary or bundle runs")
    );
    Ok(())
}

#[sinex_test]
async fn test_tether_rejects_conflicting_start_flags() -> ::xtask::sandbox::TestResult<()> {
    let ctx = test_context(false);
    let command = base_command(RunSubcommand::Tether {
        target: "prod".to_string(),
        filter: "events.>".to_string(),
        from_beginning: true,
        from_sequence: Some(42),
    });

    let err = command
        .validate_flag_compatibility(&ctx)
        .expect_err("conflicting tether start flags must be rejected");
    assert!(
        err.to_string()
            .contains("--from-beginning and --from-sequence are mutually exclusive")
    );
    Ok(())
}

#[sinex_test]
async fn test_local_run_failure_suggestion_without_journal() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(
        local_run_failure_suggestion(None),
        "Inspect the process output above"
    );
    Ok(())
}

#[sinex_test]
async fn test_local_run_failure_suggestion_with_journal() -> ::xtask::sandbox::TestResult<()> {
    let path = Path::new("/tmp/dev-journal.log");
    assert_eq!(
        local_run_failure_suggestion(Some(path)),
        "Inspect the process output above or the dev journal at /tmp/dev-journal.log"
    );
    Ok(())
}

#[sinex_test]
async fn test_stop_bundle_child_succeeds_for_exited_process() -> ::xtask::sandbox::TestResult<()> {
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg("exit 0")
        .spawn()?;
    child.wait().await?;

    stop_bundle_child("test child", &mut child).await?;
    Ok(())
}

#[sinex_test]
async fn test_stop_bundle_child_kills_child_process_group() -> ::xtask::sandbox::TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let (mut child, process_group, descendant_pid) =
        spawn_managed_persistent_child("test bundle child").await?;

    stop_bundle_child("test child", &mut child).await?;

    assert!(
        child.try_wait()?.is_some(),
        "terminated bundle child should be reaped"
    );
    assert_managed_child_group_and_descendant_gone(process_group, descendant_pid).await?;

    let status = child.wait().await?;
    assert!(
        status.signal().is_some() || !status.success(),
        "terminated bundle child should not report clean success"
    );
    Ok(())
}

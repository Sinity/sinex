use super::*;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;

fn test_context(background: bool) -> CommandContext {
    CommandContext::new(
        OutputWriter::new(OutputFormat::Silent),
        background,
        None,
        "test",
    )
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
        runtime_cli_args("sinexd", "gateway-123", None),
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
            Some("terminal.zsh-history")
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

    let bindings = default_all_source_bindings_from_manifest(manifest);

    assert_eq!(bindings.len(), 1);
    assert_eq!(bindings[0].source_id, "terminal.atuin-history");
    Ok(())
}

#[sinex_test]
async fn test_core_source_bindings_env_preserves_explicit_override()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = vec![(
        "SINEX_SOURCE_BINDINGS_PATH".to_string(),
        "/tmp/custom-bindings.json".to_string(),
    )];

    append_core_source_bindings_env(&mut env);

    assert_eq!(
        env.iter()
            .filter(|(key, _)| key == "SINEX_SOURCE_BINDINGS_PATH")
            .count(),
        1
    );
    assert_eq!(env[0].1, "/tmp/custom-bindings.json");
    Ok(())
}

#[sinex_test]
async fn test_core_source_bindings_env_defaults_to_checkout_manifest()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = Vec::new();

    append_core_source_bindings_env(&mut env);

    assert!(
        env.iter().any(|(key, value)| {
            key == "SINEX_SOURCE_BINDINGS_PATH"
                && value.ends_with(".agent/dev/dev-source-bindings.json")
        }),
        "core run should default to the checkout dev source binding manifest when it exists: {env:?}"
    );
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
    });

    assert!(command.runs_bundle());
    assert!(!command.runs_single_binary());
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
            Some("terminal.zsh-history")
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
    assert!(coordinates.jobs_dir.contains(".sinex/state/jobs"));
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
async fn test_logs_reject_background_mode() -> ::xtask::sandbox::TestResult<()> {
    let ctx = test_context(true);
    let mut command = base_command(RunSubcommand::RuntimeModule {
        name: "fs-source".to_string(),
        instance_id: None,
    });
    command.logs = true;

    let err = command
        .validate_flag_compatibility(&ctx)
        .expect_err("background logs must be rejected");
    assert!(
        err.to_string()
            .contains("--logs and --dev-journal are incompatible with --bg")
    );
    Ok(())
}

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

    let mut command = tokio::process::Command::new("sh");
    configure_managed_child_tokio(&mut command);
    command
        .arg("-c")
        .arg("sleep 30 & echo $!; wait")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut lines = BufReader::new(stdout).lines();
    let sleep_pid = lines
        .next_line()
        .await?
        .expect("shell should print background child pid")
        .parse::<i32>()?;

    stop_bundle_child("test child", &mut child).await?;

    assert!(
        child.try_wait()?.is_some(),
        "terminated bundle child should be reaped"
    );
    assert_ne!(
        unsafe { libc::kill(sleep_pid, 0) },
        0,
        "background process in the bundle child group should be gone"
    );

    let status = child.wait().await?;
    assert!(
        status.signal().is_some() || !status.success(),
        "terminated bundle child should not report clean success"
    );
    Ok(())
}

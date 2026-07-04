// Tests unwrap parser fixtures and runtime-target data written in the same
// test body.
#![allow(clippy::expect_used)]
use super::*;
use std::collections::BTreeSet;
use xtask::sandbox::prelude::*;

use xtask::sandbox::EnvGuard;

fn parse_cli(args: &[&str]) -> color_eyre::Result<(clap::ArgMatches, Cli)> {
    let matches = Cli::command().try_get_matches_from(args)?;
    let cli = Cli::from_arg_matches(&matches).map_err(|error| eyre!(error.to_string()))?;
    Ok((matches, cli))
}

fn parsed_command_path(args: &[&str]) -> color_eyre::Result<String> {
    let (_, cli) = parse_cli(args)?;
    let command = cli
        .command
        .as_ref()
        .ok_or_else(|| eyre!("test command must include a subcommand"))?;
    Ok(command_path(command))
}

fn clap_leaf_command_paths() -> BTreeSet<String> {
    fn collect(prefix: &mut Vec<String>, command: &clap::Command, out: &mut BTreeSet<String>) {
        let visible_children: Vec<&clap::Command> = command
            .get_subcommands()
            .filter(|subcommand| !subcommand.is_hide_set())
            .collect();

        if visible_children.is_empty() {
            if !prefix.is_empty() {
                out.insert(prefix.join(" "));
            }
            return;
        }

        for child in visible_children {
            prefix.push(child.get_name().to_string());
            collect(prefix, child, out);
            prefix.pop();
        }
    }

    let command = Cli::command();
    let mut paths = BTreeSet::new();
    collect(&mut Vec::new(), &command, &mut paths);
    // `ops verify` has an optional `baseline` subcommand; the parent command
    // itself remains executable and needs a format-capability entry.
    paths.insert("ops verify".to_string());
    // Hidden, executable completion endpoint: omitted from public help but
    // still format-consuming and covered by the registry.
    paths.insert("_complete".to_string());
    paths
}

#[sinex_serial_test]
async fn env_token_is_not_treated_as_explicit_cli_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_API_TOKEN", "env-token");

    let (matches, cli) = parse_cli(&["sinexctl", "runtime", "health"])?;
    let token_override = cli_value_is_explicit(&matches, "token")
        .then(|| cli.token.clone())
        .flatten();

    assert_eq!(cli.token.as_deref(), Some("env-token"));
    assert_eq!(
        matches.value_source("token"),
        Some(ValueSource::EnvVariable)
    );
    assert_eq!(token_override, None);
    Ok(())
}

#[sinex_serial_test]
async fn cli_token_is_treated_as_explicit_override() -> TestResult<()> {
    let (matches, cli) = parse_cli(&["sinexctl", "--token", "cli-token", "runtime", "health"])?;
    let token_override = cli_value_is_explicit(&matches, "token")
        .then(|| cli.token.clone())
        .flatten();

    assert_eq!(
        matches.value_source("token"),
        Some(ValueSource::CommandLine)
    );
    assert_eq!(token_override.as_deref(), Some("cli-token"));
    Ok(())
}

#[sinex_serial_test]
async fn rpc_url_is_only_explicit_when_passed_on_command_line() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.clear("SINEX_API_URL");

    let (default_matches, default_cli) = parse_cli(&["sinexctl", "runtime", "health"])?;
    assert!(
        !cli_value_is_explicit(&default_matches, "rpc_url"),
        "default RPC URL must not be treated as an explicit override"
    );
    assert_eq!(default_cli.rpc_url, None);

    let explicit_default = default_rpc_url();
    let (explicit_matches, explicit_cli) = parse_cli(&[
        "sinexctl",
        "--rpc-url",
        explicit_default.as_str(),
        "runtime",
        "health",
    ])?;
    assert!(
        cli_value_is_explicit(&explicit_matches, "rpc_url"),
        "explicit --rpc-url must remain an explicit override even when equal to the default"
    );
    assert_eq!(
        explicit_cli.rpc_url.as_deref(),
        Some(explicit_default.as_str())
    );
    Ok(())
}

#[sinex_serial_test]
async fn runtime_automata_command_is_registered() -> TestResult<()> {
    let (_matches, cli) = parse_cli(&["sinexctl", "runtime", "automata"])?;

    assert!(
        matches!(cli.command, Some(Commands::Runtime { .. })),
        "automata command must remain exposed under the runtime operator surface"
    );
    Ok(())
}

#[sinex_serial_test]
async fn env_provided_rpc_url_is_not_treated_as_cli_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_API_URL", "https://env-only:9443");

    let (matches, cli) = parse_cli(&["sinexctl", "runtime", "health"])?;
    assert!(
        !cli_value_is_explicit(&matches, "rpc_url"),
        "environment-provided RPC URL must not masquerade as a command-line override"
    );
    assert_eq!(cli.rpc_url.as_deref(), Some("https://env-only:9443"));
    Ok(())
}

#[sinex_serial_test]
async fn timeout_and_format_are_only_explicit_when_passed_on_command_line() -> TestResult<()> {
    let (default_matches, default_cli) = parse_cli(&["sinexctl", "runtime", "health"])?;
    assert!(!cli_value_is_explicit(&default_matches, "timeout"));
    assert!(!cli_value_is_explicit(&default_matches, "format"));
    assert_eq!(default_cli.timeout, 30);
    assert!(matches!(default_cli.format, OutputFormat::Table));

    let (explicit_matches, explicit_cli) = parse_cli(&[
        "sinexctl",
        "--timeout",
        "45",
        "--format",
        "json",
        "runtime",
        "health",
    ])?;
    assert!(cli_value_is_explicit(&explicit_matches, "timeout"));
    assert!(cli_value_is_explicit(&explicit_matches, "format"));
    assert_eq!(explicit_cli.timeout, 45);
    assert!(matches!(explicit_cli.format, OutputFormat::Json));
    Ok(())
}

#[sinex_serial_test]
async fn runtime_target_path_can_come_from_environment() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_RUNTIME_TARGET_CONFIG",
        "/tmp/sinex-runtime-target.json",
    );

    let (matches, cli) = parse_cli(&["sinexctl", "runtime", "health"])?;

    assert_eq!(
        matches.value_source("runtime_target"),
        Some(ValueSource::EnvVariable)
    );
    assert_eq!(
        cli.runtime_target.as_deref(),
        Some(std::path::Path::new("/tmp/sinex-runtime-target.json"))
    );
    Ok(())
}

#[sinex_serial_test]
async fn runtime_target_override_populates_config() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let descriptor_path = dir.path().join("runtime-target.json");
    std::fs::write(
        &descriptor_path,
        r#"{
          "version": 1,
          "name": "prod",
          "kind": "deployed_host",
          "gateway": {
            "base_url": "https://127.0.0.1:9999",
            "token_file": "/run/agenix/sinex-api-admin-token",
            "token_role": "admin",
            "ca_cert_file": "/var/lib/sinex/run/gateway-ca.pem"
          }
        }"#,
    )?;

    let target = load_runtime_target_override(Some(descriptor_path))?
        .expect("runtime target descriptor must load");
    let mut config = Config::default();
    config.apply_runtime_target(target);

    assert_eq!(config.rpc_url, "https://127.0.0.1:9999");
    assert_eq!(
        config.token_file.as_deref(),
        Some("/run/agenix/sinex-api-admin-token")
    );
    assert_eq!(
        config.token_role,
        Some(sinex_primitives::RuntimeTargetGatewayTokenRole::Admin)
    );
    assert_eq!(
        config.ca_cert.as_deref(),
        Some("/var/lib/sinex/run/gateway-ca.pem")
    );
    assert_eq!(
        config
            .runtime_target
            .as_ref()
            .map(|target| target.name.as_str()),
        Some("prod")
    );
    Ok(())
}
#[sinex_test]
async fn list_formats_flag_parses_without_subcommand() -> TestResult<()> {
    let (_, cli) = parse_cli(&["sinexctl", "--list-formats"])?;
    assert!(cli.list_formats, "--list-formats must be parsed correctly");
    assert!(
        cli.command.is_none(),
        "--list-formats without subcommand must parse"
    );
    Ok(())
}

#[sinex_test]
async fn format_matrix_terminal_output_contains_key_commands() -> TestResult<()> {
    let output = sinexctl::render_format_matrix_terminal();
    assert!(
        output.contains("events query"),
        "matrix must list `events query`"
    );
    assert!(output.contains("recall"), "matrix must list `recall`");
    assert!(output.contains("relations"), "matrix must list `relations`");
    assert!(
        output.contains("events watch"),
        "matrix must list `events watch`"
    );
    assert!(
        output.contains("stream"),
        "matrix must mark `events watch` as streaming"
    );
    assert!(
        output.contains("events.query"),
        "matrix must expose exact backing RPC method names"
    );
    assert!(
        output.contains("events.relation_evidence"),
        "matrix must expose relation evidence RPC method name"
    );
    assert!(
        output.contains("privacy.private_mode.enable"),
        "matrix must expose privacy control RPC method names"
    );
    Ok(())
}

#[sinex_test]
async fn list_formats_json_outputs_machine_readable_catalog() -> TestResult<()> {
    let output = render_list_formats(OutputFormat::Json)?;
    let catalog: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(catalog["schema_version"], 1);
    assert!(
        catalog["docs_projection"]["command_fields"]
            .as_array()
            .expect("docs projection must list command fields")
            .iter()
            .any(|field| field.as_str() == Some("backing_rpc_methods")),
        "json list-formats output must expose the documented command field contract"
    );

    let commands = catalog["commands"]
        .as_array()
        .expect("operator surface catalog must contain command rows");
    let query = commands
        .iter()
        .find(|entry| entry["path"] == "events query")
        .expect("json list-formats output must include events query");
    assert_eq!(query["backing_rpc_methods"][0], "events.query");

    let relations = commands
        .iter()
        .find(|entry| entry["path"] == "events relations within")
        .expect("json list-formats output must include events relations within");
    assert_eq!(
        relations["backing_rpc_methods"][0],
        "events.relation_evidence"
    );

    let blob_fsck = commands
        .iter()
        .find(|entry| entry["path"] == "ops blob fsck")
        .expect("json list-formats output must include ops blob fsck");
    assert!(
        blob_fsck["mutation_guards"]
            .as_array()
            .expect("mutation guards must be an array")
            .iter()
            .any(|guard| guard.as_str() == Some("dry_run")),
        "json list-formats output must expose local mutation guards"
    );

    let rpc_methods = catalog["rpc_methods"]
        .as_array()
        .expect("operator surface catalog must contain RPC descriptor rows");
    assert!(
        rpc_methods
            .iter()
            .any(|entry| entry["name"] == "events.query"),
        "json list-formats output must include typed RPC descriptors"
    );
    assert!(
        rpc_methods
            .iter()
            .any(|entry| entry["name"] == "events.relation_evidence"),
        "json list-formats output must include relation evidence RPC descriptor"
    );

    let mcp_surfaces = catalog["mcp_surfaces"]
        .as_array()
        .expect("operator surface catalog must contain MCP surface rows");
    let source_readiness = mcp_surfaces
        .iter()
        .find(|entry| entry["name"] == "sinex_source_readiness")
        .expect("json list-formats output must include MCP source readiness");
    assert_eq!(
        source_readiness["backing_rpc_methods"][0],
        "sources.readiness.list"
    );
    Ok(())
}

#[sinex_test]
async fn events_relations_command_path_parses_as_read_only_surface() -> TestResult<()> {
    let path = parsed_command_path(&[
        "sinexctl",
        "events",
        "relations",
        "within",
        "--within-secs",
        "60",
        "--seed-query-json",
        "{}",
    ])?;
    assert_eq!(path, "events relations within");
    Ok(())
}

#[sinex_test]
async fn list_formats_dot_is_rejected() -> TestResult<()> {
    let err = render_list_formats(OutputFormat::Dot).unwrap_err();
    assert!(
        err.to_string().contains("--format dot"),
        "dot rejection should name the unsupported format"
    );
    Ok(())
}

#[sinex_test]
async fn validate_format_rejects_dot_for_runtime_health() -> TestResult<()> {
    let result = sinexctl::validate_format("runtime health", sinexctl::OutputFormat::Dot);
    assert!(result.is_err(), "runtime health must reject dot format");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("runtime health"),
        "error must name the command"
    );
    Ok(())
}

#[sinex_test]
async fn validate_format_accepts_ndjson_for_runtime_list() -> TestResult<()> {
    // `runtime list` renders through render_envelope and advertises ndjson,
    // so `runtime list --format ndjson` must be reachable (Codex review,
    // PR #1771 — the rendering path existed but the registry rejected it).
    assert!(
        sinexctl::validate_format("runtime list", sinexctl::OutputFormat::Ndjson).is_ok(),
        "runtime list must accept ndjson format"
    );
    Ok(())
}

#[sinex_test]
async fn validate_format_rejects_ndjson_for_finite_view_envelopes() -> TestResult<()> {
    for command in [
        "docs chunks",
        "docs get",
        "docs search",
        "events recent",
        "ops dlq cleanup-plan",
        "ops dlq peek",
        "ops dlq triage",
        "ops lifecycle status",
        "ops lifecycle tombstone list",
        "metrics telemetry event-engine-validation",
        "metrics telemetry gateway-stats",
        "metrics throughput",
        "ops audit",
        "metrics report calendar",
        "metrics report today",
        "metrics report yesterday",
        "privacy audit",
        "privacy export",
        "privacy policy list",
        "privacy private-mode status",
        "ops replay list",
        "ops replay preview",
        "ops replay status",
        "ops blob fsck",
        "ops blob migrate",
        "ops blob sweep-orphans",
        "ops blob verify-integrity",
        "runtime health",
        "runtime modules",
        "runtime status",
        "semantic curation duplicates",
        "semantic curation proposals",
        "semantic epoch list",
        "semantic lane diffs",
        "semantic lane list",
        "semantic lane outputs",
        "sources coverage",
        "sources list",
        "sources show",
        "sources status",
        "tasks list",
        "tasks state",
        "show",
    ] {
        let result = sinexctl::validate_format(command, sinexctl::OutputFormat::Ndjson);
        assert!(
            result.is_err(),
            "{command} is a finite ViewEnvelope and must reject ndjson"
        );
    }
    Ok(())
}

#[sinex_test]
async fn formatless_commands_are_not_format_consumers() -> TestResult<()> {
    // Formatless commands ignore --format; a config `default_format` must
    // not be validated against them.
    assert!(
        !sinexctl::command_consumes_format("ops demo"),
        "ops demo is formatless"
    );
    assert!(
        sinexctl::command_consumes_format("runtime list"),
        "runtime list consumes a format"
    );
    Ok(())
}

#[sinex_test]
async fn validate_format_accepts_dot_for_trace() -> TestResult<()> {
    assert!(
        sinexctl::validate_format("events trace", sinexctl::OutputFormat::Dot).is_ok(),
        "events trace must accept dot format"
    );
    Ok(())
}

#[sinex_test]
async fn command_path_preserves_format_registry_leaf_commands() -> TestResult<()> {
    let cases = [
        (
            vec![
                "sinexctl",
                "query",
                "events where source = \"terminal.fish-history\" limit 10",
            ],
            "query",
        ),
        (
            vec![
                "sinexctl",
                "recall",
                "--at",
                "2026-07-02T19:00:00Z",
                "--window",
                "30m",
            ],
            "recall",
        ),
        (
            vec!["sinexctl", "ops", "dlq", "peek", "-n", "5"],
            "ops dlq peek",
        ),
        (
            vec!["sinexctl", "ops", "dlq", "triage", "--tail", "5"],
            "ops dlq triage",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "dlq",
                "cleanup-plan",
                "--tail",
                "5",
            ],
            "ops dlq cleanup-plan",
        ),
        (
            vec!["sinexctl", "ops", "dlq", "requeue", "--all"],
            "ops dlq requeue",
        ),
        (
            vec!["sinexctl", "ops", "dlq", "purge", "--confirm"],
            "ops dlq purge",
        ),
        (vec!["sinexctl", "config", "init"], "config init"),
        (vec!["sinexctl", "config", "path"], "config path"),
        (vec!["sinexctl", "config", "edit"], "config edit"),
        (
            vec!["sinexctl", "runtime", "gateway", "ping"],
            "runtime gateway ping",
        ),
        (
            vec!["sinexctl", "runtime", "gateway", "version"],
            "runtime gateway version",
        ),
        (vec!["sinexctl", "runtime", "health"], "runtime health"),
        (
            vec!["sinexctl", "metrics", "report", "yesterday"],
            "metrics report yesterday",
        ),
        (
            vec!["sinexctl", "metrics", "report", "calendar"],
            "metrics report calendar",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "audit",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "ops audit",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "blob",
                "fsck",
                "--content-store-path",
                "/tmp/sinex-cas",
            ],
            "ops blob fsck",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "lifecycle",
                "tombstone",
                "approve",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--yes-i-understand-data-is-gone",
            ],
            "ops lifecycle tombstone approve",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "lifecycle",
                "tombstone",
                "preview",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "ops lifecycle tombstone preview",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "lifecycle",
                "tombstone",
                "cancel",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "ops lifecycle tombstone cancel",
        ),
        (
            vec!["sinexctl", "ops", "lifecycle", "tombstone", "list"],
            "ops lifecycle tombstone list",
        ),
        (
            vec!["sinexctl", "record", "task", "--title", "fixture"],
            "record task",
        ),
        (
            vec![
                "sinexctl",
                "record",
                "health",
                "intake",
                "--substance",
                "caffeine",
                "--at",
                "2026-05-19T10:00:00Z",
            ],
            "record health intake",
        ),
        (
            vec![
                "sinexctl",
                "record",
                "health",
                "effect",
                "--effect",
                "calm",
                "--at",
                "2026-05-19T11:00:00Z",
            ],
            "record health effect",
        ),
        (
            vec![
                "sinexctl",
                "tasks",
                "complete",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "tasks complete",
        ),
        (
            vec![
                "sinexctl",
                "tasks",
                "state",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "tasks state",
        ),
        (vec!["sinexctl", "privacy", "audit"], "privacy audit"),
        (
            vec![
                "sinexctl", "privacy", "export", "--since", "24h", "--source", "terminal",
            ],
            "privacy export",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "instructions",
                "hyprland-workspace",
                "--workspace",
                "4",
                "--dry-run",
            ],
            "ops instructions hyprland-workspace",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "state",
                "snapshot",
                "--output",
                "/tmp/sinex-state.tar.zst",
            ],
            "ops state snapshot",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "state",
                "inspect",
                "--archive",
                "/tmp/sinex-state.tar.zst",
            ],
            "ops state inspect",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "state",
                "restore",
                "--archive",
                "/tmp/sinex-state.tar.zst",
                "--target-dir",
                "/tmp/sinex-restore",
                "--dry-run",
            ],
            "ops state restore",
        ),
        (
            vec!["sinexctl", "semantic", "curation", "proposals"],
            "semantic curation proposals",
        ),
        (
            vec!["sinexctl", "semantic", "curation", "duplicates"],
            "semantic curation duplicates",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "curation",
                "judge",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--decision",
                "accept",
            ],
            "semantic curation judge",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "curation",
                "duplicate-judge",
                "--source",
                "webhistory",
                "--event-type",
                "page.visited",
                "--equivalence-key",
                "visit-1",
                "--event-id",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--event-id",
                "0196ed62-8f7a-7000-8000-000000000002",
                "--action",
                "merge",
            ],
            "semantic curation duplicate-judge",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "curation",
                "finalize",
                "0196ed62-8f7a-7000-8000-000000000002",
            ],
            "semantic curation finalize",
        ),
        (
            vec!["sinexctl", "semantic", "llm", "prompts"],
            "semantic llm prompts",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "llm",
                "route-explain",
                "--request-json",
                "{}",
                "--policy-json",
                "{}",
            ],
            "semantic llm route-explain",
        ),
        (
            vec!["sinexctl", "semantic", "llm", "budget-report"],
            "semantic llm budget-report",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "seed-canonical-graph",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "semantic lane seed-canonical-graph",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "epoch",
                "create",
                "--name",
                "fixture",
                "--scope-kind",
                "event_set",
                "--input-id",
                "event:1",
                "--input-set-hash",
                "hash",
                "--config-hash",
                "config",
            ],
            "semantic epoch create",
        ),
        (
            vec!["sinexctl", "semantic", "epoch", "list"],
            "semantic epoch list",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "create",
                "--name",
                "fixture",
                "--candidate-epoch-id",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--scope-kind",
                "event_set",
                "--input-id",
                "event:1",
                "--input-set-hash",
                "hash",
                "--purpose",
                "fixture",
            ],
            "semantic lane create",
        ),
        (
            vec![
                "sinexctl", "semantic", "lane", "list", "--status", "planned",
            ],
            "semantic lane list",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "status",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--status",
                "running",
            ],
            "semantic lane status",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "discard",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "semantic lane discard",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "outputs",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "semantic lane outputs",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "write-outputs",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--outputs-json",
                r#"{"entities":[],"relations":[]}"#,
            ],
            "semantic lane write-outputs",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "diffs",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "semantic lane diffs",
        ),
        (
            vec![
                "sinexctl",
                "semantic",
                "lane",
                "compare",
                "--baseline-lane-id",
                "0196ed62-8f7a-7000-8000-000000000001",
                "--candidate-lane-id",
                "0196ed62-8f7a-7000-8000-000000000002",
            ],
            "semantic lane compare",
        ),
        (
            vec![
                "sinexctl",
                "ops",
                "lifecycle",
                "tombstone",
                "status",
                "0196ed62-8f7a-7000-8000-000000000001",
            ],
            "ops lifecycle tombstone status",
        ),
        (vec!["sinexctl", "show", "source-material:0196ed62"], "show"),
    ];

    for (args, expected) in cases {
        let actual = parsed_command_path(&args)?;
        assert_eq!(actual, expected, "wrong command path for {args:?}");
        sinexctl::validate_format(&actual, OutputFormat::Table).map_err(|msg| eyre!(msg))?;
    }

    Ok(())
}

#[sinex_test]
async fn show_catalog_ref_executes_before_gateway_client_is_required() -> TestResult<()> {
    let (_, cli) = parse_cli(&["sinexctl", "show", "command:show"])?;
    let command = cli
        .command
        .expect("show command should parse as a concrete subcommand");
    let Commands::Show(show) = command else {
        panic!("expected parsed command to be show");
    };

    assert!(show.execute_local_if_supported(OutputFormat::Table)?);
    Ok(())
}

#[sinex_test]
async fn format_registry_exactly_covers_clap_leaf_commands() -> TestResult<()> {
    let clap_paths = clap_leaf_command_paths();
    let registry_paths: BTreeSet<String> = sinexctl::format_registry()
        .keys()
        .map(|key| (*key).to_string())
        .collect();

    let missing: Vec<&String> = clap_paths.difference(&registry_paths).collect();
    let extra: Vec<&String> = registry_paths.difference(&clap_paths).collect();

    assert!(
        missing.is_empty() && extra.is_empty(),
        "output-format registry must exactly match clap leaf commands\nmissing: {missing:#?}\nextra: {extra:#?}"
    );

    Ok(())
}

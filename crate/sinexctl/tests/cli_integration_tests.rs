//! CLI integration tests for sinexctl
//!
//! These tests verify CLI argument parsing, help text, completion,
//! and error handling without requiring a running gateway.

use assert_cmd::Command;
use assert_cmd::cargo;
use predicates::prelude::*;
use xtask::sandbox::sinex_test;

/// Helper to create a sinexctl command
fn sinexctl() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

mod help_tests {
    use super::*;

    #[sinex_test]
    async fn bare_sinexctl_renders_command_center() -> TestResult<()> {
        sinexctl()
            .args(["--format", "table"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Sinex command center"))
            .stdout(predicate::str::contains("Primary actions"))
            .stdout(predicate::str::contains("sinexctl now"))
            .stdout(predicate::str::contains("Root groups"))
            .stdout(predicate::str::contains("sources"));
        Ok(())
    }

    #[sinex_test]
    async fn bare_sinexctl_json_is_view_envelope() -> TestResult<()> {
        let output = sinexctl().args(["--format", "json"]).output()?;

        assert!(
            output.status.success(),
            "bare sinexctl -f json failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout)?;
        let value: serde_json::Value = serde_json::from_str(&stdout)?;

        assert_eq!(value["schema_version"], "sinex.view-envelope/v3");
        assert_eq!(value["source_surface"], "sinexctl.command_center");
        assert_eq!(value["payload"]["schema_version"], 1);
        assert_eq!(
            value["payload"]["primary_actions"][0]["command"],
            "sinexctl now"
        );
        assert_eq!(value["payload"]["root_groups"][0]["root"], "events");
        Ok(())
    }

    #[sinex_test]
    async fn test_help_flag() -> TestResult<()> {
        sinexctl()
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Usage: sinexctl"))
            .stdout(predicate::str::contains("Commands:"))
            .stdout(predicate::str::contains("events"))
            .stdout(predicate::str::contains("ops"))
            .stdout(predicate::str::contains("docs"))
            .stdout(predicate::str::contains("semantic"))
            .stdout(predicate::str::contains("metrics"))
            .stdout(predicate::str::contains("config"))
            .stdout(predicate::str::contains("sources"))
            .stdout(predicate::str::contains("  audit").not())
            .stdout(predicate::str::contains("  blob").not())
            .stdout(predicate::str::contains("  state").not())
            .stdout(predicate::str::contains("  admin").not())
            .stdout(predicate::str::contains("relations").not())
            .stdout(predicate::str::contains("documents").not())
            .stdout(predicate::str::contains("semantics").not())
            .stdout(predicate::str::contains("  dlq").not())
            .stdout(predicate::str::contains("  replay").not())
            .stdout(predicate::str::contains("  lifecycle").not())
            .stdout(predicate::str::contains("_complete").not())
            .stdout(predicate::str::contains("completions").not());
        Ok(())
    }

    #[sinex_test]
    async fn ops_help_contains_maintenance_subsurfaces() -> TestResult<()> {
        sinexctl()
            .args(["ops", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("dlq"))
            .stdout(predicate::str::contains("replay"))
            .stdout(predicate::str::contains("lifecycle"))
            .stdout(predicate::str::contains("audit"))
            .stdout(predicate::str::contains("blob"))
            .stdout(predicate::str::contains("state"));
        Ok(())
    }

    #[sinex_test]
    async fn structured_complete_is_hidden_but_callable() -> TestResult<()> {
        sinexctl()
            .args([
                "_complete",
                "--line",
                "sinexctl events source:wm",
                "--cursor",
                "24",
                "--format",
                "json",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("\"schema_version\""))
            .stdout(predicate::str::contains("source:wm.hyprland"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_query_replaces_top_level_query_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "query", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Query/search events"))
            .stdout(predicate::str::contains("EXAMPLES"))
            .stdout(predicate::str::contains("--since"))
            .stdout(predicate::str::contains("--source"))
            .stdout(predicate::str::contains("--event-type"))
            .stdout(predicate::str::contains("--interactive"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Event search"))
            .stdout(predicate::str::contains("query"))
            .stdout(predicate::str::contains("recent"))
            .stdout(predicate::str::contains("trace"))
            .stdout(predicate::str::contains("inspect"))
            .stdout(predicate::str::contains("annotate"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_query_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "query", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Query/search events"))
            .stdout(predicate::str::contains("--since"))
            .stdout(predicate::str::contains("--source"))
            .stdout(predicate::str::contains("--event-type"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_inspect_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "inspect", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Event ID"))
            .stdout(predicate::str::contains("EXAMPLES"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_trace_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "trace", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Trace event provenance chain"))
            .stdout(predicate::str::contains("--direction"))
            .stdout(predicate::str::contains("--max-depth"));
        Ok(())
    }

    #[sinex_test]
    async fn test_events_annotate_help() -> TestResult<()> {
        sinexctl()
            .args(["events", "annotate", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Event UUID"))
            .stdout(predicate::str::contains("--note"))
            .stdout(predicate::str::contains("--kind"));
        Ok(())
    }

    #[sinex_test]
    async fn test_runtime_help() -> TestResult<()> {
        sinexctl()
            .args(["runtime", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Runtime module operations"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("modules"))
            .stdout(predicate::str::contains("automata"))
            .stdout(predicate::str::contains("status"))
            .stdout(predicate::str::contains("drain"))
            .stdout(predicate::str::contains("resume"))
            .stdout(predicate::str::contains("set-horizon"));
        Ok(())
    }

    #[sinex_test]
    async fn test_sources_help() -> TestResult<()> {
        sinexctl()
            .args(["sources", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Source material inventory and staging",
            ))
            .stdout(predicate::str::contains("stage"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("show"))
            .stdout(predicate::str::contains("coverage"));
        Ok(())
    }

    #[sinex_test]
    async fn test_sources_stage_help() -> TestResult<()> {
        sinexctl()
            .args(["sources", "stage", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Stage a file as source material"))
            .stdout(predicate::str::contains("--reason"))
            .stdout(predicate::str::contains("--format"))
            .stdout(predicate::str::contains("--tag"));
        Ok(())
    }

    #[sinex_test]
    async fn test_dlq_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "dlq", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Dead letter queue"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("peek"))
            .stdout(predicate::str::contains("requeue"))
            .stdout(predicate::str::contains("purge"));
        Ok(())
    }

    #[sinex_test]
    async fn test_replay_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "replay", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Replay operations"))
            .stdout(predicate::str::contains("plan"))
            .stdout(predicate::str::contains("submit"))
            .stdout(predicate::str::contains("watch"))
            .stdout(predicate::str::contains("list"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ops_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Operations log"))
            .stdout(predicate::str::contains("start"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("get"))
            .stdout(predicate::str::contains("cancel"));
        Ok(())
    }

    #[sinex_test]
    async fn test_ops_start_help_does_not_expose_dead_operator_flag() -> TestResult<()> {
        sinexctl()
            .args(["ops", "start", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("--operator").not())
            .stdout(predicate::str::contains("--operation-type"))
            .stdout(predicate::str::contains("--scope"));
        Ok(())
    }

    #[sinex_test]
    async fn test_instructions_hyprland_help_mentions_default_socket_resolution() -> TestResult<()>
    {
        sinexctl()
            .args(["instructions", "hyprland-workspace", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "sinexctl instructions hyprland-workspace --workspace 4\n",
            ))
            .stdout(predicate::str::contains("--socket-path"))
            .stdout(predicate::str::contains("XDG_RUNTIME_DIR"))
            .stdout(predicate::str::contains("HYPRLAND_INSTANCE_SIGNATURE"));
        Ok(())
    }

    #[sinex_test]
    async fn test_config_help() -> TestResult<()> {
        sinexctl()
            .args(["config", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Configuration management"))
            .stdout(predicate::str::contains("init"))
            .stdout(predicate::str::contains("show"))
            .stdout(predicate::str::contains("path"))
            .stdout(predicate::str::contains("edit"));
        Ok(())
    }

    #[sinex_test]
    async fn test_blob_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "blob", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Blob and content-store maintenance"))
            .stdout(predicate::str::contains("sweep-orphans"));
        Ok(())
    }

    #[sinex_test]
    async fn test_blob_sweep_orphans_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "blob", "sweep-orphans", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Reclaim unused content-store keys",
            ))
            .stdout(predicate::str::contains("--content-store-path"))
            .stdout(predicate::str::contains("--apply"));
        Ok(())
    }

    #[sinex_test]
    async fn test_blob_verify_integrity_help() -> TestResult<()> {
        sinexctl()
            .args(["ops", "blob", "verify-integrity", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("anchor_payload_hash"))
            .stdout(predicate::str::contains("--content-store-path"))
            .stdout(predicate::str::contains("--material-id"))
            .stdout(predicate::str::contains("--limit"));
        Ok(())
    }

    #[sinex_test]
    async fn test_verify_help_exposes_evidence_flags() -> TestResult<()> {
        sinexctl()
            .args(["verify", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Check bounded runtime evidence and optional smoke probes",
            ))
            .stdout(predicate::str::contains("--gateway-smoke").not())
            .stdout(predicate::str::contains("--automata-smoke").not())
            .stdout(predicate::str::contains("--document-smoke"))
            .stdout(predicate::str::contains("--source-evidence"))
            .stdout(predicate::str::contains("--historical-evidence"))
            .stdout(predicate::str::contains("--source-proof").not())
            .stdout(predicate::str::contains("--historical-proof").not());
        Ok(())
    }
}

mod version_tests {
    use super::*;

    #[sinex_test]
    async fn test_version_flag() -> TestResult<()> {
        sinexctl()
            .arg("--version")
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
        Ok(())
    }
}

mod completion_endpoint_tests {
    use super::*;

    #[sinex_test]
    async fn public_completions_root_is_removed() -> TestResult<()> {
        sinexctl()
            .args(["completions", "bash"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("unrecognized subcommand"));
        Ok(())
    }

    #[sinex_test]
    async fn structured_completion_replaces_shell_script_generator() -> TestResult<()> {
        sinexctl()
            .args([
                "_complete",
                "--line",
                "sinexctl ",
                "--cursor",
                "9",
                "--format",
                "json",
            ])
            .assert()
            .success()
            .stdout(predicate::str::contains("\"candidates\""))
            .stdout(predicate::str::contains("\"events\""))
            .stdout(predicate::str::contains("\"docs\""))
            .stdout(predicate::str::contains("\"semantic\""));
        Ok(())
    }
}

mod config_tests {
    use super::*;

    #[sinex_test]
    async fn test_config_path() -> TestResult<()> {
        sinexctl()
            .args(["config", "path"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"))
            .stdout(predicate::str::contains(".toml").or(predicate::str::contains("config")));
        Ok(())
    }

    #[sinex_test]
    async fn test_config_show_default_format() -> TestResult<()> {
        // Config show should work even without a config file (shows defaults)
        sinexctl()
            .args(["config", "show"])
            .assert()
            .success()
            .stdout(predicate::str::contains("rpc_url"));
        Ok(())
    }

    #[sinex_test]
    async fn test_config_show_json_format() -> TestResult<()> {
        sinexctl()
            .args(["config", "show", "-f", "json"])
            .assert()
            .success()
            .stdout(predicate::str::starts_with("{"))
            .stdout(predicate::str::contains("rpc_url"));
        Ok(())
    }
}

mod error_handling_tests {
    use super::*;

    #[sinex_test]
    async fn test_invalid_command() -> TestResult<()> {
        sinexctl()
            .arg("nonexistent-command")
            .assert()
            .failure()
            .stderr(predicate::str::contains("error"));
        Ok(())
    }

    #[sinex_test]
    async fn test_missing_required_args() -> TestResult<()> {
        // runtime status requires a module name
        sinexctl()
            .args(["runtime", "status"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("required"));
        Ok(())
    }

    #[sinex_test]
    async fn test_invalid_output_format() -> TestResult<()> {
        sinexctl()
            .args(["events", "query", "-f", "invalid-format"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("invalid"));
        Ok(())
    }

    #[sinex_test]
    async fn test_dlq_requeue_requires_id_or_all() -> TestResult<()> {
        // dlq requeue without --event-id or --all should fail
        // Note: This will try to connect to gateway, so we check for the validation error
        // or connection error
        sinexctl().args(["dlq", "requeue"]).assert().failure();
        Ok(())
    }
}

mod output_format_tests {
    use super::*;

    #[sinex_test]
    async fn test_valid_output_formats() -> TestResult<()> {
        // Test that format flag is recognized
        for format in ["table", "json", "yaml"] {
            sinexctl()
                .args(["events", "query", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains(format));
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_query_format_flag_short() -> TestResult<()> {
        // -f should be recognized
        sinexctl()
            .args(["events", "query", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("-f"));
        Ok(())
    }
}

mod environment_tests {
    use super::*;

    #[sinex_test]
    async fn test_rpc_url_env_recognized() -> TestResult<()> {
        // Help should mention the environment variable
        sinexctl().args(["--help"]).assert().success().stdout(
            predicate::str::contains("SINEX_API_URL").or(predicate::str::contains("rpc-url")),
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_token_env_recognized() -> TestResult<()> {
        sinexctl()
            .args(["--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("token").or(predicate::str::contains("SINEX")));
        Ok(())
    }
}

mod shortcut_command_tests {
    use super::*;

    #[sinex_test]
    async fn test_status_command_exists() -> TestResult<()> {
        sinexctl()
            .args(["status", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("status"));
        Ok(())
    }

    #[sinex_test]
    async fn event_shortcut_roots_are_pruned() -> TestResult<()> {
        for root in [
            "query",
            "recent",
            "errors",
            "watch",
            "trace",
            "annotate",
            "timeline",
            "explain",
            "modules",
            "automata",
            "throughput",
            "telemetry",
            "report",
        ] {
            sinexctl()
                .args([root, "--help"])
                .assert()
                .failure()
                .stderr(predicate::str::contains("unrecognized subcommand"));
        }
        Ok(())
    }
}

mod tui_tests {
    use super::*;

    #[sinex_test]
    async fn test_tui_help() -> TestResult<()> {
        sinexctl()
            .args(["tui", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("TUI"))
            .stdout(predicate::str::contains("--tab"))
            .stdout(predicate::str::contains("operations"))
            .stdout(predicate::str::contains("--refresh"))
            .stdout(predicate::str::contains("KEYBOARD SHORTCUTS"));
        Ok(())
    }
}

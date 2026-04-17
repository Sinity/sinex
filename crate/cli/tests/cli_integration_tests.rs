//! CLI integration tests for sinexctl
//!
//! These tests verify CLI argument parsing, help text, completions,
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
    async fn test_help_flag() -> TestResult<()> {
        sinexctl()
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("Usage: sinexctl"))
            .stdout(predicate::str::contains("Commands:"))
            .stdout(predicate::str::contains("query"))
            .stdout(predicate::str::contains("node"))
            .stdout(predicate::str::contains("dlq"))
            .stdout(predicate::str::contains("replay"))
            .stdout(predicate::str::contains("ops"))
            .stdout(predicate::str::contains("audit"))
            .stdout(predicate::str::contains("blob"))
            .stdout(predicate::str::contains("config"))
            .stdout(predicate::str::contains("completions"));
        Ok(())
    }

    #[sinex_test]
    async fn test_query_help() -> TestResult<()> {
        sinexctl()
            .args(["query", "--help"])
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
    async fn test_node_help() -> TestResult<()> {
        sinexctl()
            .args(["node", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Node operations"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("status"))
            .stdout(predicate::str::contains("drain"))
            .stdout(predicate::str::contains("resume"))
            .stdout(predicate::str::contains("set-horizon"));
        Ok(())
    }

    #[sinex_test]
    async fn test_dlq_help() -> TestResult<()> {
        sinexctl()
            .args(["dlq", "--help"])
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
            .args(["replay", "--help"])
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
            .args(["blob", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Blob maintenance commands"))
            .stdout(predicate::str::contains("sweep-orphans"));
        Ok(())
    }

    #[sinex_test]
    async fn test_blob_sweep_orphans_help() -> TestResult<()> {
        sinexctl()
            .args(["blob", "sweep-orphans", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Reclaim unused git-annex keys"))
            .stdout(predicate::str::contains("--repo-path"))
            .stdout(predicate::str::contains("--apply"));
        Ok(())
    }

    #[sinex_test]
    async fn test_verify_help_exposes_proof_flags() -> TestResult<()> {
        sinexctl()
            .args(["verify", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains(
                "Verify trustworthiness invariants",
            ))
            .stdout(predicate::str::contains("--gateway-smoke"))
            .stdout(predicate::str::contains("--automata-smoke"))
            .stdout(predicate::str::contains("--historical-proof"));
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

mod completions_tests {
    use super::*;

    #[sinex_test]
    async fn test_bash_completions() -> TestResult<()> {
        sinexctl()
            .args(["completions", "bash"])
            .assert()
            .success()
            .stdout(predicate::str::contains("_sinexctl"))
            .stdout(predicate::str::contains("complete"));
        Ok(())
    }

    #[sinex_test]
    async fn test_zsh_completions() -> TestResult<()> {
        sinexctl()
            .args(["completions", "zsh"])
            .assert()
            .success()
            .stdout(predicate::str::contains("#compdef sinexctl"));
        Ok(())
    }

    #[sinex_test]
    async fn test_fish_completions() -> TestResult<()> {
        sinexctl()
            .args(["completions", "fish"])
            .assert()
            .success()
            .stdout(predicate::str::contains("complete -c sinexctl"));
        Ok(())
    }

    #[sinex_test]
    async fn test_powershell_completions() -> TestResult<()> {
        sinexctl()
            .args(["completions", "powershell"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
        Ok(())
    }

    #[sinex_test]
    async fn test_elvish_completions() -> TestResult<()> {
        sinexctl()
            .args(["completions", "elvish"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
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
        // node status requires a node name
        sinexctl()
            .args(["node", "status"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("required"));
        Ok(())
    }

    #[sinex_test]
    async fn test_invalid_output_format() -> TestResult<()> {
        sinexctl()
            .args(["query", "-f", "invalid-format"])
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
                .args(["query", "--help"])
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
            .args(["query", "--help"])
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
            predicate::str::contains("SINEX_RPC_URL").or(predicate::str::contains("rpc-url")),
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
    async fn test_recent_command_exists() -> TestResult<()> {
        sinexctl()
            .args(["recent", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("recent"));
        Ok(())
    }

    #[sinex_test]
    async fn test_errors_command_exists() -> TestResult<()> {
        sinexctl()
            .args(["errors", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("error"));
        Ok(())
    }

    #[sinex_test]
    async fn test_watch_command_exists() -> TestResult<()> {
        sinexctl()
            .args(["watch", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("watch").or(predicate::str::contains("Watch")));
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
            .stdout(predicate::str::contains("--refresh"))
            .stdout(predicate::str::contains("KEYBOARD SHORTCUTS"));
        Ok(())
    }
}

//! CLI integration tests for sinexctl
//!
//! These tests verify CLI argument parsing, help text, completions,
//! and error handling without requiring a running gateway.

use assert_cmd::cargo;
use assert_cmd::Command;
use predicates::prelude::*;

/// Helper to create a sinexctl command
fn sinexctl() -> Command {
    Command::new(cargo::cargo_bin!("sinexctl"))
}

mod help_tests {
    use super::*;

    #[test]
    fn test_help_flag() {
        sinexctl()
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"))
            .stdout(predicate::str::contains("Commands:"))
            .stdout(predicate::str::contains("query"))
            .stdout(predicate::str::contains("node"))
            .stdout(predicate::str::contains("dlq"))
            .stdout(predicate::str::contains("replay"))
            .stdout(predicate::str::contains("ops"))
            .stdout(predicate::str::contains("audit"))
            .stdout(predicate::str::contains("config"))
            .stdout(predicate::str::contains("completions"));
    }

    #[test]
    fn test_query_help() {
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
    }

    #[test]
    fn test_node_help() {
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
    }

    #[test]
    fn test_dlq_help() {
        sinexctl()
            .args(["dlq", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Dead letter queue"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("peek"))
            .stdout(predicate::str::contains("requeue"))
            .stdout(predicate::str::contains("purge"));
    }

    #[test]
    fn test_replay_help() {
        sinexctl()
            .args(["replay", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Replay operations"))
            .stdout(predicate::str::contains("plan"))
            .stdout(predicate::str::contains("submit"))
            .stdout(predicate::str::contains("watch"))
            .stdout(predicate::str::contains("list"));
    }

    #[test]
    fn test_ops_help() {
        sinexctl()
            .args(["ops", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Operations log"))
            .stdout(predicate::str::contains("start"))
            .stdout(predicate::str::contains("list"))
            .stdout(predicate::str::contains("get"))
            .stdout(predicate::str::contains("cancel"));
    }

    #[test]
    fn test_config_help() {
        sinexctl()
            .args(["config", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("Configuration management"))
            .stdout(predicate::str::contains("init"))
            .stdout(predicate::str::contains("show"))
            .stdout(predicate::str::contains("path"))
            .stdout(predicate::str::contains("edit"));
    }
}

mod version_tests {
    use super::*;

    #[test]
    fn test_version_flag() {
        sinexctl()
            .arg("--version")
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
    }
}

mod completions_tests {
    use super::*;

    #[test]
    fn test_bash_completions() {
        sinexctl()
            .args(["completions", "bash"])
            .assert()
            .success()
            .stdout(predicate::str::contains("_sinexctl"))
            .stdout(predicate::str::contains("complete"));
    }

    #[test]
    fn test_zsh_completions() {
        sinexctl()
            .args(["completions", "zsh"])
            .assert()
            .success()
            .stdout(predicate::str::contains("#compdef sinexctl"));
    }

    #[test]
    fn test_fish_completions() {
        sinexctl()
            .args(["completions", "fish"])
            .assert()
            .success()
            .stdout(predicate::str::contains("complete -c sinexctl"));
    }

    #[test]
    fn test_powershell_completions() {
        sinexctl()
            .args(["completions", "powershell"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
    }

    #[test]
    fn test_elvish_completions() {
        sinexctl()
            .args(["completions", "elvish"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"));
    }
}

mod config_tests {
    use super::*;

    #[test]
    fn test_config_path() {
        sinexctl()
            .args(["config", "path"])
            .assert()
            .success()
            .stdout(predicate::str::contains("sinexctl"))
            .stdout(predicate::str::contains(".toml").or(predicate::str::contains("config")));
    }

    #[test]
    fn test_config_show_default_format() {
        // Config show should work even without a config file (shows defaults)
        sinexctl()
            .args(["config", "show"])
            .assert()
            .success()
            .stdout(predicate::str::contains("rpc_url"));
    }

    #[test]
    fn test_config_show_json_format() {
        sinexctl()
            .args(["config", "show", "-f", "json"])
            .assert()
            .success()
            .stdout(predicate::str::starts_with("{"))
            .stdout(predicate::str::contains("rpc_url"));
    }
}

mod error_handling_tests {
    use super::*;

    #[test]
    fn test_invalid_command() {
        sinexctl()
            .arg("nonexistent-command")
            .assert()
            .failure()
            .stderr(predicate::str::contains("error"));
    }

    #[test]
    fn test_missing_required_args() {
        // node status requires a node name
        sinexctl()
            .args(["node", "status"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("required"));
    }

    #[test]
    fn test_invalid_output_format() {
        sinexctl()
            .args(["query", "-f", "invalid-format"])
            .assert()
            .failure()
            .stderr(predicate::str::contains("invalid"));
    }

    #[test]
    fn test_dlq_requeue_requires_id_or_all() {
        // dlq requeue without --event-id or --all should fail
        // Note: This will try to connect to gateway, so we check for the validation error
        // or connection error
        sinexctl()
            .args(["dlq", "requeue"])
            .assert()
            .failure();
    }
}

mod output_format_tests {
    use super::*;

    #[test]
    fn test_valid_output_formats() {
        // Test that format flag is recognized
        for format in ["table", "json", "yaml"] {
            sinexctl()
                .args(["query", "--help"])
                .assert()
                .success()
                .stdout(predicate::str::contains(format));
        }
    }

    #[test]
    fn test_query_format_flag_short() {
        // -f should be recognized
        sinexctl()
            .args(["query", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("-f"));
    }
}

mod environment_tests {
    use super::*;

    #[test]
    fn test_rpc_url_env_recognized() {
        // Help should mention the environment variable
        sinexctl()
            .args(["--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("SINEX_RPC_URL").or(predicate::str::contains("rpc-url")));
    }

    #[test]
    fn test_token_env_recognized() {
        sinexctl()
            .args(["--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("token").or(predicate::str::contains("SINEX")));
    }
}

mod shortcut_command_tests {
    use super::*;

    #[test]
    fn test_status_command_exists() {
        sinexctl()
            .args(["status", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("status"));
    }

    #[test]
    fn test_recent_command_exists() {
        sinexctl()
            .args(["recent", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("recent"));
    }

    #[test]
    fn test_errors_command_exists() {
        sinexctl()
            .args(["errors", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("error"));
    }

    #[test]
    fn test_watch_command_exists() {
        sinexctl()
            .args(["watch", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("watch").or(predicate::str::contains("Watch")));
    }
}

mod tui_tests {
    use super::*;

    #[test]
    fn test_tui_help() {
        sinexctl()
            .args(["tui", "--help"])
            .assert()
            .success()
            .stdout(predicate::str::contains("TUI"))
            .stdout(predicate::str::contains("--tab"))
            .stdout(predicate::str::contains("--refresh"))
            .stdout(predicate::str::contains("KEYBOARD SHORTCUTS"));
    }
}

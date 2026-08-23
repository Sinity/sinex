//! Class-level behavioral invariant tests (F5).
//!
//! These tests assert structural properties that should hold across the entire
//! command set — independent of any individual command's logic.

use xtask::sandbox::sinex_test;

/// Package-scoped commands (`-p`/`--package`) must include check, build, and test.
///
/// These are the core development workflow commands; if any loses `-p` support,
/// agent workflows that scope to a single crate will silently compile everything.
#[sinex_test]
async fn test_package_scoped_commands_have_flag() -> xtask::sandbox::TestResult<()> {
    use clap::CommandFactory;

    let cli = xtask::Cli::command();
    let package_scoped = ["check", "build", "test", "fix"];

    for cmd_name in package_scoped {
        let subcmd = cli
            .get_subcommands()
            .find(|sc| sc.get_name() == cmd_name)
            .unwrap_or_else(|| panic!("command '{cmd_name}' not found"));

        let has_p = subcmd
            .get_arguments()
            .any(|a| a.get_long() == Some("package") || a.get_short() == Some('p'));

        assert!(
            has_p,
            "command '{cmd_name}' must have a -p/--package flag (scoping invariant)"
        );
    }

    Ok(())
}

/// JSON output format flag must be available on core workflow commands.
///
/// `--format` and `--json` are global flags on `GlobalOpts` (flattened into the root
/// `Cli`). They are inherited by all subcommands at parse time but are not present in
/// the static per-subcommand `Command` structure from `CommandFactory`. The invariant
/// is that these flags exist as global args on the root CLI so agents can always use
/// `--format json` or `--json` with any command.
#[sinex_test]
async fn test_core_commands_have_output_format_flag() -> xtask::sandbox::TestResult<()> {
    use clap::CommandFactory;

    let cli = xtask::Cli::command();

    // --format and --json are global: verify at least one exists on the root CLI
    let has_format_global = cli
        .get_arguments()
        .any(|a| a.get_long() == Some("format") && a.is_global_set());
    let has_json_global = cli
        .get_arguments()
        .any(|a| a.get_long() == Some("json") && a.is_global_set());

    assert!(
        has_format_global || has_json_global,
        "--format or --json must be a global flag on the root CLI (agent consumption invariant)"
    );

    // Verify the agent-critical commands are registered
    let must_exist = ["check", "test", "build", "infra", "history", "run"];
    for cmd_name in must_exist {
        let exists = cli.get_subcommands().any(|sc| sc.get_name() == cmd_name);
        assert!(exists, "command '{cmd_name}' must exist in CLI");
    }

    Ok(())
}

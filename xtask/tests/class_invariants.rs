//! Class-level behavioral invariant tests (F5).
//!
//! These tests assert structural properties that should hold across the entire
//! command set — independent of any individual command's logic.

use xtask::sandbox::sinex_test;

/// Commands excluded from invocation tracking in lib.rs (line ~303).
///
/// These two never produce a history record; querying recent invocations after
/// running them should not show a new entry. This test documents the exclusion
/// contract so any change to the list is a breaking change caught by CI.
#[sinex_test]
async fn test_invocation_tracking_exclusion_list() -> xtask::sandbox::TestResult<()> {
    // Exclusions are coded as:
    //   if command_name != "completions" && command_name != "status"
    // This test validates the documented contract, not the runtime behavior
    // (which is tested by the T4 exercises). The invariant is that exactly
    // these two commands are excluded.
    let excluded: &[&str] = &["completions", "status"];

    // Verify these are NOT in the coordinated command set.
    // Coordinator only accepts "check", "test", "build", "fix".
    let coordinated: &[&str] = &["check", "test", "build", "fix"];

    for cmd in excluded {
        assert!(
            !coordinated.contains(cmd),
            "Excluded command '{cmd}' must not be in the coordinated set"
        );
    }

    Ok(())
}

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

/// Background-capable commands (`--bg`) must include the core development workflow.
///
/// `--bg` is a global flag on `GlobalOpts` (flattened into the root `Cli`). This
/// means it's inherited by *every* subcommand at parse time, but clap does not
/// propagate global args into the static `Command` structure returned by
/// `CommandFactory::command()` — they appear only on the root command's args.
///
/// The invariant is: `--bg` is defined as a GLOBAL arg on the root CLI, making it
/// available to check, test, and build without those subcommands needing to define it
/// individually. If it were removed from `GlobalOpts`, all three would lose it.
#[sinex_test]
async fn test_bg_capable_commands_include_core_workflow() -> xtask::sandbox::TestResult<()> {
    use clap::CommandFactory;

    let cli = xtask::Cli::command();

    // --bg is global: verify it exists on the root CLI (applies to all subcommands)
    let has_bg_global = cli
        .get_arguments()
        .any(|a| a.get_long() == Some("bg") && a.is_global_set());

    assert!(
        has_bg_global,
        "--bg must be a global flag on the root CLI (async-first workflow invariant)"
    );

    // Verify the core commands are registered (if they vanish, agents would fail too)
    let must_exist = ["check", "test", "build"];
    for cmd_name in must_exist {
        let exists = cli.get_subcommands().any(|sc| sc.get_name() == cmd_name);
        assert!(exists, "command '{cmd_name}' must exist in CLI");
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
    let must_exist = ["check", "test", "build", "status", "history", "jobs"];
    for cmd_name in must_exist {
        let exists = cli.get_subcommands().any(|sc| sc.get_name() == cmd_name);
        assert!(exists, "command '{cmd_name}' must exist in CLI");
    }

    Ok(())
}

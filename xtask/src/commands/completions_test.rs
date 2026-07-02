use super::*;
use crate::sandbox::sinex_test;
use std::os::unix::process::ExitStatusExt;

#[sinex_test]
async fn test_completions_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CompletionsCommand {
        subcommand: CompletionsSubcommand::Bash,
    };
    assert_eq!(cmd.name(), "completions");
    Ok(())
}

#[sinex_test]
async fn test_completions_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = CompletionsCommand {
        subcommand: CompletionsSubcommand::Zsh,
    };
    let metadata = cmd.metadata();

    assert_eq!(metadata.category, Some("utility"));
    assert!(!metadata.track_in_history);
    assert!(!metadata.modifies_state);
    Ok(())
}

#[sinex_test]
async fn test_all_subcommand_variants() -> ::xtask::sandbox::TestResult<()> {
    for sub in [
        CompletionsSubcommand::Bash,
        CompletionsSubcommand::Zsh,
        CompletionsSubcommand::Fish,
        CompletionsSubcommand::PowerShell,
        CompletionsSubcommand::ListPackages,
        CompletionsSubcommand::ListRunTargets,
    ] {
        let cmd = CompletionsCommand { subcommand: sub };
        assert_eq!(cmd.name(), "completions");
    }
    Ok(())
}

#[sinex_test]
async fn test_list_run_targets_non_empty() -> ::xtask::sandbox::TestResult<()> {
    let targets = crate::commands::run::list_run_targets();
    assert!(!targets.is_empty(), "run targets should not be empty");
    assert!(targets.contains(&"event_engine".to_string()));
    assert!(targets.contains(&"core".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_postprocess_zsh_packages() -> ::xtask::sandbox::TestResult<()> {
    let input = "':PACKAGES:_default'";
    let output = postprocess_zsh(input);
    assert!(
        output.contains("xtask completions list-packages"),
        "zsh post-processor should inject dynamic package completion"
    );
    assert!(
        !output.contains(":PACKAGES:_default"),
        "zsh post-processor should remove static fallback"
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_packages_from_metadata_output_reports_invalid_json()
-> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: br#"{"packages":"nope"}"#.to_vec(),
        stderr: Vec::new(),
    };

    let error = workspace_packages_from_metadata_output(&output)
        .expect_err("invalid cargo metadata JSON should surface");
    assert!(error.to_string().contains("packages array"));
    Ok(())
}

#[sinex_test]
async fn test_workspace_packages_from_metadata_output_reports_failed_status()
-> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(2 << 8),
        stdout: Vec::new(),
        stderr: b"metadata boom".to_vec(),
    };

    let error = workspace_packages_from_metadata_output(&output)
        .expect_err("cargo metadata failure should surface");
    assert!(error.to_string().contains("exit code 2"));
    assert!(error.to_string().contains("metadata boom"));
    Ok(())
}

#[sinex_test]
async fn test_workspace_packages_from_metadata_output_reports_missing_package_name()
-> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout: br#"{"packages":[{"version":"0.1.0"}]}"#.to_vec(),
        stderr: Vec::new(),
    };

    let error = workspace_packages_from_metadata_output(&output)
        .expect_err("metadata entries without names should surface");
    assert!(error.to_string().contains("package entry 0"));
    assert!(error.to_string().contains("name"));
    Ok(())
}

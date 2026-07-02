use super::*;
use crate::command::CommandContext;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;
use ::xtask::sandbox::EnvGuard;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Output;

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

fn write_executable_script(path: &std::path::Path, body: &str) -> ::xtask::sandbox::TestResult<()> {
    fs::write(path, body)?;
    let mut permissions = fs::metadata(path)?.permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

#[sinex_test]
async fn test_collect_changed_files_reports_git_failures() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("git"),
        r"#!/bin/sh
printf 'fatal: synthetic git failure\n' >&2
exit 128
",
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let error = collect_changed_files().expect_err("git failure should surface");
    assert!(error.to_string().contains("git diff --name-only HEAD"));
    assert!(error.to_string().contains("synthetic git failure"));
    Ok(())
}

#[sinex_test]
async fn test_collect_diagnostic_files_reports_unavailable_history_db()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let invalid_db_path = temp.path().join("history-db-dir");
    fs::create_dir(&invalid_db_path)?;
    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "snapshot",
        invalid_db_path,
    );

    let error = collect_diagnostic_files(&ctx).expect_err("history DB failure should surface");
    assert!(error.to_string().contains("history DB unavailable"));
    Ok(())
}

#[sinex_test]
async fn test_collect_changed_files_deduplicates_head_and_cached()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("git"),
        r#"#!/bin/sh
if [ "$1" = "diff" ] && [ "$2" = "--name-only" ] && [ "$3" = "HEAD" ]; then
  printf 'a.rs\nshared.rs\n'
  exit 0
fi
if [ "$1" = "diff" ] && [ "$2" = "--name-only" ] && [ "$3" = "--cached" ]; then
  printf 'b.rs\nshared.rs\n'
  exit 0
fi
printf 'unexpected git invocation: %s\n' "$*" >&2
exit 1
"#,
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    assert_eq!(
        collect_changed_files()?,
        vec!["a.rs".to_string(), "shared.rs".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn test_collect_crate_scope_reports_metadata_failures() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("cargo"),
        r"#!/bin/sh
printf 'cargo metadata exploded\n' >&2
exit 101
",
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let error = collect_crate_scope("sinex-db").expect_err("metadata failure should surface");
    assert!(error.to_string().contains("workspace package metadata"));
    assert!(error.to_string().contains("cargo metadata exploded"));
    Ok(())
}

#[sinex_test]
async fn test_collect_crate_scope_reports_unknown_workspace_package()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("cargo"),
        r#"#!/bin/sh
printf '%s\n' '{"packages":[{"id":"path+file:///realm/project/sinex/crate/sinex-db#0.1.0","name":"sinex-db","manifest_path":"/realm/project/sinex/crate/sinex-db/Cargo.toml","dependencies":[]}],"workspace_members":["path+file:///realm/project/sinex/crate/sinex-db#0.1.0"]}'
"#,
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let error =
        collect_crate_scope("missing-crate").expect_err("unknown workspace package should surface");
    assert!(
        error.to_string().contains("missing-crate"),
        "unexpected error: {error:?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_collect_crate_scope_reports_malformed_package_metadata()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("cargo"),
        r#"#!/bin/sh
printf '%s\n' '{"packages":[{"name":"sinex-db"}],"workspace_members":[]}'
"#,
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let error = collect_crate_scope("sinex-db").expect_err("malformed metadata should surface");
    let message = format!("{error:#}");
    assert!(message.contains("workspace package metadata"));
    assert!(message.contains("cargo metadata JSON"));
    Ok(())
}

#[sinex_test]
async fn test_collect_crate_scope_reports_manifest_outside_workspace()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let bin_dir = temp.path().join("bin");
    fs::create_dir_all(&bin_dir)?;
    write_executable_script(
        &bin_dir.join("cargo"),
        r#"#!/bin/sh
printf '%s\n' '{"packages":[{"id":"path+file:///tmp/outside#0.1.0","name":"sinex-db","manifest_path":"/tmp/outside/Cargo.toml","dependencies":[]}],"workspace_members":["path+file:///tmp/outside#0.1.0"]}'
"#,
    )?;

    let mut env = EnvGuard::new();
    env.set("PATH", bin_dir.display().to_string());

    let error = collect_crate_scope("sinex-db").expect_err("workspace path drift should surface");
    let message = error.to_string();
    assert!(message.contains("outside workspace root"));
    assert!(message.contains("/tmp/outside/Cargo.toml"));
    Ok(())
}

#[sinex_test]
async fn test_read_snapshot_output_reports_missing_file() -> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let missing = temp.path().join("missing.xml");
    let error = read_snapshot_output(&missing).expect_err("missing snapshot should error");
    assert!(error.to_string().contains("failed to read snapshot output"));
    assert!(error.to_string().contains("missing.xml"));
    Ok(())
}

#[sinex_test]
async fn test_probe_repomix_reports_probe_failures() -> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        let probe = probe_repomix(Ok(Output {
            status: std::process::ExitStatus::from_raw(512),
            stdout: Vec::new(),
            stderr: b"which exploded".to_vec(),
        }));
        assert_eq!(
            probe,
            RepomixProbe::ProbeFailed("which exploded".to_string())
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_probe_repomix_reports_missing_binary() -> ::xtask::sandbox::TestResult<()> {
    #[cfg(unix)]
    {
        let probe = probe_repomix(Ok(Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: Vec::new(),
        }));
        assert_eq!(probe, RepomixProbe::Missing);
    }
    Ok(())
}

#[sinex_test]
async fn test_push_context_field_includes_issue_line() -> ::xtask::sandbox::TestResult<()> {
    let mut lines = vec!["[xtask-context]".to_string()];
    push_context_field(
        &mut lines,
        "recent_runs",
        SnapshotContextField {
            value: "[]".to_string(),
            issue: Some("history unavailable".to_string()),
        },
    );

    assert_eq!(lines[1], "recent_runs: []");
    assert_eq!(lines[2], "recent_runs_issue: \"history unavailable\"");
    Ok(())
}

#[sinex_test]
async fn test_build_context_block_reports_unavailable_history_db()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let invalid_db_path = temp.path().join("history-db-dir");
    fs::create_dir(&invalid_db_path)?;
    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        false,
        None,
        "snapshot",
        invalid_db_path,
    );

    let block = build_context_block(&ctx);
    assert!(block.contains("recent_runs_issue:"));
    assert!(block.contains("active_diagnostics_issue:"));
    assert!(block.contains("active_jobs_issue:"));
    Ok(())
}

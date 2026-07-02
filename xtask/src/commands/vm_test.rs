use super::*;
use crate::command::CommandContext;
use crate::history::HistoryDb;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::sinex_test;
use tempfile::tempdir;

fn silent_ctx() -> CommandContext {
    CommandContext::new(OutputWriter::new(OutputFormat::Silent), false, None, "vm")
}

#[sinex_test]
async fn test_background_coordination_plan_for_validate_normalizes_to_single_lane()
-> ::xtask::sandbox::TestResult<()> {
    let command = VmCommand {
        subcommand: VmSubcommand::Test {
            category: Some("integration".to_string()),
            timeout: DEFAULT_TIMEOUT_SECS,
            keep_failed: false,
            list: false,
            validate: true,
            tests: vec!["runtime-matrix".to_string()],
        },
    };

    let (spawn_args, coordination_args) = command
        .background_coordination_plan()
        .expect("validate should coordinate in background");

    assert_eq!(
        spawn_args,
        vec!["test".to_string(), "--validate".to_string()]
    );
    assert_eq!(
        coordination_args,
        vec!["--scope=vm:validate:all-scenarios".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn test_background_coordination_plan_for_run_tracks_selected_tests()
-> ::xtask::sandbox::TestResult<()> {
    let command = VmCommand {
        subcommand: VmSubcommand::Test {
            category: None,
            timeout: 1337,
            keep_failed: true,
            list: false,
            validate: false,
            tests: vec!["replay-smoke".to_string(), "basic".to_string()],
        },
    };

    let (spawn_args, coordination_args) = command
        .background_coordination_plan()
        .expect("vm runs should coordinate in background");

    assert_eq!(
        spawn_args,
        vec![
            "test".to_string(),
            "--timeout=1337".to_string(),
            "--keep-failed".to_string(),
            "--".to_string(),
            "replay-smoke".to_string(),
            "basic".to_string(),
        ]
    );
    assert_eq!(
        coordination_args,
        vec!["--scope=vm:run:tests:basic,replay-smoke:timeout=1337:keep_failed=1".to_string()]
    );
    Ok(())
}

fn git(root: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new("git").args(args).current_dir(root).output()?;
    if output.status.success() {
        return Ok(());
    }

    bail!(
        "git {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    );
}

fn init_git_repo(root: &Path) -> Result<()> {
    git(root, &["init"])?;
    git(root, &["config", "user.email", "xtask-vm-test@example.com"])?;
    git(root, &["config", "user.name", "xtask vm test"])?;
    std::fs::write(root.join("flake.nix"), "{ outputs = _: {}; }\n")?;
    git(root, &["add", "flake.nix"])?;
    git(root, &["commit", "-m", "init"])?;
    Ok(())
}

#[sinex_test]
async fn test_prepare_vm_flake_input_uses_workspace_flake_for_clean_checkout()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    init_git_repo(temp.path())?;

    let input = prepare_vm_flake_input(temp.path())?;
    assert_eq!(input.flake_ref(), ".");
    assert!(input.stage_report().is_none());
    Ok(())
}

#[sinex_test]
async fn test_prepare_vm_flake_input_stages_dirty_checkout_with_untracked_workspace_files()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    init_git_repo(temp.path())?;

    let new_crate = temp.path().join("crate/sinexd-extra/Cargo.toml");
    std::fs::create_dir_all(new_crate.parent().expect("new crate parent"))?;
    std::fs::write(&new_crate, "[package]\nname = \"sinexd-extra\"\n")?;

    let mut input = prepare_vm_flake_input(temp.path())?;
    let staged_root = input
        .stage_report()
        .expect("dirty checkout should use staged flake input")
        .staged_root
        .clone();
    assert!(input.flake_ref().starts_with("path:"));
    assert!(
        Path::new(&staged_root)
            .join("crate/sinexd-extra/Cargo.toml")
            .is_file(),
        "staged checkout should include untracked crate files"
    );

    input.preserve_stage();
    std::fs::remove_dir_all(staged_root)?;
    Ok(())
}

#[sinex_test]
async fn test_validate_rejects_run_only_flags() -> ::xtask::sandbox::TestResult<()> {
    let error = execute_test(
        Some("smoke"),
        DEFAULT_TIMEOUT_SECS,
        true,
        false,
        true,
        &["basic".to_string()],
        &silent_ctx(),
    )
    .await
    .expect_err("validate should reject run-only selection flags");

    let message = format!("{error:#}");
    assert!(message.contains("does not accept category selection"));
    Ok(())
}

#[sinex_test]
async fn test_discover_vm_test_files_reports_scenarios_dir_failures()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let scenarios_dir = temp.path().join("tests/e2e/nixos-vm/test-scenarios");
    std::fs::create_dir_all(scenarios_dir.parent().unwrap())?;
    std::fs::write(&scenarios_dir, "not a directory")?;

    let error = discover_vm_test_files(temp.path()).unwrap_err();
    assert!(format!("{error:#}").contains("failed to read VM scenarios directory"));
    Ok(())
}

#[sinex_test]
async fn test_discover_vm_test_files_includes_preflight_and_sorted_scenarios()
-> ::xtask::sandbox::TestResult<()> {
    let temp = tempfile::tempdir()?;
    let vm_root = temp.path().join("tests/e2e/nixos-vm");
    let scenarios_dir = vm_root.join("test-scenarios");
    std::fs::create_dir_all(&scenarios_dir)?;
    std::fs::write(vm_root.join("preflight_deployment_test.nix"), "")?;
    std::fs::write(scenarios_dir.join("b-test.nix"), "")?;
    std::fs::write(scenarios_dir.join("a-test.nix"), "")?;
    std::fs::write(scenarios_dir.join("notes.txt"), "")?;

    let files = discover_vm_test_files(temp.path())?;
    let labels: Vec<_> = files
        .iter()
        .map(|path| {
            path.strip_prefix(temp.path())
                .unwrap()
                .to_string_lossy()
                .to_string()
        })
        .collect();

    assert_eq!(
        labels,
        vec![
            "tests/e2e/nixos-vm/preflight_deployment_test.nix",
            "tests/e2e/nixos-vm/test-scenarios/a-test.nix",
            "tests/e2e/nixos-vm/test-scenarios/b-test.nix",
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_display_vm_test_label_falls_back_to_full_path() -> ::xtask::sandbox::TestResult<()> {
    let root = Path::new("/");
    assert_eq!(display_vm_test_label(root), root.display().to_string());
    Ok(())
}

#[sinex_test]
async fn test_append_stream_task_output_surfaces_join_failures() -> ::xtask::sandbox::TestResult<()>
{
    let mut combined_output = String::new();
    let handle = tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        Result::<String>::Ok(String::from("unreachable"))
    });
    handle.abort();

    append_stream_task_output(&mut combined_output, "stdout", handle.await);

    assert!(combined_output.contains("Failed to collect VM stdout output"));
    assert!(combined_output.contains("cancelled"));
    Ok(())
}

#[sinex_test]
async fn test_collect_vm_stream_output_collects_utf8_lines() -> ::xtask::sandbox::TestResult<()> {
    use tokio::io::AsyncWriteExt;

    let (reader, mut writer) = tokio::io::duplex(64);
    writer.write_all(b"alpha\nbeta\n").await?;
    drop(writer);

    let output = collect_vm_stream_output(Some(BufReader::new(reader)), "stdout", None).await?;
    assert_eq!(output, "alpha\nbeta\n");
    Ok(())
}

#[sinex_test]
async fn test_collect_vm_stream_output_surfaces_invalid_utf8() -> ::xtask::sandbox::TestResult<()> {
    use tokio::io::AsyncWriteExt;

    let (reader, mut writer) = tokio::io::duplex(64);
    writer.write_all(&[0xff, b'\n']).await?;
    drop(writer);

    let error = collect_vm_stream_output(Some(BufReader::new(reader)), "stderr", None)
        .await
        .expect_err("invalid utf8 should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read VM stderr output"));
    assert!(message.contains("valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn test_strip_ansi_escape_sequences_preserves_utf8_text() -> ::xtask::sandbox::TestResult<()>
{
    let input = "\u{1b}[31mVerification FAILED\u{1b}[0m – żółć";
    assert_eq!(
        strip_ansi_escape_sequences(input),
        "Verification FAILED – żółć"
    );
    Ok(())
}

#[sinex_test]
async fn test_classify_vm_progress_line_extracts_subtest_from_prefixed_output()
-> ::xtask::sandbox::TestResult<()> {
    let line = "vm-test > subtest: source-material replay stays ordered";
    assert_eq!(
        classify_vm_progress_line(line),
        Some("Subtest: source-material replay stays ordered".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn test_classify_vm_progress_line_strips_ansi_failure_prefixes()
-> ::xtask::sandbox::TestResult<()> {
    let line = "\u{1b}[31mvm-test > RequestedAssertionFailed: browser evidence missing\u{1b}[0m";
    assert_eq!(
        classify_vm_progress_line(line),
        Some("RequestedAssertionFailed: browser evidence missing".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn test_classify_vm_progress_line_summarizes_vm_outcome_report()
-> ::xtask::sandbox::TestResult<()> {
    let line = r#"vm-test > VM_OUTCOME_SUMMARY {"evidence_missing":1,"failed":0,"inconclusive":2,"items":[],"passed":3,"skipped":4,"total":10}"#;
    assert_eq!(
        classify_vm_progress_line(line),
        Some(
            "VM outcome summary: 3/10 passed, 4 skipped, 2 inconclusive, 1 evidence-missing, 0 failed"
                .to_string()
        )
    );
    Ok(())
}

#[sinex_test]
async fn test_classify_vm_progress_line_ignores_nix_build_noise() -> ::xtask::sandbox::TestResult<()>
{
    let line = "building '/nix/store/abc123-sinex-vm-basic.drv'...";
    assert_eq!(classify_vm_progress_line(line), None);
    Ok(())
}

#[sinex_test]
async fn test_update_vm_progress_summary_records_terminal_summary()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("vm", None, None, None)?;

    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Silent),
        true,
        Some(invocation_id),
        "vm",
        db_path.clone(),
    );

    update_vm_progress_summary(
        &ctx,
        "basic",
        25.0,
        1,
        4,
        "RequestedAssertionFailed: browser evidence missing",
    );

    let stored = HistoryDb::open_query(&db_path)?
        .get_progress(invocation_id)?
        .expect("vm progress should be stored");

    assert_eq!(stored.phase.as_deref(), Some("vm-test"));
    assert_eq!(stored.pct_done, Some(25.0));
    assert_eq!(stored.items_done, Some(1));
    assert_eq!(stored.items_total, Some(4));
    assert_eq!(
        stored.terminal_summary.as_deref(),
        Some("basic: RequestedAssertionFailed: browser evidence missing")
    );
    Ok(())
}

#[sinex_test]
async fn test_append_stream_task_output_surfaces_stream_errors() -> ::xtask::sandbox::TestResult<()>
{
    let mut combined_output = String::new();
    append_stream_task_output(
        &mut combined_output,
        "stderr",
        Ok(Err(color_eyre::eyre::eyre!("stream exploded"))),
    );

    assert!(combined_output.contains("stream exploded"));
    Ok(())
}

#[sinex_test]
async fn test_terminate_vm_test_process_tree_kills_child_process_group()
-> ::xtask::sandbox::TestResult<()> {
    use std::os::unix::process::ExitStatusExt;

    let mut command = tokio::process::Command::new("sh");
    command.args(["-c", "sleep 30 & echo $!; wait"]);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::null());
    configure_process_group_leader(&mut command);

    let mut child = command.spawn()?;
    let stdout = child.stdout.take().expect("stdout should be piped");
    let mut lines = BufReader::new(stdout).lines();
    let sleep_pid = lines
        .next_line()
        .await?
        .expect("shell should print background child pid")
        .parse::<i32>()?;

    terminate_vm_test_process_tree(&mut child).await?;

    assert!(
        child.try_wait()?.is_some(),
        "terminated VM helper child should be reaped"
    );
    assert_ne!(
        unsafe { libc::kill(sleep_pid, 0) },
        0,
        "background process in the VM helper group should be gone"
    );

    let status = child.wait().await?;
    assert!(
        status.signal().is_some() || !status.success(),
        "terminated child should not report clean success"
    );
    Ok(())
}

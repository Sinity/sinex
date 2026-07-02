use super::*;
use crate::sandbox::sinex_test;
use clap::Parser;
use tempfile::tempdir;

#[sinex_test]
async fn test_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::List {
            limit: 10,
            active: false,
        },
    };
    assert_eq!(cmd.name(), "jobs");
    Ok(())
}

#[sinex_test]
async fn test_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::Prune { older_than: 7 },
    };
    let metadata = cmd.metadata();
    assert!(metadata.modifies_state);
    assert!(!metadata.track_in_history);
    assert_eq!(
        metadata.history_access,
        crate::command::HistoryAccessMode::ReadWrite
    );
    Ok(())
}

#[sinex_test]
async fn test_observational_jobs_metadata_uses_query_history()
-> ::xtask::sandbox::TestResult<()> {
    let cmd = JobsCommand {
        subcommand: JobsSubcommand::Status {
            id: 42,
            follow: false,
        },
    };
    let metadata = cmd.metadata();
    assert!(!metadata.modifies_state);
    assert!(!metadata.track_in_history);
    assert_eq!(
        metadata.history_access,
        crate::command::HistoryAccessMode::Query
    );
    Ok(())
}

#[sinex_test]
async fn test_truncate_str() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(truncate_str("short", 10), "short");
    assert_eq!(truncate_str("verylongstring", 10), "verylon...");
    assert_eq!(truncate_str("exactly10!", 10), "exactly10!");
    Ok(())
}

#[sinex_test]
async fn test_status_to_str() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(status_to_str(JobLifecycleStatus::Running), "running");
    assert_eq!(status_to_str(JobLifecycleStatus::Completed), "completed");
    assert_eq!(status_to_str(JobLifecycleStatus::Failed), "failed");
    assert_eq!(status_to_str(JobLifecycleStatus::Orphaned), "orphaned");
    assert_eq!(status_to_str(JobLifecycleStatus::Killed), "killed");
    Ok(())
}

#[sinex_test]
async fn jobs_output_accepts_explicit_stdout_selector() -> ::xtask::sandbox::TestResult<()> {
    let cli = crate::Cli::try_parse_from(["xtask", "jobs", "output", "42", "--stdout"])?;
    let Some(crate::Commands::Jobs(JobsCommand {
        subcommand: JobsSubcommand::Output { id, stdout, stderr },
    })) = cli.command
    else {
        panic!("expected jobs output command");
    };

    assert_eq!(id, 42);
    assert!(stdout);
    assert!(!stderr);
    Ok(())
}

#[sinex_test]
async fn jobs_output_rejects_conflicting_stream_selectors() -> ::xtask::sandbox::TestResult<()>
{
    let Err(error) =
        crate::Cli::try_parse_from(["xtask", "jobs", "output", "42", "--stdout", "--stderr"])
    else {
        panic!("stdout and stderr selectors should conflict")
    };

    assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    Ok(())
}

// ── Log handoff tests (#1139) ─────────────────────────────────────────

/// Simulate the follow loop's file→DB handoff and verify exact output
/// preservation: no dropped lines, no duplicated lines, correct order.
#[sinex_test]
async fn test_follow_log_handoff_preserves_output() -> ::xtask::sandbox::TestResult<()> {
    // Build numbered-line content: "1\n2\n...100\n"
    let lines: Vec<String> = (1..=100).map(|i| i.to_string()).collect();
    let content = lines.join("\n") + "\n";
    let content_bytes = &content;

    // Phase 1: write to temp file (simulates running job's stdout.log)
    let dir = tempdir()?;
    let file_path = dir.path().join("stdout.log");
    std::fs::write(&file_path, content_bytes)?;

    // Phase 2: read first half from file (simulates early follow iterations)
    let (first_half, new_pos) =
        read_stdout_delta_from_file(&file_path, 0)?.expect("file should exist and be readable");
    // Read the rest from the last consumed byte.
    let (second_half, new_pos) =
        read_stdout_delta_from_file(&file_path, new_pos)?.expect("file should still exist");

    // Phase 3: simulate archiving — delete the file, read remainder from
    // a buffer (standing in for the DB-archived content).
    std::fs::remove_file(&file_path)?;
    let remainder = &content_bytes[new_pos as usize..];
    let combined = format!("{first_half}{second_half}{remainder}");

    // Phase 4: verify exact match — order, no drops, no duplicates.
    let original_lines: Vec<&str> = content.lines().collect();
    let combined_lines: Vec<&str> = combined.lines().collect();

    assert_eq!(
        original_lines,
        combined_lines,
        "log handoff must preserve exact line sequence: \
         {} original lines, {} combined lines. \
         Gaps/duplications indicate a handoff bug.",
        original_lines.len(),
        combined_lines.len()
    );

    // Also verify the delta-read semantics: new_pos advances correctly.
    assert!(
        new_pos > 0 && new_pos <= content_bytes.len() as u64,
        "new_pos {new_pos} out of range for content of {} bytes",
        content_bytes.len()
    );

    Ok(())
}

/// Verify the handoff detects when file content grows between delta-read
/// and the terminal-state DB read.
#[sinex_test]
async fn test_follow_handoff_catches_growth_before_archive() -> ::xtask::sandbox::TestResult<()>
{
    let dir = tempdir()?;
    let file_path = dir.path().join("stdout.log");

    // Write initial content
    let initial = "line1\nline2\n";
    std::fs::write(&file_path, initial)?;

    // Read position 0..end (gets "line1\nline2\n")
    let (buf1, pos1) = read_stdout_delta_from_file(&file_path, 0)?.expect("file should exist");

    // Append more content (simulating the job writing more between reads)
    std::fs::write(&file_path, format!("{initial}line3\nline4\n"))?;

    // Read from pos1 (gets "line3\nline4\n")
    let (buf2, pos2) =
        read_stdout_delta_from_file(&file_path, pos1)?.expect("file should exist");

    // Combined output should have all 4 lines in order
    let combined = format!("{buf1}{buf2}");
    let combined_lines: Vec<&str> = combined.lines().collect();
    assert_eq!(
        combined_lines,
        vec!["line1", "line2", "line3", "line4"],
        "interleaved writes between follow reads must be captured in order"
    );
    assert!(pos2 > pos1, "position must advance after new content");

    Ok(())
}

#[sinex_test]
async fn test_read_stdout_delta_from_file_reports_io_failures()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let error = read_stdout_delta_from_file(dir.path(), 0).unwrap_err();
    let message = format!("{error:#}");
    assert!(message.contains(dir.path().to_string_lossy().as_ref()));
    assert!(
        message.contains("failed to open job stdout log")
            || message.contains("failed to seek job stdout log")
            || message.contains("failed to read job stdout log")
    );
    Ok(())
}

#[sinex_test]
async fn test_read_stdout_delta_from_file_reads_new_bytes_only()
-> ::xtask::sandbox::TestResult<()> {
    let dir = tempdir()?;
    let path = dir.path().join("stdout.log");
    std::fs::write(&path, "first\nsecond\n")?;

    let first = read_stdout_delta_from_file(&path, 0)?;
    assert_eq!(first, Some(("first\nsecond\n".to_string(), 13)));

    let second = read_stdout_delta_from_file(&path, 13)?;
    assert_eq!(second, Some((String::new(), 13)));
    Ok(())
}

#[sinex_test]
async fn test_progress_probe_from_result_reports_history_errors()
-> ::xtask::sandbox::TestResult<()> {
    let probe = progress_probe_from_result(42, Some(Err(eyre!("boom"))));
    assert!(probe.progress.is_none());
    assert!(probe.issue.unwrap_or_default().contains("boom"));
    Ok(())
}

#[sinex_test]
async fn test_progress_brief_prefers_terminal_summary() -> ::xtask::sandbox::TestResult<()> {
    let progress = InvocationProgress {
        invocation_id: 42,
        phase: Some("vm-test".to_string()),
        step: Some("subtest: ignored".to_string()),
        pct_done: Some(25.0),
        items_done: Some(1),
        items_total: Some(4),
        updated_at: "2026-04-23T00:00:00Z".to_string(),
        mode: Some("indeterminate".to_string()),
        unit_kind: None,
        rate_per_sec: None,
        eta_confidence: Some("none".to_string()),
        terminal_summary: Some(
            "basic: RequestedAssertionFailed: browser evidence missing".to_string(),
        ),
    };

    assert_eq!(
        progress_brief(Some(&progress)),
        "basic: RequestedAssertionFailed: browser evidence missing"
    );
    Ok(())
}

#[sinex_test]
async fn test_progress_probe_from_result_reports_unavailable_history()
-> ::xtask::sandbox::TestResult<()> {
    let probe = progress_probe_from_result(42, None);
    assert!(probe.progress.is_none());
    assert!(
        probe
            .issue
            .unwrap_or_default()
            .contains("history DB unavailable")
    );
    Ok(())
}

#[sinex_test]
async fn test_stage_timings_probe_from_result_reports_history_errors()
-> ::xtask::sandbox::TestResult<()> {
    let probe = stage_timings_probe_from_result(42, Some(Err(eyre!("stages boom"))));
    assert!(probe.stages.is_empty());
    assert!(probe.issue.unwrap_or_default().contains("stages boom"));
    Ok(())
}

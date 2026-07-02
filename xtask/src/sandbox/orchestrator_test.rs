use super::*;
use std::fs;
use std::time::Duration;
use tokio::process::Command;

fn write_file_at(path: &std::path::Path, content: &str, modified_at: SystemTime) -> TestResult<()> {
    fs::write(path, content)?;
    let file = fs::OpenOptions::new().write(true).open(path)?;
    file.set_times(std::fs::FileTimes::new().set_modified(modified_at))?;
    Ok(())
}

#[sinex_test]
async fn source_driver_host_binary_path_uses_runtime_target_dir() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let path = source_driver_host_binary_path(tempdir.path());

    assert!(path.ends_with("sinexd"));
    assert!(path.starts_with(crate::orchestrator::get_target_dir(tempdir.path())));
    Ok(())
}

#[sinex_test]
async fn source_driver_debug_log_path_includes_sanitized_unit() -> TestResult<()> {
    let path = source_driver_debug_log_path_for_test_process("browser.history:test");
    let rendered = path.display().to_string();

    assert!(rendered.contains("browser.history_test"));
    assert!(!rendered.contains(':'));
    Ok(())
}

#[sinex_test]
async fn runtime_binary_freshness_reports_missing_binary() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let report = runtime_binary_freshness_from_inputs(
        "sinexd",
        "sinexd",
        tempdir.path().join("target/debug/sinexd"),
        &[],
        "xtask build -p sinexd".to_string(),
    )?;

    assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Missing);
    let message = report.error_message();
    assert!(message.contains("sinexd binary not found"));
    assert!(message.contains("xtask build -p sinexd"));
    Ok(())
}

#[sinex_test]
async fn runtime_binary_freshness_reports_stale_binary() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let binary = tempdir.path().join("sinexd");
    let source = tempdir.path().join("src.rs");
    write_file_at(&binary, "binary", UNIX_EPOCH + Duration::from_secs(1_000))?;
    write_file_at(&source, "source", UNIX_EPOCH + Duration::from_secs(2_000))?;

    let report = runtime_binary_freshness_from_inputs(
        "sinexd",
        "sinexd",
        binary,
        std::slice::from_ref(&source),
        "xtask build -p sinexd".to_string(),
    )?;

    assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Stale);
    assert_eq!(report.newest_input_path.as_deref(), Some(source.as_path()));
    let message = report.error_message();
    assert!(message.contains("is stale"));
    assert!(message.contains(source.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn runtime_binary_freshness_accepts_newer_binary() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let binary = tempdir.path().join("sinexd");
    let source = tempdir.path().join("src.rs");
    write_file_at(&source, "source", UNIX_EPOCH + Duration::from_secs(1_000))?;
    write_file_at(&binary, "binary", UNIX_EPOCH + Duration::from_secs(2_000))?;

    let report = runtime_binary_freshness_from_inputs(
        "sinexd",
        "sinexd",
        binary,
        &[source],
        "xtask build -p sinexd".to_string(),
    )?;

    assert_eq!(report.status, RuntimeBinaryFreshnessStatus::Fresh);
    report.ensure_fresh()?;
    Ok(())
}

#[sinex_test]
async fn runtime_binary_inputs_exclude_dev_only_xtask_sources() -> TestResult<()> {
    let workspace = find_workspace_root()?;
    let inputs = collect_runtime_binary_input_paths(&workspace, "sinexd")?;
    let sinexd_main = workspace.join("crate/sinexd/src/main.rs");

    assert!(
        inputs.iter().any(|path| path == &sinexd_main),
        "runtime binary inputs should include the target binary source"
    );
    assert!(
        inputs.iter().all(|path| {
            let relative = path.strip_prefix(&workspace).unwrap_or(path);
            !relative.starts_with("xtask/src")
        }),
        "runtime binary inputs must not include xtask dev-dependency sources: {inputs:#?}"
    );
    Ok(())
}

/// Regression: workspace Cargo.toml and Cargo.lock must not appear in the
/// runtime-binary input set. Touching either (cargo update for an
/// unrelated package, adding a workspace member) used to mark every
/// runtime binary stale and trigger a full pre-test rebuild. See #1220.
#[sinex_test]
async fn runtime_binary_inputs_exclude_workspace_manifest_and_lockfile() -> TestResult<()> {
    let workspace = find_workspace_root()?;
    let inputs = collect_runtime_binary_input_paths(&workspace, "sinexd")?;
    let workspace_manifest = workspace.join("Cargo.toml");
    let lockfile = workspace.join("Cargo.lock");

    assert!(
        !inputs.iter().any(|path| path == &workspace_manifest),
        "runtime binary inputs must NOT include workspace Cargo.toml; \
         edits there (members/shared deps) over-invalidate every runtime binary (#1220)"
    );
    assert!(
        !inputs.iter().any(|path| path == &lockfile),
        "runtime binary inputs must NOT include workspace Cargo.lock; \
         `cargo build -p <other>` bumps lockfile mtime and falsely marks \
         this binary stale (#1220). cargo's own incremental compile remains \
         the safety net for real dep-graph changes"
    );
    Ok(())
}

/// Regression: `#[cfg(test)]` source modules in `src/**/tests/` directories
/// must not appear in the runtime-binary input set. Editing them does not
/// cause Cargo to relink the binary, so marking it stale would cause
/// `xtask test` to do a spurious pre-test rebuild on every test-only edit.
#[sinex_test]
async fn runtime_binary_inputs_exclude_test_only_source_modules() -> TestResult<()> {
    let workspace = find_workspace_root()?;
    let inputs = collect_runtime_binary_input_paths(&workspace, "sinexd")?;
    let test_module = workspace.join("crate/sinexd/src/runtime/automaton/adapter/tests/mod.rs");
    let sibling_test_module = workspace.join("crate/sinexd/src/api/handlers/sources_test.rs");

    assert!(
        !inputs.iter().any(|path| path == &test_module),
        "runtime binary inputs must not include #[cfg(test)] source modules; \
         editing them does not relink the runtime binary and would falsely \
         leave tests blocked on a stale-binary guard"
    );
    assert!(
        !inputs.iter().any(|path| path == &sibling_test_module),
        "runtime binary inputs must not include *_test.rs sibling modules; \
         this is the preferred split-test layout and does not relink sinexd"
    );
    Ok(())
}

#[sinex_test]
async fn captured_output_stdout_json_lines_surfaces_invalid_json() -> TestResult<()> {
    let output = CapturedOutput {
        stdout: "{\"ok\":true}\nnot-json\n".to_string(),
        stderr: String::new(),
        exit_code: 0,
    };

    let error = output
        .stdout_json_lines()
        .expect_err("invalid JSON line should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to parse stdout JSON line 2"));
    Ok(())
}

#[sinex_test]
async fn captured_output_stderr_json_lines_rejects_non_object_values() -> TestResult<()> {
    let output = CapturedOutput {
        stdout: String::new(),
        stderr: "[]\n".to_string(),
        exit_code: 0,
    };

    let error = output
        .stderr_json_lines()
        .expect_err("non-object JSON line should surface");
    let message = format!("{error:#}");
    assert!(message.contains("stderr JSON line 1 is not an object"));
    Ok(())
}

#[sinex_test]
async fn find_workspace_root_from_surfaces_unreadable_manifest() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let workspace_root = tempdir.path().join("workspace");
    std::fs::create_dir_all(&workspace_root)?;
    std::fs::create_dir(workspace_root.join("Cargo.toml"))?;

    let error = find_workspace_root_from(workspace_root.clone())
        .expect_err("directory manifest should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read workspace candidate manifest"));
    assert!(
        message.contains(
            workspace_root
                .join("Cargo.toml")
                .display()
                .to_string()
                .as_str()
        )
    );
    Ok(())
}

#[sinex_test]
async fn read_event_engine_debug_log_reports_missing_file() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let error = read_event_engine_debug_log(&tempdir.path().join("missing.log")).unwrap_err();
    assert!(format!("{error:#}").contains("failed to read event_engine debug log"));
    Ok(())
}

#[sinex_test]
async fn read_event_engine_debug_log_treats_empty_file_as_empty() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let debug_log = tempdir.path().join("event_engine.log");
    fs::write(&debug_log, "")?;
    assert!(read_event_engine_debug_log(&debug_log)?.is_none());
    Ok(())
}

#[sinex_test]
async fn read_event_engine_debug_log_preserves_non_empty_content() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let debug_log = tempdir.path().join("event_engine.log");
    fs::write(&debug_log, "line one\nline two\n")?;
    assert_eq!(
        read_event_engine_debug_log(&debug_log)?,
        Some("line one\nline two\n".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn format_event_engine_debug_context_includes_path_size_and_content() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let debug_log = tempdir.path().join("event_engine.log");
    fs::write(&debug_log, "startup failed\nmissing stream\n")?;
    let context = format_event_engine_debug_context(&debug_log);

    assert!(context.contains(debug_log.display().to_string().as_str()));
    assert!(context.contains("(30 bytes)"));
    assert!(context.contains("startup failed\nmissing stream\n"));
    Ok(())
}

#[sinex_test]
async fn format_event_engine_debug_context_uses_tail_for_long_logs() -> TestResult<()> {
    let tempdir = tempfile::tempdir()?;
    let debug_log = tempdir.path().join("event_engine.log");
    let content = format!("{}\nFINAL ROOT CAUSE\n", "startup chatter\n".repeat(400));
    fs::write(&debug_log, &content)?;
    let context = format_event_engine_debug_context(&debug_log);

    assert!(context.contains(debug_log.display().to_string().as_str()));
    assert!(context.contains("trailing excerpt"));
    assert!(context.contains("FINAL ROOT CAUSE"));
    assert!(
        !context.contains("startup chatte\n"),
        "excerpt should start on a line boundary: {context}"
    );
    assert!(!context.contains(content.as_str()));
    Ok(())
}

#[sinex_test]
async fn terminate_test_child_accepts_exited_process() -> TestResult<()> {
    let mut child = Command::new("true").spawn()?;
    child.wait().await?;
    terminate_test_child(&mut child, "unit-test child").await?;
    Ok(())
}

#[sinex_test]
async fn terminate_test_child_kills_running_process() -> TestResult<()> {
    let mut child = Command::new("sleep").arg("30").spawn()?;
    terminate_test_child(&mut child, "unit-test child").await?;
    Ok(())
}

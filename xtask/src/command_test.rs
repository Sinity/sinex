use super::*;
use crate::sandbox::sinex_test;

struct TestCommand {
    should_fail: bool,
}

impl XtaskCommand for TestCommand {
    fn name(&self) -> &'static str {
        "test-command"
    }

    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        if self.should_fail {
            Ok(CommandResult::failure(StructuredError {
                code: "TEST_ERROR".to_string(),
                message: "Test failure".to_string(),
                location: None,
                suggestion: None,
            }))
        } else {
            Ok(CommandResult::success().with_message("Test passed"))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

#[sinex_test]
async fn test_command_success() -> TestResult<()> {
    let cmd = TestCommand { should_fail: false };
    let ctx = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Silent),
        false,
        None,
        "test",
    );
    let result = cmd.execute(&ctx).await.expect("should not error");

    assert!(result.is_success());
    assert_eq!(result.message, Some("Test passed".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_command_failure() -> TestResult<()> {
    let cmd = TestCommand { should_fail: true };
    let ctx = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Silent),
        false,
        None,
        "test",
    );
    let result = cmd.execute(&ctx).await.expect("should not error");

    assert!(result.is_failure());
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "TEST_ERROR");
    Ok(())
}

#[sinex_test]
async fn test_command_metadata() -> TestResult<()> {
    let cmd = TestCommand { should_fail: false };
    let metadata = cmd.metadata();

    assert_eq!(metadata.category, Some("check"));
    assert!(metadata.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_command_result_builder() -> TestResult<()> {
    let result = CommandResult::success()
        .with_message("All checks passed")
        .with_details(vec!["Check 1", "Check 2"])
        .with_warnings(vec!["Warning 1"]);

    assert!(result.is_success());
    assert_eq!(result.message, Some("All checks passed".to_string()));
    assert_eq!(result.details.len(), 2);
    assert_eq!(result.warnings.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_command_result_partial() -> TestResult<()> {
    let result = CommandResult::partial()
        .with_message("Some checks failed")
        .with_detail("Completed: 3/5");

    assert_eq!(result.status, Status::Partial);
    assert_eq!(result.details.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_error() -> TestResult<()> {
    let result = CommandResult::success().with_error(StructuredError {
        code: "ERR001".to_string(),
        message: "Test error".to_string(),
        location: None,
        suggestion: None,
    });

    assert!(result.is_failure());
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].code, "ERR001");
    Ok(())
}

#[sinex_test]
async fn test_command_result_duration() -> TestResult<()> {
    let duration = std::time::Duration::from_secs(5);
    let result = CommandResult::success().with_duration(duration);

    assert_eq!(result.duration_secs, Some(5.0));
    Ok(())
}

#[sinex_test]
async fn test_resolve_coordination_fingerprint_uses_scope_key() -> TestResult<()> {
    let args = vec!["-p".to_string(), "xtask".to_string()];
    let (fingerprint, scope) = CommandContext::resolve_coordination_fingerprint(
        "check",
        &args,
        Ok("tree-fingerprint".to_string()),
    )?;

    assert_eq!(fingerprint, "tree-fingerprint");
    assert_eq!(scope, crate::coordinator::compute_scope_key("check", &args));
    Ok(())
}

#[sinex_test]
async fn test_resolve_coordination_fingerprint_propagates_errors() -> TestResult<()> {
    let args = vec!["-p".to_string(), "xtask".to_string()];
    let error = CommandContext::resolve_coordination_fingerprint(
        "check",
        &args,
        Err(color_eyre::eyre::eyre!("git failure")),
    )
    .expect_err("expected fingerprint error");

    assert!(error.to_string().contains("git failure"));
    Ok(())
}

#[sinex_test]
async fn test_command_context_elapsed() -> TestResult<()> {
    let ctx = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Silent),
        false,
        None,
        "test",
    );
    let baseline = ctx.elapsed();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    loop {
        let elapsed = ctx.elapsed();
        if elapsed > baseline {
            return Ok(());
        }

        assert!(
            std::time::Instant::now() < deadline,
            "CommandContext::elapsed() never advanced past {baseline:?}"
        );
        std::thread::yield_now();
    }
}

#[sinex_test]
async fn test_command_context_is_human() -> TestResult<()> {
    let ctx_human = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Human),
        false,
        None,
        "test",
    );
    assert!(ctx_human.is_human());

    let ctx_json = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Json),
        false,
        None,
        "test",
    );
    assert!(!ctx_json.is_human());
    Ok(())
}

#[sinex_test]
async fn test_command_context_is_json() -> TestResult<()> {
    let ctx_json = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Json),
        false,
        None,
        "test",
    );
    assert!(ctx_json.is_json());

    let ctx_human = CommandContext::new(
        OutputWriter::new(crate::output::OutputFormat::Human),
        false,
        None,
        "test",
    );
    assert!(!ctx_human.is_json());
    Ok(())
}

#[sinex_test]
async fn test_command_metadata_builders() -> TestResult<()> {
    let build_meta = CommandMetadata::build();
    assert_eq!(build_meta.category, Some("build"));
    assert!(build_meta.modifies_state);
    assert!(build_meta.timeout.is_some());

    let test_meta = CommandMetadata::test();
    assert_eq!(test_meta.category, Some("test"));
    assert!(!test_meta.modifies_state);
    assert!(test_meta.track_in_history);
    assert_eq!(test_meta.history_access, HistoryAccessMode::ReadWrite);

    let db_meta = CommandMetadata::database();
    assert_eq!(db_meta.category, Some("database"));
    assert!(db_meta.modifies_state);

    let utility_meta = CommandMetadata::utility();
    assert!(!utility_meta.track_in_history);
    assert_eq!(utility_meta.history_access, HistoryAccessMode::None);

    let observational_meta = CommandMetadata::analysis()
        .with_history_tracking(false)
        .with_history_access(HistoryAccessMode::Query);
    assert!(!observational_meta.track_in_history);
    assert_eq!(observational_meta.history_access, HistoryAccessMode::Query);
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_detail() -> TestResult<()> {
    let result = CommandResult::success()
        .with_detail("First detail")
        .with_detail("Second detail");

    assert_eq!(result.details.len(), 2);
    assert_eq!(result.details[0], "First detail");
    assert_eq!(result.details[1], "Second detail");
    Ok(())
}

#[sinex_test]
async fn test_command_result_with_warning() -> TestResult<()> {
    let result = CommandResult::success().with_warning("This is a warning");

    assert_eq!(result.warnings.len(), 1);
    assert_eq!(result.warnings[0], "This is a warning");
    Ok(())
}

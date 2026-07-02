use super::*;
use crate::history::HistoryDb;
use crate::output::{OutputFormat, OutputWriter};
use crate::sandbox::prelude::*;

#[sinex_test]
async fn freshness_explain_marks_exact_test_reuse_enabled() -> TestResult<()> {
    let ctx = CommandContext::new(
        OutputWriter::new(OutputFormat::Json),
        false,
        None,
        "freshness",
    );
    let command = FreshnessExplainCommand {
        command: "test".to_string(),
        args: vec!["-p".to_string(), "xtask".to_string()],
    };

    let result = command.execute(&ctx)?;
    let data = result.data.expect("freshness explain should emit data");

    assert_eq!(data["command"], "test");
    assert_eq!(data["fresh_reuse_enabled"], true);
    assert_ne!(data["reuse"]["decision"], "disabled");
    assert_eq!(data["proof_kind"], "test.nextest.exact");
    assert_eq!(data["scope"]["kind"], "packages");
    Ok(())
}

#[sinex_test]
async fn freshness_explain_test_hits_test_proof_units() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("history.db");
    let db = HistoryDb::open(&db_path)?;
    let args = vec!["-p".to_string(), "xtask".to_string()];
    let planning_ctx = CommandContext::new(
        OutputWriter::new(OutputFormat::Json),
        false,
        None,
        "freshness",
    );
    let semantic_args = explain_args_for_command("test", &args, &planning_ctx)?;
    let key = coordinator::explain_freshness("test", &semantic_args)?;
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        &key.proof_kind,
        &key.scope_key,
        &key.tree_fingerprint,
        r#"{"scope":"packages:xtask"}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);
    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Json),
        false,
        None,
        "freshness",
        db_path,
    );
    let command = FreshnessExplainCommand {
        command: "test".to_string(),
        args,
    };

    let result = command.execute(&ctx)?;
    let data = result.data.expect("freshness explain should emit data");

    assert_eq!(data["reuse"]["decision"], "hit");
    assert_eq!(data["reuse"]["last_completed"]["source"], "test_proof_unit");
    assert_eq!(
        data["reuse"]["last_completed"]["invocation_id"],
        invocation_id
    );
    Ok(())
}

#[sinex_test]
async fn freshness_explain_test_uses_resolved_test_semantics() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("history.db");
    let raw_args = vec![
        "-p".to_string(),
        "xtask".to_string(),
        "-E".to_string(),
        "test(command_catalog_exposes_core_public_surface)".to_string(),
    ];
    let planning_ctx = CommandContext::new(
        OutputWriter::new(OutputFormat::Json),
        false,
        None,
        "freshness",
    );
    let semantic_args = TestCommand {
        packages: vec!["xtask".to_string()],
        filter: Some("test(command_catalog_exposes_core_public_surface)".to_string()),
        ..Default::default()
    }
    .freshness_explain_args(Some(&planning_ctx))?;
    assert!(
        semantic_args.contains(&"--lib".to_string()),
        "simple package-scoped unit-test filters should explain the same inferred --lib key as execution: {semantic_args:?}"
    );
    assert_ne!(
        coordinator::compute_scope_key("test", &raw_args),
        coordinator::compute_scope_key("test", &semantic_args),
        "fixture must cover the raw-vs-semantic key mismatch"
    );

    let key = coordinator::explain_freshness("test", &semantic_args)?;
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        invocation_id,
        &key.proof_kind,
        &key.scope_key,
        &key.tree_fingerprint,
        r#"{"scope":"packages:xtask","lib":true}"#,
        true,
    )?;
    db.finish_invocation(
        invocation_id,
        crate::history::InvocationStatus::Success,
        Some(0),
        0.1,
    )?;
    drop(db);

    let ctx = CommandContext::new_with_db_override(
        OutputWriter::new(OutputFormat::Json),
        false,
        None,
        "freshness",
        db_path,
    );
    let command = FreshnessExplainCommand {
        command: "test".to_string(),
        args: raw_args,
    };

    let result = command.execute(&ctx)?;
    let data = result.data.expect("freshness explain should emit data");

    assert_eq!(data["scope_key"], key.scope_key);
    assert_eq!(data["reuse"]["decision"], "hit");
    assert_eq!(
        data["reuse"]["last_completed"]["invocation_id"],
        invocation_id
    );
    Ok(())
}

#[sinex_test]
async fn freshness_explain_reports_shared_inputs_for_scoped_check() -> TestResult<()> {
    let explanation =
        coordinator::explain_freshness("check", &["-p".to_string(), "xtask".to_string()])?;

    assert!(explanation.fresh_reuse_enabled);
    assert!(
        explanation
            .shared_inputs
            .contains(&"Cargo.lock".to_string())
    );
    assert!(matches!(
        explanation.scope,
        FreshnessScopeExplanation::Packages { .. }
    ));
    Ok(())
}

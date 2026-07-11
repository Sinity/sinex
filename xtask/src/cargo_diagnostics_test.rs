use super::*;
use crate::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn test_parse_empty_output() -> TestResult<()> {
    let result = parse_cargo_json_output("", true)?;
    assert_eq!(result.errors, 0);
    assert_eq!(result.warnings, 0);
    assert!(result.success);
    assert!(result.compiled_packages.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_compact_render_keeps_unrendered_errors_visible() -> TestResult<()> {
    let diagnostic = CompilerDiagnostic {
        level: "error".to_string(),
        code: Some("E0063".to_string()),
        message: "missing field `groups`".to_string(),
        file_path: Some("crate/sinexctl/tests/common/mock_client.rs".to_string()),
        line: Some(367),
        column: Some(32),
        rendered: None,
        suggestion: Some("add the missing field".to_string()),
        package: Some("sinexctl".to_string()),
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
    };

    let rendered = diagnostic.rendered_or_compact();
    assert!(rendered.contains("mock_client.rs:367:32"));
    assert!(rendered.contains("error[E0063]: missing field `groups`"));
    assert!(rendered.contains("help: add the missing field"));
    Ok(())
}

#[sinex_test]
async fn test_failed_unparsed_cargo_output_becomes_visible_diagnostic() -> TestResult<()> {
    let output = r#"{"reason":"build-finished","success":false}"#;
    let result = parse_cargo_json_output(output, false)?;
    assert_eq!(result.errors, 1);
    let diagnostic = &result.diagnostics[0];
    assert_eq!(
        diagnostic.code.as_deref(),
        Some("XTASK_UNPARSED_CARGO_FAILURE")
    );
    assert!(diagnostic.rendered_or_compact().contains("raw output tail"));
    assert!(
        diagnostic
            .rendered_or_compact()
            .contains("\"build-finished\"")
    );
    Ok(())
}

#[sinex_test]
async fn test_stderr_only_cargo_failure_becomes_visible_diagnostic() -> TestResult<()> {
    let result = parse_cargo_json_output_with_stderr(
        "",
        false,
        b"manifest parse failed before compiler startup",
    )?;
    assert_eq!(result.errors, 1);
    let diagnostic = &result.diagnostics[0];
    assert_eq!(
        diagnostic.code.as_deref(),
        Some("XTASK_UNPARSED_CARGO_FAILURE")
    );
    assert!(diagnostic.message.contains("stderr tail"));
    assert!(diagnostic.message.contains("manifest parse failed"));
    Ok(())
}

#[sinex_test]
async fn test_cargo_stderr_tail_is_bounded_and_marked() -> TestResult<()> {
    let mut tail = Vec::new();
    bounded_tail_append(&mut tail, &vec![b'a'; CARGO_STDERR_TAIL_LIMIT + 1024]);
    assert!(tail.len() <= CARGO_STDERR_TAIL_LIMIT);
    assert!(String::from_utf8_lossy(&tail).contains("stderr truncated"));
    Ok(())
}

#[sinex_test]
async fn test_extract_package_name_registry() -> TestResult<()> {
    // Format 1: registry packages — "registry+URL#name@version"
    let id = "registry+https://github.com/rust-lang/crates.io-index#proc-macro2@1.0.103";
    assert_eq!(extract_package_name(id), Some("proc-macro2".into()));

    let id = "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.200";
    assert_eq!(extract_package_name(id), Some("serde".into()));
    Ok(())
}

#[sinex_test]
async fn test_extract_package_name_local_dir_equals_name() -> TestResult<()> {
    // Format 2: local workspace, directory name = crate name — "#version" only
    let id = "path+file:///realm/project/sinex/crate/sinex-primitives#0.1.0";
    assert_eq!(extract_package_name(id), Some("sinex-primitives".into()));

    let id = "path+file:///realm/project/sinex/xtask#0.1.0";
    assert_eq!(extract_package_name(id), Some("xtask".into()));
    Ok(())
}

#[sinex_test]
async fn test_extract_package_name_local_explicit() -> TestResult<()> {
    // Format 3: local workspace, explicit name — "#name@version"
    let id = "path+file:///realm/project/sinex/xtask/macros#xtask-macros@0.1.0";
    assert_eq!(extract_package_name(id), Some("xtask-macros".into()));

    let id = "path+file:///realm/project/sinex#sinex-db@0.2.0";
    assert_eq!(extract_package_name(id), Some("sinex-db".into()));
    Ok(())
}

#[sinex_test]
async fn test_parse_compiler_message_with_package() -> TestResult<()> {
    let json_line = r#"{"reason":"compiler-message","package_id":"path+file:///realm/project/sinex#sinex-db@0.1.0","message":{"level":"warning","code":{"code":"unused_imports","explanation":null},"message":"unused import","spans":[{"file_name":"src/lib.rs","byte_start":42,"byte_end":55,"line_start":3,"line_end":3,"column_start":5,"column_end":18,"is_primary":true}],"children":[{"level":"help","message":"remove the import","spans":[{"byte_start":42,"byte_end":55,"suggestion_applicability":"MachineApplicable","suggested_replacement":""}]}],"rendered":"warning: unused import"}}"#;

    let result = parse_cargo_json_output(json_line, true)?;
    assert_eq!(result.warnings, 1);
    assert!(
        result.compiled_packages.is_empty(),
        "compiler-message diagnostics are attribution, not proof of a fresh compile"
    );

    let diag = &result.diagnostics[0];
    assert_eq!(diag.package.as_deref(), Some("sinex-db"));
    assert_eq!(diag.code.as_deref(), Some("unused_imports"));
    assert_eq!(diag.fix_applicability.as_deref(), Some("MachineApplicable"));
    assert_eq!(diag.fix_replacement.as_deref(), Some(""));
    assert_eq!(diag.fix_byte_start, Some(42));
    assert_eq!(diag.fix_byte_end, Some(55));
    Ok(())
}

#[sinex_test]
async fn test_parse_cargo_json_output_deduplicates_identical_diagnostics() -> TestResult<()> {
    let json_line = r#"{"reason":"compiler-message","package_id":"path+file:///realm/project/sinex#sinex-db@0.1.0","message":{"level":"warning","code":{"code":"unused_imports","explanation":null},"message":"unused import","spans":[{"file_name":"src/lib.rs","byte_start":42,"byte_end":55,"line_start":3,"line_end":3,"column_start":5,"column_end":18,"is_primary":true}],"children":[{"level":"help","message":"remove the import","spans":[{"byte_start":42,"byte_end":55,"suggestion_applicability":"MachineApplicable","suggested_replacement":""}]}],"rendered":"warning: unused import"}}"#;
    let output = format!("{json_line}\n{json_line}\n{json_line}\n");

    let result = parse_cargo_json_output(&output, true)?;

    assert_eq!(result.warnings, 1);
    assert_eq!(result.errors, 0);
    assert_eq!(result.diagnostics.len(), 1);
    Ok(())
}

#[sinex_test]
async fn test_compiled_packages_tracked_from_non_fresh_artifacts() -> TestResult<()> {
    // compiler-artifact messages also carry package_id. Only non-fresh
    // artifacts are compiled work; fresh artifacts are dependency graph
    // visibility, not rebuild evidence.
    let lines = [
        r#"{"reason":"compiler-artifact","fresh":false,"package_id":"path+file:///realm/project/sinex#sinex-primitives@0.1.0","target":{"name":"sinex-primitives"}}"#,
        r#"{"reason":"compiler-artifact","fresh":true,"package_id":"path+file:///realm/project/sinex#zerovec@0.11.5","target":{"name":"zerovec"}}"#,
        r#"{"reason":"compiler-message","package_id":"path+file:///realm/project/sinex#sinex-db@0.1.0","message":{"level":"warning","message":"unused","spans":[],"children":[]}}"#,
    ];
    let output = lines.join("\n");
    let result = parse_cargo_json_output(&output, true)?;
    assert_eq!(result.compiled_packages.len(), 1);
    assert!(result.compiled_packages.contains("sinex-primitives"));
    assert!(!result.compiled_packages.contains("zerovec"));
    assert!(!result.compiled_packages.contains("sinex-db"));
    Ok(())
}

#[sinex_test]
async fn test_track_progress_artifact_counts_unique_target_packages() -> TestResult<()> {
    let targets = std::collections::HashSet::from(["xtask".to_string()]);
    let mut seen = std::collections::HashSet::new();

    let dep_line = r#"{"reason":"compiler-artifact","package_id":"registry+https://example.invalid#indexmap@2.7.1","target":{"name":"indexmap"}}"#;
    let first_target = r#"{"reason":"compiler-artifact","package_id":"path+file:///realm/project/sinex#xtask@0.4.2","target":{"name":"xtask"}}"#;
    let duplicate_target = r#"{"reason":"compiler-artifact","package_id":"path+file:///realm/project/sinex#xtask@0.4.2","target":{"name":"xtask","kind":["test"]}}"#;

    assert_eq!(
        track_progress_artifact(dep_line, Some(&targets), &mut seen),
        None,
        "dependency artifacts should not advance package progress"
    );
    assert_eq!(
        track_progress_artifact(first_target, Some(&targets), &mut seen),
        Some(1),
        "first target package artifact should advance progress once"
    );
    assert_eq!(
        track_progress_artifact(duplicate_target, Some(&targets), &mut seen),
        None,
        "duplicate artifacts for the same target package should not inflate progress"
    );
    Ok(())
}

#[sinex_test]
async fn test_run_cargo_with_timeout_rejects_invalid_timeout_override() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_CARGO_TIMEOUT", "bogus");

    let (stdout, success, _stderr_tail) = run_cargo_with_timeout(&["--version"])?;
    assert!(success);
    assert!(String::from_utf8(stdout)?.contains("cargo"));
    Ok(())
}

#[sinex_test]
async fn test_run_cargo_with_timeout_rejects_zero_timeout_override() -> TestResult<()> {
    let mut _guard = EnvGuard::new();
    _guard.set("SINEX_CARGO_TIMEOUT", "0");

    let (stdout, success, _stderr_tail) = run_cargo_with_timeout(&["--version"])?;
    assert!(success);
    assert!(String::from_utf8(stdout)?.contains("cargo"));
    Ok(())
}

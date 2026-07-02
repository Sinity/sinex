use super::*;
use crate::sandbox::sinex_test;
use std::os::unix::process::ExitStatusExt;

#[sinex_test]
async fn test_lint_forbidden_command_name() -> ::xtask::sandbox::TestResult<()> {
    let cmd = LintForbiddenCommand;
    assert_eq!(cmd.name(), "lint-forbidden");
    Ok(())
}

#[sinex_test]
async fn test_lint_forbidden_command_metadata() -> ::xtask::sandbox::TestResult<()> {
    let cmd = LintForbiddenCommand;
    let metadata = cmd.metadata();

    assert_eq!(metadata.category, Some("check"));
    assert!(metadata.timeout.is_some());
    Ok(())
}

#[sinex_test]
async fn test_is_tests_path() -> ::xtask::sandbox::TestResult<()> {
    assert!(is_tests_path("tests/foo.rs"));
    assert!(is_tests_path("crate/lib/foo/tests/bar.rs"));
    assert!(!is_tests_path("crate/lib/foo/src/test_utils.rs"));
    Ok(())
}

#[sinex_test]
async fn test_attribute_policy_does_not_blanket_exempt_xtask_source()
-> ::xtask::sandbox::TestResult<()> {
    assert!(is_dedicated_test_path("tests/foo.rs"));
    assert!(is_dedicated_test_path("xtask/tests/scenarios.rs"));
    assert!(!is_dedicated_test_path("xtask/src/commands/check.rs"));
    assert!(!is_dedicated_test_path("crate/sinexd/src/runtime/foo.rs"));
    Ok(())
}

#[sinex_test]
async fn test_filter_allowlist() -> ::xtask::sandbox::TestResult<()> {
    let matches = vec![
        "crate/foo/src/main.rs:10:test".to_string(),
        "crate/bar/src/lib.rs:20:test".to_string(),
        "tests/integration.rs:30:test".to_string(),
    ];
    let allow = ["crate/foo/src/main.rs"];
    let filtered = filter_allowlist(matches, &allow, is_tests_path)?;

    assert_eq!(filtered.len(), 1);
    assert!(filtered[0].contains("crate/bar/src/lib.rs"));
    Ok(())
}

#[sinex_test]
async fn test_filter_allowlist_rejects_malformed_match_lines() -> ::xtask::sandbox::TestResult<()> {
    let error = filter_allowlist(vec!["malformed line".to_string()], &[], |_| false)
        .expect_err("malformed ripgrep output should fail");
    assert!(format!("{error:#}").contains("missing a file prefix"));
    Ok(())
}

#[sinex_test]
async fn test_filter_allowlist_rejects_empty_match_paths() -> ::xtask::sandbox::TestResult<()> {
    let error = filter_allowlist(vec![":10:test".to_string()], &[], |_| false)
        .expect_err("empty file path should fail");
    assert!(format!("{error:#}").contains("empty file path"));
    Ok(())
}

#[sinex_test]
async fn test_transport_publish_family_inventory_is_current() -> ::xtask::sandbox::TestResult<()> {
    let violations = check_transport_publish_family_inventory()?;
    assert!(
        violations.is_empty(),
        "direct publish sites must be assigned to a transport family: {violations:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn test_ensure_rg_completed_reports_signal_termination() -> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(9),
        stdout: Vec::new(),
        stderr: b"killed".to_vec(),
    };

    let error =
        ensure_rg_completed(&output, "ripgrep").expect_err("signal termination should surface");
    assert!(error.to_string().contains("terminated by signal"));
    assert!(error.to_string().contains("killed"));
    Ok(())
}

#[sinex_test]
async fn test_ensure_rg_completed_allows_no_matches() -> ::xtask::sandbox::TestResult<()> {
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(1 << 8),
        stdout: Vec::new(),
        stderr: Vec::new(),
    };

    ensure_rg_completed(&output, "ripgrep")?;
    Ok(())
}

#[sinex_test]
async fn coherence_boundary_skip_keeps_scope_on_core_runtime() -> ::xtask::sandbox::TestResult<()> {
    assert!(is_coherence_boundary_skip("tests/e2e/tests/foo.rs"));
    assert!(is_coherence_boundary_skip(
        "crate/sinexd/src/runtime/tests.rs"
    ));
    assert!(is_coherence_boundary_skip(
        "crate/sinexctl/src/commands/foo.rs"
    ));
    assert!(!is_coherence_boundary_skip(
        "crate/sinexd/src/runtime/foo.rs"
    ));
    assert!(!is_coherence_boundary_skip(
        "crate/sinex-db/src/repositories/foo.rs"
    ));
    Ok(())
}

#[sinex_test]
async fn test_parse_ast_grep_summary_tracks_blocking_and_advisory_findings()
-> ::xtask::sandbox::TestResult<()> {
    let summary = parse_ast_grep_summary(
        r#"{"file":"crate/lib/foo.rs","ruleId":"dbg-macro","severity":"error","message":"dbg!()","range":{"start":{"line":7,"column":13}}}
{"file":"crate/lib/bar.rs","ruleId":"context-erasure","severity":"warning","message":"map_err(|_| ...)","range":{"start":{"line":11,"column":5}}}
{"file":"crate/lib/baz.rs","ruleId":"string-from-literal","severity":"hint","message":"String::from","range":{"start":{"line":3,"column":9}}}"#,
    )?;

    assert_eq!(summary.error_count(), 1);
    assert_eq!(summary.warning_count(), 1);
    assert_eq!(summary.hint_count(), 1);
    assert_eq!(summary.error_findings()[0].file, "crate/lib/foo.rs");
    assert_eq!(summary.error_findings()[0].rule_id, "dbg-macro");
    Ok(())
}

#[sinex_test]
async fn test_parse_ast_grep_summary_rejects_invalid_json() -> ::xtask::sandbox::TestResult<()> {
    let error =
        parse_ast_grep_summary("not-json").expect_err("invalid ast-grep output should fail");
    assert!(format!("{error:#}").contains("failed to parse ast-grep JSON output"));
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Ignored-test contract gate self-tests
// ─────────────────────────────────────────────────────────────────────────

#[sinex_test]
async fn ignored_test_contract_accepts_categorized_reasons() -> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore = "heavy: run with xtask test --heavy"]
        async fn heavy_case() {}

        #[ignore = "long: performance matrix, run with xtask test --heavy"]
        async fn long_case() {}

        #[ignore = "external: requires git-annex on PATH"]
        async fn external_case() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert!(
        violations.is_empty(),
        "categorized ignore reasons should pass: {violations:?}"
    );
    Ok(())
}

#[sinex_test]
async fn ignored_test_contract_rejects_bare_ignore() -> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore]
        async fn silently_skipped() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("ignored tests must use"));
    Ok(())
}

#[sinex_test]
async fn ignored_test_contract_rejects_uncategorized_reason() -> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore = "flaky on my machine"]
        async fn ambiguous_skip() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("must start with one of"));
    Ok(())
}

#[sinex_test]
async fn ignored_test_contract_rejects_empty_rationale_after_category()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore = "heavy:"]
        async fn unlabeled_heavy_case() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("operator-visible rationale"));
    Ok(())
}

#[sinex_test]
async fn ignored_test_contract_rejects_unrouted_heavy_reason() -> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore = "heavy: slow on laptops"]
        async fn unrouted_heavy_case() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("xtask test --heavy"));
    Ok(())
}

#[sinex_test]
async fn ignored_test_contract_rejects_external_without_prerequisite()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        #[ignore = "external: flaky network service"]
        async fn vague_external_case() {}
    "#;

    let violations = ignored_test_contract_violations_for_file("fixture.rs", fixture);
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("requires"));
    Ok(())
}

#[sinex_test]
async fn property_strategy_fallback_contract_accepts_expectations()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        "[a-z]+".prop_map(|raw| EventSource::new(raw).expect("generated source"));
    "#;

    let violations = property_strategy_domain_fallback_violations_for_file(
        "crate/sinex-primitives/tests/strategies.rs",
        fixture,
    );
    assert!(
        violations.is_empty(),
        "typed-valid generated domains should pass"
    );
    Ok(())
}

#[sinex_test]
async fn property_strategy_fallback_contract_rejects_fixed_fallback()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        EventSource::new(raw).unwrap_or_else(|_| EventSource::from_static("test.source"));
    "#;

    let violations = property_strategy_domain_fallback_violations_for_file(
        "crate/sinex-primitives/tests/strategies.rs",
        fixture,
    );
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("typed-valid values"));
    Ok(())
}

#[sinex_test]
async fn vm_suite_evidence_contract_accepts_typed_requirement() -> ::xtask::sandbox::TestResult<()>
{
    let fixture = r#"
        runner.require_evidence(
            name,
            EvidenceKind::Database,
            false,
            "database probe failed",
            MissingEvidencePolicy::Block,
        );
    "#;

    let violations = vm_suite_evidence_kind_violations_for_file(
        "tests/vm-suite/src/categories/smoke.rs",
        fixture,
    );
    assert!(
        violations.is_empty(),
        "typed evidence requirement should pass"
    );
    Ok(())
}

#[sinex_test]
async fn vm_suite_evidence_contract_rejects_raw_evidence_missing()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        runner.record(name, TestOutcome::EvidenceMissing, "probe failed");
    "#;

    let violations = vm_suite_evidence_kind_violations_for_file(
        "tests/vm-suite/src/categories/smoke.rs",
        fixture,
    );
    assert_eq!(violations.len(), 1);
    assert!(violations[0].contains("require_evidence"));
    Ok(())
}

#[sinex_test]
async fn vm_suite_evidence_contract_ignores_comments() -> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        // TestOutcome::EvidenceMissing appears in prose only.
    "#;

    let violations = vm_suite_evidence_kind_violations_for_file(
        "tests/vm-suite/src/categories/smoke.rs",
        fixture,
    );
    assert!(violations.is_empty(), "comment-only references should pass");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// Privacy metadata gate self-tests
//
// These tests exercise the gate logic directly on fixture strings without
// invoking ripgrep or the filesystem. They pin the gate's catch/pass
// semantics so regressions in indicator matching are immediately visible.
// ─────────────────────────────────────────────────────────────────────────

/// Helper: run the gate logic against a synthetic file content string.
/// Returns a list of violation messages (empty = pass).
fn run_privacy_gate_on_fixture(contents: &str) -> Vec<String> {
    let mut violations = Vec::new();

    let has_non_public_tier = NON_PUBLIC_TIER_PATTERNS
        .iter()
        .any(|pat| contents.contains(pat));
    if !has_non_public_tier {
        return violations;
    }

    let has_privacy_metadata = PRIVACY_METADATA_INDICATORS
        .iter()
        .any(|ind| contents.contains(ind));
    if has_privacy_metadata {
        return violations;
    }

    let unit_ids = extract_source_ids(contents);
    let tiers = extract_non_public_tiers(contents);
    violations.push(format!(
        "fixture: source(s) [{units}] with privacy tier [{tiers}] missing privacy metadata",
        units = unit_ids.join(", "),
        tiers = tiers.join(", "),
    ));
    violations
}

#[sinex_test]
async fn privacy_gate_catches_sensitive_unit_without_privacy_metadata()
-> ::xtask::sandbox::TestResult<()> {
    // Planted violation: Sensitive tier, no privacy indicator.
    // IMPORTANT: the comment below must NOT contain privacy indicator strings.
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.planted",
                privacy_tier: PrivacyTier::Sensitive,
            }
        }

        fn parse_record(&self, record: SourceRecord) -> Vec<ParsedEventIntent> {
            // This stub does no sanitisation — intentional gate target
            vec![]
        }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        !violations.is_empty(),
        "gate must fire on Sensitive tier without privacy metadata"
    );
    assert!(
        violations[0].contains("stub.planted"),
        "violation must name the source id; got: {}",
        violations[0]
    );
    assert!(
        violations[0].contains("PrivacyTier::Sensitive"),
        "violation must name the privacy tier; got: {}",
        violations[0]
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_catches_secret_unit_without_privacy_metadata()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.secret",
                privacy_tier: PrivacyTier::Secret,
            }
        }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(!violations.is_empty(), "gate must fire on Secret tier");
    Ok(())
}

#[sinex_test]
async fn privacy_gate_passes_public_unit_without_privacy_call() -> ::xtask::sandbox::TestResult<()>
{
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "noop",
                privacy_tier: PrivacyTier::Public,
            }
        }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        violations.is_empty(),
        "Public tier must not require privacy metadata"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_ignores_privacy_engine_call_without_metadata()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.sensitive",
                privacy_tier: PrivacyTier::Sensitive,
            }
        }

        fn parse_record(&self, record: SourceRecord) -> Vec<ParsedEventIntent> {
            let engine = PrivacyEngine::new(PrivacyConfig::default()).unwrap();
            let result = engine.process(&text, ctx);
            vec![]
        }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        !violations.is_empty(),
        "a local privacy engine call alone must not satisfy the metadata gate"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_passes_with_processing_context_metadata() -> ::xtask::sandbox::TestResult<()>
{
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.irc",
                privacy_tier: PrivacyTier::Sensitive,
            }
        }

        fn build_contexts() -> Vec<ProcessingContext::Command> {
            vec![]
        }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        violations.is_empty(),
        "ProcessingContext:: satisfies the gate"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_passes_with_declarative_default_privacy_context()
-> ::xtask::sandbox::TestResult<()> {
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.declarative",
                privacy_tier: PrivacyTier::Sensitive,
            }
        }

        #[derive(SourceRecord)]
        #[source_record(
            id = "stub-declarative",
            default_privacy_context = "Command"
        )]
        pub struct StubRecord { pub field: String }
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        violations.is_empty(),
        "default_privacy_context = satisfies the gate"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_passes_with_explicit_allow() -> ::xtask::sandbox::TestResult<()> {
    // Escape hatch: `#[allow(missing_privacy_metadata, reason = "...")]`
    let fixture = r#"
        register_source_contract! {
            SourceContract {
                id: "stub.exempt",
                privacy_tier: PrivacyTier::Sensitive,
            }
        }

        #[allow(missing_privacy_metadata, reason = "descriptor-only source")]
        fn parse_record(&self) {}
    "#;

    let violations = run_privacy_gate_on_fixture(fixture);
    assert!(
        violations.is_empty(),
        "#[allow(missing_privacy_metadata satisfies the gate"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_gate_live_workspace_has_no_violations() -> ::xtask::sandbox::TestResult<()> {
    // Run the actual gate against the live workspace. This catches regressions
    // where the gate logic is sound but the existing codebase drifts.
    let violations = check_privacy_metadata_for_sensitive_units()?;
    assert!(
        violations.is_empty(),
        "privacy metadata gate found violations in live workspace: {violations:#?}"
    );
    Ok(())
}

#[sinex_test]
async fn provider_secret_literal_pattern_catches_known_shapes() -> ::xtask::sandbox::TestResult<()>
{
    let pattern = provider_shaped_secret_pattern();
    let regex = regex::Regex::new(&pattern)?;

    let classic_github = ["ghp", "_", "ABCDEFghijklmnopqrstuvwxyz1234567890"].concat();
    let fine_grained_github = [
        "github",
        "_pat",
        "_",
        "11ABCDEFG0abcdefghijklmnopqrstuvwxyz1234567",
    ]
    .concat();
    let aws_access_key = ["AKIA", "IOSFODNN7EXAMPLE"].concat();

    assert!(
        regex.is_match(&classic_github),
        "classic github token must match"
    );
    assert!(
        regex.is_match(&fine_grained_github),
        "fine-grained github token must match"
    );
    assert!(regex.is_match(&aws_access_key), "aws access key must match");
    Ok(())
}

#[sinex_test]
async fn provider_secret_literal_live_workspace_has_no_violations()
-> ::xtask::sandbox::TestResult<()> {
    let violations = check_provider_shaped_secret_literals()?;
    assert!(
        violations.is_empty(),
        "provider-shaped secret literal gate found violations in live workspace: \
         {violations:#?}"
    );
    Ok(())
}

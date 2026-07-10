use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn percentage_helpers_are_stable() -> ::xtask::sandbox::TestResult<()> {
    assert!((percent_increase(110.0, 100.0) - 10.0).abs() < f64::EPSILON);
    assert!((percent_drop(100.0, 92.0) - 8.0).abs() < f64::EPSILON);
    assert_eq!(percent_increase(100.0, 0.0), 0.0);
    assert_eq!(percent_drop(0.0, 100.0), 0.0);
    Ok(())
}

#[sinex_test]
async fn prometheus_render_contains_expected_metrics() -> ::xtask::sandbox::TestResult<()> {
    let report = PerfVerificationReport {
        generated_at: "2026-01-01T00:00:00Z".to_string(),
        profile: "fast".to_string(),
        runs: 2,
        threads: vec![12],
        bench_output_dir: "/tmp/bench".to_string(),
        history_db: "/tmp/history.db".to_string(),
        contracts_path: "/tmp/contracts.toml".to_string(),
        latest_run_id: 42,
        passed: true,
        failure_count: 0,
        scenarios: vec![ScenarioVerification {
            scenario_key: "t=12".to_string(),
            threads: 12,
            current: ScenarioMeasurement {
                median_ms: 100.0,
                p95_ms: 120.0,
                throughput_runs_per_sec: 8.5,
                sample_count: 2,
            },
            baseline: None,
            thresholds: ResolvedThresholds {
                max_median_ms: None,
                max_p95_ms: None,
                min_throughput_runs_per_sec: None,
                median_regression_pct: None,
                p95_regression_pct: None,
                throughput_regression_pct: None,
                enforce_baseline: false,
            },
            checks: vec![],
            passed: true,
        }],
    };

    let rendered = render_prometheus(&report);
    assert!(rendered.contains("verify_perf_overall_pass 1"));
    assert!(rendered.contains("verify_perf_scenario_pass{scenario=\"t=12\"} 1"));
    assert!(rendered.contains("verify_perf_median_ms{scenario=\"t=12\"} 100.000000"));
    Ok(())
}

fn valid_phase_manifest() -> PhaseVerificationManifest {
    PhaseVerificationManifest {
        version: 1,
        phases: vec![PhaseVerificationPhase {
            id: "1".to_string(),
            title: "Source foundation".to_string(),
            issues: vec![1054, 1128],
            required_checks: vec![
                "git diff --check".to_string(),
                "xtask test --dry-run --all --exclude sinex-e2e-tests".to_string(),
            ],
            boundary_checks: vec!["xtask schema strict-diff".to_string()],
            impact_gates: vec![PhaseImpactGate {
                impact: "schema".to_string(),
                commands: vec!["xtask docs check".to_string()],
            }],
            evidence_manifest: vec![PhaseEvidenceManifestItem {
                ac_id: "phase-1.schema".to_string(),
                status: "satisfied".to_string(),
                evidence_kind: "schema".to_string(),
                surface: "schema strict-diff".to_string(),
                evidence: "schema drift is checked by the phase boundary gate".to_string(),
                command: Some("xtask schema strict-diff".to_string()),
                artifact: None,
            }],
        }],
    }
}

#[sinex_test]
async fn phase_manifest_validation_accepts_supported_commands() -> ::xtask::sandbox::TestResult<()>
{
    validate_phase_manifest(&valid_phase_manifest())?;
    Ok(())
}

#[sinex_test]
async fn phase_manifest_validation_rejects_duplicate_phase_ids() -> ::xtask::sandbox::TestResult<()>
{
    let mut manifest = valid_phase_manifest();
    manifest.phases.push(manifest.phases[0].clone());

    let error = validate_phase_manifest(&manifest).expect_err("duplicate id must fail");
    assert!(format!("{error:#}").contains("duplicate phase id"));
    Ok(())
}

#[sinex_test]
async fn phase_manifest_validation_rejects_empty_required_checks()
-> ::xtask::sandbox::TestResult<()> {
    let mut manifest = valid_phase_manifest();
    manifest.phases[0].required_checks.clear();

    let error = validate_phase_manifest(&manifest).expect_err("empty checks must fail");
    assert!(format!("{error:#}").contains("must define at least one required check"));
    Ok(())
}

#[sinex_test]
async fn phase_manifest_validation_rejects_unsupported_commands() -> ::xtask::sandbox::TestResult<()>
{
    let mut manifest = valid_phase_manifest();
    manifest.phases[0]
        .required_checks
        .push("python -m pytest".to_string());

    let error = validate_phase_manifest(&manifest).expect_err("unsupported command must fail");
    assert!(format!("{error:#}").contains("unsupported phase verification command"));
    Ok(())
}

#[sinex_test]
async fn phase_manifest_validation_rejects_grep_only_behavior_evidence()
-> ::xtask::sandbox::TestResult<()> {
    let mut manifest = valid_phase_manifest();
    manifest.phases[0].evidence_manifest = vec![PhaseEvidenceManifestItem {
        ac_id: "phase-1.runtime".to_string(),
        status: "satisfied".to_string(),
        evidence_kind: "runtime".to_string(),
        surface: "source".to_string(),
        evidence: "source text contains the desired value".to_string(),
        command: Some("rg -n desired xtask/src/commands/verify.rs".to_string()),
        artifact: None,
    }];

    let errors = validate_phase_evidence_manifest(&manifest.phases[0]);
    let reasons = errors
        .iter()
        .map(|error| error.reason.as_str())
        .collect::<Vec<_>>();
    assert!(
        reasons.iter().any(|reason| reason.contains("source text")),
        "expected source-text rejection, got {reasons:?}"
    );
    assert!(
        reasons.iter().any(|reason| reason.contains("grep-only")),
        "expected grep-only rejection, got {reasons:?}"
    );

    let error = validate_phase_manifest(&manifest).expect_err("grep-only phase evidence must fail");
    assert!(format!("{error:#}").contains("source text"));
    Ok(())
}

// ==========================================================================
// Closure subcommand unit tests
// ==========================================================================

#[sinex_test]
async fn extract_closure_commands_returns_empty_for_no_verify_section()
-> ::xtask::sandbox::TestResult<()> {
    let body = "## Summary\nSome text.\n\n```bash\necho hello\n```\n";
    let cmds = extract_closure_command_entries(body, "body");
    assert!(
        cmds.is_empty(),
        "no verify section should yield no commands, got: {cmds:?}"
    );
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_finds_commands_in_verify_section()
-> ::xtask::sandbox::TestResult<()> {
    let body =
        "## Closure verification commands\n\n```bash\ngit log --oneline -3\nxtask check\n```\n";
    let cmds = extract_closure_command_entries(body, "body");
    assert_eq!(cmds.len(), 2, "expected 2 commands, got: {cmds:?}");
    assert!(cmds[0].command.contains("git log"));
    assert!(cmds[1].command.contains("xtask check"));
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_strips_dollar_prompt() -> ::xtask::sandbox::TestResult<()> {
    let body = "## Verification\n\n```bash\n$ git show HEAD --stat\n```\n";
    let cmds = extract_closure_command_entries(body, "body");
    assert_eq!(cmds.len(), 1);
    assert_eq!(cmds[0].command, "git show HEAD --stat");
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_ignores_comment_lines() -> ::xtask::sandbox::TestResult<()> {
    let body = "## Verification\n\n```bash\n# this is a comment\nxtask check\n```\n";
    let cmds = extract_closure_command_entries(body, "body");
    assert_eq!(cmds.len(), 1);
    assert_eq!(cmds[0].command, "xtask check");
    Ok(())
}

#[sinex_test]
async fn extract_closure_command_entries_preserve_source_location()
-> ::xtask::sandbox::TestResult<()> {
    let body = "## Verification\n\n```bash\nxtask check -p xtask\n```\n";
    let cmds = extract_closure_command_entries(body, "comment[0]@2026-05-19T00:00:00Z");
    assert_eq!(cmds.len(), 1);
    assert_eq!(cmds[0].command, "xtask check -p xtask");
    assert_eq!(cmds[0].source, "comment[0]@2026-05-19T00:00:00Z");
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_skips_prose_inside_fenced_blocks()
-> ::xtask::sandbox::TestResult<()> {
    // Bead close reasons sometimes describe verification narratively inside
    // a fenced block. The verifier must not try to execute prose as a
    // shell command. Regression test for #1552.
    let body = "## Verification\n\n```\n\
        git push pre-push drift guard passes\n\
        xtask\n\
        python script outputs success\n\
        xtask check -p xtask\n\
        ```\n";
    let cmds = extract_closure_command_entries(body, "body");
    let extracted: Vec<&str> = cmds.iter().map(|c| c.command.as_str()).collect();
    assert_eq!(
        extracted,
        vec!["xtask check -p xtask"],
        "only the runnable command should be extracted; prose and bare commands must be skipped (got {extracted:?})",
    );
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_skips_verifier_self_rerun_instructions()
-> ::xtask::sandbox::TestResult<()> {
    let body = "\
## Closure verification failed

`xtask verify closure sinex-e7e9` returned a non-zero status for this Bead.

Re-run locally with:

```bash
xtask verify closure sinex-e7e9
```

Either add the missing evidence to `close_reason`, or re-open the Bead if closure was premature.
";
    let cmds = extract_closure_command_entries(body, "comment[0]@2026-06-06T03:11:40Z");
    assert!(
        cmds.is_empty(),
        "verifier rerun instructions must not become recursive closure evidence: {cmds:?}"
    );
    Ok(())
}

#[sinex_test]
async fn looks_like_runnable_command_filters_prose_and_bare_commands()
-> ::xtask::sandbox::TestResult<()> {
    assert!(!looks_like_runnable_command(""));
    assert!(!looks_like_runnable_command("xtask"));
    assert!(!looks_like_runnable_command("git"));
    assert!(!looks_like_runnable_command(
        "git push pre-push drift guard"
    ));
    assert!(!looks_like_runnable_command("python -m pytest"));
    assert!(looks_like_runnable_command("xtask check -p xtask"));
    assert!(looks_like_runnable_command("git log --oneline -3"));
    assert!(looks_like_runnable_command("gh pr view 1234"));
    assert!(looks_like_runnable_command("bd show sinex-e7e9 --json"));
    assert!(looks_like_runnable_command(
        "SINEX_FOO=bar xtask test -p xtask"
    ));
    Ok(())
}

#[sinex_test]
async fn closure_verifier_self_command_detects_env_prefixed_forms()
-> ::xtask::sandbox::TestResult<()> {
    assert!(is_closure_verifier_self_command(
        "xtask verify closure sinex-e7e9"
    ));
    assert!(is_closure_verifier_self_command(
        "RUST_LOG=debug xtask verify closure sinex-e7e9 --json"
    ));
    // "source-worker" was a subcommand removed in Wave-B (#1081); used here
    // as an arbitrary non-closure-verifier string for the negative assertion.
    assert!(!is_closure_verifier_self_command(
        "xtask verify source-worker"
    ));
    assert!(!is_closure_verifier_self_command("xtask check --full"));
    Ok(())
}

#[sinex_test]
async fn extract_closure_commands_finds_inline_comment_verification()
-> ::xtask::sandbox::TestResult<()> {
    let body = "\
Verification:

- `SINEX_PREFLIGHT_SKIP_DISK_CHECK=1 xtask check -p sinexctl` - passed.
- `xtask test -p sinexctl -E 'test(mcp)'` - passed.
";
    let cmds = extract_closure_command_entries(body, "body");
    assert_eq!(cmds.len(), 2);
    assert!(
        cmds[0]
            .command
            .starts_with("SINEX_PREFLIGHT_SKIP_DISK_CHECK=1 xtask check")
    );
    assert!(cmds[1].command.starts_with("xtask test -p sinexctl"));
    Ok(())
}

#[sinex_test]
async fn collect_closure_evidence_reads_bead_close_reason() -> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "closed".to_string(),
        acceptance_criteria: "- command is runnable".to_string(),
        close_reason: "## Verification\n\n```bash\nxtask check -p xtask\n```".to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    assert_eq!(evidence.commands.len(), 1);
    assert_eq!(evidence.commands[0].command, "xtask check -p xtask");
    assert_eq!(evidence.commands[0].source, "close_reason");
    Ok(())
}

#[sinex_test]
async fn collect_closure_evidence_is_empty_without_commands_or_matrix()
-> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "closed".to_string(),
        acceptance_criteria: "- behavior is proven".to_string(),
        close_reason: "Text-only landing claim.".to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    assert!(evidence.commands.is_empty());
    assert!(evidence.matrix_items.is_empty());
    Ok(())
}

#[sinex_test]
async fn bead_payload_parser_requires_one_matching_top_level_record()
-> ::xtask::sandbox::TestResult<()> {
    let payload = parse_bead_closure_payload(
        br#"[{"id":"sinex-e7e9","status":"closed","acceptance_criteria":"- AC","close_reason":"proof"}]"#,
        "sinex-e7e9",
    )?;
    assert_eq!(payload.id, "sinex-e7e9");

    let mismatch = parse_bead_closure_payload(
        br#"[{"id":"sinex-other1","status":"closed"}]"#,
        "sinex-e7e9",
    )
    .expect_err("mismatched id must fail closed");
    assert!(format!("{mismatch:#}").contains("while `sinex-e7e9` was requested"));

    let empty = parse_bead_closure_payload(b"[]", "sinex-e7e9")
        .expect_err("empty bd response must fail closed");
    assert!(format!("{empty:#}").contains("expected exactly one"));
    assert!(looks_like_bead_id("sinex-dffy"));
    assert!(looks_like_bead_id("sinex-r6d.12"));
    assert!(!looks_like_bead_id("2462"));
    Ok(())
}

#[sinex_test]
async fn bead_closure_contract_requires_closed_status_and_every_ac_disposition()
-> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "open".to_string(),
        acceptance_criteria: "- first behavior\n- second behavior".to_string(),
        close_reason: "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Status |
| --- | --- | --- | --- | --- | --- |
| AC-1 | runtime | closure verifier | first behavior is exercised | xtask test -p xtask -E 'test(closure)' | Satisfied |
"
        .to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    let criteria = extract_bead_acceptance_criteria(&payload.acceptance_criteria);
    let errors = validate_bead_closure_contract(&payload, &criteria, &evidence);
    assert!(errors.iter().any(|error| error.source == "bd.status"));
    assert!(
        errors
            .iter()
            .any(|error| error.ac_id.as_deref() == Some("AC-2"))
    );
    Ok(())
}

#[sinex_test]
async fn bead_closure_contract_rejects_unowned_deferral_and_prose_only_satisfaction()
-> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "closed".to_string(),
        acceptance_criteria: "- first behavior\n- second behavior".to_string(),
        close_reason: "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Status |
| --- | --- | --- | --- | --- |
| AC-1 | runtime | closure verifier | implemented in PR #1 | Satisfied |
| AC-2 | runtime | deferred work | fix this later | Deferred |
"
        .to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    let criteria = extract_bead_acceptance_criteria(&payload.acceptance_criteria);
    let errors = validate_bead_closure_contract(&payload, &criteria, &evidence);
    let reasons = errors
        .iter()
        .map(|error| error.reason.as_str())
        .collect::<Vec<_>>();
    assert!(reasons.iter().any(|reason| reason.contains("runnable command")));
    assert!(reasons.iter().any(|reason| reason.contains("follow-up Bead")));
    Ok(())
}

#[sinex_test]
async fn bead_closure_contract_accepts_complete_manifest()
-> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "closed".to_string(),
        acceptance_criteria: "- first behavior\n- second behavior".to_string(),
        close_reason: "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Status |
| --- | --- | --- | --- | --- | --- |
| AC-1 | runtime | closure verifier | first behavior is exercised | xtask test -p xtask -E 'test(closure)' | Satisfied |
| AC-2 | runtime | follow-up | owned by sinex-a1b2 | - | Deferred |
"
        .to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    let criteria = extract_bead_acceptance_criteria(&payload.acceptance_criteria);
    assert!(
        validate_bead_closure_contract(&payload, &criteria, &evidence).is_empty()
    );
    Ok(())
}

#[sinex_test]
async fn extract_closure_matrix_items_reports_checkbox_status() -> ::xtask::sandbox::TestResult<()>
{
    let body = "\
## Acceptance Criteria Drift

- [x] AC #1 satisfied by PR
- [ ] AC #2 deferred to #123
- [ ] AC #3 still unclear
";
    let items = extract_closure_matrix_items(body, "body");
    assert_eq!(items.len(), 3);
    assert_eq!(items[0].status, "checked");
    assert_eq!(items[1].status, "deferred");
    assert_eq!(items[2].status, "unchecked");
    Ok(())
}

#[sinex_test]
async fn closure_matrix_validation_rejects_unchecked_and_failed_items()
-> ::xtask::sandbox::TestResult<()> {
    let body = "## Acceptance Criteria Drift

- [x] AC #1 satisfied by PR
- [ ] AC #2 still missing
- ❌ AC #3 failed in verification
";
    let items = extract_closure_matrix_items(body, "body");
    let errors = validate_closure_matrix_items(&items);

    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|error| error.status == "unchecked"));
    assert!(errors.iter().any(|error| error.status == "failed"));
    assert!(
        errors
            .iter()
            .any(|error| error.reason.contains("not closed")),
        "matrix errors must explain why closure is blocked: {errors:?}"
    );
    Ok(())
}

#[sinex_test]
async fn closure_matrix_validation_allows_checked_deferred_and_misframed_items()
-> ::xtask::sandbox::TestResult<()> {
    let body = "## Acceptance Matrix

- [x] AC #1 satisfied by PR
- [ ] AC #2 tracked by follow-up #123
- [ ] AC #3 misframed; replaced by #124
";
    let items = extract_closure_matrix_items(body, "comment[0]");
    let statuses = items
        .iter()
        .map(|item| item.status.as_str())
        .collect::<Vec<_>>();
    assert_eq!(statuses, vec!["checked", "deferred", "misframed"]);
    assert!(
        validate_closure_matrix_items(&items).is_empty(),
        "checked, deferred, and misframed rows are explicit closure states"
    );
    Ok(())
}

#[sinex_test]
async fn extract_closure_matrix_items_reports_markdown_table_status()
-> ::xtask::sandbox::TestResult<()> {
    let body = "\
## Acceptance Matrix

| Acceptance criterion | Evidence | Status |
| --- | --- | --- |
| EvidenceWindow v0 is declared complete | `relations.rs` defines the DTOs. | Satisfied |
| Privacy enforcement is owned elsewhere | Follow-up owner is recorded in sinex-abcd. | Satisfied with owner |
";
    let items = extract_closure_matrix_items(body, "body");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].status, "satisfied");
    assert_eq!(
        items[0].text,
        "EvidenceWindow v0 is declared complete | `relations.rs` defines the DTOs."
    );
    assert_eq!(items[1].status, "satisfied");
    assert_eq!(
        items[1].text,
        "Privacy enforcement is owned elsewhere | Follow-up owner is recorded in sinex-abcd."
    );
    Ok(())
}

#[sinex_test]
async fn extract_closure_matrix_items_reads_plain_acceptance_matrix_label()
-> ::xtask::sandbox::TestResult<()> {
    let body = "\
## Closeout — audit ledger exhausted

Acceptance matrix:

| AC | Evidence | Status |
| --- | --- | --- |
| Every finding has a ledger state | Follow-up owner is recorded in sinex-efgh. | Satisfied |
";
    let items = extract_closure_matrix_items(body, "comment[0]");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].status, "satisfied");
    assert_eq!(
        items[0].text,
        "Every finding has a ledger state | Follow-up owner is recorded in sinex-efgh."
    );
    Ok(())
}

#[sinex_test]
async fn closure_evidence_manifest_parses_behavior_rows() -> ::xtask::sandbox::TestResult<()> {
    let body = "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Artifact | Status |
| --- | --- | --- | --- | --- | --- | --- |
| AC-1 | runtime | xtask infra status JSON | `sinexd` current-checkout state is emitted in JSON and warning paths. | xtask test -p xtask -E 'test(current_checkout_status_reports_dev_local_sinexd)' | - | Satisfied |
| AC-2 | docs | command guide | Guide documents the explicit local runtime surface. | xtask docs command-guide --check | xtask/docs/command-guide.md | Satisfied |
";
    let items = extract_closure_evidence_manifest_items(body, "body");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].ac_id, "AC-1");
    assert_eq!(items[0].evidence_kind, "runtime");
    assert_eq!(
        items[0].command.as_deref(),
        Some("xtask test -p xtask -E 'test(current_checkout_status_reports_dev_local_sinexd)'")
    );
    assert!(validate_closure_evidence_manifest(&items).is_empty());
    Ok(())
}

#[sinex_test]
async fn closure_evidence_manifest_rejects_grep_only_runtime_claim()
-> ::xtask::sandbox::TestResult<()> {
    let body = "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Status |
| --- | --- | --- | --- | --- | --- |
| AC-1 | runtime | source | source text contains the new field | rg -n sinexd xtask/src/infra/stack.rs | Satisfied |
";
    let items = extract_closure_evidence_manifest_items(body, "comment[0]");
    assert_eq!(items.len(), 1);
    let errors = validate_closure_evidence_manifest(&items);
    let reasons = errors
        .iter()
        .map(|error| error.reason.as_str())
        .collect::<Vec<_>>();
    assert!(
        reasons.iter().any(|reason| reason.contains("source text")),
        "expected source-text rejection, got {reasons:?}"
    );
    assert!(
        reasons.iter().any(|reason| reason.contains("grep-only")),
        "expected grep-only rejection, got {reasons:?}"
    );
    Ok(())
}

#[sinex_test]
async fn closure_manifest_status_does_not_accept_substring_false_positives()
-> ::xtask::sandbox::TestResult<()> {
    assert_eq!(normalize_manifest_status("Passed"), "satisfied");
    assert_eq!(normalize_manifest_status("bypass pending"), "bypass pending");
    Ok(())
}

#[sinex_test]
async fn collect_closure_evidence_includes_manifest_items() -> ::xtask::sandbox::TestResult<()> {
    let payload = BeadClosurePayload {
        id: "sinex-e7e9".to_string(),
        status: "closed".to_string(),
        acceptance_criteria: "- strict-diff behavior is proven".to_string(),
        close_reason: "\
## Closure Evidence Manifest

| AC | Evidence kind | Surface | Evidence | Command | Status |
| --- | --- | --- | --- | --- | --- |
| AC-1 | schema | strict-diff report | inline check drift is rejected by strict-diff output | xtask test -p sinex-schema -E 'test(strict_diff)' | Satisfied |
"
        .to_string(),
    };
    let evidence = collect_closure_evidence(&payload);
    assert_eq!(evidence.commands.len(), 1);
    assert_eq!(
        evidence.commands[0].source,
        "close_reason:manifest:AC-1"
    );
    assert_eq!(evidence.manifest_items.len(), 1);
    assert_eq!(evidence.manifest_items[0].source, "close_reason");
    assert!(validate_closure_evidence_manifest(&evidence.manifest_items).is_empty());
    Ok(())
}

#[sinex_test]
async fn closure_evidence_readiness_rejects_commands_without_manifest()
-> ::xtask::sandbox::TestResult<()> {
    let evidence = ClosureEvidence {
        commands: vec![ClosureCommand {
            command: "xtask check -p xtask".to_string(),
            source: "comment[0]".to_string(),
        }],
        matrix_items: Vec::new(),
        manifest_items: Vec::new(),
    };

    let errors = validate_closure_evidence_readiness(&evidence);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].source, "comment[0]");
    assert!(
        errors[0].reason.contains("Closure Evidence Manifest row"),
        "commands-only closeout must ask for AC-to-behavior evidence: {errors:?}"
    );
    Ok(())
}

#[sinex_test]
async fn closure_evidence_readiness_rejects_matrix_without_manifest()
-> ::xtask::sandbox::TestResult<()> {
    let evidence = ClosureEvidence {
        commands: Vec::new(),
        matrix_items: vec![ClosureMatrixItem {
            source: "body".to_string(),
            status: "checked".to_string(),
            text: "AC-1 satisfied by PR".to_string(),
        }],
        manifest_items: Vec::new(),
    };

    let errors = validate_closure_evidence_readiness(&evidence);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].source, "body");
    assert!(
        errors[0].reason.contains("Closure Evidence Manifest row"),
        "matrix-only closeout must map AC claims to behavior evidence: {errors:?}"
    );
    Ok(())
}

#[sinex_test]
async fn closure_evidence_readiness_accepts_behavior_manifest_without_commands()
-> ::xtask::sandbox::TestResult<()> {
    let evidence = ClosureEvidence {
        commands: Vec::new(),
        matrix_items: Vec::new(),
        manifest_items: vec![ClosureEvidenceManifestItem {
            source: "body".to_string(),
            ac_id: "AC-1".to_string(),
            status: "satisfied".to_string(),
            evidence_kind: "runtime".to_string(),
            surface: "replay fake runtime integration test".to_string(),
            evidence: "fake scan runtime failures propagate through the join helper".to_string(),
            command: Some("xtask test -p sinexd -E 'test(replay_control)'".to_string()),
            artifact: None,
        }],
    };

    assert!(
        validate_closure_evidence_readiness(&evidence).is_empty(),
        "manifest rows that name behavior surfaces should satisfy readiness"
    );
    Ok(())
}

#[sinex_test]
async fn preview_output_truncates_long_text() -> ::xtask::sandbox::TestResult<()> {
    let long = "a".repeat(300);
    let preview = preview_output(long.as_bytes(), 200);
    assert!(
        preview.chars().count() <= 210,
        "preview too long: {}",
        preview.chars().count()
    );
    assert!(preview.ends_with('…'), "should end with ellipsis");
    Ok(())
}

#[sinex_test]
async fn preview_output_preserves_short_text() -> ::xtask::sandbox::TestResult<()> {
    let short = b"hello world";
    let preview = preview_output(short, 200);
    assert_eq!(preview, "hello world");
    Ok(())
}

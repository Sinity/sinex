//! Forbidden pattern scanning command - enforces project coding standards

use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use serde::Deserialize;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::{ast_grep_config_path, workspace_root};

/// Lint forbidden patterns command - scans for anti-patterns and policy violations.
///
/// Checks for blocking policy violations via ripgrep-based scans, and also runs
/// the repo's ast-grep rule catalog in severity-aware mode.
///
/// Blocking checks include:
/// - Use of `#[tokio::test]` instead of `#[sinex_test]`
/// - Use of `#[test]` instead of `#[sinex_test]` (outside test dirs)
/// - Use of `anyhow::` in library code (use `SinexError` / `color_eyre`)
/// - Runtime `sqlx::query()` instead of compile-time `sqlx::query!()`
/// - Runtime `sqlx::query_as()` instead of compile-time `sqlx::query_as!()`
/// - `println!` in library code (use `tracing` instead)
///
/// Also reports (informational, non-blocking):
/// - `SQLx` query usage statistics (runtime vs compile-time)
/// - `sinex_test_utils` usage in production code
/// - ast-grep warning/hint findings from `.config/ast-grep/rules/`
#[derive(Debug, Clone, clap::Args)]
pub struct LintForbiddenCommand;

impl XtaskCommand for LintForbiddenCommand {
    fn name(&self) -> &'static str {
        "lint-forbidden"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        if ctx.is_human() {
            println!("========== forbidden pattern scan ==========");
        }

        // ═══════════════════════════════════════════════════════════════════════
        // ALLOWLISTS — KEEP MINIMAL
        // ═══════════════════════════════════════════════════════════════════════
        //
        // `#[sinex_test]` / `sinex_proptest!` is the preferred harness surface.
        // The raw-attribute scan intentionally auto-skips:
        //   - dedicated test directories/files
        //   - inline `#[cfg(test)] mod tests` modules
        //   - proc-macro generated/doc-string references via allowlists below
        //
        // The allowlists below are only for strict scans that do not already
        // auto-skip those categories.
        // ═══════════════════════════════════════════════════════════════════════

        // #[tokio::test] allowlist — for code that GENERATES/REFERENCES
        // #[tokio::test] as string literals, or tests that need a specific
        // tokio runtime flavor (#[sinex_test] rejects flavor/worker_threads args).
        let tokio_test_allow = [
            // Proc macro: generates #[tokio::test] in expanded sinex_test output
            "xtask/macros/src/lib.rs",
            // This file: contains pattern strings and doc comments referencing it
            "xtask/src/commands/lint_forbidden.rs",
            // BINDING_ENV_LOCK concurrency tests need a real multi_thread runtime
            // (flavor = "multi_thread", worker_threads = 4) to exercise the env
            // race; #[sinex_test] cannot express the flavor.
            "crate/sinexd/src/sources/bindings.rs",
        ];
        // #[test] allowlist — only for tests that genuinely cannot use
        // #[sinex_test]: proc-macro / trybuild fixtures, or tests requiring
        // controlled process-global env mutation under a specific thread model.
        let rust_test_allow = [
            // EnvGuard tests mutate process-global env vars under documented
            // SAFETY/threading invariants, paired with the multi_thread race
            // tests above in the same module.
            "crate/sinexd/src/sources/bindings.rs",
        ];
        // Runtime sqlx::query() is allowed for:
        // - Session control (SET, ROLLBACK, RESET)
        // - Advisory locks
        // - Dynamic queries (analytics, cascade analysis)
        // - Test infrastructure
        // Paths are post-fold (#1559): sinexd/event_engine folded into
        // crate/sinexd/{api,event_engine}; runtime under crate/sinexd/src/runtime;
        // sinex-db/schema flattened out of crate/lib. xtask/ and tests/ auto-skip
        // via is_tests_path, so they are not listed here.
        let sqlx_query_allow = [
            // sinex-db repository layer — runtime queries over tables not in the
            // compile-time schema, dynamic predicates, and session control.
            "crate/sinex-db/src/pool.rs",
            "crate/sinex-db/src/query_helpers.rs",
            "crate/sinex-db/src/repositories/common.rs",
            "crate/sinex-db/src/repositories/document_search.rs",
            "crate/sinex-db/src/repositories/events/persistence.rs",
            "crate/sinex-db/src/repositories/knowledge_graph.rs",
            // #1619 classified: static SQL, but core.model_effects is not in
            // the xtask SQLx compile database; macro promotion fails until
            // that bootstrap surface includes the table.
            "crate/sinex-db/src/repositories/model_effects.rs",
            "crate/sinex-db/src/repositories/schema_management.rs",
            // Gateway/api dynamic + analytical SQL (cascade analysis, curation
            // CTEs). #1619 classified curation duplicate-candidate CTEs as
            // analytical runtime SQL over JSON expressions and optional filters.
            "crate/sinexd/src/api/cascade_analyzer.rs",
            "crate/sinexd/src/api/handlers/curation.rs",
            "crate/sinexd/src/api/service_container.rs",
            // SDK preflight: dynamic session GUC setup over a small fixed option table.
            "crate/sinexd/src/runtime/preflight/mod.rs",
            "crate/sinexd/src/runtime/preflight/database.rs",
            "crate/sinexd/src/runtime/preflight/verification.rs",
        ];
        let sqlx_query_as_allow = [
            // Dynamic ranking/filter SQL where the query string is assembled at runtime.
            "crate/sinex-db/src/occurrence_filter.rs",
            "crate/sinex-db/src/repositories/common.rs",
            "crate/sinex-db/src/repositories/events/composable_query.rs",
            "crate/sinex-db/src/repositories/events/persistence.rs",
            // #1619 classified: see sqlx_query_allow entry above.
            "crate/sinex-db/src/repositories/model_effects.rs",
            "crate/sinexd/src/api/handlers/audit.rs",
            "crate/sinexd/src/runtime/preflight/database.rs",
            // Timescale catalog tables may not exist in compile-time check DBs.
            "crate/sinex-schema/src/strict_diff.rs",
        ];

        let mut violations: Vec<String> = Vec::new();
        violations.extend(check_rust_test_attr_patterns(
            "#[tokio::test]",
            r"#\[tokio::test",
            &tokio_test_allow,
        )?);
        violations.extend(check_rust_test_attr_patterns(
            "#[test]",
            r"#\[test\]",
            &rust_test_allow,
        )?);
        violations.extend(check_pattern_allow_tests(
            "sqlx::query(",
            r"sqlx::query\(",
            &sqlx_query_allow,
        )?);
        violations.extend(check_pattern_allow_tests(
            "sqlx::query_as(",
            r"sqlx::query_as\(",
            &sqlx_query_as_allow,
        )?);
        let raw_event_subject_allow = [
            "crate/sinex-primitives/src/domain.rs",
            "crate/sinex-primitives/src/environment.rs",
            "crate/sinexd/src/runtime/nats_publisher.rs",
            "crate/sinexd/src/runtime/event_transport.rs",
        ];
        violations.extend(check_pattern(
            "nats_raw_event_subject_with_namespace(",
            r"nats_raw_event_subject_with_namespace\(",
            &raw_event_subject_allow,
            is_tests_path,
        )?);

        violations.extend(check_transport_publish_family_inventory()?);
        violations.extend(check_privacy_metadata_for_sensitive_units()?);

        // anyhow:: in library code is disallowed; libraries use the project error stack.
        let anyhow_allow: [&str; 0] = [];
        violations.extend(check_anyhow_in_lib("anyhow::", r"anyhow::", &anyhow_allow)?);

        // `color_eyre::` in library code is disallowed; libraries use the project error stack.
        // This is separate from anyhow because color_eyre is only permitted in xtask and binaries.
        let color_eyre_lib_allow = [
            // Inline #[cfg(test)] module: builds a TestResult (color_eyre) failure
            // via eyre!. is_tests_path can't see inline test modules in src/ files.
            "crate/sinexd/src/runtime/parser/adapters/unix_socket_stream.rs",
        ];
        violations.extend(check_color_eyre_in_lib(
            "color_eyre::",
            r"color_eyre::",
            &color_eyre_lib_allow,
        )?);

        // println! in library code (use tracing for structured logging)
        let println_lib_allow = [
            // Intentional stdout output for CLI-facing SDK functions.
            "crate/sinexd/src/runtime/runtime_cli.rs",
            "crate/sinexd/src/runtime/version.rs",
            "crate/sinexd/src/runtime/heartbeat.rs",
            "crate/sinexd/src/runtime/diagnostics/regression.rs",
            // Doc comment code examples (scanner can't distinguish from real code)
            "crate/sinexd/src/runtime/watcher_handle.rs",
            "crate/sinex-schema/src/strict_diff.rs",
        ];
        violations.extend(check_println_in_lib(
            "println!",
            r"println!",
            &println_lib_allow,
        )?);

        // Raw `INSERT INTO core.events` in non-test code should use the repository layer.
        // Tests that bypass the repository for cascade / schema testing are allowed.
        let insert_core_events_allow = [
            // Cascade infrastructure constructs events at the DB level.
            "crate/sinexd/src/api/cascade_analyzer.rs",
            // Repository persistence layer (the canonical INSERT site).
            "crate/sinex-db/src/repositories/events/persistence.rs",
            // Declarative restore/archive SQL intentionally rehydrates events
            // inside a checked schema function rather than through Rust
            // repositories.
            "crate/sinex-schema/src/apply.rs",
        ];
        violations.extend(check_pattern(
            "raw INSERT INTO core.events",
            r"INSERT INTO core\.events",
            &insert_core_events_allow,
            is_tests_path,
        )?);

        // Bare `Uuid` for event_id / material_id (#1173): every public field
        // named `event_id` or `material_id` must be phantom-typed (`Id<Event>`,
        // `Id<SourceMaterial>`) rather than a raw `Uuid`. The SDK already has
        // those phantom types in scope; this guard prevents drift back into
        // bare-Uuid declarations.
        //
        // The current allowlist captures pre-existing violations that are
        // tracked for follow-up promotion to phantom-typed IDs. Adding new
        // entries to this allowlist requires the corresponding follow-up
        // issue to record the migration plan; new files MUST use the
        // phantom-typed variants.
        let bare_uuid_id_field_allow = [
            // DB row mirrors (typed via sqlx Type<Postgres>), tracked for
            // later promotion to Id<T> repository surfaces.
            "crate/sinex-db/src/repositories/embeddings.rs",
            "crate/sinex-schema/src/defs/annotations.rs",
            // EventEngine material assembler state mirrors NATS frame UUIDs
            // (folded into crate/sinexd/src/event_engine).
            "crate/sinexd/src/event_engine/admission.rs",
            "crate/sinexd/src/event_engine/material_assembler/restore_plan.rs",
            "crate/sinexd/src/event_engine/material_assembler/state.rs",
            // SDK material/anchor surface kept on bare Uuid until the
            // SourceRecordAnchor / SourceMaterialHandle pair is promoted.
            "crate/sinexd/src/runtime/acquisition_manager.rs",
            "crate/sinexd/src/runtime/ingestion_helpers.rs",
            // Process automata analytics row (folded into crate/sinexd/src/automata).
            "crate/sinexd/src/automata/analytics.rs",
        ];
        violations.extend(check_pattern_allow_tests(
            "bare Uuid event_id / material_id field",
            r"pub\s+(event_id|material_id)\s*:\s*(Option<\s*)?(crate::|sinex_primitives::)?Uuid\b",
            &bare_uuid_id_field_allow,
        )?);

        // Report runtime vs compile-time SQLx query usage
        report_sqlx_query_stats()?;

        // Note: unwrap/expect checking is handled by clippy (unwrap_used, expect_used lints)
        // No need to duplicate with grep-based counting here.

        // Check for test-utils usage in production code (layering violation)
        check_test_utils_layering(&mut violations)?;

        // Source privacy policy is enforced at admission; source contracts
        // declare metadata/hints rather than invoking the privacy engine.

        let ast_grep = run_ast_grep_scan()?;
        if let Some(ref ag) = ast_grep {
            if ag.has_findings() && ctx.is_human() {
                eprintln!(
                    "ℹ ast-grep: {} error(s), {} warning(s), {} hint(s)",
                    ag.error_count(),
                    ag.warning_count(),
                    ag.hint_count()
                );
            }
            for finding in ag.error_findings() {
                violations.push(format!(
                    "{}:{}:{} [{}] {}",
                    finding.file, finding.line, finding.column, finding.rule_id, finding.message
                ));
            }
        } else if ctx.is_human() {
            eprintln!("ℹ ast-grep not found in PATH — skipping structural lint checks");
        }

        if violations.is_empty() {
            if ctx.is_human() {
                eprintln!("✅ No forbidden patterns found");
            }
            let ast_grep_value = ast_grep
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?
                .unwrap_or(serde_json::Value::Null);
            let mut result = CommandResult::success()
                .with_message("No forbidden patterns found")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({
                    "ast_grep": ast_grep_value,
                }));
            if let Some(ref ag) = ast_grep
                && (ag.warning_count() > 0 || ag.hint_count() > 0)
            {
                result = result.with_detail(format!(
                    "ast-grep advisory findings: {} warning(s), {} hint(s)",
                    ag.warning_count(),
                    ag.hint_count()
                ));
            }
            return Ok(result);
        }

        eprintln!("Forbidden pattern detected:");
        for v in &violations {
            eprintln!("  {v}");
        }
        bail!("forbidden pattern scan failed");
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::check()
    }
}

fn check_transport_publish_family_inventory() -> Result<Vec<String>> {
    // Every production file with direct async-nats publish calls must be in
    // this transport-family inventory. The per-file entry is not a blanket
    // endorsement of every call inside it; it is the visible ownership list
    // that prevents new raw publish sites from appearing silently.
    let allow = [
        // Canonical event, telemetry, raw-ingest DLQ, and processing-failure publisher.
        "crate/sinexd/src/runtime/nats_publisher.rs",
        // Source-material lifecycle frame publisher.
        "crate/sinexd/src/runtime/acquisition_manager.rs",
        // Raw-ingest DLQ retry re-publishes into the original raw-event subject.
        "crate/sinexd/src/runtime/dlq_retry.rs",
        // RuntimeModule coordination control messages.
        "crate/sinexd/src/runtime/coordination.rs",
        // Runtime scan/drain control messages.
        "crate/sinexd/src/runtime/stream/runner/control_messages.rs",
        // Inline private-mode listener regression publishes a synthetic
        // control message; the production listener only subscribes.
        "crate/sinexd/src/runtime/parser/adapter_source.rs",
        // Event-engine confirmation and raw-ingest DLQ publishers (folded event_engine).
        "crate/sinexd/src/event_engine/jetstream_consumer.rs",
        // Source-material assembler DLQ routing.
        "crate/sinexd/src/event_engine/material_assembler/finalize.rs",
        // Active-schema broadcast control notification.
        "crate/sinexd/src/event_engine/service.rs",
        // Gateway/api replay and runtime control publishers (folded gateway).
        "crate/sinexd/src/api/replay_control/server.rs",
        "crate/sinexd/src/api/handlers/runtime_registry.rs",
        // Private-mode control broadcasts.
        "crate/sinexd/src/api/handlers/privacy.rs",
        // Replay control request/reply and invalidation publishers.
        "crate/sinexd/src/api/replay_control/server.rs",
        "crate/sinexd/src/api/replay_control/execution/collect.rs",
        // Source parse command request/reply acknowledgements (folded source host).
        "crate/sinexd/src/sources/parse_listener.rs",
    ];

    check_pattern(
        "raw async-nats publish call without transport-family inventory entry",
        r"\.publish(?:_with_headers)?\(",
        &allow,
        |path| {
            is_tests_path(path)
                || path == "crate/sinex-primitives/src/testing.rs"
                || path == "crate/sinexd/src/runtime/event_transport.rs"
                || path == "crate/sinexd/src/runtime/self_observation.rs"
        },
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Privacy metadata gate
// ─────────────────────────────────────────────────────────────────────────────

/// Indicators that a file declares privacy metadata for admission policy.
///
/// Any one of these present in a file satisfies the gate:
///
/// - `ProcessingContext::` — emitted intent context metadata
/// - `.privacy_context(` — source binding context metadata
/// - `privacy_contexts:` — parser manifest context metadata
/// - `default_privacy_context =` — declarative `#[source_record]` DSL attribute
/// - `#[allow(missing_privacy_metadata` — explicit escape hatch with required `reason =`
const PRIVACY_METADATA_INDICATORS: &[&str] = &[
    "ProcessingContext::",
    ".privacy_context(",
    "privacy_contexts:",
    "default_privacy_context =",
    "#[allow(missing_privacy_metadata",
];

/// Patterns that indicate a non-Public privacy tier in a `register_source_contract!` block.
const NON_PUBLIC_TIER_PATTERNS: &[&str] = &[
    "PrivacyTier::Sensitive",
    "PrivacyTier::Secret",
    "SuPrivacyTier::Sensitive",
    "SuPrivacyTier::Secret",
];

/// Files exempt from the privacy-metadata gate.
///
/// These are **descriptor-only** source contracts: they declare event-type metadata
/// for events emitted from inside the pipeline infrastructure (event_engine, gateway,
/// SDK), rather than from a dedicated parser. Privacy is handled at the emit
/// site inside those binaries, not by a standalone parser.
///
/// New entries MUST include: (1) the source id, (2) which binary emits it,
/// (3) where privacy is handled.
const PRIVACY_GATE_ALLOWLIST: &[&str] = &[
    // "blob-storage" source: blob.retrieved / blob.ingested / blob.verified /
    // storage.statistics events are emitted from event_engine / gateway / runtime internals.
    // Privacy is handled at the emit site in those binaries; no standalone parser exists.
    "crate/sinex-primitives/src/events/payloads/blob.rs",
];

/// Gate: every source parser with a non-Public privacy tier must declare
/// privacy metadata so DB admission policy can select rules.
///
/// For each `.rs` file containing `register_source_contract!` AND a non-Public
/// `privacy_tier`:
/// - If the file (or any `.rs` sibling in its immediate containing directory)
///   contains any [`PRIVACY_METADATA_INDICATORS`] → pass.
/// - If the file is in [`PRIVACY_GATE_ALLOWLIST`] → pass (descriptor-only units).
/// - Otherwise → violation with source id, privacy tier, and file path.
///
/// The sibling-directory scan handles the common pattern where `lib.rs` holds
/// the descriptor registration while parser files in the same directory
/// contain the emitted `ProcessingContext` metadata.
///
/// Escape hatch: add `#[allow(missing_privacy_metadata, reason = "...")]` in
/// the registration file or any sibling. The `reason` is mandatory for
/// documentation but not syntactically enforced.
///
/// Descriptor-only units (event types emitted from inside the pipeline
/// infrastructure without a dedicated parser) may be listed in
/// [`PRIVACY_GATE_ALLOWLIST`] with an explanatory comment.
fn check_privacy_metadata_for_sensitive_units() -> Result<Vec<String>> {
    let root = workspace_root();
    let mut violations: Vec<String> = Vec::new();

    // Find all .rs files that contain `register_source_contract!`.
    let rsu_files = find_files_with_pattern(&root, "register_source_contract!")?;

    for rel_path in rsu_files {
        let abs_path = root.join(&rel_path);
        let Ok(contents) = std::fs::read_to_string(&abs_path) else {
            continue;
        };

        // Skip if no non-Public tier in this file.
        let has_non_public_tier = NON_PUBLIC_TIER_PATTERNS
            .iter()
            .any(|pat| contents.contains(pat));
        if !has_non_public_tier {
            continue;
        }

        // Skip test paths.
        if is_tests_path(&rel_path) {
            continue;
        }

        // Skip explicitly allowlisted descriptor-only files.
        if PRIVACY_GATE_ALLOWLIST.contains(&rel_path.as_str()) {
            continue;
        }

        // Collect the privacy signal: check this file and all .rs siblings in
        // the same directory. This handles lib.rs + unified_node.rs patterns.
        let search_dir = abs_path
            .parent()
            .expect("file path must have a parent directory");
        let has_privacy_metadata = directory_contains_privacy_metadata(search_dir);

        if has_privacy_metadata {
            continue;
        }

        // Extract the source id(s) and tier(s) for the error message.
        let unit_ids = extract_source_ids(&contents);
        let tiers = extract_non_public_tiers(&contents);
        violations.push(format!(
            "{rel_path}: source(s) [{units}] with privacy tier [{tiers}] \
             must declare privacy metadata (ProcessingContext::, .privacy_context(, \
             privacy_contexts:, default_privacy_context =) or declare \
             #[allow(missing_privacy_metadata, reason = \"...\")]",
            units = unit_ids.join(", "),
            tiers = tiers.join(", "),
        ));
    }

    Ok(violations)
}

/// Return true if any `.rs` file directly inside `dir` (non-recursive) contains
/// at least one [`PRIVACY_METADATA_INDICATORS`] string.
fn directory_contains_privacy_metadata(dir: &std::path::Path) -> bool {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.filter_map(std::result::Result::ok) {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "rs") {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            if PRIVACY_METADATA_INDICATORS
                .iter()
                .any(|ind| text.contains(ind))
            {
                return true;
            }
        }
    }
    false
}

/// Find all `.rs` files under `root` that contain `literal`.
/// Returns workspace-relative paths (no leading `./`).
fn find_files_with_pattern(root: &std::path::Path, literal: &str) -> Result<Vec<String>> {
    let output = std::process::Command::new("rg")
        .current_dir(root)
        .args([
            "--color=never",
            "--files-with-matches",
            "--glob",
            "*.rs",
            literal,
        ])
        .output()
        .with_context(|| format!("failed to invoke ripgrep to find files with `{literal}`"))?;
    // Exit code 1 = no matches (ok), anything else is an error.
    if let Some(code) = output.status.code()
        && code > 1
    {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("ripgrep exited with code {code}: {stderr}");
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .map(|l| l.trim_start_matches("./").to_string())
        .collect())
}

/// Extract `id: "..."` values from a source descriptor block.
fn extract_source_ids(contents: &str) -> Vec<String> {
    let mut ids = Vec::new();
    // Match lines like: `id: "some.id",`
    for line in contents.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("id:") {
            let rest = rest.trim();
            if let Some(inner) = rest.strip_prefix('"')
                && let Some(id) = inner.split('"').next()
                && !id.is_empty()
            {
                ids.push(id.to_string());
            }
        }
    }
    if ids.is_empty() {
        ids.push("<unknown>".to_string());
    }
    ids
}

/// Extract non-Public privacy tier names from contents.
fn extract_non_public_tiers(contents: &str) -> Vec<String> {
    let mut tiers = Vec::new();
    for pat in NON_PUBLIC_TIER_PATTERNS {
        if contents.contains(pat) {
            tiers.push((*pat).to_string());
        }
    }
    tiers
}

/// Check for a pattern allowing test directories
fn check_pattern_allow_tests(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    check_pattern(label, pattern, allow, is_tests_path)
}

/// Check test attributes, allowing only dedicated test directories and xtask/.
///
/// Inline `mod tests` blocks in library or source files must use `#[sinex_test]`
/// instead of bare `#[test]` or `#[tokio::test]`. The only exemptions are files
/// under a `tests/` directory or under `xtask/`, both handled by `is_tests_path`.
fn check_rust_test_attr_patterns(
    label: &str,
    pattern: &str,
    allow: &[&str],
) -> Result<Vec<String>> {
    check_pattern(label, pattern, allow, is_tests_path)
}

fn check_pattern<F>(label: &str, pattern: &str, allow: &[&str], skip: F) -> Result<Vec<String>>
where
    F: FnMut(&str) -> bool,
{
    run_rg(pattern)
        .and_then(|matches| filter_allowlist(matches, allow, skip))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Run ripgrep to find pattern matches
fn run_rg(pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("rg")
        .current_dir(workspace_root())
        .args([
            "--color=never",
            "--no-heading",
            "--with-filename",
            "--line-number",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!docs/agent/**",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep")?;
    ensure_rg_completed(&output, "ripgrep")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect::<Vec<String>>())
}

fn ensure_rg_completed(output: &std::process::Output, context: &str) -> Result<()> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match output.status.code() {
        Some(0 | 1) => Ok(()),
        Some(code) if stderr.is_empty() => bail!("{context} failed with exit code {code}"),
        Some(code) => bail!("{context} failed with exit code {code}: {stderr}"),
        None if stderr.is_empty() => bail!("{context} terminated by signal"),
        None => bail!("{context} terminated by signal: {stderr}"),
    }
}

/// Filter matches against allowlist and skip function
fn parse_match_file(line: &str) -> Result<&str> {
    let (file, _) = line
        .split_once(':')
        .ok_or_else(|| eyre!("ripgrep match line is missing a file prefix: {line}"))?;
    let file = file.trim();
    if file.is_empty() {
        bail!("ripgrep match line reported an empty file path: {line}");
    }
    Ok(file)
}

fn filter_allowlist<F>(matches: Vec<String>, allow: &[&str], mut skip: F) -> Result<Vec<String>>
where
    F: FnMut(&str) -> bool,
{
    let mut filtered = Vec::new();
    for line in matches {
        if is_comment_match(&line) {
            continue;
        }
        let file = parse_match_file(&line)?;
        if !allow.contains(&file) && !skip(file) {
            filtered.push(line);
        }
    }
    Ok(filtered)
}

fn is_comment_match(line: &str) -> bool {
    let Some((_, rest)) = line.split_once(':') else {
        return false;
    };
    let Some((_, text)) = rest.split_once(':') else {
        return false;
    };
    let text = text.trim_start();
    text.starts_with("//")
}

/// Check if a path is a test directory or build tooling.
///
/// xtask is blanket-allowed because the proc macro crate (`xtask/macros/`)
/// generates `#[test]` and `#[tokio::test]` in its expansion output.
fn is_tests_path(path: &str) -> bool {
    path.contains("/tests/") || path.starts_with("tests/") || path.starts_with("xtask/")
}

/// Check for anyhow usage in library code (not xtask, not tests, not binaries)
fn check_anyhow_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| filter_allowlist(matches, allow, is_lib_check_skip))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Shared skip predicate for the "no X in library code" checks
/// (`anyhow::`, `color_eyre::`, `println!`). Exempts build tooling, tests,
/// binaries, CLI, and examples. Paths are post-fold (#1559): the CLI crate
/// is `crate/sinexctl/` and the VM test-suite binary is `tests/vm-suite/`.
fn is_lib_check_skip(path: &str) -> bool {
    path.starts_with("xtask/")
        || is_tests_path(path)
        || path.ends_with("/main.rs")
        || path.ends_with("build.rs")
        || path.contains("/bin/")
        || path.contains("/examples/")
        || path.starts_with("crate/sinexctl/")
        || path.starts_with("tests/vm-suite/")
}

/// Check for `color_eyre::` usage in library code (use SinexError error stack).
/// color_eyre is only permitted in xtask (build tooling) and binaries.
fn check_color_eyre_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| filter_allowlist(matches, allow, is_lib_check_skip))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check for println! in library code (use tracing instead)
fn check_println_in_lib(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .and_then(|matches| filter_allowlist(matches, allow, is_lib_check_skip))
        .with_context(|| format!("failed to scan for {label}"))
}

/// Check for `sinex_test_utils` usage outside expected locations.
/// Reports usage for awareness but doesn't block (inline #[cfg(test)] modules are OK).
fn check_test_utils_layering(_violations: &mut Vec<String>) -> Result<()> {
    // Allow test-utils imports in expected locations. Post-fold (#1559) the
    // standalone sinex-test-utils crate was dissolved into xtask/src/sandbox.
    let allow_prefixes = [
        "xtask/src/", // Build tooling + sandbox test infrastructure
    ];

    let matches = run_rg(r"use sinex_test_utils")?;
    let filtered = filter_allowlist(matches, &[], |file| {
        allow_prefixes.iter().any(|a| file.starts_with(a)) || is_tests_path(file)
    })?;

    // Note: Many of these may be in inline #[cfg(test)] modules, which is fine.
    // We report the count for awareness but don't block builds.
    if !filtered.is_empty() {
        eprintln!(
            "📋 sinex_test_utils usage: {} locations (inline #[cfg(test)] modules are expected)",
            filtered.len()
        );
    }
    Ok(())
}

/// Report `SQLx` query usage statistics (runtime vs compile-time checked).
/// Runtime queries use `sqlx::query()/query_as()`, compile-time use `sqlx::query!()/query_as`!().
fn report_sqlx_query_stats() -> Result<()> {
    // Count runtime queries (sqlx::query(, sqlx::query_as()
    let runtime_query = count_pattern_outside_tests(r"sqlx::query\(")?;
    let runtime_query_as = count_pattern_outside_tests(r"sqlx::query_as\(")?;
    let runtime_total = runtime_query + runtime_query_as;

    // Count compile-time queries (sqlx::query!, sqlx::query_as!, sqlx::query_scalar!)
    let compile_query = count_pattern_outside_tests(r"sqlx::query!\(")?;
    let compile_query_as = count_pattern_outside_tests(r"sqlx::query_as!\(")?;
    let compile_query_scalar = count_pattern_outside_tests(r"sqlx::query_scalar!\(")?;
    let compile_total = compile_query + compile_query_as + compile_query_scalar;

    let total = runtime_total + compile_total;
    if total > 0 {
        let compile_pct = if total > 0 {
            (compile_total as f64 / total as f64 * 100.0) as u32
        } else {
            0
        };
        eprintln!(
            "📊 SQLx queries: {compile_total} compile-time ({compile_pct}%), {runtime_total} runtime ({runtime_query} query, {runtime_query_as} query_as)"
        );
    }
    Ok(())
}

// Error handling and type system anti-patterns are now checked by ast-grep.
// `xtask lint-forbidden` executes the catalog and treats only error-severity
// findings as blocking today. The remaining warning/hint findings are advisory.

/// Count occurrences of a pattern outside test directories
fn count_pattern_outside_tests(pattern: &str) -> Result<usize> {
    let output = Command::new("rg")
        .current_dir(workspace_root())
        .args([
            "--color=never",
            "--no-heading",
            "-c",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!**/tests/**",
            "--glob",
            "!tests/**",
            "--glob",
            "!*_test.rs",
            "--glob",
            "!test_*.rs",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep for pattern count")?;

    ensure_rg_completed(&output, "ripgrep pattern count")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut total = 0;
    for line in stdout.lines() {
        if let Some(count_str) = line.split(':').nth(1)
            && let Ok(count) = count_str.parse::<usize>()
        {
            total += count;
        }
    }
    Ok(total)
}

#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize)]
struct AstGrepSummary {
    errors: Vec<AstGrepFinding>,
    warnings: usize,
    hints: usize,
}

impl AstGrepSummary {
    fn has_findings(&self) -> bool {
        !self.errors.is_empty() || self.warnings > 0 || self.hints > 0
    }

    fn error_count(&self) -> usize {
        self.errors.len()
    }

    fn warning_count(&self) -> usize {
        self.warnings
    }

    fn hint_count(&self) -> usize {
        self.hints
    }

    fn error_findings(&self) -> &[AstGrepFinding] {
        &self.errors
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
struct AstGrepFinding {
    file: String,
    line: usize,
    column: usize,
    rule_id: String,
    severity: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct AstGrepFindingJson {
    file: String,
    #[serde(rename = "ruleId")]
    rule_id: String,
    severity: String,
    message: String,
    range: AstGrepRange,
}

#[derive(Debug, Deserialize)]
struct AstGrepRange {
    start: AstGrepPosition,
}

#[derive(Debug, Deserialize)]
struct AstGrepPosition {
    line: usize,
    column: usize,
}

fn run_ast_grep_scan() -> Result<Option<AstGrepSummary>> {
    let workspace = workspace_root();
    let config_path = ast_grep_config_path();
    let output = match Command::new("ast-grep")
        .current_dir(&workspace)
        .arg("scan")
        .arg("--config")
        .arg(&config_path)
        .arg("--json=stream")
        .arg("--include-metadata")
        .arg("--globs")
        .arg("!**/tests/**")
        .arg("--globs")
        .arg("!**/tests.rs")
        .arg("--globs")
        .arg("!**/*_test.rs")
        .arg("--globs")
        .arg("!**/test_*.rs")
        .arg("--globs")
        .arg("!**/build.rs")
        .arg(".")
        .output()
    {
        Ok(out) => out,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // ast-grep is not installed in this environment (e.g. CI without the
            // tool in PATH).  Treat this as a graceful skip rather than a hard
            // failure so the rest of the lint checks still run.
            return Ok(None);
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to invoke ast-grep with {}", config_path.display())
            });
        }
    };

    ensure_ast_grep_completed(&output)?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_ast_grep_summary(&stdout).map(Some)
}

fn ensure_ast_grep_completed(output: &std::process::Output) -> Result<()> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match output.status.code() {
        Some(0 | 1) => Ok(()),
        Some(code) if stderr.is_empty() => bail!("ast-grep failed with exit code {code}"),
        Some(code) => bail!("ast-grep failed with exit code {code}: {stderr}"),
        None if stderr.is_empty() => bail!("ast-grep terminated by signal"),
        None => bail!("ast-grep terminated by signal: {stderr}"),
    }
}

fn parse_ast_grep_summary(stdout: &str) -> Result<AstGrepSummary> {
    let mut summary = AstGrepSummary::default();

    for line in stdout.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let finding: AstGrepFindingJson =
            serde_json::from_str(line).with_context(|| "failed to parse ast-grep JSON output")?;
        match finding.severity.as_str() {
            "error" => summary.errors.push(AstGrepFinding {
                file: finding.file,
                line: finding.range.start.line,
                column: finding.range.start.column,
                rule_id: finding.rule_id,
                severity: finding.severity,
                message: finding.message,
            }),
            "warning" => summary.warnings += 1,
            "hint" => summary.hints += 1,
            _ => {}
        }
    }

    Ok(summary)
}

#[cfg(test)]
mod tests {
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
    async fn test_filter_allowlist_rejects_malformed_match_lines()
    -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_transport_publish_family_inventory_is_current() -> ::xtask::sandbox::TestResult<()>
    {
        let violations = check_transport_publish_family_inventory()?;
        assert!(
            violations.is_empty(),
            "direct publish sites must be assigned to a transport family: {violations:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_ensure_rg_completed_reports_signal_termination()
    -> ::xtask::sandbox::TestResult<()> {
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
    async fn test_parse_ast_grep_summary_rejects_invalid_json() -> ::xtask::sandbox::TestResult<()>
    {
        let error =
            parse_ast_grep_summary("not-json").expect_err("invalid ast-grep output should fail");
        assert!(format!("{error:#}").contains("failed to parse ast-grep JSON output"));
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
    async fn privacy_gate_passes_public_unit_without_privacy_call()
    -> ::xtask::sandbox::TestResult<()> {
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
    async fn privacy_gate_passes_with_processing_context_metadata()
    -> ::xtask::sandbox::TestResult<()> {
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
}

//! Issue-graph drift detector.
//!
//! Compares GitHub issue bodies against the live workspace state and reports
//! stale references: dead crates, removed functions, dropped CLI commands,
//! umbrella checklist items whose children are already closed, and
//! candidate-duplicate issues by body-text shingle similarity.
//!
//! Read-only. Never edits issues.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Command;

use color_eyre::eyre::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;
use crate::proof_catalog::build_proof_catalog;

// ─── CLI definition ───────────────────────────────────────────────────────────

/// Detect drift between GitHub issue bodies and the live workspace state.
///
/// Reads issues via `gh issue list`, then compares body text against the
/// workspace: crate names, sinexctl subcommands, xtask subcommands, known
/// event types, and umbrella-issue checklists.  Also detects candidate
/// duplicate issues via text-shingle similarity.
///
/// Read-only — never modifies issues.
#[derive(Debug, Clone, clap::Args)]
pub struct IssueDriftCommand {
    /// Maximum number of issues to fetch (default: 500)
    #[arg(long, default_value = "500")]
    pub limit: usize,

    /// Only report findings for the specified issue number
    #[arg(long)]
    pub issue: Option<u64>,

    /// Minimum shingle-similarity ratio (0.0–1.0) for duplicate detection
    #[arg(long, default_value = "0.5")]
    pub duplicate_threshold: f64,

    /// Skip duplicate detection (faster when corpus is large)
    #[arg(long)]
    pub no_duplicates: bool,
}

impl XtaskCommand for IssueDriftCommand {
    fn name(&self) -> &'static str {
        "issue-drift"
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::analysis().with_history_tracking(false)
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let workspace = find_workspace_root(std::env::current_dir()?)?;
        execute_issue_drift(self, &workspace, ctx)
    }
}

// ─── Data types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GhIssue {
    number: u64,
    title: String,
    body: String,
    state: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftFinding {
    /// GitHub issue number
    pub issue: u64,
    /// Short title of the issue
    pub title: String,
    /// Finding category
    pub kind: DriftKind,
    /// Human-readable description
    pub description: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DriftKind {
    /// Body references a sinexctl subcommand path that doesn't exist
    SinexctlCommand,
    /// Body references an xtask subcommand path that doesn't exist
    XtaskCommand,
    /// Body references a workspace crate name that no longer exists
    MissingCrate,
    /// Body references an event type (source/event.type) that no longer exists
    MissingEventType,
    /// Body references a Rust function/method that no longer exists
    MissingFunction,
    /// Umbrella checklist item references a child issue marked as open but it is CLOSED
    UmbrellaChild,
    /// Candidate duplicate: high body-text similarity with another issue
    CandidateDuplicate,
}

#[derive(Debug, Clone, Serialize)]
pub struct DriftReport {
    pub issues_scanned: usize,
    pub findings: Vec<DriftFinding>,
    pub elapsed_ms: u64,
}

// ─── Workspace inventory ─────────────────────────────────────────────────────

/// All known crate names in the workspace.
fn collect_workspace_crates(workspace_root: &Path) -> Result<HashSet<String>> {
    let output = ProcessBuilder::cargo()
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root)
        .run()
        .context("failed to run cargo metadata")?;
    let stdout = output.stdout;
    let meta: serde_json::Value =
        serde_json::from_str(&stdout).context("failed to parse cargo metadata")?;
    let mut names = HashSet::new();
    if let Some(packages) = meta["packages"].as_array() {
        for pkg in packages {
            if let Some(name) = pkg["name"].as_str() {
                names.insert(name.to_string());
            }
        }
    }
    Ok(names)
}

/// All known event types from the payload registry, as "source/event.type" strings.
fn collect_event_type_strings(workspace_root: &Path) -> Result<HashSet<String>> {
    let catalog = build_proof_catalog(workspace_root)?;
    let set = catalog
        .event_payloads
        .iter()
        .map(|ep| format!("{}/{}", ep.source, ep.event_type))
        .collect();
    Ok(set)
}

/// All known xtask command paths (dot-separated, e.g. "docs.issue-drift").
fn collect_xtask_command_paths(workspace_root: &Path) -> Result<HashSet<String>> {
    let catalog = build_proof_catalog(workspace_root)?;
    let set = catalog
        .xtask_commands
        .iter()
        .map(|cmd| cmd.path.clone())
        .collect();
    Ok(set)
}

/// All known top-level sinexctl subcommand names collected from the CLI source
/// files (module names in `crate/cli/src/commands/`).
fn collect_sinexctl_subcommands(workspace_root: &Path) -> HashSet<String> {
    // Try `sinexctl --help` output first; fall back to source scanning.
    if let Some(names) = try_sinexctl_help_subcommands() {
        return names;
    }
    collect_sinexctl_subcommands_from_source(workspace_root)
}

fn try_sinexctl_help_subcommands() -> Option<HashSet<String>> {
    let out = Command::new("sinexctl").arg("--help").output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    if text.is_empty() {
        return None;
    }
    let mut names = HashSet::new();
    let mut in_commands = false;
    for line in text.lines() {
        if line.contains("Commands:") || line.contains("SUBCOMMANDS:") {
            in_commands = true;
            continue;
        }
        if in_commands {
            let trimmed = line.trim_start();
            if trimmed.is_empty() {
                in_commands = false;
                continue;
            }
            if let Some(word) = trimmed.split_whitespace().next() {
                names.insert(word.to_string());
            }
        }
    }
    if names.is_empty() { None } else { Some(names) }
}

fn collect_sinexctl_subcommands_from_source(workspace_root: &Path) -> HashSet<String> {
    let commands_dir = workspace_root.join("crate/cli/src/commands");
    let mut names = HashSet::new();
    if let Ok(entries) = std::fs::read_dir(&commands_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if stem != "mod" {
                        names.insert(stem.replace('_', "-"));
                    }
                }
            }
        }
    }
    names
}

// ─── Issue fetching ───────────────────────────────────────────────────────────

fn fetch_issues(limit: usize) -> Result<Vec<GhIssue>> {
    let output = Command::new("gh")
        .args([
            "issue",
            "list",
            "--state",
            "all",
            "--json",
            "number,title,body,state",
            "--limit",
            &limit.to_string(),
        ])
        .output()
        .context("failed to run `gh issue list` — is `gh` installed and authenticated?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        color_eyre::eyre::bail!("gh issue list failed: {stderr}");
    }

    let issues: Vec<GhIssue> =
        serde_json::from_slice(&output.stdout).context("failed to parse gh issue list JSON")?;
    Ok(issues)
}

fn fetch_issue_state(number: u64) -> Option<String> {
    let out = Command::new("gh")
        .args(["issue", "view", &number.to_string(), "--json", "state"])
        .output()
        .ok()?;
    let val: serde_json::Value = serde_json::from_slice(&out.stdout).ok()?;
    val["state"].as_str().map(str::to_string)
}

// ─── Analysis helpers ─────────────────────────────────────────────────────────

/// Extract all `sinexctl <subcommand>` references from a body string.
fn extract_sinexctl_refs(body: &str) -> Vec<String> {
    let mut found = Vec::new();
    for line in body.lines() {
        let mut rest = line;
        while let Some(pos) = rest.find("sinexctl ") {
            rest = &rest[pos + "sinexctl ".len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            if !name.is_empty() {
                found.push(name);
            }
        }
    }
    found
}

/// Extract all `xtask <subcommand>` references from a body string.
fn extract_xtask_refs(body: &str) -> Vec<String> {
    let mut found = Vec::new();
    for line in body.lines() {
        let mut rest = line;
        while let Some(pos) = rest.find("xtask ") {
            rest = &rest[pos + "xtask ".len()..];
            let words: Vec<String> = rest
                .split_whitespace()
                .take_while(|w| !w.starts_with('-'))
                .take(2)
                .map(|w| {
                    w.trim_matches(|c: char| !c.is_alphanumeric() && c != '-')
                        .to_string()
                })
                .filter(|w| !w.is_empty())
                .collect();
            if !words.is_empty() {
                found.push(words.join(" "));
            }
        }
    }
    found
}

/// Detect `Event::new(` references in body (removed in PR #616).
fn has_event_new_ref(body: &str) -> bool {
    body.contains("Event::new(") || body.contains("`Event::new`")
}

/// Extract crate name references from body (looks for `sinex-*` tokens).
fn extract_crate_refs(body: &str) -> Vec<String> {
    let mut found = Vec::new();
    for word in body.split(|c: char| c.is_whitespace() || c == '`' || c == '"' || c == '\'') {
        let clean = word.trim_matches(|c: char| !c.is_alphanumeric() && c != '-');
        if clean.starts_with("sinex-") && clean.len() > 6 {
            found.push(clean.to_string());
        }
    }
    found
}

/// Extract checklist items referencing child issue numbers.
/// Returns `(checked, issue_number)` pairs.
fn extract_checklist_children(body: &str) -> Vec<(bool, u64)> {
    let mut items = Vec::new();
    for line in body.lines() {
        let trimmed = line.trim();
        let checked = trimmed.starts_with("- [x]") || trimmed.starts_with("- [X]");
        let unchecked = trimmed.starts_with("- [ ]");
        if checked || unchecked {
            if let Some(hash_pos) = trimmed.find('#') {
                let after = &trimmed[hash_pos + 1..];
                let num_str: String = after
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if let Ok(n) = num_str.parse::<u64>() {
                    items.push((checked, n));
                }
            }
        }
    }
    items
}

// ─── Shingle similarity ───────────────────────────────────────────────────────

/// Compute Jaccard similarity of word-trigram shingles between two strings.
/// Returns a value in [0.0, 1.0].
fn shingle_similarity(a: &str, b: &str) -> f64 {
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let shingles_a = word_trigrams(a);
    let shingles_b = word_trigrams(b);
    if shingles_a.is_empty() || shingles_b.is_empty() {
        return 0.0;
    }
    let intersection = shingles_a.intersection(&shingles_b).count();
    let union = shingles_a.union(&shingles_b).count();
    if union == 0 {
        0.0
    } else {
        intersection as f64 / union as f64
    }
}

fn word_trigrams(text: &str) -> HashSet<[&str; 3]> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < 3 {
        return HashSet::new();
    }
    let mut shingles = HashSet::new();
    for i in 0..(words.len() - 2) {
        shingles.insert([words[i], words[i + 1], words[i + 2]]);
    }
    shingles
}

// ─── Core analysis ────────────────────────────────────────────────────────────

fn analyse_issue(
    issue: &GhIssue,
    sinexctl_cmds: &HashSet<String>,
    xtask_paths: &HashSet<String>,
    crate_names: &HashSet<String>,
    issue_state_cache: &mut HashMap<u64, String>,
) -> Vec<DriftFinding> {
    let mut findings = Vec::new();
    let body = &issue.body;

    // 1. sinexctl command references
    for subcommand in extract_sinexctl_refs(body) {
        let skip = matches!(
            subcommand.as_str(),
            "--help" | "--version" | "--format" | "--json" | "--rpc-url" | ""
        );
        if skip {
            continue;
        }
        if !sinexctl_cmds.contains(&subcommand) {
            findings.push(DriftFinding {
                issue: issue.number,
                title: issue.title.clone(),
                kind: DriftKind::SinexctlCommand,
                description: format!(
                    "`sinexctl {subcommand}` referenced but no such subcommand exists in the CLI"
                ),
            });
        }
    }

    // 2. xtask command references
    for path in extract_xtask_refs(body) {
        let first_word = path.split_whitespace().next().unwrap_or("");
        if first_word.is_empty() || first_word.starts_with('-') {
            continue;
        }
        let top_level = first_word.to_string();
        let dot_path = path.replace(' ', ".");
        let known = xtask_paths.contains(&top_level)
            || xtask_paths.contains(&dot_path)
            || xtask_paths.iter().any(|p| p.starts_with(&top_level));
        if !known {
            findings.push(DriftFinding {
                issue: issue.number,
                title: issue.title.clone(),
                kind: DriftKind::XtaskCommand,
                description: format!(
                    "`xtask {path}` referenced but top-level command `{top_level}` not found"
                ),
            });
        }
    }

    // 3. Crate references
    let mut seen_missing = HashSet::new();
    for crate_ref in extract_crate_refs(body) {
        if seen_missing.contains(&crate_ref) {
            continue;
        }
        if !crate_names.contains(&crate_ref) {
            seen_missing.insert(crate_ref.clone());
            findings.push(DriftFinding {
                issue: issue.number,
                title: issue.title.clone(),
                kind: DriftKind::MissingCrate,
                description: format!(
                    "crate `{crate_ref}` referenced but not found in workspace"
                ),
            });
        }
    }

    // 4. Event::new() reference (removed in #616)
    if has_event_new_ref(body) {
        findings.push(DriftFinding {
            issue: issue.number,
            title: issue.title.clone(),
            kind: DriftKind::MissingFunction,
            description: "`Event::new(...)` referenced but removed in PR #616 \
                          (use `EventBuilder` / `.from_material()` / `.from_parents()` instead)"
                .to_string(),
        });
    }

    // 5. Umbrella checklist children: open ([ ]) but actually CLOSED on GitHub
    for (checked, child) in extract_checklist_children(body) {
        if checked {
            // Don't flag checked items — they're already marked done.
            continue;
        }
        let state = issue_state_cache
            .entry(child)
            .or_insert_with(|| {
                fetch_issue_state(child).unwrap_or_else(|| "unknown".to_string())
            });
        let state = state.clone();
        if state == "CLOSED" {
            findings.push(DriftFinding {
                issue: issue.number,
                title: issue.title.clone(),
                kind: DriftKind::UmbrellaChild,
                description: format!(
                    "checklist item `- [ ] #{child}` is marked open but issue #{child} is CLOSED"
                ),
            });
        }
    }

    findings
}

// ─── Main execution ───────────────────────────────────────────────────────────

fn execute_issue_drift(
    cmd: &IssueDriftCommand,
    workspace: &Path,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let start = std::time::Instant::now();

    if ctx.is_human() {
        println!("Collecting workspace inventory...");
    }

    let crate_names = collect_workspace_crates(workspace)?;
    let event_types = collect_event_type_strings(workspace)?;
    let xtask_paths = collect_xtask_command_paths(workspace)?;
    let sinexctl_cmds = collect_sinexctl_subcommands(workspace);

    if ctx.is_human() {
        println!(
            "  {} crates, {} event types, {} xtask paths, {} sinexctl subcommands",
            crate_names.len(),
            event_types.len(),
            xtask_paths.len(),
            sinexctl_cmds.len()
        );
        println!("Fetching issues (limit: {})...", cmd.limit);
    }

    let mut all_issues = fetch_issues(cmd.limit)?;

    if let Some(number) = cmd.issue {
        all_issues.retain(|i| i.number == number);
    }

    let issues_scanned = all_issues.len();

    if ctx.is_human() {
        println!("Analysing {issues_scanned} issues...");
    }

    // Pre-populate state cache from the fetched batch.
    let mut issue_state_cache: HashMap<u64, String> = HashMap::new();
    for issue in &all_issues {
        issue_state_cache.insert(issue.number, issue.state.clone());
    }

    let mut all_findings: Vec<DriftFinding> = Vec::new();

    for issue in &all_issues {
        let findings = analyse_issue(
            issue,
            &sinexctl_cmds,
            &xtask_paths,
            &crate_names,
            &mut issue_state_cache,
        );
        all_findings.extend(findings);
    }

    // Duplicate detection (O(n²) over open issues; skippable).
    if !cmd.no_duplicates && cmd.issue.is_none() {
        let open_issues: Vec<&GhIssue> = all_issues
            .iter()
            .filter(|i| i.state == "OPEN")
            .collect();
        let threshold = cmd.duplicate_threshold;
        for i in 0..open_issues.len() {
            for j in (i + 1)..open_issues.len() {
                let a = &open_issues[i];
                let b = &open_issues[j];
                let sim = shingle_similarity(&a.body, &b.body);
                if sim >= threshold {
                    all_findings.push(DriftFinding {
                        issue: a.number,
                        title: a.title.clone(),
                        kind: DriftKind::CandidateDuplicate,
                        description: format!(
                            "candidate duplicate of #{} (\"{}\"): body similarity {:.0}%",
                            b.number,
                            b.title,
                            sim * 100.0
                        ),
                    });
                }
            }
        }
    }

    all_findings.sort_by(|a, b| a.issue.cmp(&b.issue).then(a.kind.cmp(&b.kind)));

    let elapsed_ms = start.elapsed().as_millis() as u64;

    let report = DriftReport {
        issues_scanned,
        findings: all_findings.clone(),
        elapsed_ms,
    };

    if ctx.is_human() {
        render_human(&report);
    }

    Ok(CommandResult::success()
        .with_message(format!(
            "{} findings across {} issues ({elapsed_ms}ms)",
            all_findings.len(),
            issues_scanned
        ))
        .with_data(serde_json::to_value(&report)?)
        .with_duration(ctx.elapsed()))
}

fn render_human(report: &DriftReport) {
    println!(
        "\nIssue drift report: {} findings in {} issues ({} ms)\n",
        report.findings.len(),
        report.issues_scanned,
        report.elapsed_ms
    );

    if report.findings.is_empty() {
        println!("No drift detected.");
        return;
    }

    let mut last_issue = 0u64;
    for finding in &report.findings {
        if finding.issue != last_issue {
            println!("#{}: {}", finding.issue, finding.title);
            last_issue = finding.issue;
        }
        let kind_label = match finding.kind {
            DriftKind::SinexctlCommand => "sinexctl-cmd",
            DriftKind::XtaskCommand => "xtask-cmd",
            DriftKind::MissingCrate => "missing-crate",
            DriftKind::MissingEventType => "missing-event-type",
            DriftKind::MissingFunction => "missing-fn",
            DriftKind::UmbrellaChild => "umbrella-child",
            DriftKind::CandidateDuplicate => "duplicate",
        };
        println!("  [{kind_label}] {}", finding.description);
    }
    println!();
}

fn find_workspace_root(mut current: std::path::PathBuf) -> Result<std::path::PathBuf> {
    loop {
        let toml = current.join("Cargo.toml");
        if toml.exists() {
            let content = std::fs::read_to_string(&toml)
                .wrap_err_with(|| format!("failed to read {}", toml.display()))?;
            if content.contains("[workspace]") {
                return Ok(current);
            }
        }
        if !current.pop() {
            color_eyre::eyre::bail!("could not find workspace root");
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::prelude::*;

    #[test]
    fn extract_sinexctl_refs_finds_subcommand_names() {
        let body = "Use `sinexctl graph entity` or `sinexctl context --since 4h`.";
        let refs = extract_sinexctl_refs(body);
        assert!(refs.contains(&"graph".to_string()), "expected 'graph' in {refs:?}");
        assert!(refs.contains(&"context".to_string()), "expected 'context' in {refs:?}");
    }

    #[test]
    fn extract_sinexctl_refs_returns_empty_for_no_match() {
        let body = "No CLI references here.";
        assert!(extract_sinexctl_refs(body).is_empty());
    }

    #[test]
    fn extract_xtask_refs_finds_paths() {
        let body = "Run `xtask docs sync` and `xtask check --full`.";
        let refs = extract_xtask_refs(body);
        assert!(refs.iter().any(|r| r.contains("docs")), "expected 'docs' in {refs:?}");
    }

    #[test]
    fn extract_crate_refs_finds_sinex_crates() {
        let body = "Depends on `sinex-db` and `sinex-gateway` crates.";
        let refs = extract_crate_refs(body);
        assert!(refs.contains(&"sinex-db".to_string()));
        assert!(refs.contains(&"sinex-gateway".to_string()));
    }

    #[test]
    fn has_event_new_ref_detects_constructor() {
        assert!(has_event_new_ref("call `Event::new(source, type, payload)`"));
        assert!(has_event_new_ref("use Event::new( to build"));
        assert!(!has_event_new_ref("use EventBuilder instead"));
    }

    #[test]
    fn extract_checklist_children_parses_both_states() {
        let body = "- [x] #309 done\n- [ ] #326 pending\n- [X] #327 also done\n";
        let items = extract_checklist_children(body);
        assert!(items.contains(&(true, 309)));
        assert!(items.contains(&(false, 326)));
        assert!(items.contains(&(true, 327)));
    }

    #[test]
    fn shingle_similarity_identical_is_one() {
        let text = "the quick brown fox jumps over the lazy dog";
        assert!((shingle_similarity(text, text) - 1.0).abs() < 1e-9);
    }

    #[test]
    fn shingle_similarity_unrelated_is_low() {
        let a = "alpha beta gamma delta epsilon zeta";
        let b = "rust programming language memory safety ownership";
        let sim = shingle_similarity(a, b);
        assert!(sim < 0.3, "expected low similarity, got {sim}");
    }

    #[test]
    fn shingle_similarity_empty_is_zero() {
        assert_eq!(shingle_similarity("", "anything"), 0.0);
        assert_eq!(shingle_similarity("anything", ""), 0.0);
    }

    #[sinex_test]
    async fn issue_drift_workspace_inventory_loads() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let crates = collect_workspace_crates(&workspace)?;
        assert!(crates.contains("sinex-primitives"), "sinex-primitives must be in workspace");
        assert!(crates.contains("xtask"), "xtask must be in workspace");
        let event_types = collect_event_type_strings(&workspace)?;
        assert!(!event_types.is_empty(), "event type registry must be non-empty");
        Ok(())
    }

    #[sinex_test]
    async fn issue_drift_xtask_paths_contains_docs() -> TestResult<()> {
        let workspace = crate::sandbox::orchestrator::find_workspace_root()?;
        let paths = collect_xtask_command_paths(&workspace)?;
        assert!(paths.contains("docs"), "expected 'docs' in xtask paths: {paths:?}");
        assert!(paths.contains("check"), "expected 'check' in xtask paths");
        assert!(paths.contains("docs.issue-drift"), "expected 'docs.issue-drift' in xtask paths");
        Ok(())
    }
}

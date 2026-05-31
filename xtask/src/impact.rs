//! Machine-derived test impact planning.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use color_eyre::eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

use crate::{affected, coordinator, history::HistoryDb};

pub const IMPACT_PLANNER_VERSION: &str = "impact-v2";
pub const IMPACT_COVERAGE_SCHEMA_VERSION: &str = "coverage-regions-v2";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum ImpactMode {
    Off,
    #[default]
    Balanced,
    Aggressive,
}

impl ImpactMode {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Balanced => "balanced",
            Self::Aggressive => "aggressive",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangedItem {
    pub path: String,
    pub package: Option<String>,
    pub hunks: Vec<ChangedHunk>,
    pub rust_items: Vec<RustItemSpan>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangedHunk {
    pub line_start: u32,
    pub line_end: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustItemSpan {
    pub file_path: String,
    pub item_kind: String,
    pub item_name: String,
    pub line_start: u32,
    pub line_end: u32,
    pub signature_hash: String,
    pub body_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImpactPlan {
    pub planner_version: String,
    pub mode: ImpactMode,
    pub coverage_schema_version: String,
    pub changed: Vec<ChangedItem>,
    pub affected_packages: Vec<String>,
    pub impacted_tests: Vec<ImpactedTest>,
    pub impact_filter: Option<String>,
    pub scope_args: Vec<String>,
    pub decisions: Vec<ImpactDecision>,
    pub accepted_risks: Vec<String>,
    pub evidence_gaps: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImpactedTest {
    pub package: Option<String>,
    pub test_name: String,
    pub evidence: Vec<ImpactEvidence>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImpactEvidence {
    pub source: ImpactEvidenceSource,
    pub subject: String,
    pub reason: String,
    pub line_start: Option<u32>,
    pub line_end: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImpactEvidenceSource {
    CoverageRegion,
    DependencyEdge,
    RustItemSpan,
    TestExecutionManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImpactDecision {
    pub action: ImpactAction,
    pub reason: String,
    pub subject: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImpactAction {
    RunImpactedTests,
    RunPackage,
    RunWorkspace,
    ReuseExactProof,
    SkipPackage,
    AuditSkippedTests,
}

impl ImpactPlan {
    #[must_use]
    pub fn is_workspace(&self) -> bool {
        self.affected_packages.is_empty()
            && self
                .decisions
                .iter()
                .any(|decision| decision.action == ImpactAction::RunWorkspace)
    }

    #[must_use]
    pub fn can_reuse_exact_proof(&self) -> bool {
        self.decisions
            .iter()
            .any(|decision| decision.action == ImpactAction::ReuseExactProof)
    }
}

pub fn plan_default_test_impact() -> Result<ImpactPlan> {
    plan_default_test_impact_with_history(None)
}

pub fn plan_default_test_impact_with_history(history: Option<&HistoryDb>) -> Result<ImpactPlan> {
    plan_default_test_impact_with_history_and_mode(history, ImpactMode::Balanced)
}

pub fn plan_default_test_impact_with_history_and_mode(
    history: Option<&HistoryDb>,
    mode: ImpactMode,
) -> Result<ImpactPlan> {
    let changed_files = affected::changed_files()?;
    let hunks = changed_hunks()?;
    let item_index = RustItemIndex::for_changed_files(&changed_files)?;
    let affected_packages = affected::affected_packages()?;
    let impacted_tests = match history {
        Some(history) => {
            history.impacted_tests_for_changed_files_and_hunks(&changed_files, &hunks)?
        }
        None => Vec::new(),
    };
    plan_from_changed_files_with_mode(
        changed_files,
        &hunks,
        &item_index,
        affected_packages,
        impacted_tests,
        mode,
    )
}

pub fn plan_from_changed_files(
    changed_files: Vec<String>,
    affected_packages: Vec<String>,
    impacted_tests: Vec<ImpactedTest>,
) -> Result<ImpactPlan> {
    plan_from_changed_files_with_mode(
        changed_files,
        &[],
        &RustItemIndex::default(),
        affected_packages,
        impacted_tests,
        ImpactMode::Balanced,
    )
}

pub fn plan_from_changed_files_with_mode(
    changed_files: Vec<String>,
    hunks: &[FileChangedHunks],
    item_index: &RustItemIndex,
    affected_packages: Vec<String>,
    impacted_tests: Vec<ImpactedTest>,
    mode: ImpactMode,
) -> Result<ImpactPlan> {
    if matches!(mode, ImpactMode::Off) {
        let mut changed = changed_items(changed_files, hunks, item_index);
        changed.sort_by(|left, right| left.path.cmp(&right.path));
        let mut decisions = Vec::new();
        let affected_packages = affected_packages.into_iter().collect::<BTreeSet<_>>();
        if affected_packages.is_empty() {
            decisions.push(ImpactDecision {
                action: ImpactAction::RunWorkspace,
                reason: "impact mode is off and changes do not map to package scope".to_string(),
                subject: Some("workspace".to_string()),
            });
        } else {
            for package in &affected_packages {
                decisions.push(ImpactDecision {
                    action: ImpactAction::RunPackage,
                    reason: "impact mode is off; package scope is the default affected scope"
                        .to_string(),
                    subject: Some(package.clone()),
                });
            }
        }
        return Ok(ImpactPlan {
            planner_version: IMPACT_PLANNER_VERSION.to_string(),
            mode,
            coverage_schema_version: IMPACT_COVERAGE_SCHEMA_VERSION.to_string(),
            changed,
            affected_packages: affected_packages.into_iter().collect(),
            impacted_tests,
            impact_filter: None,
            scope_args: Vec::new(),
            decisions,
            accepted_risks: Vec::new(),
            evidence_gaps: Vec::new(),
        });
    }

    let mut changed = changed_files
        .into_iter()
        .map(|path| {
            let file_hunks = hunks
                .iter()
                .find(|item| item.path == path)
                .map_or_else(Vec::new, |item| item.hunks.clone());
            let rust_items = item_index.items_for_file(&path);
            let content_hash = hash_file_if_exists(&path);
            ChangedItem {
                package: affected::package_for_path(&path),
                path,
                hunks: file_hunks,
                rust_items,
                content_hash,
            }
        })
        .collect::<Vec<_>>();
    changed.sort_by(|left, right| left.path.cmp(&right.path));

    let mut affected_packages = affected_packages.into_iter().collect::<BTreeSet<_>>();
    let workspace_wide = changed.iter().any(|item| {
        item.path == "Cargo.toml"
            || item.path == "Cargo.lock"
            || item.path == "flake.nix"
            || item.path == "flake.lock"
            || item.path.starts_with(".config/")
    });

    let mut decisions = Vec::new();
    let mut accepted_risks = Vec::new();
    let mut evidence_gaps = Vec::new();
    let mut impact_filter = None;
    let scope_args = if changed.is_empty() {
        decisions.push(ImpactDecision {
            action: ImpactAction::ReuseExactProof,
            reason: "no git diff or untracked workspace files changed the test input".to_string(),
            subject: Some("workspace".to_string()),
        });
        Vec::new()
    } else if !impacted_tests.is_empty() {
        let mut packages = impacted_tests
            .iter()
            .filter_map(|test| test.package.clone())
            .collect::<BTreeSet<_>>();
        if packages.is_empty() {
            packages.clone_from(&affected_packages);
        }
        for item in &changed {
            if !changed_item_fully_covered(item, &impacted_tests) {
                evidence_gaps.push(item.path.clone());
            }
        }
        if evidence_gaps.is_empty() || matches!(mode, ImpactMode::Aggressive) {
            let filter = impacted_tests
                .iter()
                .map(|test| format!("test({})", test.test_name))
                .collect::<Vec<_>>()
                .join(" or ");
            impact_filter = Some(filter.clone());
            decisions.push(ImpactDecision {
                action: ImpactAction::RunImpactedTests,
                reason: if evidence_gaps.is_empty() {
                    "history recorded hunk-level coverage or dependency edges for every changed hunk"
                } else {
                    "aggressive impact mode accepts incomplete evidence and records the risk"
                }
                    .to_string(),
                subject: Some(format!("{} test(s)", impacted_tests.len())),
            });
            if matches!(mode, ImpactMode::Aggressive) && !evidence_gaps.is_empty() {
                accepted_risks.push(format!(
                    "aggressive impact mode skipped package fallback despite {} uncovered changed file(s)",
                    evidence_gaps.len()
                ));
                decisions.push(ImpactDecision {
                    action: ImpactAction::AuditSkippedTests,
                    reason: "aggressive impact selection requires skipped-scope audit sampling"
                        .to_string(),
                    subject: Some("impact-audit".to_string()),
                });
            }
            packages
                .iter()
                .flat_map(|package| ["-p".to_string(), package.clone()])
                .chain(["-E".to_string(), filter])
                .collect()
        } else {
            accepted_risks.push(format!(
                "test-level impact evidence missing for {} changed file(s); package scope remains the fallback for uncovered files",
                evidence_gaps.len()
            ));
            let fallback_packages = if affected_packages.is_empty() {
                packages.clone()
            } else {
                affected_packages.clone()
            };
            for package in &fallback_packages {
                decisions.push(ImpactDecision {
                    action: ImpactAction::RunPackage,
                    reason:
                        "test-level evidence was incomplete; package scope covers uncovered changes"
                            .to_string(),
                    subject: Some(package.clone()),
                });
            }
            fallback_packages
                .iter()
                .flat_map(|package| ["-p".to_string(), package.clone()])
                .collect()
        }
    } else if workspace_wide || affected_packages.is_empty() {
        decisions.push(ImpactDecision {
            action: ImpactAction::RunWorkspace,
            reason: if workspace_wide {
                "workspace-level file changed".to_string()
            } else {
                "changed files did not map to a Rust package; run the workspace".to_string()
            },
            subject: Some("workspace".to_string()),
        });
        affected_packages.clear();
        Vec::new()
    } else {
        let packages = affected_packages.iter().cloned().collect::<Vec<_>>();
        for package in &packages {
            decisions.push(ImpactDecision {
                action: ImpactAction::RunPackage,
                reason: "package owns a changed file or transitively depends on one".to_string(),
                subject: Some(package.clone()),
            });
        }
        accepted_risks.push(
            "dynamic/runtime dependencies outside package ownership are not proven by static package scope"
                .to_string(),
        );
        packages
            .iter()
            .flat_map(|package| ["-p".to_string(), package.clone()])
            .collect()
    };

    Ok(ImpactPlan {
        planner_version: IMPACT_PLANNER_VERSION.to_string(),
        mode,
        coverage_schema_version: IMPACT_COVERAGE_SCHEMA_VERSION.to_string(),
        changed,
        affected_packages: affected_packages.into_iter().collect(),
        impacted_tests,
        impact_filter,
        scope_args,
        decisions,
        accepted_risks,
        evidence_gaps,
    })
}

#[must_use]
pub fn packages_for_plan(plan: &ImpactPlan) -> Option<Vec<String>> {
    if plan.is_workspace() || plan.can_reuse_exact_proof() {
        return None;
    }
    let packages = plan
        .scope_args
        .chunks(2)
        .filter_map(|chunk| {
            if chunk.first().map(String::as_str) == Some("-p") {
                chunk.get(1).cloned()
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    (!packages.is_empty()).then_some(packages)
}

#[must_use]
pub fn exact_proof_args_for_plan(plan: &ImpactPlan) -> Vec<String> {
    plan.scope_args.clone()
}

pub fn exact_test_proof_key(args: &[String]) -> Result<(String, String, String)> {
    let proof_kind = coordinator::proof_kind("test", args);
    let input_fingerprint = coordinator::current_scoped_tree_fingerprint("test", args)?;
    let scope_key = coordinator::compute_scope_key("test", args);
    Ok((proof_kind, input_fingerprint, scope_key))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileChangedHunks {
    pub path: String,
    pub hunks: Vec<ChangedHunk>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustItemIndex {
    pub items: Vec<RustItemSpan>,
}

impl RustItemIndex {
    pub fn for_changed_files(changed_files: &[String]) -> Result<Self> {
        let mut items = Vec::new();
        for path in changed_files {
            if !path.ends_with(".rs") {
                continue;
            }
            let parsed = rust_items_for_file(Path::new(path))?;
            items.extend(parsed);
        }
        Ok(Self { items })
    }

    #[must_use]
    pub fn items_for_file(&self, path: &str) -> Vec<RustItemSpan> {
        self.items
            .iter()
            .filter(|item| item.file_path == path)
            .cloned()
            .collect()
    }
}

fn changed_items(
    changed_files: Vec<String>,
    hunks: &[FileChangedHunks],
    item_index: &RustItemIndex,
) -> Vec<ChangedItem> {
    changed_files
        .into_iter()
        .map(|path| {
            let file_hunks = hunks
                .iter()
                .find(|item| item.path == path)
                .map_or_else(Vec::new, |item| item.hunks.clone());
            ChangedItem {
                package: affected::package_for_path(&path),
                rust_items: item_index.items_for_file(&path),
                content_hash: hash_file_if_exists(&path),
                path,
                hunks: file_hunks,
            }
        })
        .collect()
}

fn changed_item_fully_covered(item: &ChangedItem, impacted_tests: &[ImpactedTest]) -> bool {
    let file_evidence = impacted_tests
        .iter()
        .flat_map(|test| &test.evidence)
        .filter(|evidence| evidence.subject == item.path)
        .collect::<Vec<_>>();
    if file_evidence.is_empty() {
        return false;
    }
    if item.hunks.is_empty() {
        return true;
    }
    item.hunks.iter().all(|hunk| {
        file_evidence
            .iter()
            .any(|evidence| match (evidence.line_start, evidence.line_end) {
                (Some(start), Some(end)) => {
                    ranges_overlap(hunk.line_start, hunk.line_end, start, end)
                }
                _ => false,
            })
    })
}

#[must_use]
pub fn ranges_overlap(left_start: u32, left_end: u32, right_start: u32, right_end: u32) -> bool {
    left_start <= right_end && right_start <= left_end
}

pub fn changed_hunks() -> Result<Vec<FileChangedHunks>> {
    let output = Command::new("git")
        .args(["diff", "--unified=0", "--no-ext-diff", "--", "*.rs"])
        .output()
        .context("failed to run git diff for impact hunks")?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(parse_unified_zero_hunks(&text))
}

#[must_use]
pub fn parse_unified_zero_hunks(diff: &str) -> Vec<FileChangedHunks> {
    let mut files = Vec::<FileChangedHunks>::new();
    let mut current_path: Option<String> = None;
    for line in diff.lines() {
        if let Some(path) = line.strip_prefix("+++ b/") {
            current_path = Some(path.to_string());
            files.push(FileChangedHunks {
                path: path.to_string(),
                hunks: Vec::new(),
            });
            continue;
        }
        let Some(rest) = line.strip_prefix("@@ ") else {
            continue;
        };
        let Some(path) = current_path.as_ref() else {
            continue;
        };
        let Some(new_part) = rest.split_whitespace().find(|part| part.starts_with('+')) else {
            continue;
        };
        let hunk = parse_new_hunk_range(new_part);
        if let Some(hunk) = hunk
            && let Some(file) = files.iter_mut().find(|file| file.path == *path)
        {
            file.hunks.push(hunk);
        }
    }
    files.retain(|file| !file.hunks.is_empty());
    files
}

fn parse_new_hunk_range(raw: &str) -> Option<ChangedHunk> {
    let raw = raw.strip_prefix('+')?;
    let (start, len) = raw
        .split_once(',')
        .map_or((raw, "1"), |(start, len)| (start, len));
    let start = start.parse::<u32>().ok()?;
    let len = len.parse::<u32>().ok()?;
    let line_end = if len == 0 { start } else { start + len - 1 };
    Some(ChangedHunk {
        line_start: start,
        line_end,
    })
}

fn hash_file_if_exists(path: &str) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
}

pub fn rust_items_for_file(path: &Path) -> Result<Vec<RustItemSpan>> {
    let rendered = fs::read_to_string(path)
        .with_context(|| format!("failed to read Rust source {}", path.display()))?;
    let _ = syn::parse_file(&rendered)
        .with_context(|| format!("failed to parse Rust source {}", path.display()))?;
    Ok(scan_rust_items(path, &rendered))
}

fn scan_rust_items(path: &Path, source: &str) -> Vec<RustItemSpan> {
    let mut items = Vec::new();
    let lines = source.lines().collect::<Vec<_>>();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx].trim_start();
        let Some((kind, name)) = parse_item_header(line) else {
            idx += 1;
            continue;
        };
        let start = idx + 1;
        let mut end = start;
        let mut brace_balance = line.matches('{').count() as i64 - line.matches('}').count() as i64;
        let mut cursor = idx + 1;
        while cursor < lines.len() {
            let cursor_line = lines[cursor];
            brace_balance += cursor_line.matches('{').count() as i64;
            brace_balance -= cursor_line.matches('}').count() as i64;
            end = cursor + 1;
            if brace_balance <= 0 && (line.contains('{') || cursor_line.trim_end().ends_with(';')) {
                break;
            }
            cursor += 1;
        }
        let span_text = lines[start - 1..end].join("\n");
        let signature = lines[start - 1].trim();
        items.push(RustItemSpan {
            file_path: path.to_string_lossy().into_owned(),
            item_kind: kind.to_string(),
            item_name: name,
            line_start: u32::try_from(start).unwrap_or(u32::MAX),
            line_end: u32::try_from(end).unwrap_or(u32::MAX),
            signature_hash: format!("{:x}", Sha256::digest(signature.as_bytes())),
            body_hash: format!("{:x}", Sha256::digest(span_text.as_bytes())),
        });
        idx = end.max(idx + 1);
    }
    items
}

fn parse_item_header(line: &str) -> Option<(&'static str, String)> {
    let line = line
        .strip_prefix("pub(crate) ")
        .or_else(|| line.strip_prefix("pub(super) "))
        .or_else(|| line.strip_prefix("pub "))
        .unwrap_or(line);
    for keyword in [
        "async fn",
        "fn",
        "struct",
        "enum",
        "trait",
        "impl",
        "mod",
        "macro_rules!",
    ] {
        if let Some(rest) = line.strip_prefix(keyword) {
            let kind = if keyword == "async fn" { "fn" } else { keyword };
            let name = rest
                .trim_start()
                .trim_start_matches('!')
                .split(|ch: char| {
                    ch == '<' || ch == '(' || ch == '{' || ch == ':' || ch.is_whitespace()
                })
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            if !name.is_empty() {
                return Some((kind, name));
            }
        }
    }
    None
}

pub fn workspace_rust_item_index() -> Result<RustItemIndex> {
    let root = crate::config::workspace_root();
    let mut items = Vec::new();
    for entry in WalkDir::new(&root)
        .into_iter()
        .filter_entry(|entry| {
            let path = entry.path();
            !path.components().any(|component| {
                let value = component.as_os_str().to_string_lossy();
                matches!(value.as_ref(), ".git" | "target" | ".sinex" | ".direnv")
            })
        })
        .flatten()
    {
        let path = entry.path();
        if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
            continue;
        }
        let relative = path.strip_prefix(&root).unwrap_or(path);
        let relative_path = PathBuf::from(relative);
        if let Ok(mut parsed) = rust_items_for_file(&relative_path) {
            items.append(&mut parsed);
        }
    }
    Ok(RustItemIndex { items })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::prelude::*;

    #[sinex_test]
    async fn impact_plan_reuses_exact_proof_for_empty_change_set() -> TestResult<()> {
        let plan = plan_from_changed_files(Vec::new(), Vec::new(), Vec::new())?;

        assert!(plan.can_reuse_exact_proof());
        assert!(plan.scope_args.is_empty());
        assert_eq!(plan.decisions[0].action, ImpactAction::ReuseExactProof);
        Ok(())
    }

    #[sinex_test]
    async fn impact_plan_runs_affected_packages_for_code_changes() -> TestResult<()> {
        let plan = plan_from_changed_files(
            vec!["xtask/src/commands/test.rs".to_string()],
            vec!["xtask".to_string()],
            Vec::new(),
        )?;

        assert_eq!(plan.affected_packages, vec!["xtask"]);
        assert_eq!(plan.scope_args, vec!["-p".to_string(), "xtask".to_string()]);
        assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
        assert!(!plan.accepted_risks.is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn impact_plan_runs_workspace_for_workspace_level_changes() -> TestResult<()> {
        let plan = plan_from_changed_files(
            vec![".config/nextest.toml".to_string()],
            vec!["xtask".to_string()],
            Vec::new(),
        )?;

        assert!(plan.is_workspace());
        assert!(plan.affected_packages.is_empty());
        assert_eq!(plan.decisions[0].action, ImpactAction::RunWorkspace);
        Ok(())
    }

    #[sinex_test]
    async fn impact_plan_uses_test_level_evidence_when_available() -> TestResult<()> {
        let plan = plan_from_changed_files(
            vec!["crate/sinexd/src/node_sdk/stage_as_you_go.rs".to_string()],
            vec!["sinexd".to_string()],
            vec![ImpactedTest {
                package: Some("sinexd".to_string()),
                test_name: "stage_as_you_go_records_material".to_string(),
                evidence: vec![ImpactEvidence {
                    source: ImpactEvidenceSource::CoverageRegion,
                    subject: "crate/sinexd/src/node_sdk/stage_as_you_go.rs".to_string(),
                    reason: "covered line range".to_string(),
                    line_start: None,
                    line_end: None,
                }],
            }],
        )?;

        assert_eq!(plan.decisions[0].action, ImpactAction::RunImpactedTests);
        assert_eq!(
            plan.impact_filter.as_deref(),
            Some("test(stage_as_you_go_records_material)")
        );
        assert_eq!(
            plan.scope_args,
            vec![
                "-p".to_string(),
                "sinexd".to_string(),
                "-E".to_string(),
                "test(stage_as_you_go_records_material)".to_string()
            ]
        );
        Ok(())
    }

    #[sinex_test]
    async fn impact_plan_falls_back_when_changed_hunk_is_not_covered() -> TestResult<()> {
        let plan = plan_from_changed_files_with_mode(
            vec!["xtask/src/impact.rs".to_string()],
            &[FileChangedHunks {
                path: "xtask/src/impact.rs".to_string(),
                hunks: vec![ChangedHunk {
                    line_start: 90,
                    line_end: 90,
                }],
            }],
            &RustItemIndex::default(),
            vec!["xtask".to_string()],
            vec![ImpactedTest {
                package: Some("xtask".to_string()),
                test_name: "impact_manifest_test".to_string(),
                evidence: vec![ImpactEvidence {
                    source: ImpactEvidenceSource::CoverageRegion,
                    subject: "xtask/src/impact.rs".to_string(),
                    reason: "covered line range".to_string(),
                    line_start: Some(10),
                    line_end: Some(20),
                }],
            }],
            ImpactMode::Balanced,
        )?;

        assert_eq!(plan.decisions[0].action, ImpactAction::RunPackage);
        assert_eq!(plan.evidence_gaps, vec!["xtask/src/impact.rs"]);
        Ok(())
    }

    #[sinex_test]
    async fn parse_unified_zero_hunks_extracts_new_line_ranges() -> TestResult<()> {
        let hunks = parse_unified_zero_hunks(
            "\
diff --git a/xtask/src/impact.rs b/xtask/src/impact.rs\n\
--- a/xtask/src/impact.rs\n\
+++ b/xtask/src/impact.rs\n\
@@ -10,0 +11,2 @@\n\
+one\n\
+two\n",
        );

        assert_eq!(
            hunks,
            vec![FileChangedHunks {
                path: "xtask/src/impact.rs".to_string(),
                hunks: vec![ChangedHunk {
                    line_start: 11,
                    line_end: 12,
                }],
            }]
        );
        Ok(())
    }
}

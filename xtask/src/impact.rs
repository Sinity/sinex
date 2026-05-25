//! Machine-derived test impact planning.

use std::collections::BTreeSet;

use color_eyre::eyre::Result;
use serde::{Deserialize, Serialize};

use crate::{affected, coordinator, history::HistoryDb};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangedItem {
    pub path: String,
    pub package: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ImpactPlan {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImpactEvidenceSource {
    CoverageRegion,
    DependencyEdge,
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
    let changed_files = affected::changed_files()?;
    let affected_packages = affected::affected_packages()?;
    let impacted_tests = match history {
        Some(history) => history.impacted_tests_for_changed_files(&changed_files)?,
        None => Vec::new(),
    };
    plan_from_changed_files(changed_files, affected_packages, impacted_tests)
}

pub fn plan_from_changed_files(
    changed_files: Vec<String>,
    affected_packages: Vec<String>,
    impacted_tests: Vec<ImpactedTest>,
) -> Result<ImpactPlan> {
    let mut changed = changed_files
        .into_iter()
        .map(|path| ChangedItem {
            package: affected::package_for_path(&path),
            path,
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
            packages = affected_packages.clone();
        }
        let covered_subjects = impacted_tests
            .iter()
            .flat_map(|test| {
                test.evidence
                    .iter()
                    .map(|evidence| evidence.subject.clone())
            })
            .collect::<BTreeSet<_>>();
        for item in &changed {
            if !covered_subjects.contains(&item.path) {
                evidence_gaps.push(item.path.clone());
            }
        }
        if evidence_gaps.is_empty() {
            let filter = impacted_tests
                .iter()
                .map(|test| format!("test({})", test.test_name))
                .collect::<Vec<_>>()
                .join(" or ");
            impact_filter = Some(filter.clone());
            decisions.push(ImpactDecision {
                action: ImpactAction::RunImpactedTests,
                reason: "history recorded test-to-file coverage or dependency edges for every changed file"
                    .to_string(),
                subject: Some(format!("{} test(s)", impacted_tests.len())),
            });
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
            vec!["crate/lib/sinex-node-sdk/src/stage_as_you_go.rs".to_string()],
            vec!["sinex-node-sdk".to_string()],
            vec![ImpactedTest {
                package: Some("sinex-node-sdk".to_string()),
                test_name: "stage_as_you_go_records_material".to_string(),
                evidence: vec![ImpactEvidence {
                    source: ImpactEvidenceSource::CoverageRegion,
                    subject: "crate/lib/sinex-node-sdk/src/stage_as_you_go.rs".to_string(),
                    reason: "covered line range".to_string(),
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
                "sinex-node-sdk".to_string(),
                "-E".to_string(),
                "test(stage_as_you_go_records_material)".to_string()
            ]
        );
        Ok(())
    }
}

use color_eyre::eyre::Result;

use crate::command::{CommandContext, WorkloadScope};
use crate::process::ProcessBuilder;

pub(super) const HEAVY_TEST_THREAD_CAP: usize = 4;
const INGESTD_RUNTIME_TEST_PACKAGES: &[&str] = &["sinex-ingestd", "sinex-node-sdk"];

#[derive(Debug, Clone, Copy)]
pub(super) struct RuntimeBinaryRequirement {
    pub(super) package: &'static str,
    pub(super) binary: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NextestExecutionPlan {
    pub(super) runner_packages: Vec<String>,
    pub(super) workload_scope: WorkloadScope,
}

pub(super) fn normalize_packages(packages: &[String]) -> Vec<String> {
    let mut packages = packages.to_vec();
    packages.sort();
    packages.dedup();
    packages
}

pub(super) fn default_heavy_test_threads(cpu_count: usize) -> usize {
    cpu_count.clamp(1, HEAVY_TEST_THREAD_CAP)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "inferred packages computed from args"
)]
pub(super) fn resolve_nextest_execution_plan(
    explicit_packages: &[String],
    inferred_packages: Vec<String>,
    affected_packages: Option<Vec<String>>,
) -> NextestExecutionPlan {
    let explicit_packages = normalize_packages(explicit_packages);
    if !explicit_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: explicit_packages.clone(),
            workload_scope: WorkloadScope::Packages(explicit_packages),
        };
    }

    let inferred_packages = normalize_packages(&inferred_packages);
    if !inferred_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: inferred_packages.clone(),
            workload_scope: WorkloadScope::Packages(inferred_packages),
        };
    }

    if let Some(affected_packages) = affected_packages {
        let affected_packages = normalize_packages(&affected_packages);
        if !affected_packages.is_empty() {
            return NextestExecutionPlan {
                runner_packages: affected_packages.clone(),
                workload_scope: WorkloadScope::Affected(affected_packages),
            };
        }
    }

    NextestExecutionPlan {
        runner_packages: Vec::new(),
        workload_scope: WorkloadScope::Workspace,
    }
}

pub(super) fn runtime_binary_requirements_for_plan(
    execution_plan: &NextestExecutionPlan,
) -> Vec<RuntimeBinaryRequirement> {
    if workload_scope_includes_any(
        &execution_plan.workload_scope,
        INGESTD_RUNTIME_TEST_PACKAGES,
    ) {
        return vec![RuntimeBinaryRequirement {
            package: "sinex-ingestd",
            binary: "sinex-ingestd",
        }];
    }
    Vec::new()
}

fn workload_scope_includes_any(scope: &WorkloadScope, packages: &[&str]) -> bool {
    match scope {
        WorkloadScope::Workspace => true,
        WorkloadScope::Packages(selected) | WorkloadScope::Affected(selected) => selected
            .iter()
            .any(|package| packages.iter().any(|candidate| package == candidate)),
    }
}

pub(super) fn prepare_runtime_binaries_for_plan(
    ctx: &CommandContext,
    execution_plan: &NextestExecutionPlan,
) -> Result<Vec<serde_json::Value>> {
    let requirements = runtime_binary_requirements_for_plan(execution_plan);
    if requirements.is_empty() {
        return Ok(Vec::new());
    }

    let workspace_root = crate::sandbox::orchestrator::find_workspace_root()?;
    let mut reports = Vec::new();
    for requirement in requirements {
        let before = crate::sandbox::orchestrator::check_runtime_binary_freshness(
            &workspace_root,
            requirement.package,
            requirement.binary,
        )?;
        if ctx.is_human() && !before.is_fresh() {
            eprintln!(
                "→ Preparing stale/missing runtime binary for tests: {} ({})",
                requirement.binary,
                before.status.as_str()
            );
        }
        if !before.is_fresh() {
            ProcessBuilder::cargo()
                .args(["build", "-p", requirement.package])
                .with_description(format!(
                    "building test runtime binary {}",
                    requirement.binary
                ))
                .run_ok()?;
        }
        let after = crate::sandbox::orchestrator::check_runtime_binary_freshness(
            &workspace_root,
            requirement.package,
            requirement.binary,
        )?;
        after.ensure_fresh()?;
        reports.push(serde_json::json!({
            "binary": after.binary_name,
            "package": after.package,
            "before": before.to_json(),
            "after": after.to_json(),
            "rebuilt": !before.is_fresh(),
        }));
    }
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_prefers_explicit_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &["sinex-db".into(), "xtask".into()],
            vec!["sinex-gateway".into()],
            Some(vec!["sinex-e2e-tests".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-db".into(), "xtask".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_prefers_inferred_packages_over_affected_scope()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            vec!["sinex-gateway".into()],
            Some(vec!["xtask".into(), "sinex-db".into(), "xtask".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-gateway".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-gateway".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_falls_back_to_affected_when_no_inference()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            Vec::new(),
            Some(vec!["xtask".into(), "sinex-db".into(), "xtask".into()]),
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                workload_scope: WorkloadScope::Affected(vec!["sinex-db".into(), "xtask".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_falls_back_to_inferred_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            vec!["sinex-e2e-tests".into(), "sinex-e2e-tests".into()],
            None,
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-e2e-tests".into()],
                workload_scope: WorkloadScope::Packages(vec!["sinex-e2e-tests".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_ingestd_for_workspace()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: Vec::new(),
            workload_scope: WorkloadScope::Workspace,
        };

        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinex-ingestd");
        assert_eq!(requirements[0].binary, "sinex-ingestd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_ingestd_for_node_sdk_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-node-sdk".to_string()],
            workload_scope: WorkloadScope::Packages(vec!["sinex-node-sdk".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinex-ingestd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_skip_unrelated_package_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["xtask".to_string()],
            workload_scope: WorkloadScope::Packages(vec!["xtask".to_string()]),
        };

        assert!(runtime_binary_requirements_for_plan(&plan).is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn test_default_heavy_test_threads_caps_parallelism() -> ::xtask::sandbox::TestResult<()>
    {
        assert_eq!(default_heavy_test_threads(1), 1);
        assert_eq!(default_heavy_test_threads(2), 2);
        assert_eq!(default_heavy_test_threads(4), 4);
        assert_eq!(default_heavy_test_threads(24), 4);
        Ok(())
    }
}

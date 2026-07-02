use color_eyre::eyre::Result;
use serde::Serialize;

use crate::command::{CommandContext, WorkloadScope};
use crate::process::ProcessBuilder;

pub(super) const HEAVY_TEST_THREAD_CAP: usize = 4;
// Packages whose integration tests require the `sinexd` runtime binary.
//
// `sinexd` is the single runtime binary hosting both the event engine and the
// operator API. Test fixtures spawn it as an engine-only process
// (`SINEX_API_ENABLED=false`) and/or as an API-only `rpc-server` subprocess, so
// any package using either fixture needs this binary built.
const SINEXD_RUNTIME_TEST_PACKAGES: &[&str] = &[
    "sinex-db",
    "sinex-e2e-tests",
    "sinexd",
    "sinex-workspace-tests",
];
const SINEXCTL_RUNTIME_TEST_PACKAGES: &[&str] = &["sinex-workspace-tests"];
const SINEXD_RUNTIME_INDEPENDENT_TEST_BINARIES: &[&str] = &[
    "browser_history_parser_test",
    "registry_dispatch_test",
    "terminal_history_parser_test",
    "transport_security_test",
];

#[derive(Debug, Clone, Copy, Serialize)]
pub(super) struct RuntimeBinaryRequirement {
    pub(super) package: &'static str,
    pub(super) binary: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NextestExecutionPlan {
    pub(super) runner_packages: Vec<String>,
    pub(super) excluded_packages: Vec<String>,
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
    excluded_packages: &[String],
) -> NextestExecutionPlan {
    let explicit_packages = normalize_packages(explicit_packages);
    if !explicit_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: explicit_packages.clone(),
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(explicit_packages),
        };
    }

    let inferred_packages = normalize_packages(&inferred_packages);
    if !inferred_packages.is_empty() {
        return NextestExecutionPlan {
            runner_packages: inferred_packages.clone(),
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(inferred_packages),
        };
    }

    if let Some(affected_packages) = affected_packages {
        let affected_packages = normalize_packages(&affected_packages);
        if !affected_packages.is_empty() {
            return NextestExecutionPlan {
                runner_packages: affected_packages.clone(),
                excluded_packages: Vec::new(),
                workload_scope: WorkloadScope::Affected(affected_packages),
            };
        }
    }

    NextestExecutionPlan {
        runner_packages: Vec::new(),
        excluded_packages: normalize_packages(excluded_packages),
        workload_scope: WorkloadScope::Workspace,
    }
}

pub(super) fn runtime_binary_requirements_for_plan(
    execution_plan: &NextestExecutionPlan,
) -> Vec<RuntimeBinaryRequirement> {
    let mut requirements = Vec::new();
    if workload_scope_includes_any(&execution_plan.workload_scope, SINEXD_RUNTIME_TEST_PACKAGES) {
        // Single fold-era binary hosting both the event engine and the gateway.
        requirements.push(RuntimeBinaryRequirement {
            package: "sinexd",
            binary: "sinexd",
        });
    }
    if workload_scope_includes_any(
        &execution_plan.workload_scope,
        SINEXCTL_RUNTIME_TEST_PACKAGES,
    ) {
        requirements.push(RuntimeBinaryRequirement {
            package: "sinexctl",
            binary: "sinexctl",
        });
    }
    requirements
}

pub(super) fn runtime_binary_requirements_for_target(
    execution_plan: &NextestExecutionPlan,
    lib_target: bool,
    test_binaries: &[String],
    _filter: Option<&str>,
) -> Vec<RuntimeBinaryRequirement> {
    if lib_target {
        return Vec::new();
    }

    let mut requirements = runtime_binary_requirements_for_plan(execution_plan);
    if sinexd_runtime_independent_target(execution_plan, test_binaries) {
        requirements.retain(|requirement| requirement.package != "sinexd");
    }
    requirements
}

fn workload_scope_includes_any(scope: &WorkloadScope, packages: &[&str]) -> bool {
    match scope {
        WorkloadScope::Workspace => true,
        WorkloadScope::Packages(selected) | WorkloadScope::Affected(selected) => selected
            .iter()
            .any(|package| packages.iter().any(|candidate| package == candidate)),
    }
}

fn sinexd_runtime_independent_target(
    execution_plan: &NextestExecutionPlan,
    test_binaries: &[String],
) -> bool {
    if test_binaries.is_empty() {
        return false;
    }
    match &execution_plan.workload_scope {
        WorkloadScope::Packages(selected) | WorkloadScope::Affected(selected)
            if selected.len() == 1 && selected[0] == "sinexd" =>
        {
            test_binaries.iter().all(|binary| {
                SINEXD_RUNTIME_INDEPENDENT_TEST_BINARIES
                    .iter()
                    .any(|candidate| candidate == binary)
            })
        }
        _ => false,
    }
}

pub(super) fn prepare_runtime_binaries_for_plan(
    ctx: &CommandContext,
    requirements: &[RuntimeBinaryRequirement],
) -> Result<Vec<serde_json::Value>> {
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
        if !runtime_binary_manifest_only_stale_after_build(&after) {
            after.ensure_fresh()?;
        }
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

fn runtime_binary_manifest_only_stale_after_build(
    report: &crate::sandbox::orchestrator::RuntimeBinaryFreshnessReport,
) -> bool {
    if report.status != crate::sandbox::orchestrator::RuntimeBinaryFreshnessStatus::Stale {
        return false;
    }

    report
        .newest_input_path
        .as_deref()
        .and_then(std::path::Path::file_name)
        .is_some_and(|file_name| file_name == "Cargo.toml")
}

#[cfg(test)]
#[path = "plan_test.rs"]
mod tests;

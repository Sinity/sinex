use color_eyre::eyre::Result;
use serde::Serialize;

use crate::command::{CommandContext, WorkloadScope};
use crate::process::ProcessBuilder;

pub(super) const HEAVY_TEST_THREAD_CAP: usize = 4;
// Packages whose integration tests require the `sinexd` runtime binary.
//
// Post Wave-B fold (#1223) and the gateway fold (#1559), `sinexd` is the single
// runtime binary hosting both the event engine (formerly `sinex-ingestd`) and
// the operator API / gateway (formerly `sinex-gateway`). Test fixtures spawn it
// as the engine-only ingestd (`SINEX_API_ENABLED=false`) and/or as the
// gateway-only `rpc-server` subprocess, so any package using either fixture
// needs this binary built.
const SINEXD_RUNTIME_TEST_PACKAGES: &[&str] = &[
    "sinex-db",
    "sinex-e2e-tests",
    "sinexd",
    "sinex-workspace-tests",
];
const DATABASE_TEST_PACKAGES: &[&str] = &[
    "sinex-db",
    "sinex-e2e-tests",
    "sinexd",
    "sinex-schema",
    "sinex-workspace-tests",
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
    requirements
}

pub(super) fn runtime_binary_requirements_for_target(
    execution_plan: &NextestExecutionPlan,
    lib_target: bool,
    test_binaries: &[String],
    filter: Option<&str>,
) -> Vec<RuntimeBinaryRequirement> {
    if lib_target {
        return Vec::new();
    }

    let mut requirements = runtime_binary_requirements_for_plan(execution_plan);
    if workload_scope_includes_any(&execution_plan.workload_scope, &["sinex-source-worker"])
        && source_worker_production_path_requires_ingestd(test_binaries, filter)
    {
        push_runtime_requirement(&mut requirements, "sinexd", "sinexd");
    }

    requirements
}

fn push_runtime_requirement(
    requirements: &mut Vec<RuntimeBinaryRequirement>,
    package: &'static str,
    binary: &'static str,
) {
    if requirements
        .iter()
        .any(|requirement| requirement.package == package)
    {
        return;
    }

    requirements.push(RuntimeBinaryRequirement { package, binary });
}

fn source_worker_production_path_requires_ingestd(
    test_binaries: &[String],
    filter: Option<&str>,
) -> bool {
    let production_path_selected = test_binaries.is_empty()
        || test_binaries
            .iter()
            .any(|binary| binary == "production_path");
    if !production_path_selected {
        return false;
    }

    let Some(filter) = filter else {
        return true;
    };

    filter.contains("binary_path")
        || filter.contains("source_worker_binary")
        || filter.contains("source_worker_binary_scan_private_mode_matrix")
}

pub(super) fn test_database_required_for_plan(execution_plan: &NextestExecutionPlan) -> bool {
    workload_scope_includes_any(&execution_plan.workload_scope, DATABASE_TEST_PACKAGES)
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
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;
    use crate::sandbox::orchestrator::{
        RuntimeBinaryFreshnessReport, RuntimeBinaryFreshnessStatus,
    };
    use std::path::PathBuf;
    use std::time::SystemTime;

    fn stale_runtime_report(newest_input_path: impl Into<PathBuf>) -> RuntimeBinaryFreshnessReport {
        RuntimeBinaryFreshnessReport {
            package: "sinexd".into(),
            binary_name: "sinexd".into(),
            binary_path: PathBuf::from("target/debug/sinexd"),
            status: RuntimeBinaryFreshnessStatus::Stale,
            binary_modified_at: Some(SystemTime::UNIX_EPOCH),
            newest_input_path: Some(newest_input_path.into()),
            newest_input_modified_at: Some(SystemTime::UNIX_EPOCH),
            input_count: 1,
            build_command: "xtask build -p sinexd".into(),
        }
    }

    #[sinex_test]
    async fn runtime_binary_manifest_stale_after_build_is_cargo_authoritative()
    -> ::xtask::sandbox::TestResult<()> {
        let report = stale_runtime_report("crate/sinexd/Cargo.toml");

        assert!(runtime_binary_manifest_only_stale_after_build(&report));
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_source_stale_after_build_still_blocks()
    -> ::xtask::sandbox::TestResult<()> {
        let report = stale_runtime_report("crate/sinexd/src/main.rs");

        assert!(!runtime_binary_manifest_only_stale_after_build(&report));
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_prefers_explicit_packages()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &["sinex-db".into(), "xtask".into()],
            vec!["sinexd".into()],
            Some(vec!["sinex-e2e-tests".into()]),
            &[],
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                excluded_packages: Vec::new(),
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
            vec!["sinexd".into()],
            Some(vec!["xtask".into(), "sinex-db".into(), "xtask".into()]),
            &[],
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinexd".into()],
                excluded_packages: Vec::new(),
                workload_scope: WorkloadScope::Packages(vec!["sinexd".into()]),
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
            &[],
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-db".into(), "xtask".into()],
                excluded_packages: Vec::new(),
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
            &[],
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: vec!["sinex-e2e-tests".into()],
                excluded_packages: Vec::new(),
                workload_scope: WorkloadScope::Packages(vec!["sinex-e2e-tests".into()]),
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_resolve_nextest_execution_plan_carries_workspace_excludes()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = resolve_nextest_execution_plan(
            &[],
            Vec::new(),
            None,
            &["sinex-e2e-tests".into(), "sinex-e2e-tests".into()],
        );

        assert_eq!(
            plan,
            NextestExecutionPlan {
                runner_packages: Vec::new(),
                excluded_packages: vec!["sinex-e2e-tests".into()],
                workload_scope: WorkloadScope::Workspace,
            }
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_runtime_binaries_for_workspace()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: Vec::new(),
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Workspace,
        };

        // A single `sinexd` runtime binary hosts both engine and gateway.
        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        assert_eq!(requirements[0].binary, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_ingestd_for_node_sdk_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinexd".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_ingestd_for_db_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-db".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-db".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        assert_eq!(requirements[0].binary, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_runtime_binaries_for_e2e_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-e2e-tests".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-e2e-tests".to_string()]),
        };

        // e2e tests drive both the engine and gateway, both served by `sinexd`.
        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        assert_eq!(requirements[0].binary, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_sinexd_for_workspace_tests()
    -> ::xtask::sandbox::TestResult<()> {
        // sinex-workspace-tests includes the gateway-driving TestCoreStack
        // fixture, which spawns the `sinexd rpc-server` subprocess.
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-workspace-tests".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-workspace-tests".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_plan(&plan);
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_skip_unrelated_package_tests()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["xtask".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["xtask".to_string()]),
        };

        assert!(runtime_binary_requirements_for_plan(&plan).is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_skip_lib_only_targets() -> ::xtask::sandbox::TestResult<()>
    {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinexd".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
        };

        assert!(runtime_binary_requirements_for_target(&plan, true, &[], None).is_empty());
        assert!(!runtime_binary_requirements_for_target(&plan, false, &[], None).is_empty());
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_include_ingestd_for_source_worker_production_path()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-source-worker".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-source-worker".to_string()]),
        };

        assert!(
            runtime_binary_requirements_for_target(
                &plan,
                false,
                &["parse_listener_integration_test".to_string()],
                None,
            )
            .is_empty(),
            "non-production-path source-worker integration tests should not pay ingestd prep"
        );

        let requirements = runtime_binary_requirements_for_target(
            &plan,
            false,
            &["production_path".to_string()],
            None,
        );
        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        assert_eq!(requirements[0].binary, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_skip_source_worker_parser_only_production_path_filters()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-source-worker".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-source-worker".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_target(
            &plan,
            false,
            &["production_path".to_string()],
            Some("test(desktop_activitywatch_web_obligations)"),
        );

        assert!(
            requirements.is_empty(),
            "parser-only production_path filters should not rebuild ingestd"
        );
        Ok(())
    }

    #[sinex_test]
    async fn runtime_binary_requirements_keep_source_worker_binary_path_filters()
    -> ::xtask::sandbox::TestResult<()> {
        let plan = NextestExecutionPlan {
            runner_packages: vec!["sinex-source-worker".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-source-worker".to_string()]),
        };

        let requirements = runtime_binary_requirements_for_target(
            &plan,
            false,
            &["production_path".to_string()],
            Some("test(source_worker_binary_scan_private_mode_matrix)"),
        );

        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].package, "sinexd");
        Ok(())
    }

    #[sinex_test]
    async fn database_requirement_tracks_db_backed_test_plans() -> ::xtask::sandbox::TestResult<()>
    {
        let workspace = NextestExecutionPlan {
            runner_packages: Vec::new(),
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Workspace,
        };
        assert!(test_database_required_for_plan(&workspace));

        let db_package = NextestExecutionPlan {
            runner_packages: vec!["sinex-db".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["sinex-db".to_string()]),
        };
        assert!(test_database_required_for_plan(&db_package));

        let xtask_package = NextestExecutionPlan {
            runner_packages: vec!["xtask".to_string()],
            excluded_packages: Vec::new(),
            workload_scope: WorkloadScope::Packages(vec!["xtask".to_string()]),
        };
        assert!(!test_database_required_for_plan(&xtask_package));
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

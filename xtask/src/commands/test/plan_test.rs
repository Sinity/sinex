use super::*;
use crate::sandbox::orchestrator::{
    RuntimeBinaryFreshnessReport, RuntimeBinaryFreshnessStatus,
};
use crate::sandbox::sinex_test;
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

    // Workspace tests spawn both the unified daemon and sinexctl CLI.
    let requirements = runtime_binary_requirements_for_plan(&plan);
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].package, "sinexd");
    assert_eq!(requirements[0].binary, "sinexd");
    assert_eq!(requirements[1].package, "sinexctl");
    assert_eq!(requirements[1].binary, "sinexctl");
    Ok(())
}

#[sinex_test]
async fn runtime_binary_requirements_include_event_engine_for_runtime_tests()
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
async fn runtime_binary_requirements_include_event_engine_for_db_tests()
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
    // sinex-workspace-tests includes gateway-driving fixtures that spawn
    // `sinexd rpc-server` and CLI fixtures that spawn `sinexctl`.
    let plan = NextestExecutionPlan {
        runner_packages: vec!["sinex-workspace-tests".to_string()],
        excluded_packages: Vec::new(),
        workload_scope: WorkloadScope::Packages(vec!["sinex-workspace-tests".to_string()]),
    };

    let requirements = runtime_binary_requirements_for_plan(&plan);
    assert_eq!(requirements.len(), 2);
    assert_eq!(requirements[0].package, "sinexd");
    assert_eq!(requirements[1].package, "sinexctl");
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
async fn runtime_binary_requirements_skip_runtime_independent_sinexd_test_targets()
-> ::xtask::sandbox::TestResult<()> {
    let plan = NextestExecutionPlan {
        runner_packages: vec!["sinexd".to_string()],
        excluded_packages: Vec::new(),
        workload_scope: WorkloadScope::Packages(vec!["sinexd".to_string()]),
    };

    assert!(
        runtime_binary_requirements_for_target(
            &plan,
            false,
            &[
                "registry_dispatch_test".to_string(),
                "transport_security_test".to_string()
            ],
            Some("test(weechat_descriptor_registered) | test(gateway_tls_accepts_handshake)"),
        )
        .is_empty()
    );
    assert!(
        runtime_binary_requirements_for_target(
            &plan,
            false,
            &["replay_rpc_live_test".to_string()],
            Some("test(replay_rpc_live_submits_operation)"),
        )
        .iter()
        .any(|requirement| requirement.package == "sinexd")
    );
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

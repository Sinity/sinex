use super::*;
use crate::sandbox::{TestResult, sinex_test};
use std::path::Path;
use tempfile::tempdir;

fn run_git(args: &[&str], cwd: &Path) -> TestResult<()> {
    let output = Command::new("git").args(args).current_dir(cwd).output()?;
    if !output.status.success() {
        return Err(color_eyre::eyre::eyre!(
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(())
}

#[sinex_test]
async fn test_path_to_package() -> TestResult<()> {
    // Post-fold flat layout: crate/<package>/... -> <package>
    assert_eq!(
        package_for_path("crate/sinex-db/src/lib.rs"),
        Some("sinex-db".to_string())
    );
    assert_eq!(
        package_for_path("crate/sinexd/src/main.rs"),
        Some("sinexd".to_string())
    );
    assert_eq!(
        package_for_path("crate/sinex-primitives/src/lib.rs"),
        Some("sinex-primitives".to_string())
    );

    // CLI crate is crate/sinexctl directly (no more crate/cli/)
    assert_eq!(
        package_for_path("crate/sinexctl/src/main.rs"),
        Some("sinexctl".to_string())
    );
    assert_eq!(
        package_for_path("crate/sinexctl/Cargo.toml"),
        Some("sinexctl".to_string())
    );

    // xtask
    assert_eq!(
        package_for_path("xtask/src/lib.rs"),
        Some("xtask".to_string())
    );
    assert_eq!(
        package_for_path("xtask/Cargo.toml"),
        Some("xtask".to_string())
    );

    // Test crates are workspace members under the top-level tests/ tree.
    assert_eq!(
        package_for_path("tests/e2e/tests/some_test.rs"),
        Some("sinex-e2e-tests".to_string())
    );
    assert_eq!(
        package_for_path("tests/workspace/Cargo.toml"),
        Some("sinex-workspace-tests".to_string())
    );

    // Non-package paths return None (workspace-level handled upstream)
    assert_eq!(package_for_path("README.md"), None);
    assert_eq!(package_for_path("Cargo.toml"), None);
    assert_eq!(package_for_path("Cargo.lock"), None);
    assert_eq!(package_for_path(".config/nextest.toml"), None);
    // Other top-level tests/ entries are shared fixtures, not packages.
    assert_eq!(package_for_path("tests/fixtures/tls/ca.pem"), None);
    // A dotfile directly under crate/ is not a package.
    assert_eq!(
        package_for_path("crate/.sinex/test-artifacts/report.json"),
        None
    );
    Ok(())
}

#[sinex_test]
async fn test_transitive_dependents() -> TestResult<()> {
    let mut reverse_deps = HashMap::new();
    reverse_deps.insert(
        "a".to_string(),
        HashSet::from(["b".to_string(), "c".to_string()]),
    );
    reverse_deps.insert("b".to_string(), HashSet::from(["d".to_string()]));

    let changed = HashSet::from(["a".to_string()]);
    let affected = transitive_dependents(&changed, &reverse_deps);

    assert!(affected.contains("a"));
    assert!(affected.contains("b"));
    assert!(affected.contains("c"));
    assert!(affected.contains("d"));
    Ok(())
}

#[sinex_test]
async fn test_files_to_packages_maps_multiple() -> TestResult<()> {
    let files = vec![
        "crate/sinex-db/src/lib.rs".into(),
        "crate/sinexd/src/main.rs".into(),
        "xtask/src/affected.rs".into(),
    ];
    let pkgs = files_to_packages(&files);
    assert!(pkgs.contains("sinex-db"));
    assert!(pkgs.contains("sinexd"));
    assert!(pkgs.contains("xtask"));
    assert_eq!(pkgs.len(), 3);
    Ok(())
}

#[sinex_test]
async fn test_files_to_packages_deduplicates() -> TestResult<()> {
    let files = vec![
        "crate/sinex-db/src/lib.rs".into(),
        "crate/sinex-db/src/pool.rs".into(),
        "crate/sinex-db/Cargo.toml".into(),
    ];
    let pkgs = files_to_packages(&files);
    assert_eq!(pkgs.len(), 1);
    assert!(pkgs.contains("sinex-db"));
    Ok(())
}

#[sinex_test]
async fn test_files_to_packages_ignores_non_package_files() -> TestResult<()> {
    let files = vec![
        "README.md".into(),
        ".github/workflows/ci.yml".into(),
        "README.md".into(),
    ];
    let pkgs = files_to_packages(&files);
    assert!(pkgs.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_extract_simple_test_name_terms_accepts_boolean_test_names() -> TestResult<()> {
    let names = extract_simple_test_name_terms("(test(alpha_case) | test(beta::gamma$delta))")
        .expect("simple boolean test-name filter should parse");
    assert_eq!(names, vec!["alpha_case", "beta::gamma$delta"]);
    Ok(())
}

#[sinex_test]
async fn test_extract_simple_test_name_terms_rejects_complex_filter_predicates() -> TestResult<()> {
    assert!(extract_simple_test_name_terms("package(xtask) & test(alpha_case)").is_none());
    assert!(extract_simple_test_name_terms("not test(alpha_case)").is_none());
    Ok(())
}

#[sinex_test]
async fn test_infer_packages_for_test_filter_maps_matching_sources() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let graceful = repo
        .path()
        .join("tests/e2e/tests/graceful_shutdown_test.rs");
    let xtask_test = repo.path().join("xtask/src/example_test.rs");
    let workspace_test = repo.path().join("tests/workspace/tests/smoke.rs");
    fs::create_dir_all(graceful.parent().expect("graceful parent"))?;
    fs::create_dir_all(xtask_test.parent().expect("xtask parent"))?;
    fs::create_dir_all(workspace_test.parent().expect("workspace parent"))?;
    fs::write(
        &graceful,
        "#[sinex_test]\nasync fn test_concurrent_service_shutdown() {}\n",
    )?;
    fs::write(
        &xtask_test,
        "#[sinex_test]\nasync fn test_compile_scope() {}\n",
    )?;
    fs::write(
        &workspace_test,
        "#[sinex_test]\nasync fn workspace_smoke_test() {}\n",
    )?;

    let inferred = infer_packages_for_test_filter_in(
        repo.path(),
        "test(test_concurrent_service_shutdown) | test(workspace_smoke_test) | test(test_compile_scope)",
    )?;
    assert_eq!(
        inferred,
        vec![
            "sinex-e2e-tests".to_string(),
            "sinex-workspace-tests".to_string(),
            "xtask".to_string(),
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_for_test_filter_maps_integration_tests() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let large_payload = repo.path().join("tests/e2e/tests/large_payload_test.rs");
    let workspace_test = repo.path().join("tests/workspace/tests/smoke.rs");
    fs::create_dir_all(large_payload.parent().expect("large payload parent"))?;
    fs::create_dir_all(workspace_test.parent().expect("workspace parent"))?;
    fs::write(
        &large_payload,
        "#[sinex_test]\nasync fn test_batch_large_payloads() {}\n",
    )?;
    fs::write(
        &workspace_test,
        "#[sinex_test]\nasync fn workspace_smoke_test() {}\n",
    )?;

    let inferred = infer_test_binaries_for_test_filter_in(
        repo.path(),
        "test(test_batch_large_payloads) | test(workspace_smoke_test)",
    )?;
    assert_eq!(
        inferred,
        vec!["large_payload_test".to_string(), "smoke".to_string()]
    );
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_maps_flat_crate_integration_tests() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let registry = repo
        .path()
        .join("crate/sinexd/tests/sources/registry_dispatch_test.rs");
    fs::create_dir_all(registry.parent().expect("registry parent"))?;
    fs::write(
        repo.path().join("crate/sinexd/Cargo.toml"),
        r#"
[[test]]
name = "registry_dispatch_test"
path = "tests/sources/registry_dispatch_test.rs"
"#,
    )?;
    fs::write(
        &registry,
        "#[sinex_test]\nasync fn weechat_descriptor_registered() {}\n",
    )?;

    let inferred =
        infer_test_binaries_for_test_filter_in(repo.path(), "test(weechat_descriptor_registered)")?;
    assert_eq!(inferred, vec!["registry_dispatch_test".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_for_test_filter_requires_complete_coverage() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let large_payload = repo.path().join("tests/e2e/tests/large_payload_test.rs");
    let inline_test = repo.path().join("xtask/src/inline_tests.rs");
    fs::create_dir_all(large_payload.parent().expect("large payload parent"))?;
    fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
    fs::write(
        &large_payload,
        "#[sinex_test]\nasync fn test_batch_large_payloads() {}\n",
    )?;
    fs::write(
        &inline_test,
        "#[sinex_test]\nasync fn inline_unit_test() {}\n",
    )?;

    let inferred = infer_test_binaries_for_test_filter_in(
        repo.path(),
        "test(test_batch_large_payloads) | test(inline_unit_test)",
    )?;
    assert!(inferred.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_for_test_filter_maps_inline_unit_tests() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let inline_test = repo.path().join("crate/sinexd/src/coordination.rs");
    fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
    fs::write(
        &inline_test,
        "#[sinex_test]\nasync fn leader_maintenance_heartbeat_refreshes_registered_metadata() {}\n",
    )?;

    assert!(infer_lib_target_for_test_filter_in(
        repo.path(),
        "test(leader_maintenance_heartbeat_refreshes_registered_metadata)",
    )?);
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_accepts_nextest_name_fragments() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let inline_test = repo.path().join("xtask/src/config.rs");
    fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
    fs::write(
        &inline_test,
        "\
pub fn workspace_state_dir_for() {}\n\
\n\
#[sinex_test]\n\
async fn test_workspace_state_dir_rejects_sinnix_dev_cache_state() {}\n\
\n\
#[sinex_test]\n\
async fn test_workspace_state_dir_honors_explicit_temp_override() {}\n\
",
    )?;

    assert!(infer_lib_target_for_test_filter_in(
        repo.path(),
        "test(workspace_state_dir)",
    )?);
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_accepts_sibling_test_file_stems() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let sibling_test = repo
        .path()
        .join("crate/sinexd/src/api/handlers/source_status_test.rs");
    fs::create_dir_all(sibling_test.parent().expect("sibling test parent"))?;
    fs::write(
        &sibling_test,
        "#[sinex_test]\nasync fn module_names_include_runtime_aliases() {}\n",
    )?;

    assert!(infer_lib_target_for_test_filter_in(
        repo.path(),
        "test(source_status)",
    )?);
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_honors_selected_package_boundary() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let sibling_test = repo
        .path()
        .join("crate/sinexd/src/api/handlers/source_status_test.rs");
    let other_package_test = repo.path().join("crate/sinex-db/tests/repositories_state.rs");
    fs::create_dir_all(sibling_test.parent().expect("sibling test parent"))?;
    fs::create_dir_all(other_package_test.parent().expect("other test parent"))?;
    fs::write(
        &sibling_test,
        "#[sinex_test]\nasync fn module_names_include_runtime_aliases() {}\n",
    )?;
    fs::write(
        &other_package_test,
        "#[sinex_test]\nasync fn source_status_treats_recent_output_as_runtime_liveness() {}\n",
    )?;
    let selected = HashSet::from(["sinexd"]);

    assert!(infer_lib_target_for_test_filter_in_packages(
        repo.path(),
        "test(source_status)",
        Some(&selected),
    )?);
    assert!(!infer_lib_target_for_test_filter_in_packages(
        repo.path(),
        "test(source_status)",
        None,
    )?);
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_ignores_non_test_function_fragments() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let source = repo.path().join("xtask/src/config.rs");
    fs::create_dir_all(source.parent().expect("source parent"))?;
    fs::write(&source, "pub fn workspace_state_dir_for() {}\n")?;

    assert!(!infer_lib_target_for_test_filter_in(
        repo.path(),
        "test(workspace_state_dir)",
    )?);
    Ok(())
}

#[sinex_test]
async fn test_infer_lib_target_rejects_mixed_integration_coverage() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let inline_test = repo.path().join("xtask/src/inline_tests.rs");
    let integration_test = repo.path().join("xtask/tests/command_contract.rs");
    fs::create_dir_all(inline_test.parent().expect("inline parent"))?;
    fs::create_dir_all(integration_test.parent().expect("integration parent"))?;
    fs::write(
        &inline_test,
        "#[sinex_test]\nasync fn inline_unit_test() {}\n",
    )?;
    fs::write(
        &integration_test,
        "#[sinex_test]\nasync fn command_catalog_exposes_core_public_surface() {}\n",
    )?;

    assert!(!infer_lib_target_for_test_filter_in(
        repo.path(),
        "test(inline_unit_test) | test(command_catalog_exposes_core_public_surface)",
    )?);
    Ok(())
}

#[sinex_test]
async fn test_simple_test_name_term_count_rejects_complex_filters() -> TestResult<()> {
    assert_eq!(
        simple_test_name_term_count("test(one) | test(two)"),
        Some(2)
    );
    assert_eq!(simple_test_name_term_count("!test(one)"), None);
    assert_eq!(simple_test_name_term_count("package(foo)"), None);
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_maps_nested_integration_modules() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let root = repo
        .path()
        .join("crate/sinexd/tests/sources/production_path.rs");
    let nested = repo
        .path()
        .join("crate/sinexd/tests/sources/production_path/browser.rs");
    fs::create_dir_all(nested.parent().expect("nested parent"))?;
    fs::write(
        &root,
        "#[path = \"production_path/browser.rs\"] mod browser;\n",
    )?;
    fs::write(
        &nested,
        "#[sinex_test]\nasync fn browser_history_qutebrowser_initial_ingestion() {}\n",
    )?;

    let inferred = infer_test_binaries_for_test_filter_in(
        repo.path(),
        "test(browser_history_qutebrowser_initial_ingestion)",
    )?;
    assert_eq!(inferred, vec!["production_path".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_maps_deep_nested_integration_modules() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let root = repo
        .path()
        .join("crate/sinexd/tests/sources/production_path.rs");
    let nested = repo
        .path()
        .join("crate/sinexd/tests/sources/production_path/obligations/initial_ingestion.rs");
    fs::create_dir_all(nested.parent().expect("nested parent"))?;
    fs::write(
        &root,
        "#[path = \"production_path/obligations/mod.rs\"] mod obligations;\n",
    )?;
    fs::write(
        &nested,
        "#[sinex_test]\nasync fn source_driver_host_scan_private_mode_matrix() {}\n",
    )?;

    let inferred = infer_test_binaries_for_test_filter_in(
        repo.path(),
        "test(source_driver_host_scan_private_mode_matrix)",
    )?;
    assert_eq!(inferred, vec!["production_path".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_infer_test_binaries_maps_aggregated_nested_integration_modules() -> TestResult<()> {
    let repo = tempfile::tempdir()?;
    let root = repo.path().join("crate/sinexd/tests/integration_tests.rs");
    let nested = repo
        .path()
        .join("crate/sinexd/tests/integration/runtime_lifecycle_test.rs");
    fs::create_dir_all(nested.parent().expect("nested parent"))?;
    fs::write(&root, "mod integration;\nmod support;\n")?;
    fs::write(
        &nested,
        "#[sinex_test]\nasync fn test_runtime_concurrent_lifecycle() {}\n",
    )?;

    let inferred = infer_test_binaries_for_test_filter_in(
        repo.path(),
        "test(test_runtime_concurrent_lifecycle)",
    )?;
    assert_eq!(inferred, vec!["integration_tests".to_string()]);
    Ok(())
}

#[sinex_test]
async fn test_active_dependency_closure_respects_optional_feature_deps() -> TestResult<()> {
    let mut dependency_specs = HashMap::new();
    dependency_specs.insert(
        "xtask".to_string(),
        vec![
            DependencySpec {
                name: "sinex-db".to_string(),
                optional: false,
            },
            DependencySpec {
                name: "sinexd".to_string(),
                optional: true,
            },
        ],
    );
    dependency_specs.insert("sinex-db".to_string(), Vec::new());
    dependency_specs.insert("sinexd".to_string(), Vec::new());

    let mut xtask_features = HashMap::new();
    xtask_features.insert("extra-feature".to_string(), vec!["dep:sinexd".to_string()]);
    let mut features = HashMap::new();
    features.insert("xtask".to_string(), xtask_features);

    let default_closure = active_package_dependency_closure_from_metadata(
        &["xtask".to_string()],
        &[],
        &dependency_specs,
        &features,
    );
    assert_eq!(
        default_closure,
        vec!["sinex-db".to_string(), "xtask".to_string()]
    );

    let feature_closure = active_package_dependency_closure_from_metadata(
        &["xtask".to_string()],
        &["extra-feature".to_string()],
        &dependency_specs,
        &features,
    );
    assert_eq!(
        feature_closure,
        vec![
            "sinex-db".to_string(),
            "sinexd".to_string(),
            "xtask".to_string()
        ]
    );
    Ok(())
}

#[sinex_test]
async fn test_affected_summary_empty() -> TestResult<()> {
    let summary = affected_summary(&[]);
    assert!(summary.contains("No packages affected"));
    Ok(())
}

#[sinex_test]
async fn test_affected_summary_with_packages() -> TestResult<()> {
    let pkgs = vec!["sinex-db".into(), "xtask".into()];
    let summary = affected_summary(&pkgs);
    assert!(summary.contains("2 packages affected"));
    assert!(summary.contains("sinex-db"));
    assert!(summary.contains("xtask"));
    Ok(())
}

#[sinex_test]
async fn test_transitive_dependents_no_deps() -> TestResult<()> {
    let reverse_deps = HashMap::new();
    let changed = HashSet::from(["orphan".to_string()]);
    let affected = transitive_dependents(&changed, &reverse_deps);
    assert_eq!(affected.len(), 1);
    assert!(affected.contains("orphan"));
    Ok(())
}

#[sinex_test]
async fn test_transitive_dependents_diamond() -> TestResult<()> {
    // Diamond: A depends on B and C, both B and C depend on D
    //   A
    //  / \
    // B   C
    //  \ /
    //   D
    let mut reverse_deps = HashMap::new();
    reverse_deps.insert(
        "d".to_string(),
        HashSet::from(["b".to_string(), "c".to_string()]),
    );
    reverse_deps.insert("b".to_string(), HashSet::from(["a".to_string()]));
    reverse_deps.insert("c".to_string(), HashSet::from(["a".to_string()]));

    let changed = HashSet::from(["d".to_string()]);
    let affected = transitive_dependents(&changed, &reverse_deps);
    // All four should be affected
    assert_eq!(affected.len(), 4);
    assert!(affected.contains("a"));
    assert!(affected.contains("b"));
    assert!(affected.contains("c"));
    assert!(affected.contains("d"));
    Ok(())
}

#[sinex_test]
async fn test_parse_workspace_metadata_rejects_missing_package_name() -> TestResult<()> {
    let metadata = serde_json::json!({
        "packages": [
            {
                "dependencies": []
            }
        ]
    });

    let error = WorkspaceMetadata::parse_metadata(&metadata)
        .expect_err("missing package name should surface");
    assert!(format!("{error:#}").contains("package[0] is missing a string name"));
    Ok(())
}

#[sinex_test]
async fn test_parse_workspace_metadata_rejects_missing_dependency_name() -> TestResult<()> {
    let metadata = serde_json::json!({
        "packages": [
            {
                "name": "xtask",
                "dependencies": [
                    {}
                ]
            }
        ]
    });

    let error = WorkspaceMetadata::parse_metadata(&metadata)
        .expect_err("missing dependency name should surface");
    assert!(format!("{error:#}").contains("package[xtask] dependency[0] is missing a string name"));
    Ok(())
}

#[sinex_test]
async fn test_parse_workspace_metadata_builds_reverse_deps() -> TestResult<()> {
    let metadata = serde_json::json!({
        "packages": [
            {
                "name": "xtask",
                "dependencies": [
                    { "name": "sinex-primitives" }
                ]
            },
            {
                "name": "sinex-primitives",
                "dependencies": []
            }
        ]
    });

    let parsed = WorkspaceMetadata::parse_metadata(&metadata)?;
    assert_eq!(
        parsed.packages,
        vec!["xtask".to_string(), "sinex-primitives".to_string()]
    );
    assert_eq!(
        parsed.reverse_deps.get("sinex-primitives"),
        Some(&HashSet::from(["xtask".to_string()]))
    );
    assert_eq!(
        parsed
            .dependency_specs
            .get("xtask")
            .map(|deps| deps.iter().map(|dep| dep.name.as_str()).collect::<Vec<_>>()),
        Some(vec!["sinex-primitives"])
    );
    Ok(())
}

#[sinex_test]
async fn test_path_to_package_underscore_to_hyphen() -> TestResult<()> {
    // Package directories with underscores should map to hyphenated package names
    assert_eq!(
        package_for_path("crate/sinex_primitives/src/lib.rs"),
        Some("sinex-primitives".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn test_changed_files_includes_untracked_workspace_files() -> TestResult<()> {
    let repo = tempdir()?;
    run_git(&["init", "-q"], repo.path())?;
    run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
    std::fs::write(repo.path().join("README.md"), "hello\n")?;
    run_git(&["add", "README.md"], repo.path())?;
    run_git(&["commit", "-qm", "init"], repo.path())?;

    std::fs::create_dir_all(repo.path().join("xtask/src"))?;
    std::fs::write(repo.path().join("xtask/src/new.rs"), "// untracked\n")?;

    let changed = changed_files_in(repo.path())?;
    assert!(changed.contains(&"xtask/src/new.rs".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_changed_files_includes_untracked_workspace_scope_files() -> TestResult<()> {
    let repo = tempdir()?;
    run_git(&["init", "-q"], repo.path())?;
    run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
    std::fs::write(repo.path().join("README.md"), "hello\n")?;
    run_git(&["add", "README.md"], repo.path())?;
    run_git(&["commit", "-qm", "init"], repo.path())?;

    std::fs::create_dir_all(repo.path().join(".config"))?;
    std::fs::write(
        repo.path().join(".config/nextest.toml"),
        "[profile.default]\n",
    )?;
    std::fs::write(repo.path().join("flake.nix"), "{ }\n")?;

    let changed = changed_files_in(repo.path())?;
    assert!(changed.contains(&".config/nextest.toml".to_string()));
    assert!(changed.contains(&"flake.nix".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_changed_files_surfaces_git_failures() -> TestResult<()> {
    let dir = tempfile::Builder::new()
        .prefix("xtask-nongit-")
        .tempdir_in("/tmp")?;

    let error = changed_files_in(dir.path()).expect_err("non-git directory should surface");
    assert!(format!("{error:#}").contains("git diff --name-only HEAD failed"));
    Ok(())
}

#[sinex_test]
async fn test_nixos_modules_dirty_detects_untracked_nixos_files() -> TestResult<()> {
    let repo = tempdir()?;
    run_git(&["init", "-q"], repo.path())?;
    run_git(&["config", "user.name", "Sinex Test"], repo.path())?;
    run_git(&["config", "user.email", "sinex@example.test"], repo.path())?;
    std::fs::write(repo.path().join("README.md"), "hello\n")?;
    run_git(&["add", "README.md"], repo.path())?;
    run_git(&["commit", "-qm", "init"], repo.path())?;

    std::fs::create_dir_all(repo.path().join("nixos/modules"))?;
    std::fs::write(repo.path().join("nixos/modules/example.nix"), "{}\n")?;

    assert!(nixos_modules_dirty_in(repo.path())?);
    Ok(())
}

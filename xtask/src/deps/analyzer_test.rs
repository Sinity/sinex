use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_workspace_analyzer_new() -> TestResult<()> {
    // Should be able to create analyzer for the xtask workspace
    let result = WorkspaceAnalyzer::new();
    assert!(
        result.is_ok(),
        "Failed to create WorkspaceAnalyzer: {:?}",
        result.err()
    );
    Ok(())
}

#[sinex_test]
async fn test_workspace_packages() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let packages = analyzer
        .workspace_packages()
        .expect("Failed to get packages");

    // xtask should be in the workspace
    assert!(!packages.is_empty(), "No workspace packages found");

    // All should be marked as workspace members
    for pkg in &packages {
        assert!(
            pkg.is_workspace,
            "Package {} not marked as workspace",
            pkg.name
        );
        assert!(!pkg.name.is_empty(), "Package has empty name");
        assert!(
            !pkg.version.is_empty(),
            "Package {} has empty version",
            pkg.name
        );
    }

    // xtask itself should be present
    let has_xtask = packages.iter().any(|p| p.name == "xtask");
    assert!(has_xtask, "xtask package not found in workspace");
    Ok(())
}

#[sinex_test]
async fn test_all_dependencies() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let deps = analyzer
        .all_dependencies()
        .expect("Failed to get dependencies");

    // Should have some dependencies
    assert!(!deps.is_empty(), "No dependencies found");

    // Verify structure
    for dep in &deps {
        assert!(!dep.dependent.is_empty(), "Dependency has empty dependent");
        assert!(
            !dep.dependency.is_empty(),
            "Dependency has empty dependency"
        );
        assert!(!dep.kind.is_empty(), "Dependency has empty kind");
    }
    Ok(())
}

#[sinex_test]
async fn test_find_duplicates() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let duplicates = analyzer
        .find_duplicates()
        .expect("Failed to find duplicates");

    // May or may not have duplicates - just verify structure
    for dup in &duplicates {
        assert!(!dup.name.is_empty(), "Duplicate has empty name");
        assert!(
            dup.versions.len() >= 2,
            "Duplicate {} has less than 2 versions",
            dup.name
        );

        // Versions should be sorted
        let mut sorted_versions = dup.versions.clone();
        sorted_versions.sort();
        assert_eq!(
            dup.versions, sorted_versions,
            "Versions for {} not sorted",
            dup.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_package_info_structure() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let packages = analyzer
        .workspace_packages()
        .expect("Failed to get packages");

    // Verify at least one package has all fields populated
    assert!(!packages.is_empty(), "No packages to test");

    let pkg = &packages[0];
    assert!(!pkg.name.is_empty());
    assert!(!pkg.version.is_empty());
    // Version should be parseable as semver format (X.Y.Z)
    let parts: Vec<&str> = pkg.version.split('.').collect();
    assert!(
        parts.len() >= 2,
        "Version {} doesn't look like semver",
        pkg.version
    );
    Ok(())
}

#[sinex_test]
async fn test_duplicates_sorted_by_name() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let duplicates = analyzer
        .find_duplicates()
        .expect("Failed to find duplicates");

    // If we have duplicates, verify they're sorted by name
    if duplicates.len() > 1 {
        for i in 1..duplicates.len() {
            assert!(
                duplicates[i - 1].name <= duplicates[i].name,
                "Duplicates not sorted: {} should come before {}",
                duplicates[i - 1].name,
                duplicates[i].name
            );
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_duplicates_classify_workspace_debt_explicitly() -> TestResult<()> {
    let analyzer = WorkspaceAnalyzer::new().expect("Failed to create analyzer");
    let duplicates = analyzer
        .find_duplicates()
        .expect("Failed to find duplicates");

    for dup in &duplicates {
        let has_direct_roots = dup.direct_workspace_root_count > 0;
        assert_eq!(
            dup.classification.is_direct_workspace(),
            has_direct_roots,
            "{} direct classification did not match direct roots",
            dup.name
        );
        assert_eq!(
            dup.classification.is_transitive_upstream(),
            !has_direct_roots,
            "{} transitive classification did not match direct roots",
            dup.name
        );

        let value = serde_json::to_value(dup)?;
        assert!(
            value.get("classification").is_some(),
            "{} missing duplicate classification in JSON shape",
            dup.name
        );
        assert!(
            value.get("direct_workspace_debt").is_none(),
            "{} still exposes removed duplicate boolean",
            dup.name
        );
        assert!(
            value.get("transitive_only").is_none(),
            "{} still exposes removed duplicate boolean",
            dup.name
        );
    }
    Ok(())
}

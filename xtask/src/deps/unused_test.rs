use super::*;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn test_parse_machete_output_empty() -> TestResult<()> {
    let json = r#"{"unused":[]}"#;
    let report = UnusedDetector::parse_machete_output(json).unwrap();

    assert_eq!(report.unused.len(), 0);
    assert_eq!(report.tool, "cargo-machete");
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_output_single_package() -> TestResult<()> {
    let json = r#"{
        "unused": [
            {
                "package": "sinex-db",
                "dependencies": ["serde", "tokio"]
            }
        ]
    }"#;

    let report = UnusedDetector::parse_machete_output(json).unwrap();

    assert_eq!(report.unused.len(), 2);
    assert_eq!(report.tool, "cargo-machete");
    assert_eq!(report.unused[0].package, "sinex-db");
    assert_eq!(report.unused[0].dependency, "serde");
    assert_eq!(report.unused[1].package, "sinex-db");
    assert_eq!(report.unused[1].dependency, "tokio");
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_output_multiple_packages() -> TestResult<()> {
    let json = r#"{
        "unused": [
            {
                "package": "sinex-db",
                "dependencies": ["serde"]
            },
            {
                "package": "sinexd",
                "dependencies": ["anyhow", "tokio"]
            }
        ]
    }"#;

    let report = UnusedDetector::parse_machete_output(json).unwrap();

    assert_eq!(report.unused.len(), 3);
    assert_eq!(report.unused[0].package, "sinex-db");
    assert_eq!(report.unused[1].package, "sinexd");
    assert_eq!(report.unused[2].package, "sinexd");
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_output_invalid_json() -> TestResult<()> {
    let json = "not valid json";
    let result = UnusedDetector::parse_machete_output(json);

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_stdout_text_output() -> TestResult<()> {
    let report = UnusedDetector::parse_machete_stdout(
        "sinex-db -- ./crate/sinex-db/Cargo.toml:\n    serde\n    tokio\n",
    )?;

    assert_eq!(report.tool, "cargo-machete");
    assert_eq!(report.unused.len(), 2);
    assert_eq!(report.unused[0].package, "sinex-db");
    assert_eq!(report.unused[0].dependency, "serde");
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_text_output_ignores_footer() -> TestResult<()> {
    let report = UnusedDetector::parse_machete_text_output(
        r#"cargo-machete found the following unused dependencies in this directory:
sinex-db -- ./crate/sinex-db/Cargo.toml:
serde
tokio-test

If you believe cargo-machete has detected an unused dependency incorrectly,
you can add the dependency to the list of dependencies to ignore in the
`[package.metadata.cargo-machete]` section of the appropriate Cargo.toml.
For example:

[package.metadata.cargo-machete]
ignored = ["prost"]

Done!
"#,
    )?;

    let deps: Vec<_> = report
        .unused
        .iter()
        .map(|dep| dep.dependency.as_str())
        .collect();
    assert_eq!(deps, vec!["serde", "tokio-test"]);
    assert!(!deps.contains(&"If you believe cargo-machete"));
    assert!(!deps.contains(&"ignored"));
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_text_output_accepts_clean_success_message() -> TestResult<()> {
    let report = UnusedDetector::parse_machete_text_output(
        "cargo-machete didn't find any unused dependencies in this directory. Good job!\n",
    )?;

    assert!(report.unused.is_empty());
    assert_eq!(report.tool, "cargo-machete");
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_text_output_rejects_unindented_dependency() -> TestResult<()> {
    let error = UnusedDetector::parse_machete_text_output("sinex-db -- ./Cargo.toml:\nserde\n")
        .expect_err("dependency entries must be indented");
    assert!(
        format!("{error:#}")
            .contains("cargo-machete emitted a dependency line before any package header")
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_stdout_rejects_malformed_json() -> TestResult<()> {
    let error = UnusedDetector::parse_machete_stdout("{not valid json")
        .expect_err("malformed JSON-looking output should fail");
    assert!(format!("{error:#}").contains("failed to parse"));
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_text_output_rejects_dependency_before_header() -> TestResult<()> {
    let error = UnusedDetector::parse_machete_text_output("serde\n")
        .expect_err("dependency without package header should fail");
    assert!(
        format!("{error:#}")
            .contains("cargo-machete emitted a dependency line before any package header")
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_machete_text_output_rejects_empty_package_name() -> TestResult<()> {
    let error = UnusedDetector::parse_machete_text_output(" -- ./Cargo.toml:\n    serde\n")
        .expect_err("empty package header should fail");
    assert!(format!("{error:#}").contains("empty package name"));
    Ok(())
}

#[sinex_test]
async fn test_parse_udeps_output_empty() -> TestResult<()> {
    let json = r#"{"unused_deps":{}}"#;
    let report = UnusedDetector::parse_udeps_output(json).unwrap();

    assert_eq!(report.unused.len(), 0);
    assert_eq!(report.tool, "cargo-udeps");
    Ok(())
}

#[sinex_test]
async fn test_parse_udeps_output_single_package() -> TestResult<()> {
    let json = r#"{
        "unused_deps": {
            "sinex-db": ["serde", "tokio"]
        }
    }"#;

    let report = UnusedDetector::parse_udeps_output(json).unwrap();

    assert_eq!(report.unused.len(), 2);
    assert_eq!(report.tool, "cargo-udeps");

    // Check both dependencies are present (order may vary due to HashMap)
    let deps: Vec<_> = report
        .unused
        .iter()
        .map(|d| d.dependency.as_str())
        .collect();
    assert!(deps.contains(&"serde"));
    assert!(deps.contains(&"tokio"));
    Ok(())
}

#[sinex_test]
async fn test_parse_udeps_output_invalid_json() -> TestResult<()> {
    let json = "not valid json";
    let result = UnusedDetector::parse_udeps_output(json);

    assert!(result.is_err());
    Ok(())
}

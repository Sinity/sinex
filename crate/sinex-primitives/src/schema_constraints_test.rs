use super::*;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn check_clause_quotes_single_quotes() -> TestResult<()> {
    let spec = DbCheckSpec {
        schema: "core",
        table: "t",
        column: "c",
        version: 1,
        allowed_values: &["a", "it's"],
        enum_name: "E",
    };
    assert_eq!(spec.check_clause(), "c IN ('a', 'it''s')");
    Ok(())
}

#[sinex_test]
async fn constraint_names() -> TestResult<()> {
    let spec = DbCheckSpec {
        schema: "core",
        table: "manifests",
        column: "manifest_type",
        version: 1,
        allowed_values: &["source"],
        enum_name: "ModuleKind",
    };
    assert_eq!(spec.constraint_name(), "manifest_type_check_v1");
    assert_eq!(spec.constraint_name_prefix(), "manifest_type_check_v");
    assert_eq!(
        spec.legacy_constraint_name(),
        "manifests_manifest_type_check"
    );
    assert_eq!(spec.qualified_table(), "core.manifests");
    Ok(())
}

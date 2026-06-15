use std::path::PathBuf;

use serde_json::Value;
use sinexd::sources::privacy_coverage::{
    PRIVACY_COVERAGE_ARTIFACT_PATH, render_privacy_coverage_matrix,
};
use xtask::sandbox::prelude::*;

fn rendered_matrix() -> TestResult<Value> {
    Ok(serde_json::from_str(&render_privacy_coverage_matrix()?)?)
}

fn entry<'a>(matrix: &'a Value, source_id: &str) -> &'a Value {
    matrix["entries"]
        .as_array()
        .expect("entries array")
        .iter()
        .find(|entry| entry["source_id"] == source_id)
        .unwrap_or_else(|| panic!("missing privacy coverage entry for {source_id}"))
}

#[sinex_test]
async fn privacy_coverage_matrix_includes_source_contract_privacy_tiers() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let weechat = entry(&matrix, "weechat.message");

    assert_eq!(weechat["source_contract"]["privacy_tier"], "sensitive");
    assert_eq!(weechat["runtime_binding"]["privacy_context"], "command");
    assert_eq!(
        weechat["surface_behaviors"]["privacy_export"],
        "metadata_only_export"
    );
    assert_eq!(
        weechat["surface_behaviors"]["query_recent_tui_logs"],
        "not_applicable"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_includes_declarative_field_metadata() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let weechat = entry(&matrix, "weechat.message");

    assert_eq!(weechat["field_metadata_status"], "available");
    let fields = weechat["field_privacy_metadata"]
        .as_array()
        .expect("declarative field rows");
    assert!(
        fields.iter().any(|field| {
            field["field_name"] == "message"
                && field["field_type"] == "string"
                && field["field_class"] == "column_index"
                && field["effective_privacy_context"] == "command"
        }),
        "weechat.message field metadata should include message column"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_marks_imperative_field_metadata_unavailable() -> TestResult<()> {
    let matrix = rendered_matrix()?;
    let bash = entry(&matrix, "terminal.bash-history");

    assert_eq!(bash["field_metadata_status"], "unavailable");
    assert_eq!(bash["field_metadata_behavior"], "unclassified");
    assert!(
        bash["caveats"]
            .to_string()
            .contains("field-level metadata unavailable"),
        "imperative parser entries must carry an explicit caveat"
    );
    Ok(())
}

#[sinex_test]
async fn privacy_coverage_matrix_artifact_matches_inventory() -> TestResult<()> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..");
    let artifact = workspace_root.join(PRIVACY_COVERAGE_ARTIFACT_PATH);

    let rendered = render_privacy_coverage_matrix().expect("render privacy coverage matrix");
    let committed = std::fs::read_to_string(&artifact).unwrap_or_else(|e| {
        panic!(
            "failed to read committed privacy coverage matrix at {}: {e}\n\
             run `sinexd export-privacy-coverage-matrix` to generate it",
            artifact.display()
        )
    });

    assert_eq!(
        committed,
        rendered,
        "privacy coverage matrix artifact is stale — run `sinexd export-privacy-coverage-matrix` to regenerate ({})",
        artifact.display()
    );
    Ok(())
}

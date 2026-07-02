use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn evidence_bundle_view_schema_exposes_stable_context_fields()
-> xtask::sandbox::TestResult<()> {
    let schema = serde_json::to_value(schemars::schema_for!(EvidenceBundleView))
        .expect("EvidenceBundleView schema serializes");
    let schema_text = serde_json::to_string(&schema).expect("schema renders as JSON text");

    for field in [
        "target_refs",
        "diagnostic_excerpts",
        "omitted_sections",
        "caveats",
        "disclosure_caveats",
        "actions",
        "package_completeness",
    ] {
        assert!(
            schema_text.contains(field),
            "EvidenceBundleView schema should expose `{field}`"
        );
    }
    Ok(())
}

use super::*;
use sinex_primitives::query::LineageNode;
use sinex_primitives::testing::event_fixture;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use xtask::sandbox::prelude::sinex_test;

fn lineage_fixture() -> LineageResult {
    let root = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.root"),
        json!({ "message": "root" }),
    );
    let parent = event_fixture(
        sinex_primitives::EventSource::from_static("test"),
        sinex_primitives::EventType::from_static("test.parent"),
        json!({ "message": "parent" }),
    );

    LineageResult {
        root,
        ancestors: vec![LineageNode {
            event: parent,
            depth: 1,
        }],
        descendants: Vec::new(),
        material_links: Vec::new(),
    }
}

#[sinex_test]
async fn explain_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let result = lineage_fixture();
    let output = render_explain_machine_output(
        &result,
        "01912345-6789-7abc-def0-123456789abc",
        OutputFormat::Json,
    )?
    .ok_or_else(|| color_eyre::eyre::eyre!("json output expected"))?;
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(value["source_surface"], "sinexctl.events.explain");
    assert_eq!(
        value["query_echo"]["event_id"],
        "01912345-6789-7abc-def0-123456789abc"
    );
    assert_eq!(value["payload"]["root"]["event_type"], "test.root");
    assert_eq!(value["payload"]["ancestors"][0]["depth"], 1);
    Ok(())
}

#[sinex_test]
async fn explain_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let result = lineage_fixture();
    let err = render_explain_machine_output(&result, "id", OutputFormat::Ndjson);
    assert!(err.is_err(), "explain must remain a finite view");
    Ok(())
}

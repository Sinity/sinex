use super::*;
use serde_json::json;
use sinex_primitives::events::{DynamicPayload, SourceMaterial};
use sinex_primitives::ids::Id;
use sinex_primitives::query::SourceMaterialLinkInfo;
use xtask::sandbox::prelude::sinex_test;

fn material_event(source: &str, event_type: &str) -> Event<JsonValue> {
    let mut event = DynamicPayload::new(source, event_type, json!({}))
        .from_material(Id::<SourceMaterial>::new())
        .build()
        .expect("material test event should build");
    event.id = Some(Id::new());
    event
}

fn synthesis_event(
    source: &str,
    event_type: &str,
    parents: impl IntoIterator<Item = Id<Event<JsonValue>>>,
) -> Event<JsonValue> {
    let mut event = DynamicPayload::new(source, event_type, json!({}))
        .from_parents(parents)
        .expect("derived test event should accept non-empty parents")
        .build()
        .expect("derived test event should build");
    event.id = Some(Id::new());
    event
}

fn event_id(event: &Event<JsonValue>) -> String {
    event.id.expect("test event should have an id").to_string()
}

#[sinex_test]
async fn dot_renderer_uses_provenance_edges_instead_of_flattening_to_root()
-> xtask::sandbox::TestResult<()> {
    let ancestor = material_event("fs", "file.created");
    let root = synthesis_event(
        "process",
        "document.parsed",
        [ancestor.id.expect("ancestor id")],
    );
    let descendant =
        synthesis_event("process", "document.chunked", [root.id.expect("root id")]);

    let dot = render_dot(&LineageResult {
        root: root.clone(),
        ancestors: vec![LineageNode {
            event: ancestor.clone(),
            depth: 1,
        }],
        descendants: vec![LineageNode {
            event: descendant.clone(),
            depth: 1,
        }],
        material_links: Vec::new(),
    });

    let ancestor_id = event_id(&ancestor);
    let root_id = event_id(&root);
    let descendant_id = event_id(&descendant);

    assert!(
        dot.contains(&format!("\"{ancestor_id}\" -> \"{root_id}\"")),
        "DOT should render the ancestor event as the root's derived parent"
    );
    assert!(
        dot.contains(&format!("\"{root_id}\" -> \"{descendant_id}\"")),
        "DOT should render the root event as the descendant's derived parent"
    );
    assert!(
        dot.contains("color=\"#8250df\""),
        "derived edges should be visually distinct"
    );
    Ok(())
}

#[sinex_test]
async fn dot_renderer_includes_material_evidence_and_legend() -> xtask::sandbox::TestResult<()>
{
    let root = material_event("fs", "file.created");
    let from_material_id = Id::<SourceMaterial>::new().to_uuid();
    let to_material_id = Id::<SourceMaterial>::new().to_uuid();

    let dot = render_dot(&LineageResult {
        root,
        ancestors: Vec::new(),
        descendants: Vec::new(),
        material_links: vec![SourceMaterialLinkInfo {
            from_material_id,
            to_material_id,
            relation_type: "derived_from".to_string(),
            metadata: json!({}),
            created_at: sinex_primitives::Timestamp::now(),
        }],
    });

    assert!(
        dot.contains("label=\"legend\""),
        "DOT output should explain visual edge semantics"
    );
    assert!(
        dot.contains("style=dotted color=\"#0969da\""),
        "material provenance edge should be dotted and blue"
    );
    assert!(
        dot.contains("style=dashed color=\"#6e7781\""),
        "source-material evidence link should be dashed and gray"
    );
    Ok(())
}

#[sinex_test]
async fn trace_machine_output_uses_view_envelope_json() -> xtask::sandbox::TestResult<()> {
    let root = material_event("fs", "file.created");
    let event_id = root.id.expect("root id");
    let output = render_trace_machine_output(
        &LineageResult {
            root,
            ancestors: Vec::new(),
            descendants: Vec::new(),
            material_links: Vec::new(),
        },
        &event_id,
        OutputFormat::Json,
    )?
    .expect("json should render");
    let value: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(value["source_surface"], "sinexctl.events.trace");
    assert_eq!(value["query_echo"]["event_id"], event_id.to_string());
    assert!(value["payload"].get("root").is_some());
    Ok(())
}

#[sinex_test]
async fn trace_machine_output_rejects_ndjson() -> xtask::sandbox::TestResult<()> {
    let root = material_event("fs", "file.created");
    let event_id = root.id.expect("root id");
    let result = render_trace_machine_output(
        &LineageResult {
            root,
            ancestors: Vec::new(),
            descendants: Vec::new(),
            material_links: Vec::new(),
        },
        &event_id,
        OutputFormat::Ndjson,
    );

    assert!(result.is_err(), "trace is a finite graph view");
    Ok(())
}

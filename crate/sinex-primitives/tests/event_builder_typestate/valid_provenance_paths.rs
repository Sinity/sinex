use sinex_primitives::{DynamicPayload, Event, Id, SourceMaterial};

fn main() {
    let material_id = Id::<SourceMaterial>::new();
    let _material_event = DynamicPayload::new("test-source", "test.material", serde_json::json!({}))
        .into_builder()
        .from_material(material_id, 0)
        .build()
        .expect("material provenance builder should compile and build");

    let parent_event_id = Id::<Event<serde_json::Value>>::new();
    let _derived_event = DynamicPayload::new("test-source", "test.derived", serde_json::json!({}))
        .into_builder()
        .from_parents([parent_event_id])
        .expect("derived provenance builder should compile")
        .build()
        .expect("derived provenance builder should build");
}

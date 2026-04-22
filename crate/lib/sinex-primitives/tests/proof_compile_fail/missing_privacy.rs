use sinex_primitives::{proof::RuntimeUnitDescriptor, subject_ref};

fn main() {
    let _ = RuntimeUnitDescriptor::builder(
        subject_ref!("runtime_unit:test.missing_privacy"),
        "test.missing_privacy",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .build();
}

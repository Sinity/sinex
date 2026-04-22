use sinex_primitives::{proof::RuntimeUnitDescriptor, subject_ref};

fn main() {
    let _ = RuntimeUnitDescriptor::builder(
        subject_ref!("runtime_unit:test.missing_material"),
        "test.missing_material",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .checkpoint_policy("row_id")
    .build();
}

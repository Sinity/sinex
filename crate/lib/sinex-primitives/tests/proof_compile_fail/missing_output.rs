use sinex_primitives::{proof::RuntimeUnitDescriptor, subject_ref};

fn main() {
    let _ = RuntimeUnitDescriptor::builder(
        subject_ref!("runtime_unit:test.missing_output"),
        "test.missing_output",
        "test",
    )
    .adapter("sqlite_row_stream")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .build();
}

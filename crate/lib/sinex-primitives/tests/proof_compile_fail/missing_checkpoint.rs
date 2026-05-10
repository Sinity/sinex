use sinex_primitives::{proof::SourceUnitBinding, subject_ref};

fn main() {
    let _ = SourceUnitBinding::builder(
        subject_ref!("runtime_unit:test.missing_checkpoint"),
        "test.missing_checkpoint",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .build();
}

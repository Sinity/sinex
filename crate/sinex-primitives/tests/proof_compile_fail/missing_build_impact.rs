use sinex_primitives::{
    proof::{CheckpointFamily, RuntimeShape, SourceUnitBinding},
    subject_ref,
};

fn main() {
    let _ = SourceUnitBinding::builder(
        subject_ref!("runtime_unit:test.missing_build_impact"),
        "test.missing_build_impact",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build();
}

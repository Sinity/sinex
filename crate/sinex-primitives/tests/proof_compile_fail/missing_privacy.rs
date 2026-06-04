use sinex_primitives::{
    proof::{CheckpointFamily, RuntimeShape, SourceRuntimeBinding, SourceBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_privacy"),
        "test.missing_privacy",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();
}

use sinex_primitives::{
    source_contracts::{CheckpointFamily, RuntimeShape, SourceRuntimeBinding, SourceBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_checkpoint"),
        "test.missing_checkpoint",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();
}

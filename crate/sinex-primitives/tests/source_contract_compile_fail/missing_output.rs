use sinex_primitives::{
    source_contracts::{CheckpointFamily, RuntimeShape, SourceRuntimeBinding, SourceBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_output"),
        "test.missing_output",
        "test",
    )
    .adapter("sqlite_row_stream")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();
}

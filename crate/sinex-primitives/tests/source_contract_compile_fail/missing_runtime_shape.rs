use sinex_primitives::{
    source_contracts::{CheckpointFamily, SourceRuntimeBinding, SourceBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_runtime_shape"),
        "test.missing_runtime_shape",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .build_impact(SourceBuildImpact::ZERO)
    .build();
}

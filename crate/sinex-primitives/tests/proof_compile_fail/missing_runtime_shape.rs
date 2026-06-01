use sinex_primitives::{
    proof::{CheckpointFamily, SourceUnitBinding, SourceUnitBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceUnitBinding::builder(
        subject_ref!("runtime_unit:test.missing_runtime_shape"),
        "test.missing_runtime_shape",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .sensitivity_profile("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build();
}

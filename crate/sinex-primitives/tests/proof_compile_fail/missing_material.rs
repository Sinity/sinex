use sinex_primitives::{
    proof::{CheckpointFamily, RuntimeShape, SourceUnitBinding, SourceUnitBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceUnitBinding::builder(
        subject_ref!("runtime_unit:test.missing_material"),
        "test.missing_material",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .sensitivity_profile("command")
    .checkpoint_policy("row_id")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build();
}

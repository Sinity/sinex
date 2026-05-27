use sinex_primitives::{
    proof::{RuntimeShape, SourceUnitBinding, SourceUnitBuildImpact},
    subject_ref,
};

fn main() {
    let _ = SourceUnitBinding::builder(
        subject_ref!("runtime_unit:test.missing_checkpoint_family"),
        "test.missing_checkpoint_family",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context("command")
    .material_policy("canonical_json_lines")
    .checkpoint_policy("row_id")
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceUnitBuildImpact::ZERO)
    .build();
}

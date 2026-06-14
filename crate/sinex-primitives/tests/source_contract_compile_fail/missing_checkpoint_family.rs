use sinex_primitives::{
    source_contracts::{RuntimeShape, SourceRuntimeBinding, SourceBuildImpact},
    subject_ref,
};
use sinex_primitives::privacy::ProcessingContext;

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_checkpoint_family"),
        "test.missing_checkpoint_family",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context(ProcessingContext::Command)
    .runtime_shape(RuntimeShape::Continuous)
    .build_impact(SourceBuildImpact::ZERO)
    .build();
}

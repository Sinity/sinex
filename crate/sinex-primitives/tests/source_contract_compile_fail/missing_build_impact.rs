use sinex_primitives::{
    source_contracts::{CheckpointFamily, RuntimeShape, SourceRuntimeBinding},
    subject_ref,
};
use sinex_primitives::privacy::ProcessingContext;

fn main() {
    let _ = SourceRuntimeBinding::builder(
        subject_ref!("runtime_unit:test.missing_build_impact"),
        "test.missing_build_impact",
        "test",
    )
    .adapter("sqlite_row_stream")
    .output_event_type("test.output")
    .privacy_context(ProcessingContext::Command)
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .build();
}

// Compile-fail: a #[derive(SourceDefinition)] with an unknown `privacy_tier`
// value must not compile (#1727 slice-4 compile-fail matrix).
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.invalid-tier",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    input_shape = "json",
    adapter = "AppendOnlyFileAdapter",
    occurrence_identity = "anchor",
    privacy_tier = "SuperSensitive"
)]
pub struct InvalidPrivacyTier {
    #[source(json_pointer = "/value")]
    pub value: String,
}

fn main() {}

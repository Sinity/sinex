// Compile-fail: a #[derive(SourceDefinition)] with an unknown key in
// #[privacy(...)] must not compile (#1727 slice-4 compile-fail matrix).
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.unknown-privacy-key",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    input_shape = "json",
    adapter = "AppendOnlyFileAdapter",
    occurrence_identity = "anchor"
)]
pub struct UnknownPrivacyKey {
    #[source(json_pointer = "/text")]
    #[privacy(redaction_level = "full")]
    pub text: String,
}

fn main() {}

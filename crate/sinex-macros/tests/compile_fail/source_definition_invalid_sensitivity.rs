// Compile-fail: a #[derive(SourceDefinition)] with an unknown sensitivity hint
// in #[privacy(sensitivity = "...")] must not compile (#1727 slice-4
// compile-fail matrix).
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.invalid-sensitivity",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    input_shape = "json",
    adapter = "AppendOnlyFileAdapter",
    occurrence_identity = "anchor"
)]
pub struct InvalidSensitivity {
    #[source(json_pointer = "/text")]
    #[privacy(sensitivity = "super_private")]
    pub text: String,
}

fn main() {}

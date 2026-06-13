// Compile-fail: a #[derive(SourceDefinition)] without `occurrence_identity`
// must not compile (#1727 slice-1 compile-fail subset).
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.missing-occurrence",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    input_shape = "json",
    adapter = "AppendOnlyFileAdapter"
)]
pub struct MissingOccurrence {
    #[source(json_pointer = "/value")]
    pub value: String,
}

fn main() {}

// Compile-fail: a #[derive(SourceDefinition)] with an unsupported adapter name
// must not compile (#1727 slice-4 compile-fail matrix). "ChainedAdapter" is a
// real shape used by browser/history.rs but is not yet in the
// adapter_type_ident allowlist — the author must use explicit register_source!
// wiring (tracked in the escape-hatch follow-up issue).
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.unknown-adapter",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    input_shape = "json",
    adapter = "ChainedAdapter",
    occurrence_identity = "anchor"
)]
pub struct UnknownAdapter {
    #[source(json_pointer = "/value")]
    pub value: String,
}

fn main() {}

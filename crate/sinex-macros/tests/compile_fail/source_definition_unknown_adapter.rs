// Compile-fail: a #[derive(SourceDefinition)] with an unsupported adapter name
// must not compile (#1727 slice-4 compile-fail matrix). "ChainedAdapter" is a
// real shape used by browser/history.rs but is intentionally outside the
// adapter_type_ident allowlist. Generic or locally aliased adapters must keep
// explicit register_source! wiring so the concrete adapter stack remains
// visible at the registration site.
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

// Compile-fail: an #[event_dispatch(... => "type")] target that is not one of
// the source definition's declared event types must not compile (#1727 slice-1
// compile-fail subset). "declared.event" is allowed (listed in `event_types`);
// "undeclared.event" is not.
use sinex_macros::SourceDefinition;

#[derive(SourceDefinition)]
#[source_definition(
    id = "test.dispatch",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    event_types = "declared.event",
    input_shape = "json",
    adapter = "AppendOnlyFileAdapter",
    occurrence_identity = "anchor"
)]
pub struct DispatchUndeclared {
    #[source(json_pointer = "/kind")]
    #[event_dispatch("a" => "declared.event", "b" => "undeclared.event")]
    pub kind: String,
}

fn main() {}

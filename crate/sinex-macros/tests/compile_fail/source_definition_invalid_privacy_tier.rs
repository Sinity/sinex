// Compile-fail: `privacy_tier` is now a typed enum-path attribute, not a magic
// string. Passing a string literal (the old form) must not compile — the macro
// rejects it at parse time because the value is not a path. An invalid *variant*
// (e.g. `PrivacyTier::Bogus`) is caught one level up by the type system at the
// registration site, so the macro itself only guards the path-shaped value here.
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

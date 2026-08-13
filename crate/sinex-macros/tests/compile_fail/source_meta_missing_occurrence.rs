// Compile-fail: a #[derive(SourceMeta)] without `occurrence_identity` must not
// compile (#1727 slice-3 compile-fail subset).
use sinex_macros::SourceMeta;

#[derive(Default, SourceMeta)]
#[source_meta(
    id = "test.missing-occurrence",
    namespace = "test",
    event_source = "test.src",
    event_type = "test.event",
    adapter = "AppendOnlyFileAdapter",
    recovery_policy = sinex_primitives::source_contracts::SourceRecoveryPolicy::APPEND_STREAM
)]
pub struct MissingOccurrence;

fn main() {}

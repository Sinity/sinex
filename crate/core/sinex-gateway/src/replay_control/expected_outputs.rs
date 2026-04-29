#[derive(Debug, Clone)]
pub(super) struct ExpectedReplayOutputs {
    pub(super) minimum_visible_count: u64,
    pub(super) sources: Vec<String>,
    pub(super) event_types: Vec<String>,
    pub(super) logical_source_identifiers: Vec<String>,
}

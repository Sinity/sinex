use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const COMPLETION_RESPONSE_SCHEMA_VERSION: &str = "sinex.completion-response/v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompletionCandidateView {
    pub value: String,
    pub insert: String,
    pub replace_start: usize,
    pub replace_end: usize,
    pub display: String,
    pub kind: String,
    pub group: String,
    pub description: String,
    pub source: Option<String>,
    pub stale: bool,
    pub danger: String,
    pub privacy: String,
    pub preview: Option<String>,
    pub score: u16,
}

impl CompletionCandidateView {
    #[must_use]
    pub fn new(
        value: impl Into<String>,
        kind: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        replace_start: usize,
        replace_end: usize,
        score: u16,
    ) -> Self {
        let value = value.into();
        Self {
            insert: value.clone(),
            display: value.clone(),
            value,
            replace_start,
            replace_end,
            kind: kind.into(),
            group: group.into(),
            description: description.into(),
            source: None,
            stale: false,
            danger: "none".to_string(),
            privacy: "metadata-only".to_string(),
            preview: None,
            score,
        }
    }

    #[must_use]
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    #[must_use]
    pub fn stale(mut self, stale: bool) -> Self {
        self.stale = stale;
        self
    }

    #[must_use]
    pub fn danger(mut self, danger: impl Into<String>) -> Self {
        self.danger = danger.into();
        self
    }

    #[must_use]
    pub fn privacy(mut self, privacy: impl Into<String>) -> Self {
        self.privacy = privacy.into();
        self
    }

    #[must_use]
    pub fn preview(mut self, preview: impl Into<String>) -> Self {
        self.preview = Some(preview.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompletionResponseView {
    pub schema_version: String,
    pub line: String,
    pub cursor: usize,
    pub active_token: String,
    pub candidates: Vec<CompletionCandidateView>,
}

impl CompletionResponseView {
    #[must_use]
    pub fn new(
        line: impl Into<String>,
        cursor: usize,
        active_token: impl Into<String>,
        candidates: Vec<CompletionCandidateView>,
    ) -> Self {
        Self {
            schema_version: COMPLETION_RESPONSE_SCHEMA_VERSION.to_string(),
            line: line.into(),
            cursor,
            active_token: active_token.into(),
            candidates,
        }
    }
}

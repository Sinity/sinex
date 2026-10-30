//! The payload fields a versioned computation needs to remain estimable.
use crate::{EventId, KindRef};
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRequirement {
    pub kind: KindRef,
    /// JSON Pointers; `*` matches one array/object component. Empty means
    /// this kind contributes only metadata, not payload values.
    pub paths: Vec<String>,
    /// Branch selectors read by the computation. A transformed selector is
    /// unavailable even when its projected value selects another branch.
    #[serde(default)]
    pub when: Vec<(String, serde_json::Value)>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputUnavailableReason {
    RequiredPathTransformed,
    UndeclaredAffectedInput,
    ProjectionUnavailable,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnavailableInput {
    pub event: EventId,
    pub kind: KindRef,
    pub reason: InputUnavailableReason,
}

impl InputRequirement {
    pub fn new(kind: KindRef, paths: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            kind,
            paths: paths.into_iter().map(Into::into).collect(),
            when: vec![],
        }
    }
}

//! Privacy classes. Rules themselves are data in the store; the class travels
//! with every event so outputs can withhold or mask.
use serde::{Deserialize, Serialize};

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    crate::DbCheck,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// Plaintext, visible to every caller.
    #[default]
    Open,
    /// Plaintext at rest; values withheld from callers not granted the class.
    Guarded,
    /// Bytes and sensitive fields encrypted under a key the operator holds.
    Sealed,
}

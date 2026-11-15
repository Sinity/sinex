//! What Sinex recorded about a producer's own earlier material, separate from
//! whether its headers are permitted now.
use super::*;
use sinex_model::KindRef;

/// Expected values compare against the acknowledgment recorded for the
/// original material frame. They grant no permission and cannot select a
/// replacement source declaration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordedContinuationRequest {
    pub frame: String,
    pub frame_content_hash: String,
    pub material: MaterialId,
    pub blob: String,
    pub length: u64,
    #[serde(default)]
    pub uses: Vec<RecordedHeaderUse>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordedHeaderUse {
    pub kind: KindRef,
    pub kind_definition_hash: String,
    pub class: Class,
}

/// What Sinex recorded with the material. The daemon instance and read scope
/// at the time were not stored; neither is made up or required.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordedMaterialBinding {
    pub ack: Acknowledgment,
    pub material: MaterialId,
    pub blob: String,
    pub length: u64,
    pub source: SourceRef,
    pub source_fingerprint: String,
    pub family: String,
    /// Provenance only, including a known empty historical set. Current
    /// header permission uses the current authenticated write grant.
    pub write_kind_prefixes: Vec<String>,
    pub producer_host: Option<String>,
}

/// Even PermittedHeaders is only a current observation of these headers.
/// Submitting the frame again rechecks body, source, kinds, class and which
/// producer wrote the material.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecordedHeaders {
    NotRequested,
    PermittedHeaders,
    NotPermitted { reason: String },
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RecordedContinuation {
    Unknown {
        request: RecordedContinuationRequest,
        reason: String,
    },
    Stored {
        request: RecordedContinuationRequest,
        binding: Box<RecordedMaterialBinding>,
        headers: RecordedHeaders,
    },
}

//! Own retained material evidence, separate from current header permission.
use super::*;
use sinex_model::KindRef;

/// Expected values compare against the original owned MATERIAL receipt. They
/// grant no authority and cannot select a replacement source declaration.
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

/// These are retained owner facts. Historical owner instance and read scope
/// were not stored by this receipt path; neither is fabricated or required.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RecordedMaterialBinding {
    pub receipt: Acknowledgment,
    pub material: MaterialId,
    pub blob: String,
    pub length: u64,
    pub source: SourceRef,
    pub source_fingerprint: String,
    pub family: String,
    pub write_kind_prefixes: Vec<String>,
    pub producer_host: Option<String>,
}

/// Even PermittedHeaders is only a current observation of these headers.
/// Ordinary submit rechecks body, source, kinds, class and material ownership.
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
    Retained {
        request: RecordedContinuationRequest,
        binding: Box<RecordedMaterialBinding>,
        headers: RecordedHeaders,
    },
}

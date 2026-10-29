//! Supplied vectors and their stored derivation. Receiving a vector never
//! constructs a provider or repeats an embedding purchase.
use crate::{EventId, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Identity of one comparable vector space. The digest excludes input text.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct EmbeddingSpace {
    pub provider: String,
    pub model: String,
    pub revision: String,
    pub settings_hash: String,
    pub dimensions: u64,
}
impl EmbeddingSpace {
    pub fn id(&self) -> String {
        blake3::hash(&serde_json::to_vec(self).expect("embedding space serializes"))
            .to_hex()
            .to_string()
    }
    pub fn validate(&self) -> Result<(), String> {
        if [&self.provider, &self.model, &self.revision]
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err("embedding space needs provider, model and revision".into());
        }
        validate_digest(&self.settings_hash, "settings hash")
    }
}

/// An outside producer supplies an existing vector, parented to the target
/// reading. The input hash is the shared BLAKE3 text identity, independent of
/// physical copies. Its exact receipt remains in the producer's ingress bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "embedding.supplied", search = none, key = ["target", "input_record", "space", "input_hash"], replace = newer, time = embedded_at, quality = intrinsic)]
pub struct SuppliedEmbedding {
    pub target: EventId,
    pub input_record: String,
    pub input_hash: String,
    pub space: EmbeddingSpace,
    pub vector: Vec<f64>,
    pub embedded_at: Option<Timestamp>,
}
impl SuppliedEmbedding {
    pub fn validate(&self) -> Result<(), String> {
        validate_values(
            &self.space,
            &self.input_record,
            &self.input_hash,
            &self.vector,
        )
    }
}

/// The complete original vector and its provenance. Read-model limits affect
/// only the rebuildable projection, never admission of this stored result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema, crate::EventPayload)]
#[event(version = 2, type = "text.embedding", search = none, key = ["target", "input_record", "space", "input_hash"], replace = newer, time = embedded_at, quality = intrinsic)]
pub struct TextEmbedding {
    pub target: EventId,
    pub supplied: EventId,
    pub input_record: String,
    pub input_hash: String,
    pub space: EmbeddingSpace,
    pub vector: Vec<f64>,
    pub embedded_at: Option<Timestamp>,
}
impl TextEmbedding {
    pub fn validate(&self) -> Result<(), String> {
        validate_values(
            &self.space,
            &self.input_record,
            &self.input_hash,
            &self.vector,
        )
    }
}
fn validate_values(
    space: &EmbeddingSpace,
    input_record: &str,
    input_hash: &str,
    vector: &[f64],
) -> Result<(), String> {
    space.validate()?;
    validate_digest(input_hash, "input hash")?;
    if input_record.trim().is_empty() {
        return Err("embedding needs a stable input record".into());
    }
    if vector.len() as u64 != space.dimensions {
        return Err("embedding dimension differs from supplied count".into());
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err("embedding contains a non-finite element".into());
    }
    Ok(())
}
fn validate_digest(value: &str, name: &str) -> Result<(), String> {
    if value.len() != 64 || !value.bytes().all(|value| value.is_ascii_hexdigit()) {
        return Err(format!("embedding {name} must be a BLAKE3 digest"));
    }
    Ok(())
}

//! Compact custody declarations, independent of potentially large frame JSON.
use crate::{Class, InputRef, MaterialId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Material,
    Envelope,
}
impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Self::Material => "material",
            Self::Envelope => "envelope",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bytes {
    pub blob: String,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Material {
    pub id: MaterialId,
    pub descriptor: String,
    pub bytes: Bytes,
}
impl Material {
    pub fn from_frame(material: &MaterialFrame) -> Self {
        let mut descriptor = serde_json::to_value(material).expect("material serializes");
        descriptor.sort_all_objects();
        Self {
            id: material.id,
            descriptor: blake3::hash(
                &serde_json::to_vec(&descriptor).expect("descriptor serializes"),
            )
            .to_hex()
            .to_string(),
            bytes: Bytes {
                blob: material.blob.clone(),
                length: material.length,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Publication {
    pub version: u32,
    pub frame: String,
    pub class: Class,
    pub material: Option<Material>,
    pub envelope: Option<Bytes>,
}
impl Publication {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 || self.frame.is_empty() {
            return Err("unsupported or invalid producer byte declaration".into());
        }
        if self.class == Class::Sealed {
            return Err("sealed producer byte publication is unsupported".into());
        }
        if self.material.is_none() && self.envelope.is_none() {
            return Err("producer byte declaration has no byte roles".into());
        }
        for (_, bytes) in self.roles() {
            let hash = bytes
                .blob
                .strip_prefix("blake3:")
                .and_then(|h| blake3::Hash::from_hex(h).ok());
            if hash.is_none() || bytes.length > i64::MAX as u64 {
                return Err("invalid producer blob name or length".into());
            }
        }
        if self
            .material
            .as_ref()
            .is_some_and(|m| blake3::Hash::from_hex(&m.descriptor).is_err())
        {
            return Err("invalid producer material commitment".into());
        }
        Ok(())
    }

    pub fn roles(&self) -> impl Iterator<Item = (Role, &Bytes)> {
        self.material
            .as_ref()
            .map(|m| (Role::Material, &m.bytes))
            .into_iter()
            .chain(self.envelope.as_ref().map(|b| (Role::Envelope, b)))
    }

    /// A path token only; no producer-supplied path can escape the spool root.
    pub fn spool_key(&self, producer: &str) -> String {
        spool_key(producer, &self.frame)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MaterialFrame {
    pub id: MaterialId,
    /// What the material was read from.
    pub input: InputRef,
    /// `blake3:<hex>` of the bytes.
    pub blob: String,
    pub length: u64,
    pub parser: String,
    pub parser_version: u32,
    #[serde(default)]
    pub part_index: u32,
    #[serde(default)]
    pub previous_part: Option<MaterialId>,
    /// Lossless description: path bytes, envelopes, media type, charset.
    pub manifest: Value,
}

#[cfg(test)]
mod tests;

/// A source namespace and publication identity determine one private spool token.
pub fn spool_key(producer: &str, frame: &str) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"sinex producer publication 1\0");
    hash.update(producer.as_bytes());
    hash.update(&[0]);
    hash.update(frame.as_bytes());
    hash.finalize().to_hex().to_string()
}

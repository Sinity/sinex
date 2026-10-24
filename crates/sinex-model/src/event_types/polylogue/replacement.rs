//! References to current row batches replacing earlier units of one archive.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
pub struct ArchiveUnit {
    pub commit_seq: i64,
    pub kind: String,
    pub key: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UnitReplacement {
    pub unit: ArchiveUnit,
    /// No batch means the unit was removed by excision.
    pub batch: Option<String>,
    pub bytes: u64,
    pub rows: u64,
    pub blobs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, JsonSchema)]
pub struct ExcisionPublication {
    pub version: u32,
    pub replacements: Vec<UnitReplacement>,
    pub retired_batches: Vec<String>,
    pub digest: String,
}

impl ExcisionPublication {
    pub fn validate(&self) -> Result<(), String> {
        let mut value = serde_json::json!({"version": self.version, "replacements": self.replacements, "retired_batches": self.retired_batches});
        value.sort_all_objects();
        let hash = blake3::hash(&serde_json::to_vec(&value).map_err(|e| e.to_string())?);
        if self.version != 1 || self.digest != format!("blake3:{hash}") {
            return Err("excision replacement manifest failed its version/hash check".into());
        }
        let valid_hash = |name: &str| {
            name.strip_prefix("blake3:").is_some_and(|h| {
                h.len() == 64
                    && h.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        };
        let mut previous = None;
        for replacement in &self.replacements {
            let unit = &replacement.unit;
            if unit.commit_seq < 1
                || unit.key.is_empty()
                || ![
                    "revision",
                    "note",
                    "excision",
                    "setting",
                    "delivery",
                    "observation",
                    "delivery_event",
                    "extracted_blob",
                ]
                .contains(&unit.kind.as_str())
                || previous.is_some_and(|previous: &ArchiveUnit| previous >= unit)
            {
                return Err(
                    "replacement units are not unique known identities in archive order".into(),
                );
            }
            previous = Some(unit);
            match &replacement.batch {
                Some(hash)
                    if !valid_hash(hash) || replacement.bytes == 0 || replacement.rows == 0 =>
                {
                    return Err("invalid replacement batch".into());
                }
                None if replacement.bytes != 0
                    || replacement.rows != 0
                    || !replacement.blobs.is_empty() =>
                {
                    return Err("a tombstone cites retained bytes".into());
                }
                _ => {}
            }
            if replacement.blobs.iter().any(|h| !valid_hash(h)) {
                return Err("invalid replacement blob name".into());
            }
        }
        if self.retired_batches.iter().any(|h| !valid_hash(h)) {
            return Err("invalid retired batch name".into());
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ExcisionPublication {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            version: u32,
            replacements: Vec<UnitReplacement>,
            retired_batches: Vec<String>,
            digest: String,
        }
        let f = Fields::deserialize(deserializer)?;
        let value = Self {
            version: f.version,
            replacements: f.replacements,
            retired_batches: f.retired_batches,
            digest: f.digest,
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

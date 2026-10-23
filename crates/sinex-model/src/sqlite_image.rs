//! Physical SQLite evidence; row events still cite their JSONL material.
use crate::{Class, MaterialId, MaterialRef, RevisionId, Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageExtent {
    pub blob: String,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteImageMeta {
    pub revision: RevisionId,
    pub material: MaterialId,
    pub previous: Option<RevisionId>,
    pub base: RevisionId,
    pub database: String,
    pub host: String,
    pub captured_at: Timestamp,
    pub class: Class,
    pub full: bool,
    pub page_size: u32,
    pub page_count: u64,
    pub image_bytes: u64,
    pub image_hash: String,
    /// The evidence blob contains page payload followed by one digest per page.
    pub evidence_bytes: u64,
    pub map_offset: u64,
    pub map_bytes: u64,
    pub base_stored_bytes: u64,
    pub patch_stored_bytes: u64,
    pub read_stored_bytes: u64,
}

impl SqliteImageMeta {
    pub fn validate(&self, extents: &[ImageExtent]) -> Result<(), String> {
        if !matches!(self.page_size, 512|1024|2048|4096|8192|16384|32768|65536) || self.page_count==0 {
            return Err("invalid SQLite image page size/count".into());
        }
        let bytes=self.page_count.checked_mul(u64::from(self.page_size)).ok_or("SQLite image length overflow")?;
        let map=self.page_count.checked_mul(32).ok_or("SQLite map length overflow")?;
        if bytes!=self.image_bytes || map!=self.map_bytes || self.map_offset.checked_add(map)!=Some(self.evidence_bytes) || self.map_offset%u64::from(self.page_size)!=0 {
            return Err("SQLite evidence/map layout differs from page count".into());
        }
        if self.full && self.map_offset!=bytes || !self.full && self.map_offset>bytes {
            return Err("SQLite map offset differs from page payload".into());
        }
        let length=extents.iter().try_fold(0u64,|n,e| {
            e.offset.checked_add(e.length).ok_or("SQLite extent overflow")?;
            n.checked_add(e.length).ok_or("SQLite extent length overflow")
        })?;
        if length!=bytes || extents.iter().any(|e|e.length==0 || e.offset%u64::from(self.page_size)!=0 || e.length%u64::from(self.page_size)!=0) {
            return Err("SQLite extents do not cover the exact image".into());
        }
        if self.class==Class::Sealed && !self.full {return Err("sealed SQLite images require a full base".into());}
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WrappedMaterialKey {
    pub material: MaterialId,
    pub key_ref: String,
    pub wrapped: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqliteImagePublication {
    pub metadata: SqliteImageMeta,
    pub material: MaterialRef,
    pub extents: Vec<ImageExtent>,
    pub key: Option<WrappedMaterialKey>,
}

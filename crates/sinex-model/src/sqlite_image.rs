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

impl SqliteImagePublication {
    pub fn validate(&self)->Result<(),String> {
        let m=&self.metadata;
        m.validate(&self.extents)?;
        if self.material.manifest["sqlite_image"]!=serde_json::to_value(m).map_err(|e|e.to_string())? {return Err("SQLite image manifest differs from its publication".into());}
        if self.material.id!=m.material || self.material.length!=m.evidence_bytes || crate::MaterialRole::of(&self.material.manifest).map_err(|e|e.to_string())?!=crate::MaterialRole::SqliteImage {
            return Err("SQLite image material differs from its evidence layout".into());
        }
        if (m.class==Class::Sealed)!=self.key.is_some() || self.key.as_ref().is_some_and(|k|k.material!=m.material) {
            return Err("SQLite image key does not match its material/class".into());
        }
        let mut payload=0u64;
        for extent in &self.extents {
            if extent.blob==self.material.blob {
                if extent.offset!=payload {return Err("SQLite page payload extents are not dense and ordered".into());}
                payload=payload.checked_add(extent.length).ok_or("SQLite page payload overflow")?;
            } else if m.full {return Err("full SQLite image references another blob".into());}
        }
        if payload!=m.map_offset {return Err("SQLite map offset differs from retained page payload".into());}
        if m.full && m.base!=m.revision || !m.full && (m.previous.is_none() || m.base==m.revision) {
            return Err("SQLite image base/predecessor is inconsistent".into());
        }
        if m.image_hash.strip_prefix("blake3:").is_none_or(|h|h.len()!=64 || !h.bytes().all(|c|c.is_ascii_hexdigit())) {return Err("invalid SQLite image digest".into());}
        Ok(())
    }
}

impl SqliteImagePublication {
    pub fn validate_predecessor(&self, previous:&SqliteImageMeta, extents:&[ImageExtent])->Result<(),String> {
        let m=&self.metadata;
        if m.previous!=Some(previous.revision) || m.database!=previous.database || m.host!=previous.host {return Err("SQLite image predecessor lineage differs".into());}
        if m.full {return Ok(());}
        if m.class!=previous.class || m.class==Class::Sealed || m.page_size!=previous.page_size || m.base!=previous.base {return Err("SQLite page reuse crosses a class, key domain or base layout".into());}
        previous.validate(extents)?;
        let mut logical=0u64;let mut index=0usize;let mut old_start=0u64;
        for e in &self.extents {
            if e.blob!=self.material.blob {
                let mut copied=0u64;
                while copied<e.length {
                    let at=logical.checked_add(copied).ok_or("SQLite extent position overflow")?;
                    while index<extents.len() && old_start.checked_add(extents[index].length).ok_or("SQLite previous extent overflow")?<=at {old_start+=extents[index].length;index+=1;}
                    let old=extents.get(index).ok_or("SQLite page reuse extends past its predecessor")?;
                    if old.blob!=e.blob || old.offset.checked_add(at-old_start)!=e.offset.checked_add(copied) {return Err("SQLite retained page differs from its predecessor layout".into());}
                    copied+= (e.length-copied).min(old.length-(at-old_start));
                }
            }
            logical=logical.checked_add(e.length).ok_or("SQLite extent position overflow")?;
        }
        Ok(())
    }
}

//! A periodic physical image and its dense page map from one SQLite read view.
use super::DriverError;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use sinex_model::{
    Class, ImageExtent, MaterialId, RevisionId, SqliteImageMeta, SqliteImagePublication, Timestamp,
};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

pub type PageHash=Arc<dyn Fn(&[u8;32])->[u8;32]+Send+Sync>;
pub type Check=Arc<dyn Fn()->Result<(),String>+Send+Sync>;

#[derive(Clone,Debug,Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub scope: String,
    pub class: Class,
    pub interval_secs: u64,
    #[serde(default = "turn_pages")]
    pub turn_pages: u32,
    #[serde(default = "threshold")]
    pub rebase_threshold: f64,
}
fn turn_pages() -> u32 {
    256
}
fn threshold() -> f64 {
    1.0
}
impl Policy {
    pub fn validate(&self) -> Result<(), String> {
        if self.scope != "whole_database"
            || self.interval_secs == 0
            || self.turn_pages == 0
            || !self.rebase_threshold.is_finite()
            || self.rebase_threshold <= 0.0
        {
            Err("SQLite image policy requires whole_database scope, explicit class, positive interval/turn/rebase values".into())
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Debug)]
pub struct Previous {
    pub metadata: SqliteImageMeta,
    pub extents: Vec<ImageExtent>,
    /// Only open/guarded maps are restored without an operator unlock.
    pub map: Option<Arc<File>>,
}

#[derive(Debug)]
pub struct View(pub Mutex<Connection>);
impl View {
    pub fn page(&self, n: u64, size: u32) -> std::io::Result<Vec<u8>> {
        let conn = self
            .0
            .lock()
            .map_err(|_| std::io::Error::other("SQLite view poisoned"))?;
        let page: Vec<u8> = conn
            .query_row(
                "SELECT data FROM sqlite_dbpage('main') WHERE pgno=?1",
                [n],
                |r| r.get(0),
            )
            .map_err(std::io::Error::other)?;
        if page.len() != size as usize {
            return Err(std::io::Error::other(
                "SQLite page length differs from page size",
            ));
        }
        Ok(page)
    }
}

#[derive(Debug)]
pub struct Scan {
    pub view: Arc<View>,
    policy: Policy,
    previous: Option<Previous>,
    meta: SqliteImageMeta,
    next: u64,
    hash: blake3::Hasher,
    changed: Vec<u64>,
    map: Option<File>,
}
impl Scan {
    pub fn start(
        connection: Connection,
        policy: Policy,
        previous: Option<Previous>,
        database: String,
        host: String,
        at: Timestamp,
    ) -> Result<Self, DriverError> {
        policy.validate().map_err(DriverError::Cursor)?;
        let size: u32 = connection
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .map_err(io)?;
        let count: u64 = connection
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .map_err(io)?;
        // Establish dbpage support before returning a pending capture.
        connection
            .query_row(
                "SELECT length(data) FROM sqlite_dbpage('main') WHERE pgno=1",
                [],
                |r| r.get::<_, u64>(0),
            )
            .map_err(io)?;
        let revision = RevisionId::new();
        let full = policy.class == Class::Sealed
            || previous.as_ref().is_none_or(|p| {
                p.metadata.class != policy.class
                    || p.metadata.page_size != size
                    || p.map.is_none()
                    || p.metadata.patch_stored_bytes as f64
                        > p.metadata.base_stored_bytes as f64 * policy.rebase_threshold
                    || p.metadata.read_stored_bytes > p.metadata.base_stored_bytes.saturating_mul(2)
            });
        let length = count
            .checked_mul(u64::from(size))
            .ok_or_else(|| io("SQLite image length overflow"))?;
        let map_bytes = count
            .checked_mul(32)
            .ok_or_else(|| io("SQLite map length overflow"))?;
        let meta = SqliteImageMeta {
            revision,
            material: MaterialId::new(),
            previous: previous.as_ref().map(|p| p.metadata.revision),
            base: if full {
                revision
            } else {
                previous.as_ref().expect("previous image").metadata.base
            },
            database,
            host,
            captured_at: at,
            class: policy.class,
            full,
            page_size: size,
            page_count: count,
            image_bytes: length,
            image_hash: String::new(),
            evidence_bytes: 0,
            map_offset: 0,
            map_bytes,
            base_stored_bytes: 0,
            patch_stored_bytes: 0,
            read_stored_bytes: 0,
        };
        let map = if policy.class == Class::Sealed {
            None
        } else {
            Some(tempfile::tempfile().map_err(io)?)
        };
        Ok(Self {
            view: Arc::new(View(Mutex::new(connection))),
            policy,
            previous,
            meta,
            next: 1,
            hash: blake3::Hasher::new(),
            changed: Vec::new(),
            map,
        })
    }
    pub fn turn(&mut self) -> Result<bool, DriverError> {
        let stop = self
            .next
            .saturating_add(u64::from(self.policy.turn_pages))
            .min(self.meta.page_count.saturating_add(1));
        while self.next < stop {
            let page = self.view.page(self.next, self.meta.page_size).map_err(io)?;
            self.hash.update(&page);
            let digest = *blake3::hash(&page).as_bytes();
            if let Some(map) = &mut self.map {
                map.write_all(&digest).map_err(io)?;
            }
            if !self.meta.full {
                let previous = self.previous.as_ref().expect("previous image");
                let mut old = [0; 32];
                let same = self.next <= previous.metadata.page_count
                    && previous
                        .map
                        .as_ref()
                        .expect("plain map")
                        .read_exact_at(&mut old, (self.next - 1) * 32)
                        .map_err(io)
                        .map(|()| old == digest)?;
                if !same {
                    self.changed.push(self.next);
                }
            }
            self.next += 1;
        }
        Ok(self.next > self.meta.page_count)
    }
    pub fn finish(mut self) -> Result<Arc<Image>, DriverError> {
        if self.next <= self.meta.page_count {
            return Err(io("SQLite capture is incomplete"));
        }
        if !self.meta.full && self.changed.len() as u64 > self.meta.page_count / 2 {
            self.meta.full = true;
            self.meta.base = self.meta.revision;
            self.changed.clear();
        }
        self.meta.map_offset = if self.meta.full {
            self.meta.image_bytes
        } else {
            (self.changed.len() as u64)
                .checked_mul(u64::from(self.meta.page_size))
                .ok_or_else(|| io("SQLite patch length overflow"))?
        };
        self.meta.evidence_bytes = self
            .meta
            .map_offset
            .checked_add(self.meta.map_bytes)
            .ok_or_else(|| io("SQLite evidence length overflow"))?;
        if self.meta.class != Class::Sealed {
            self.meta.image_hash = format!("blake3:{}", self.hash.finalize().to_hex());
        }
        Ok(Arc::new(Image {
            metadata: self.meta,
            changed: Arc::new(self.changed),
            previous: self.previous,
            map: self.map.map(Arc::new),
            view: Mutex::new(Some(self.view)),
            stored: Mutex::new(None),
            check: Mutex::new(None),
        }))
    }
}
fn io(error: impl std::fmt::Display) -> DriverError {
    DriverError::Io(error.to_string())
}

pub struct Image {
    pub metadata:SqliteImageMeta,
    pub changed:Arc<Vec<u64>>,
    pub previous:Option<Previous>,
    pub map:Option<Arc<File>>,
    view:Mutex<Option<Arc<View>>>,
    stored:Mutex<Option<SqliteImagePublication>>,
    check:Mutex<Option<Check>>,
}
impl std::fmt::Debug for Image {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteImage")
            .field("metadata", &self.metadata)
            .finish()
    }
}
impl Image {
    pub fn checker(&self)->Result<Check,String> {Ok(self.check.lock().map_err(|_|"SQLite image check poisoned")?.clone().unwrap_or_else(||Arc::new(||Ok(()))))}
    pub fn set_check(&self,check:Check)->Result<(),String> {*self.check.lock().map_err(|_|"SQLite image check poisoned")?=Some(check);Ok(())}

    pub fn view(&self)->Result<Arc<View>,DriverError> {self.view.lock().map_err(|_|io("SQLite image poisoned"))?.clone().ok_or_else(||io("SQLite view already released"))}
    pub fn stored(&self)->Result<Option<SqliteImagePublication>,String> {Ok(self.stored.lock().map_err(|_|"SQLite image poisoned")?.clone())}
    pub fn retain(&self,publication:SqliteImagePublication)->Result<(),String> {
        *self.stored.lock().map_err(|_|"SQLite image poisoned")?=Some(publication);
        self.view.lock().map_err(|_|"SQLite image poisoned")?.take();
        Ok(())
    }
    pub fn reader(&self,map_hash:PageHash,full_data_only:bool)->Result<ImageReader,DriverError> {
        Ok(ImageReader {view:self.view()?,map:self.map.clone(),map_hash,size:self.metadata.page_size,count:self.metadata.page_count,changed:if self.metadata.full || full_data_only {None} else {Some(self.changed.clone())},position:0,include_map:!full_data_only,check:self.check.lock().map_err(|_|io("SQLite image check poisoned"))?.clone()})
    }
    pub fn stored(&self) -> Result<Option<SqliteImagePublication>, String> {
        Ok(self
            .stored
            .lock()
            .map_err(|_| "SQLite image poisoned")?
            .clone())
    }
    pub fn retain(&self, publication: SqliteImagePublication) -> Result<(), String> {
        *self.stored.lock().map_err(|_| "SQLite image poisoned")? = Some(publication);
        self.view
            .lock()
            .map_err(|_| "SQLite image poisoned")?
            .take();
        Ok(())
    }
    pub fn reader(
        &self,
        map_hash: Arc<dyn Fn(&[u8; 32]) -> [u8; 32] + Send + Sync>,
        full_data_only: bool,
    ) -> Result<ImageReader, DriverError> {
        Ok(ImageReader {
            view: self.view()?,
            map: self.map.clone(),
            map_hash,
            size: self.metadata.page_size,
            count: self.metadata.page_count,
            changed: if self.metadata.full || full_data_only {
                None
            } else {
                Some(self.changed.clone())
            },
            position: 0,
            include_map: !full_data_only,
            check: self
                .check
                .lock()
                .map_err(|_| io("SQLite image check poisoned"))?
                .clone(),
        })
    }
    pub fn extents(&self, blob: &str) -> Result<Vec<ImageExtent>, String> {
        if self.metadata.full {
            return Ok(vec![ImageExtent {
                blob: blob.into(),
                offset: 0,
                length: self.metadata.image_bytes,
            }]);
        }
        let previous = self.previous.as_ref().ok_or("SQLite patch has no base")?;
        overlay(
            &previous.extents,
            self.metadata.image_bytes,
            self.metadata.page_size,
            &self.changed,
            blob,
        )
    }
}

/// Split retained extents around changed pages, coalescing adjacent slices.
pub fn overlay(
    previous: &[ImageExtent],
    length: u64,
    size: u32,
    changed: &[u64],
    blob: &str,
) -> Result<Vec<ImageExtent>, String> {
    let size = u64::from(size);
    if size == 0 || length == 0 || length % size != 0 {
        return Err("invalid SQLite image length/page size".into());
    }
    let old_length = previous.iter().try_fold(0u64, |n, e| {
        if e.length == 0 || e.offset % size != 0 || e.length % size != 0 {
            return Err("unaligned SQLite extent");
        }
        e.offset
            .checked_add(e.length)
            .ok_or("SQLite extent overflow")?;
        n.checked_add(e.length)
            .ok_or("SQLite extent length overflow")
    })?;
    let mut last = 0;
    for &n in changed {
        if n <= last || n > length / size {
            return Err("SQLite changed pages are not ordered or in range".into());
        }
        last = n;
    }
    let mut out: Vec<ImageExtent> = Vec::new();
    let mut at = 0u64;
    let mut index = 0usize;
    for extent in previous {
        if at >= length {
            break;
        }
        let end = at
            .checked_add(extent.length)
            .ok_or("SQLite extent overflow")?
            .min(length);
        let mut pos = at;
        while index < changed.len() && (changed[index] - 1) * size < end {
            let start = (changed[index] - 1) * size;
            if start < pos {
                return Err("SQLite patch overlaps retained pages".into());
            }
            if start > pos {
                push(
                    &mut out,
                    ImageExtent {
                        blob: extent.blob.clone(),
                        offset: extent.offset + (pos - at),
                        length: start - pos,
                    },
                );
            }
            push(
                &mut out,
                ImageExtent {
                    blob: blob.into(),
                    offset: (index as u64) * size,
                    length: size,
                },
            );
            pos = start + size;
            index += 1;
        }
        if pos < end {
            push(
                &mut out,
                ImageExtent {
                    blob: extent.blob.clone(),
                    offset: extent.offset + (pos - at),
                    length: end - pos,
                },
            );
        }
        at = end;
    }
    let mut covered = old_length.min(length);
    while index < changed.len() {
        let start = (changed[index] - 1) * size;
        if start != covered {
            return Err("SQLite patch leaves a gap".into());
        }
        push(
            &mut out,
            ImageExtent {
                blob: blob.into(),
                offset: (index as u64) * size,
                length: size,
            },
        );
        covered = covered
            .checked_add(size)
            .ok_or("SQLite extent length overflow")?;
        index += 1;
    }
    if covered != length {
        return Err("SQLite extents leave a gap".into());
    }
    Ok(out)
}
fn push(out: &mut Vec<ImageExtent>, extent: ImageExtent) {
    if extent.length == 0 {
        return;
    }
    if let Some(last) = out.last_mut()
        && last.blob == extent.blob
        && last.offset.checked_add(last.length) == Some(extent.offset)
    {
        last.length += extent.length;
    } else {
        out.push(extent);
    }
}

pub struct ImageReader {
    view:Arc<View>,map:Option<Arc<File>>,map_hash:PageHash,size:u32,count:u64,changed:Option<Arc<Vec<u64>>>,position:u64,include_map:bool,check:Option<Check>,
}
impl ImageReader {
    pub fn data_length(&self) -> u64 {
        self.changed.as_ref().map_or(self.count, |c| c.len() as u64) * u64::from(self.size)
    }
    fn length(&self) -> u64 {
        self.data_length() + if self.include_map { self.count * 32 } else { 0 }
    }
}
impl Read for ImageReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if let Some(check) = &self.check {
            check().map_err(std::io::Error::other)?;
        }
        if out.is_empty() || self.position >= self.length() {
            return Ok(0);
        }
        let data = self.data_length();
        let (bytes, within) = if self.position < data {
            let i = self.position / u64::from(self.size);
            let n = self.changed.as_ref().map_or(i + 1, |c| c[i as usize]);
            (
                self.view.page(n, self.size)?,
                (self.position % u64::from(self.size)) as usize,
            )
        } else {
            let offset = self.position - data;
            let n = offset / 32 + 1;
            let mut hash = [0; 32];
            if let Some(map) = &self.map {
                map.read_exact_at(&mut hash, (n - 1) * 32)?;
            } else {
                hash = *blake3::hash(&self.view.page(n, self.size)?).as_bytes();
            }
            ((self.map_hash)(&hash).to_vec(), (offset % 32) as usize)
        };
        let n = out.len().min(bytes.len() - within);
        out[..n].copy_from_slice(&bytes[within..within + n]);
        self.position += n as u64;
        Ok(n)
    }
}
impl Seek for ImageReader {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        if let Some(check) = &self.check {
            check().map_err(std::io::Error::other)?;
        }
        let at = match from {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::Current(n) => i128::from(self.position) + i128::from(n),
            SeekFrom::End(n) => i128::from(self.length()) + i128::from(n),
        };
        self.position = u64::try_from(at)
            .map_err(|_| std::io::Error::other("SQLite image seek outside address range"))?;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests;

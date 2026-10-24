//! Exclusions at acquisition: before a source's new bytes are stored, every
//! record that matches an earlier excision of the source is blanked, the way
//! excision blanks stored records, so the excised content is not kept again.
use sinex_model::{Event, MaterialId};
use sinex_sources::drivers::{Acquired, StoredBytes};
use sinex_sources::{DrainInput, Limits, Source};
use sinex_store::excision::exclusions::{self, Excluded};
use sqlx::PgConnection;
use std::collections::BTreeSet;

pub(crate) struct Exclusions(Excluded);

impl Exclusions {
    pub async fn load(conn: &mut PgConnection, source: &str) -> Result<Self, String> {
        Ok(Self(
            exclusions::load(conn, source)
                .await
                .map_err(|e| e.to_string())?,
        ))
    }

    /// Which excisions these are; the batch carries it to admission.
    pub fn version(&self) -> &sinex_model::Exclusions {
        &self.0.exclusions
    }

    /// Refuse stored bytes an excision removed from this source.
    pub fn check_cited(&self, cited: &[sinex_model::Cited]) -> Result<(), String> {
        if cited.iter().any(|c| self.0.media.contains_key(&c.blob)) {
            return Err("cited media was excised from this source".into());
        }
        Ok(())
    }

    fn excludes(
        &self,
        material: MaterialId,
        anchor: u64,
        hash: [u8; 32],
        events: &[Event],
    ) -> bool {
        i64::try_from(anchor).is_ok_and(|anchor| self.0.locations.contains(&(material.0, anchor)))
            || self.0.hashes.contains(&hash)
            || events.iter().any(|event| {
                event.equivalence_key.as_ref().is_some_and(|key| {
                    self.0
                        .keys
                        .contains(&(event.event_type.to_owned(), key.0.clone()))
                })
            })
    }

    /// Blank the excluded records of `acquired`'s material. `stored` says the
    /// material is already in the store: then an excluded record is refused,
    /// since its bytes can no longer be left out.
    pub fn filter(
        &self,
        source: &dyn Source,
        mut acquired: Acquired,
        carry: &serde_json::Value,
        class: sinex_model::Class,
        stored: bool,
    ) -> Result<Acquired, String> {
        if self.0.exclusions.count == 0 {
            return Ok(acquired);
        }
        let Some(material) = &mut acquired.material else {
            return Ok(acquired);
        };
        if matches!(
            material.stored,
            Some(StoredBytes::File(_) | StoredBytes::Blob { .. })
        ) {
            return Err("excised records cannot be left out of a container's bytes".into());
        }
        let bytes = material.record_bytes();
        let length = byte_length(&bytes)?;
        let mut selected = Vec::new();
        let chunks = source
            .drainer(DrainInput {
                source: source.spec().name,
                class,
                material: material.id,
                bytes: material.record_bytes(),
                records: &material.records,
                carry: carry.clone(),
                reading_order: &*material.reading_order,
                limits: Limits {
                    max_events: 1,
                    max_bytes: 1,
                },
                manifest: &material.manifest,
            })
            .map_err(|e| e.to_string())?;
        for chunk in chunks {
            let chunk = chunk.map_err(|e| e.to_string())?;
            for outcome in chunk.outcomes {
                let (location, events) = match outcome {
                    sinex_model::RecordOutcome::Events {
                        location: Some(location),
                        events,
                    } => (location, events),
                    sinex_model::RecordOutcome::Failure { location, .. } => (location, Vec::new()),
                    _ => return Err("a parser outcome has no record location".into()),
                };
                self.check_cited(
                    &events
                        .iter()
                        .flat_map(|event| event.cites.iter().cloned())
                        .collect::<Vec<_>>(),
                )?;
                let anchor = usize::try_from(location.anchor)
                    .map_err(|_| "record anchor exceeds address space")?;
                let index = material
                    .records
                    .binary_search_by_key(&anchor, |r| r.0)
                    .map_err(|_| "a parser outcome has no acquired record")?;
                let (start, end) = material.records[index];
                let raw = bytes.record(start, end).map_err(|e| e.to_string())?;
                if self.excludes(
                    material.id,
                    location.anchor,
                    source.record_hash(&raw),
                    &events,
                ) {
                    selected.push((start, end));
                }
            }
        }
        // An aligned rewrite keeps the records it had read before too, though
        // only the new ones are drained: match those by their bytes.
        let drained = material.records.iter().copied().collect::<BTreeSet<_>>();
        let mut whole = Bytes {
            bytes: &bytes,
            length,
            position: 0,
        };
        for (start, end) in source
            .spec()
            .framer
            .ranges(&mut whole)
            .map_err(|e| e.to_string())?
        {
            if drained.contains(&(start, end)) {
                continue;
            }
            let raw = bytes.record(start, end).map_err(|e| e.to_string())?;
            if self.excludes(material.id, start as u64, source.record_hash(&raw), &[]) {
                selected.push((start, end));
            }
        }
        if selected.is_empty() {
            return Ok(acquired);
        }
        if stored {
            return Err("a stored material holds an excised record; excise the material".into());
        }
        if source.spec().driver == sinex_sources::Driver::ApiRequest {
            return Err("this request matches an excised record".into());
        }
        if !matches!(
            source.spec().driver,
            sinex_sources::Driver::AppendFile | sinex_sources::Driver::SqliteRows
        ) {
            return Err("excised records cannot yet be left out of this kind of source".into());
        }
        if class == sinex_model::Class::Sealed {
            return Err("excised records cannot be left out of sealed source material".into());
        }
        selected.sort_unstable();
        let mut blanked = crate::retained::Blanked::new(
            Bytes {
                bytes: &bytes,
                length,
                position: 0,
            },
            selected,
        );
        let mut file = tempfile::tempfile().map_err(|e| e.to_string())?;
        let copied = std::io::copy(&mut blanked, &mut file).map_err(|e| e.to_string())?;
        if copied != length {
            return Err("the source material changed length while blanking".into());
        }
        drop(blanked);
        use std::io::Seek;
        file.rewind().map_err(|e| e.to_string())?;
        material.stored = Some(StoredBytes::FramedFile(std::sync::Arc::new(file)));
        material.bytes.clear();
        Ok(acquired)
    }
}

pub(crate) fn byte_length(bytes: &sinex_sources::drain::RecordBytes<'_>) -> Result<u64, String> {
    use sinex_sources::drain::RecordBytes;
    match bytes {
        RecordBytes::Slice(bytes) => Ok(bytes.len() as u64),
        RecordBytes::File(file) => file.metadata().map(|m| m.len()).map_err(|e| e.to_string()),
        RecordBytes::Reader(reader) => Ok(reader.length()),
    }
}

/// A material's record bytes as one stream.
struct Bytes<'a, 'b> {
    bytes: &'a sinex_sources::drain::RecordBytes<'b>,
    length: u64,
    position: u64,
}
impl std::io::Read for Bytes<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = (self.length.saturating_sub(self.position)).min(out.len() as u64) as usize;
        if n == 0 {
            return Ok(0);
        }
        let start = usize::try_from(self.position)
            .map_err(|_| std::io::Error::other("material exceeds address space"))?;
        let raw = self.bytes.record(
            start,
            start
                .checked_add(n)
                .ok_or_else(|| std::io::Error::other("record range overflow"))?,
        )?;
        out[..n].copy_from_slice(&raw);
        self.position += n as u64;
        Ok(n)
    }
}
impl std::io::Seek for Bytes<'_, '_> {
    fn seek(&mut self, from: std::io::SeekFrom) -> std::io::Result<u64> {
        let position = match from {
            std::io::SeekFrom::Start(n) => i128::from(n),
            std::io::SeekFrom::Current(n) => i128::from(self.position) + i128::from(n),
            std::io::SeekFrom::End(n) => i128::from(self.length) + i128::from(n),
        };
        self.position =
            u64::try_from(position).map_err(|_| std::io::Error::other("invalid seek"))?;
        Ok(self.position)
    }
}

#[cfg(test)]
mod tests;

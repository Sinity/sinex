//! Exact append-part custody; provider changes wait until its final cursor.
use super::*;
use crate::drivers::{NewMaterial, RetainedRecords, StoredBytes};
use sinex_model::{InputRef, MaterialRef, ObservationFacts, RevisionChange};
use std::sync::Arc;

type CursorAt = Arc<dyn Fn(usize) -> Result<Value, DriverError> + Send + Sync>;

/// Archive-owned restoration supplies verified records and the transient short
/// predecessor tail. The tail is never serialized into a manifest or cursor.
pub struct AppendResume {
    pub reference: MaterialRef,
    pub stored: RetainedRecords,
    pub records: Vec<(usize, usize)>,
    pub base_cursor: Value,
    pub cursor: Value,
    pub head: Vec<u8>,
    pub input: InputRef,
    pub observation: ObservationFacts,
    pub revision: RevisionChange,
}

pub(super) struct Pending {
    reference: MaterialRef,
    stored: StoredBytes,
    records: Vec<(usize, usize)>,
    cursor_at: CursorAt,
    end: Value,
    base: Value,
    input: InputRef,
    observation: ObservationFacts,
    revision: RevisionChange,
    aligned: bool,
    vanished: Vec<sinex_model::RecordLocation>,
    physical_records: Vec<(usize, usize)>,
}
fn invalid(s: &str) -> DriverError {
    DriverError::Cursor(s.into())
}
impl Pending {
    pub fn capture(acquired: Acquired, base: Value) -> Result<Self, DriverError> {
        let m = acquired
            .material
            .ok_or_else(|| invalid("append capture has no material"))?;
        if m.stored.is_some() {
            return Err(invalid("append capture has unexpected external backing"));
        }
        let length = m.bytes.len() as u64;
        let end = (m.cursor_at)(m.bytes.len())?;
        Ok(Self {
            reference: MaterialRef {
                id: m.id,
                input: None,
                blob: String::new(),
                length,
                part_index: m.part_index,
                previous_part: m.previous_part,
                parser: String::new(),
                parser_version: 0,
                manifest: m.manifest,
            },
            stored: StoredBytes::FramedMemory(Arc::new(m.bytes)),
            physical_records: m.records.clone(),
            records: m.records,
            cursor_at: Arc::from(m.cursor_at),
            end,
            base,
            input: acquired.input,
            observation: acquired.observation,
            revision: acquired.revision,
            aligned: m.aligned,
            vanished: m.vanished,
        })
    }
    pub fn restore(resume: AppendResume) -> Result<Self, DriverError> {
        let plan = &resume.reference.manifest["append_resume"];
        if plan["version"] != 1 {
            return Err(invalid("stored append resume version is unavailable"));
        }
        let start: crate::drivers::append_file::Cursor =
            serde_json::from_value(plan["start"].clone())
                .map_err(|_| invalid("invalid stored append start cursor"))?;
        let end: crate::drivers::append_file::Cursor = serde_json::from_value(plan["end"].clone())
            .map_err(|_| invalid("invalid stored append end cursor"))?;
        let length = usize::try_from(resume.reference.length)
            .map_err(|_| invalid("append material exceeds address space"))?;
        if end
            .window
            .as_deref()
            .is_none_or(|w| w.len() != 64 || !w.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(invalid("stored final append window is unavailable"));
        }
        if start.material != Some(resume.reference.id)
            || start.through != 0
            || end.material != Some(resume.reference.id)
            || end.through != length
            || end.offset.checked_sub(start.offset) != Some(resume.reference.length)
            || end.part_index != resume.reference.part_index
            || resume.stored.bytes.length() != resume.reference.length
            || resume.stored.blob != resume.reference.blob
            || resume.reference.manifest["file_offset"].as_u64() != Some(start.offset)
        {
            return Err(invalid(
                "append resume cursor differs from retained material",
            ));
        }
        if resume.head.len() != usize::try_from(start.offset.min(4096)).unwrap() {
            return Err(invalid("append predecessor tail is unavailable"));
        }
        let mut previous = 0;
        for &(s, e) in &resume.records {
            if s < previous || e <= s || e > length {
                return Err(invalid("append resume record ranges are invalid"));
            }
            previous = e;
        }
        let physical_records = resume.records;
        let records = match resume.reference.manifest.get("records") {
            Some(value) => serde_json::from_value::<Vec<(usize, usize)>>(value.clone())
                .map_err(|_| invalid("invalid stored append reading ranges"))?,
            None => physical_records.clone(),
        };
        let physical = physical_records
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if records.iter().any(|record| !physical.contains(record)) {
            return Err(invalid(
                "append reading ranges differ from physical records",
            ));
        }
        let stored = resume.stored.clone();
        let final_cursor = plan["end"].clone();
        let final_value = final_cursor.clone();
        let cursor_at: CursorAt = Arc::new(move |through| {
            if through == length {
                return Ok(final_value.clone());
            }
            let window = {
                let mut tail = resume.head[resume
                    .head
                    .len()
                    .saturating_sub(4096usize.saturating_sub(through))..]
                    .to_vec();
                let from = through.saturating_sub(4096);
                let mut bytes = vec![0; through - from];
                // Restoration checked the backing and bounds before creating this cursor.
                // A failed read cannot produce an apparently valid progress value.
                stored
                    .bytes
                    .read_exact_at(from as u64, &mut bytes)
                    .map_err(|e| DriverError::Io(e.to_string()))?;
                tail.extend_from_slice(&bytes);
                Some(blake3::hash(&tail).to_hex().to_string())
            };
            Ok(serde_json::to_value(crate::drivers::append_file::Cursor {
                offset: start.offset + through as u64,
                size: 0,
                mtime_ns: end.mtime_ns,
                inode: end.inode,
                window,
                material: Some(resume.reference.id),
                part_index: resume.reference.part_index,
                through,
                pending_revision: None,
            })
            .expect("cursor serializes"))
        });
        let aligned = resume.reference.manifest.get("records").is_some();
        let pending = Self {
            reference: resume.reference,
            stored: StoredBytes::FramedReader(resume.stored),
            records,
            physical_records,
            cursor_at,
            end: final_cursor,
            base: resume.base_cursor,
            input: resume.input,
            observation: resume.observation,
            revision: resume.revision,
            aligned,
            vanished: Vec::new(),
        };
        pending.complete(&resume.cursor)?;
        Ok(pending)
    }
    fn through(&self, cursor: &Value) -> Result<usize, DriverError> {
        if cursor == &self.base {
            return Ok(0);
        }
        let c: crate::drivers::append_file::Cursor = serde_json::from_value(cursor.clone())
            .map_err(|_| invalid("invalid append resume cursor"))?;
        if c.material != Some(self.reference.id) || c.through as u64 > self.reference.length {
            return Err(invalid("cursor does not belong to pending append material"));
        }
        if c.through as u64 == self.reference.length && cursor != &self.end {
            return Err(invalid("final append cursor differs from frozen evidence"));
        }
        if self
            .physical_records
            .iter()
            .any(|&(start, end)| start < c.through && c.through < end)
        {
            return Err(invalid("append cursor cuts a retained record"));
        }
        Ok(c.through)
    }
    pub fn prime(&self, state: &mut AppendState) -> Result<(), DriverError> {
        let StoredBytes::FramedReader(stored) = &self.stored else {
            return Ok(());
        };
        // Verify a bounded tail before mutating the reader's remembered state.
        let mut tail = Vec::new();
        let mut length = 0usize;
        for &(start, end) in self.physical_records.iter().rev() {
            if tail.len() >= crate::drivers::append_file::STATE_RECORDS
                || (length > 0
                    && length.saturating_add(end - start) > crate::drivers::append_file::STATE_TAIL)
            {
                break;
            }
            let mut raw = vec![0; end - start];
            stored
                .bytes
                .read_exact_at(start as u64, &mut raw)
                .map_err(|e| DriverError::Io(e.to_string()))?;
            tail.push((*blake3::hash(&raw).as_bytes(), end - start, start));
            length = length.saturating_add(end - start);
        }
        if self.aligned
            || matches!(
                self.revision,
                RevisionChange::Rewritten | RevisionChange::Truncated | RevisionChange::Replaced
            )
        {
            *state = AppendState::default();
        }
        if state.remembers_material(self.reference.id) {
            return Ok(());
        }
        for (hash, length, anchor) in tail.into_iter().rev() {
            state.insert_digest(hash, length, self.reference.id, anchor);
        }
        Ok(())
    }

    pub fn complete(&self, cursor: &Value) -> Result<bool, DriverError> {
        Ok(self.through(cursor)? as u64 == self.reference.length && cursor == &self.end)
    }
    pub fn acquired(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let through = self.through(cursor)?;
        let at = self.cursor_at.clone();
        Ok(Acquired {
            input: self.input.clone(),
            observation: self.observation.clone(),
            revision: self.revision,
            cursor_unchanged: Value::Null,
            material: Some(NewMaterial {
                id: self.reference.id,
                bytes: Vec::new(),
                stored: Some(self.stored.clone()),
                records: self
                    .records
                    .iter()
                    .copied()
                    .filter(|(_, end)| *end > through)
                    .collect(),
                part_index: self.reference.part_index,
                previous_part: self.reference.previous_part,
                manifest: self.reference.manifest.clone(),
                reading_order: crate::drivers::reading_order(&self.reference.manifest),
                cursor_at: Box::new(move |end| at(end)),
                aligned: self.aligned,
                vanished: if through == 0 {
                    self.vanished.clone()
                } else {
                    Vec::new()
                },
            }),
        })
    }
}

#[cfg(test)]
mod tests;

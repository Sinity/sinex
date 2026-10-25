//! The newest append material, held in memory until all of it is admitted,
//! so a failed admission retries the same bytes before the file is read again.
use super::*;
use crate::drivers::{NewMaterial, StoredBytes};
use sinex_model::{InputRef, MaterialRef, ObservationFacts, RevisionChange};
use std::sync::Arc;

type CursorAt = Arc<dyn Fn(usize) -> Result<Value, DriverError> + Send + Sync>;

pub(super) struct HeldAppend {
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
impl HeldAppend {
    pub fn capture(acquired: Acquired, base: Value) -> Result<Self, DriverError> {
        let m = acquired
            .material
            .ok_or_else(|| invalid("append capture has no material"))?;
        if m.stored.is_some() {
            return Err(invalid("an append material is read into memory"));
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
    fn through(&self, cursor: &Value) -> Result<usize, DriverError> {
        if cursor == &self.base {
            return Ok(0);
        }
        let c: crate::drivers::append_file::Cursor = serde_json::from_value(cursor.clone())
            .map_err(|_| invalid("invalid append resume cursor"))?;
        if c.material != Some(self.reference.id) || c.through as u64 > self.reference.length {
            return Err(invalid("cursor does not belong to held append material"));
        }
        if c.through as u64 == self.reference.length && cursor != &self.end {
            return Err(invalid(
                "the final append cursor differs from the material's end",
            ));
        }
        if self
            .physical_records
            .iter()
            .any(|&(start, end)| start < c.through && c.through < end)
        {
            return Err(invalid("the append cursor falls inside a record"));
        }
        Ok(c.through)
    }
    pub fn complete(&self, cursor: &Value) -> Result<bool, DriverError> {
        Ok(self.through(cursor)? as u64 == self.reference.length && cursor == &self.end)
    }
    pub fn acquired(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let through = self.through(cursor)?;
        let at = self.cursor_at.clone();
        // A retry after the material's first batch was admitted continues the
        // revision that batch opened.
        let (observation, revision) = if through == 0 {
            (self.observation.clone(), self.revision)
        } else {
            (
                ObservationFacts {
                    changed: false,
                    ..self.observation.clone()
                },
                RevisionChange::Same,
            )
        };
        Ok(Acquired {
            input: self.input.clone(),
            observation,
            revision,
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

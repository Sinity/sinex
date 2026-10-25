//! Held material: the newest material a reader produced, kept in memory until
//! its final cursor is admitted, so a failed admission retries the same bytes
//! before the input is read again. Every reader that holds its material uses
//! this; what it remembers once the material is admitted stays its own.
use super::{Acquired, DriverError, NewMaterial, StoredBytes, cursor_material};
use serde_json::Value;
use sinex_model::{InputRef, MaterialId, ObservationFacts, RecordLocation, RevisionChange};
use std::sync::Arc;

type CursorAt = Arc<dyn Fn(usize) -> Result<Value, DriverError> + Send + Sync>;

pub struct HeldMaterial {
    id: MaterialId,
    stored: StoredBytes,
    length: usize,
    records: Vec<(usize, usize)>,
    part_index: u32,
    previous_part: Option<MaterialId>,
    manifest: sinex_model::Manifest,
    cursor_at: CursorAt,
    aligned: bool,
    removed: Vec<RecordLocation>,
    memory: Option<Arc<[u8]>>,
    input: InputRef,
    observation: ObservationFacts,
    revision: RevisionChange,
    /// The cursor the material was read after.
    base: Value,
    /// The cursor once all of it is admitted.
    end: Value,
}

impl std::fmt::Debug for HeldMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldMaterial")
            .field("id", &self.id)
            .field("length", &self.length)
            .field("records", &self.records.len())
            .finish_non_exhaustive()
    }
}

fn invalid(reason: &str) -> DriverError {
    DriverError::Cursor(reason.into())
}

impl HeldMaterial {
    /// Keep `acquired`'s material, read after the cursor `base`.
    pub fn hold(acquired: Acquired, base: &Value) -> Result<Self, DriverError> {
        let m = acquired
            .material
            .ok_or_else(|| invalid("only a material can be held"))?;
        let (stored, length) = match m.stored {
            None => {
                let length = m.bytes.len();
                (StoredBytes::FramedMemory(Arc::new(m.bytes)), length)
            }
            Some(StoredBytes::FramedFile(file)) => {
                let length = file
                    .metadata()
                    .map_err(|e| DriverError::Io(e.to_string()))?
                    .len() as usize;
                (StoredBytes::FramedFile(file), length)
            }
            Some(StoredBytes::FramedMemory(bytes)) => {
                let length = bytes.len();
                (StoredBytes::FramedMemory(bytes), length)
            }
            Some(_) => return Err(invalid("only framed bytes can be held")),
        };
        let cursor_at: CursorAt = Arc::from(m.cursor_at);
        let end = cursor_at(length)?;
        Ok(Self {
            id: m.id,
            stored,
            length,
            records: m.records,
            part_index: m.part_index,
            previous_part: m.previous_part,
            manifest: m.manifest,
            cursor_at,
            aligned: m.aligned,
            removed: m.removed,
            memory: m.memory,
            input: acquired.input,
            observation: acquired.observation,
            revision: acquired.revision,
            base: base.clone(),
            end,
        })
    }

    /// Whether `cursor` is the material's final cursor: all of it admitted.
    pub fn admitted(&self, cursor: &Value) -> bool {
        cursor == &self.end
    }

    /// How many bytes of the material `cursor` says are admitted: none at the
    /// cursor it was read after, otherwise the `through` of a cursor this
    /// material produced.
    fn through(&self, cursor: &Value) -> Result<usize, DriverError> {
        if cursor == &self.base {
            return Ok(0);
        }
        let through = match cursor_material(cursor) {
            Some((id, Some(through))) if id == self.id => through as usize,
            _ => return Err(invalid("the cursor does not belong to the held material")),
        };
        if through > self.length {
            return Err(invalid("the cursor is past the held material's end"));
        }
        if self
            .records
            .iter()
            .any(|&(start, end)| start < through && through < end)
        {
            return Err(invalid("the cursor falls inside a record"));
        }
        if cursor != &(self.cursor_at)(through)? {
            return Err(invalid(
                "the cursor differs from the one the held material gives there",
            ));
        }
        Ok(through)
    }

    /// The held material again, from where `cursor` says admission stopped.
    pub fn acquired(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let through = self.through(cursor)?;
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
        let at = self.cursor_at.clone();
        Ok(Acquired {
            input: self.input.clone(),
            observation,
            revision,
            cursor_without_material: Value::Null,
            material: Some(NewMaterial {
                id: self.id,
                bytes: Vec::new(),
                stored: Some(self.stored.clone()),
                records: self
                    .records
                    .iter()
                    .copied()
                    .filter(|&(_, end)| end > through)
                    .collect(),
                part_index: self.part_index,
                previous_part: self.previous_part,
                manifest: self.manifest.clone(),
                reading_order: super::reading_order(&self.manifest),
                cursor_at: Box::new(move |end| at(end)),
                aligned: self.aligned,
                removed: if through == 0 {
                    self.removed.clone()
                } else {
                    Vec::new()
                },
                memory: self.memory.clone(),
            }),
        })
    }
}

#[cfg(test)]
mod tests;

//! The server's answer, held in memory until all of it is admitted, so a
//! failed admission retries the same messages before the server is asked
//! again.
use super::*;
use crate::drivers::StoredBytes;

fn invalid(reason: &str) -> DriverError {
    DriverError::Cursor(reason.into())
}
fn cursor(value: &Value) -> Result<Cursor, DriverError> {
    if value.is_null() {
        Ok(Cursor::default())
    } else {
        serde_json::from_value(value.clone()).map_err(|_| invalid("invalid IMAP cursor"))
    }
}

pub(super) struct Answer {
    id: MaterialId,
    bytes: Arc<Vec<u8>>,
    records: Vec<(usize, usize)>,
    part_index: u32,
    previous_part: Option<MaterialId>,
    manifest: Value,
    /// The cursor the answer was fetched at.
    base: Cursor,
    /// The first UID of the answer: where a read that stops inside it resumes.
    first_uid: u32,
    /// The cursor after all of it.
    end: Cursor,
    observation: ObservationFacts,
}
impl Answer {
    pub fn capture(acquired: Acquired, current: &Value) -> Result<Self, DriverError> {
        let material = acquired
            .material
            .ok_or_else(|| invalid("IMAP answer has no material"))?;
        let base = cursor(current)?;
        let first_uid = first_uid(
            &base,
            material.manifest["uid_validity"].as_u64().map(|v| v as u32),
        );
        let end = cursor(&(material.cursor_at)(material.bytes.len())?)?;
        Ok(Self {
            id: material.id,
            bytes: Arc::new(material.bytes),
            records: material.records,
            part_index: material.part_index,
            previous_part: material.previous_part,
            manifest: material.manifest,
            base,
            first_uid,
            end,
            observation: acquired.observation,
        })
    }
    fn through(&self, value: &Value) -> Result<usize, DriverError> {
        let current = cursor(value)?;
        if current == self.base {
            return Ok(0);
        }
        let through = current
            .through
            .filter(|_| current.material == Some(self.id))
            .ok_or_else(|| invalid("the IMAP cursor does not belong to the held answer"))?;
        if current != at(&self.end, self.first_uid, &self.records, through)? {
            return Err(invalid("the IMAP cursor changed outside the held answer"));
        }
        Ok(through)
    }
    pub fn complete(&self, current: &Value) -> Result<bool, DriverError> {
        Ok(
            cursor(current)?.material == Some(self.id)
                && self.through(current)? == self.bytes.len(),
        )
    }
    pub fn acquired(&self, input: InputRef, current: &Value) -> Result<Acquired, DriverError> {
        let through = self.through(current)?;
        // A retry after the answer's first batch was admitted continues the
        // revision that batch opened.
        let (observation, revision) = if through == 0 {
            (self.observation.clone(), RevisionChange::Snapshot)
        } else {
            (
                ObservationFacts {
                    changed: false,
                    ..self.observation.clone()
                },
                RevisionChange::Same,
            )
        };
        let end = self.end.clone();
        let first = self.first_uid;
        let bounds = self.records.clone();
        Ok(Acquired {
            input,
            observation,
            revision,
            material: Some(NewMaterial {
                id: self.id,
                bytes: Vec::new(),
                stored: Some(StoredBytes::FramedMemory(self.bytes.clone())),
                records: self
                    .records
                    .iter()
                    .copied()
                    .filter(|r| r.1 > through)
                    .collect(),
                part_index: self.part_index,
                previous_part: self.previous_part,
                manifest: self.manifest.clone(),
                reading_order: crate::drivers::reading_order(&self.manifest),
                cursor_at: Box::new(move |through| {
                    at(&end, first, &bounds, through)
                        .map(|cursor| serde_json::to_value(cursor).expect("cursor serializes"))
                }),
                aligned: false,
                vanished: Vec::new(),
                memory: None,
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

/// Where a read after `previous` starts: its next UID, or the start when the
/// mailbox was renumbered.
pub(super) fn first_uid(previous: &Cursor, validity: Option<u32>) -> u32 {
    if previous.uid_validity.is_some() && previous.uid_validity != validity {
        1
    } else {
        previous.uid_next.max(1)
    }
}

/// The cursor `through` bytes into an answer whose final cursor is `end`. One
/// that stops inside the answer resumes at the answer's first UID.
pub(super) fn at(
    end: &Cursor,
    first_uid: u32,
    records: &[(usize, usize)],
    through: usize,
) -> Result<Cursor, DriverError> {
    let length = end
        .through
        .ok_or_else(|| invalid("the IMAP final byte end is missing"))?;
    if through > length || through != 0 && !records.iter().any(|r| r.1 == through) {
        return Err(invalid("the IMAP cursor is not at a message boundary"));
    }
    let mut next = end.clone();
    next.through = Some(through);
    if through < length {
        next.uid_next = first_uid;
    }
    Ok(next)
}

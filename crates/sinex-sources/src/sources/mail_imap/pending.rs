//! One retained IMAP answer stays ahead of any later server observation.
use super::*;
use crate::drivers::{RetainedRecords, StoredBytes};
use sinex_model::MaterialRef;
use std::collections::BTreeSet;

fn invalid(reason: &str) -> DriverError {
    DriverError::Cursor(reason.into())
}
fn cursor(value: &Value) -> Result<Cursor, DriverError> {
    if value.is_null() {
        Ok(Cursor::default())
    } else {
        serde_json::from_value(value.clone()).map_err(|_| invalid("invalid IMAP resume cursor"))
    }
}

pub(super) struct Answer {
    reference: MaterialRef,
    stored: StoredBytes,
    records: Vec<(usize, usize)>,
    base: Cursor,
    base_through: usize,
    partial_uid: u32,
    end: Cursor,
    observation: ObservationFacts,
}
impl Answer {
    pub fn capture(
        acquired: Acquired,
        current: &Value,
        mailbox: &str,
    ) -> Result<Self, DriverError> {
        let material = acquired
            .material
            .ok_or_else(|| invalid("IMAP answer has no material"))?;
        if material.stored.is_some() {
            return Err(invalid("unexpected IMAP capture backing"));
        }
        let reference = MaterialRef {
            id: material.id,
            input: None,
            blob: String::new(),
            length: material.bytes.len() as u64,
            part_index: material.part_index,
            previous_part: material.previous_part,
            parser: SPEC.parser.into(),
            parser_version: SPEC.parser_version,
            manifest: material.manifest,
        };
        Self::new(
            reference,
            StoredBytes::FramedMemory(Arc::new(material.bytes)),
            current,
            &[],
            mailbox,
        )
    }
    pub fn restore(
        reference: MaterialRef,
        stored: RetainedRecords,
        current: &Value,
        accounted: &[usize],
        mailbox: &str,
    ) -> Result<Self, DriverError> {
        if stored.bodyless
            || stored.bytes.length() != reference.length
            || stored.blob != reference.blob
        {
            return Err(invalid("original retained IMAP answer is unavailable"));
        }
        Self::new(
            reference,
            StoredBytes::FramedReader(stored),
            current,
            accounted,
            mailbox,
        )
    }
    fn new(
        reference: MaterialRef,
        stored: StoredBytes,
        current: &Value,
        accounted: &[usize],
        mailbox: &str,
    ) -> Result<Self, DriverError> {
        if reference.parser != SPEC.parser
            || reference.parser_version != SPEC.parser_version
            || reference.manifest["mailbox"] != mailbox
        {
            return Err(invalid(
                "retained IMAP answer differs from its binding/parser",
            ));
        }
        let length = usize::try_from(reference.length)
            .map_err(|_| invalid("IMAP answer exceeds address space"))?;
        let records: Vec<(usize, usize)> =
            serde_json::from_value(reference.manifest["records"].clone())
                .map_err(|_| invalid("retained IMAP record ranges are missing"))?;
        let uids: Vec<u32> = serde_json::from_value(reference.manifest["uids"].clone())
            .map_err(|_| invalid("retained IMAP UIDs are missing"))?;
        let validity: Option<u32> =
            serde_json::from_value(reference.manifest["uid_validity"].clone())
                .map_err(|_| invalid("retained IMAP UIDVALIDITY is invalid"))?;
        if records.is_empty()
            || records.len() != uids.len()
            || uids.iter().any(|uid| *uid == 0)
            || uids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("retained IMAP UID/record mapping is invalid"));
        }
        let mut previous_end = 0;
        for &(start, end) in &records {
            if start != previous_end || end < start || end > length {
                return Err(invalid(
                    "retained IMAP records do not cover the exact answer",
                ));
            }
            previous_end = end;
        }
        if previous_end != length {
            return Err(invalid("retained IMAP answer is short"));
        }
        let base = cursor(current)?;
        let plan = &reference.manifest["imap_resume"];
        let (end, partial_uid, through) = if !plan.is_null() {
            if plan["version"] != 1 {
                return Err(invalid("unsupported retained IMAP resume version"));
            }
            let previous = cursor(&plan["previous"])?;
            let end = cursor(&plan["end"])?;
            let part = if previous.material.is_some() {
                previous
                    .part_index
                    .checked_add(1)
                    .ok_or_else(|| invalid("IMAP part index overflow"))?
            } else {
                0
            };
            let partial = if previous.uid_validity.is_some() && previous.uid_validity != validity {
                1
            } else {
                previous.uid_next.max(1)
            };
            if previous.material != reference.previous_part
                || part != reference.part_index
                || end.material != Some(reference.id)
                || end.part_index != reference.part_index
                || end.uid_validity != validity
                || end.through != Some(length)
                || end.uid_next < uids.last().unwrap().saturating_add(1)
            {
                return Err(invalid(
                    "retained IMAP final cursor differs from its material",
                ));
            }
            let through = if base == previous {
                0
            } else {
                let through = base
                    .through
                    .ok_or_else(|| invalid("IMAP processed byte end is missing"))?;
                if base.material != Some(reference.id)
                    || base.part_index != reference.part_index
                    || base.uid_validity != validity
                    || base.uid_next
                        != if through < length {
                            partial
                        } else {
                            end.uid_next
                        }
                {
                    return Err(invalid("IMAP cursor differs from its retained answer"));
                }
                through
            };
            (end, partial, through)
        } else {
            // Older answers did not retain the examined sparse-window end. Only
            // fetched UIDs can be acknowledged when their suffix is recovered.
            let mut end = Cursor {
                uid_validity: validity,
                uid_next: uids.last().unwrap().saturating_add(1),
                material: Some(reference.id),
                part_index: reference.part_index,
                through: Some(length),
            };
            if base.material == Some(reference.id) {
                if base.uid_validity != validity || base.part_index != reference.part_index {
                    return Err(invalid(
                        "legacy IMAP cursor differs from its retained answer",
                    ));
                }
                if base.uid_next > *uids.last().unwrap() {
                    end.uid_next = base.uid_next;
                    (end, base.uid_next, length)
                } else {
                    let unique: BTreeSet<_> = records.iter().map(|r| r.0).collect();
                    let accounted: BTreeSet<_> = accounted.iter().copied().collect();
                    if unique.len() != records.len() || !accounted.is_subset(&unique) {
                        return Err(invalid("legacy IMAP record accounting is ambiguous"));
                    }
                    let prefix = records
                        .iter()
                        .take_while(|r| accounted.contains(&r.0))
                        .count();
                    if prefix == 0 || prefix == records.len() || prefix != accounted.len() {
                        return Err(invalid(
                            "legacy IMAP accounted records are not one proven partial prefix",
                        ));
                    }
                    (end, base.uid_next, records[prefix - 1].1)
                }
            } else if base.material == reference.previous_part {
                let partial = if base.uid_validity.is_some() && base.uid_validity != validity {
                    1
                } else {
                    base.uid_next.max(1)
                };
                (end, partial, 0)
            } else {
                return Err(invalid("legacy IMAP predecessor is unavailable"));
            }
        };
        if through > length || through != 0 && !records.iter().any(|r| r.1 == through) {
            return Err(invalid(
                "IMAP processed end is not a retained record boundary",
            ));
        }
        let read_at = reference.manifest["read_at_ns"]
            .as_i64()
            .ok_or_else(|| invalid("retained IMAP observation time is missing"))?;
        let observation = ObservationFacts {
            observed_at: Some(
                Timestamp::from_unix_ns(read_at as i128)
                    .map_err(|_| invalid("retained IMAP observation time is invalid"))?,
            ),
            size: Some(reference.length),
            changed: true,
            ..Default::default()
        };
        Ok(Self {
            reference,
            stored,
            records,
            base,
            base_through: through,
            partial_uid,
            end,
            observation,
        })
    }
    fn through(&self, value: &Value) -> Result<usize, DriverError> {
        let current = cursor(value)?;
        if current == self.base {
            return Ok(self.base_through);
        }
        let through = current
            .through
            .ok_or_else(|| invalid("IMAP resume byte end is missing"))?;
        let expected = self.at(through)?;
        if current != expected {
            return Err(invalid("IMAP cursor changed outside its retained answer"));
        }
        Ok(through)
    }
    fn at(&self, through: usize) -> Result<Cursor, DriverError> {
        let length = self.reference.length as usize;
        if through > length || through != 0 && !self.records.iter().any(|r| r.1 == through) {
            return Err(invalid("IMAP cursor is not a retained record boundary"));
        }
        let mut next = self.end.clone();
        next.through = Some(through);
        if through < length {
            next.uid_next = self.partial_uid;
        }
        Ok(next)
    }
    pub fn complete(&self, current: &Value) -> Result<bool, DriverError> {
        let current_id = cursor(current)?.material;
        Ok(current_id == Some(self.reference.id)
            && self.through(current)? == self.reference.length as usize)
    }
    pub fn acquired(&self, input: InputRef, current: &Value) -> Result<Acquired, DriverError> {
        let through = self.through(current)?;
        let first = cursor(current)?.material != Some(self.reference.id);
        let records = self
            .records
            .iter()
            .copied()
            .filter(|r| r.1 > through || first)
            .collect();
        let end = self.end.clone();
        let partial = self.partial_uid;
        let bounds = self.records.clone();
        let length = self.reference.length as usize;
        Ok(Acquired {
            image: None,
            input,
            observation: self.observation.clone(),
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id: self.reference.id,
                bytes: Vec::new(),
                stored: Some(self.stored.clone()),
                records,
                part_index: self.reference.part_index,
                previous_part: self.reference.previous_part,
                manifest: self.reference.manifest.clone(),
                reading_order: crate::drivers::reading_order(&self.reference.manifest),
                cursor_at: Box::new(move |through| {
                    if through > length || through != 0 && !bounds.iter().any(|r| r.1 == through) {
                        return Err(invalid("IMAP cursor is not a retained record boundary"));
                    }
                    let mut cursor = end.clone();
                    cursor.through = Some(through);
                    if through < length {
                        cursor.uid_next = partial;
                    }
                    Ok(serde_json::to_value(cursor).expect("cursor serializes"))
                }),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

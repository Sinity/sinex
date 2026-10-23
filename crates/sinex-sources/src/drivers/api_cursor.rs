//! `ApiCursor`: something asked for what changed since a cursor or page token
//! (a web API, a local repository's refs, a mail server). Each answer is
//! stored as one material: its records as JSON lines, exactly as the parser
//! reads them, with the cursor that asked for them in the manifest.
//!
//! The cursor advances only when the whole answer is stored: a crash between
//! batches of one answer asks again from the old cursor, and what was already
//! stored comes back as repeats.
use super::{Acquired, DriverError, NewMaterial};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sinex_model::{InputRef, MaterialId, ObservationFacts, RevisionChange, Timestamp};

/// One answer: its records, and the cursor to ask from next time.
pub struct Page {
    pub records: Vec<Map<String, Value>>,
    pub next: Value,
}

/// What a source implements to be read through `ApiCursor`.
pub trait Api {
    fn input(&self) -> InputRef;
    /// Everything new since `cursor` (`Null` the first time).
    fn fetch(&self, cursor: &Value) -> Result<Page, DriverError>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// The source's own cursor.
    pub api: Value,
    pub material: Option<MaterialId>,
    pub part_index: u32,
    #[serde(default)]
    pub last_order: i64,
}

pub struct ApiCursor<A> {
    pub api: A,
}

impl<A: Api> ApiCursor<A> {
    pub fn poll(&self, cursor: &Value) -> Result<Acquired, DriverError> {
        let previous: Cursor = if cursor.is_null() {
            Cursor::default()
        } else {
            serde_json::from_value(cursor.clone())
                .map_err(|e| DriverError::Cursor(e.to_string()))?
        };
        let asked_at = Timestamp::now();
        let page = self.api.fetch(&previous.api)?;
        let observation = ObservationFacts {
            observed_at: Some(asked_at),
            changed: !page.records.is_empty(),
            ..Default::default()
        };
        if page.records.is_empty() {
            let unchanged = Cursor {
                api: page.next,
                ..previous
            };
            return Ok(Acquired {
                image: None,
                input: self.api.input(),
                observation,
                revision: RevisionChange::Same,
                material: None,
                cursor_unchanged: serde_json::to_value(unchanged).expect("cursor serializes"),
            });
        }
        let mut bytes = Vec::new();
        let mut records = Vec::with_capacity(page.records.len());
        for record in page.records {
            let start = bytes.len();
            serde_json::to_writer(&mut bytes, &Value::Object(record))
                .expect("JSON object serializes");
            bytes.push(b'\n');
            records.push((start, bytes.len()));
        }
        let id = MaterialId::new();
        let part_index = if previous.material.is_some() {
            previous.part_index + 1
        } else {
            0
        };
        let now_ns = i64::try_from(asked_at.unix_ns()).unwrap_or(i64::MAX);
        let base = now_ns.max(previous.last_order.saturating_add(1));
        let manifest = json!({
            "media_type": "application/x-ndjson",
            "asked_with": previous.api,
            "asked_at_ns": now_ns,
            "order_base": base,
        });
        let length = bytes.len();
        let next = page.next;
        let old = previous.api.clone();
        let cursor_at = move |end: usize| -> Value {
            let complete = end >= length;
            serde_json::to_value(Cursor {
                // Until the whole answer is stored, the next poll asks again.
                api: if complete { next.clone() } else { old.clone() },
                material: Some(id),
                part_index,
                last_order: base.saturating_add(end as i64),
            })
            .expect("cursor serializes")
        };
        Ok(Acquired {
            image: None,
            input: self.api.input(),
            observation,
            revision: RevisionChange::Snapshot,
            material: Some(NewMaterial {
                id,
                bytes,
                stored: None,
                records,
                part_index,
                previous_part: previous.material,
                reading_order: super::reading_order(&manifest),
                manifest,
                cursor_at: Box::new(cursor_at),
                aligned: false,
                vanished: Vec::new(),
            }),
            cursor_unchanged: Value::Null,
        })
    }
}

#[cfg(test)]
mod tests;

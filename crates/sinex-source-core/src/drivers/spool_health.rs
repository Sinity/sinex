//! Capture writes and acknowledged batches update these counts without rereading a spool.
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};

#[derive(Clone, Copy, Debug, Default)]
pub struct Counts {
    pub bytes: u64,
    pub records: u64,
}

#[derive(Default)]
pub struct Backlog {
    pub counts: Counts,
    pub frames: usize,
    pub error: Option<String>,
    pub capture_failures: usize,
    pub cursor: Value,
    pub recovery: Counts,
    pub media: BTreeMap<String, u64>,
    pub segments: BTreeMap<u64, Counts>,
}

impl Backlog {
    pub fn captured(&mut self, bytes: u64, records: u64) {
        self.counts.bytes += bytes;
        self.counts.records += records;
    }

    pub fn captured_line(&mut self, segment: u64, bytes: u64) {
        let counts = self.segments.entry(segment).or_default();
        counts.bytes += bytes;
        counts.records += 1;
        self.captured(bytes, 1);
    }

    pub fn captured_media(&mut self, stem: String, bytes: u64) {
        self.media.insert(stem, bytes);
        self.captured(bytes, 1);
    }

    fn subtract(&mut self, counts: Counts) {
        self.counts.bytes = self.counts.bytes.saturating_sub(counts.bytes);
        self.counts.records = self.counts.records.saturating_sub(counts.records);
    }

    pub fn recovered(&mut self, cursor: &Value) {
        self.subtract(self.recovery);
        if let Some(segment) = cursor.get("segment").and_then(Value::as_u64) {
            let passed_records: u64 = self
                .segments
                .range(..segment)
                .map(|(_, counts)| counts.records)
                .sum();
            while self
                .segments
                .first_key_value()
                .is_some_and(|(index, _)| *index < segment)
            {
                let _ = self.segments.pop_first();
            }
            if let Some(counts) = self.segments.get_mut(&segment) {
                let offset = cursor
                    .pointer("/inner/offset")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                let previous = if self.cursor.get("segment") == cursor.get("segment") {
                    self.cursor
                        .pointer("/inner/offset")
                        .and_then(Value::as_u64)
                        .unwrap_or(0)
                } else {
                    0
                };
                counts.bytes = counts.bytes.saturating_sub(offset.saturating_sub(previous));
                // Earlier segments have already been removed from the map.
                counts.records = counts
                    .records
                    .saturating_sub(self.recovery.records.saturating_sub(passed_records));
            }
        }
        self.recovery = Counts::default();
        self.cursor = cursor.clone();
        if let Some(last) = cursor.get("last").and_then(Value::as_str) {
            while self
                .media
                .first_key_value()
                .is_some_and(|(stem, _)| stem.as_str() <= last)
            {
                let _ = self.media.pop_first();
            }
        }
    }

    pub fn prepare(&mut self, cursor: &Value, records: u64) {
        let segment = cursor.get("segment").and_then(Value::as_u64);
        let passed = segment
            .map(|segment| {
                self.segments
                    .range(..segment)
                    .fold(Counts::default(), |mut total, (_, counts)| {
                        total.bytes += counts.bytes;
                        total.records += counts.records;
                        total
                    })
            })
            .unwrap_or_default();
        let bytes = if let Some(last) = cursor.get("last").and_then(Value::as_str) {
            self.media
                .range(..=last.to_owned())
                .map(|(_, size)| *size)
                .sum()
        } else {
            let offset = cursor
                .pointer("/inner/offset")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let previous = if self.cursor.get("segment") == cursor.get("segment") {
                self.cursor
                    .pointer("/inner/offset")
                    .and_then(Value::as_u64)
                    .unwrap_or(0)
            } else {
                0
            };
            offset.saturating_sub(previous)
        };
        // A completed media file is one captured record, including an empty file.
        let records = if let Some(last) = cursor.get("last").and_then(Value::as_str) {
            self.media.range(..=last.to_owned()).count() as u64
        } else {
            records + passed.records
        };
        self.recovery = Counts {
            bytes: bytes + passed.bytes,
            records,
        };
    }
}

pub type Counter = Arc<Mutex<Backlog>>;
type Registry = HashMap<PathBuf, Weak<Mutex<Backlog>>>;

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}

/// Install before capture starts, with the counts read once from existing files.
pub fn register(path: &Path, backlog: Backlog) -> Counter {
    let counter = Arc::new(Mutex::new(backlog));
    let mut registry = registry().lock().unwrap();
    registry.retain(|_, counter| counter.strong_count() > 0);
    registry.insert(path.to_owned(), Arc::downgrade(&counter));
    counter
}

pub(super) fn counter(path: &Path) -> Option<Counter> {
    registry().lock().unwrap().get(path).and_then(Weak::upgrade)
}

#[cfg(test)]
mod tests;

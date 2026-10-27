//! Exercise shared acquisition, stored parts and committed restart progress.
use serde_json::Value;
use sinex_model::{
    BatchKind, Class, Event, MaterialRef, MaterialRole, ObservationFacts, ReadingOrder,
    RecordOutcome, RunId,
};
use sinex_source_core::drivers::export_archive::{
    ExportArchive,
    group::{self, Input, Route},
};
use sinex_source_core::drivers::{self, MaterialSink};
use sinex_source_core::{DrainInput, Limits, Source};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::AtomicBool};

#[derive(Default)]
struct Sink(Mutex<BTreeMap<String, Vec<u8>>>);
impl MaterialSink for Sink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
        self.0.lock().unwrap().insert(name.clone(), bytes.to_vec());
        Ok(name)
    }
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }
}

#[derive(Default)]
struct Seen {
    cursor: Value,
    carry: Value,
    events: Vec<Event>,
    failures: Vec<String>,
    materials: Vec<MaterialRef>,
}

fn pass(
    raw: &[u8],
    driver: ExportArchive,
    source: &dyn Source,
    sink: &Sink,
    seen: &mut Seen,
    stop_early: bool,
) {
    let blob = sink.put(raw).unwrap();
    let result = group::read_input(
        Input::Sequential(Box::new(std::io::Cursor::new(raw.to_vec()))),
        blob,
        raw.len() as u64,
        ObservationFacts {
            size: Some(raw.len() as u64),
            mtime_ns: Some(1),
            inode: Some(2),
            device: Some(3),
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
        vec![Route {
            class: Class::Guarded,
            driver,
            cursor: seen.cursor.clone(),
        }],
        sink,
        &mut |_, acquired| {
            let mut acquired = acquired?;
            drivers::note_previous_through(&seen.cursor, &mut acquired);
            let prepared = drivers::prepare(
                source,
                "example-binding",
                RunId::new(),
                acquired,
                seen.carry.clone(),
                sink,
                Limits {
                    max_events: 1,
                    max_bytes: usize::MAX,
                },
            )?;
            for batch in prepared.batches()? {
                let batch = batch?;
                let BatchKind::Source(progress) = batch.kind else {
                    panic!("source batch")
                };
                if let Some(material) = progress.material {
                    if !seen.materials.iter().any(|m| m.id == material.id) {
                        seen.materials.push(material);
                    }
                }
                seen.cursor = progress.cursor;
                seen.carry = progress.carry;
                for record in batch.records {
                    match record {
                        RecordOutcome::Events { events, .. } => seen.events.extend(events),
                        RecordOutcome::Failure { reason, .. } => seen.failures.push(reason),
                    }
                }
                if stop_early && !seen.events.is_empty() {
                    return Ok(false);
                }
            }
            Ok(true)
        },
    );
    if stop_early {
        assert!(result.is_ok() || matches!(result, Err(drivers::DriverError::Cancelled)));
    } else {
        result.unwrap();
        assert_eq!(seen.cursor["done"], true);
    }
}

fn values(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .map(|e| {
            serde_json::json!([
                e.payload,
                e.equivalence_key,
                e.ts_orig.at,
                e.ts_orig.quality
            ])
        })
        .collect()
}

pub(super) fn export_restart(
    raw: &[u8],
    driver: ExportArchive,
    source: &dyn Source,
    expected: usize,
    failures: usize,
) -> Vec<Event> {
    let sink = Sink::default();
    let mut whole = Seen::default();
    pass(raw, driver.clone(), source, &sink, &mut whole, false);
    assert_eq!(whole.events.len(), expected);
    assert_eq!(whole.failures.len(), failures);
    let mut parts = Seen::default();
    pass(raw, driver.clone(), source, &sink, &mut parts, true);
    assert_eq!(parts.events.len(), 1);
    // Only stored original bytes and admitted progress cross the restart.
    parts.cursor = serde_json::from_slice(&serde_json::to_vec(&parts.cursor).unwrap()).unwrap();
    parts.carry = serde_json::from_slice(&serde_json::to_vec(&parts.carry).unwrap()).unwrap();
    let original = parts
        .materials
        .iter()
        .find(|m| m.manifest.role == MaterialRole::OriginalArchive)
        .unwrap();
    let stored = sink.0.lock().unwrap()[&original.blob].clone();
    pass(&stored, driver, source, &sink, &mut parts, false);
    assert_eq!(values(&parts.events), values(&whole.events));
    assert_eq!(parts.failures, whole.failures);
    let mut carry = Value::Null;
    let mut replayed = vec![];
    for material in &parts.materials {
        if material.manifest.role == MaterialRole::OriginalArchive {
            carry = Value::Null;
            continue;
        }
        let bytes = sink.0.lock().unwrap()[&material.blob].clone();
        let through = parts
            .materials
            .iter()
            .find(|next| next.previous_part == Some(material.id))
            .and_then(|next| next.manifest.previous_through)
            .unwrap_or(material.length) as usize;
        let records = material
            .manifest
            .records
            .as_ref()
            .unwrap()
            .iter()
            .copied()
            .filter(|(_, end)| *end <= through)
            .collect::<Vec<_>>();
        let chunks = source
            .drain(DrainInput {
                source: source.spec().name,
                class: source.spec().class,
                material: material.id,
                bytes: bytes.as_slice().into(),
                records: &records,
                carry: carry.clone(),
                reading_order: &|a| ReadingOrder(a as i64),
                limits: Limits::default(),
                manifest: &material.manifest,
            })
            .unwrap();
        for chunk in chunks {
            carry = chunk.carry;
            for record in chunk.outcomes {
                if let RecordOutcome::Events { events, .. } = record {
                    replayed.extend(events);
                }
            }
        }
    }
    assert_eq!(values(&replayed), values(&parts.events));
    for event in &parts.events {
        let (location, _) = event.location().unwrap();
        let material = parts
            .materials
            .iter()
            .find(|m| m.id == location.material)
            .unwrap();
        let bytes = sink.0.lock().unwrap()[&material.blob].clone();
        let range = material
            .manifest
            .records
            .as_ref()
            .unwrap()
            .iter()
            .find(|(start, _)| *start as u64 == location.anchor)
            .unwrap();
        assert_eq!(
            event.record_hash,
            *blake3::hash(&bytes[range.0..range.1]).as_bytes()
        );
    }
    parts.events
}

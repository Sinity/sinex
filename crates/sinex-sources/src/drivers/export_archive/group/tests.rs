use super::*;
use crate::drivers;
use crate::sources::google_takeout::fit;
use sinex_model::{BatchKind, RecordOutcome, RunId};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
struct Sink {
    bytes: Mutex<BTreeMap<String, Vec<u8>>>,
    writes: AtomicU64,
}
impl MaterialSink for Sink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        let name = format!("blake3:{}", blake3::hash(bytes).to_hex());
        self.bytes
            .lock()
            .unwrap()
            .insert(name.clone(), bytes.to_vec());
        Ok(name)
    }
}

fn tar(members: &[(&str, Vec<u8>)], gzip: bool) -> Vec<u8> {
    let mut tar = tar::Builder::new(Vec::new());
    for (path, bytes) in members {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        tar.append_data(&mut header, path, bytes.as_slice())
            .unwrap();
    }
    let bytes = tar.into_inner().unwrap();
    if !gzip {
        return bytes;
    }
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(&bytes).unwrap();
    encoder.finish().unwrap()
}

fn points(n: usize) -> Vec<u8> {
    serde_json::to_vec(&json!({"Data Points": (0..n).map(|i| json!({"dataTypeName": "synthetic.steps", "startTimeNanos": i as i64 + 1, "endTimeNanos": i as i64 + 2, "fitValue": [{"intVal": i}]})).collect::<Vec<_>>()})).unwrap()
}

fn driver(budget: usize) -> ExportArchive {
    ExportArchive {
        path: "/synthetic/export.tgz".into(),
        host: "synthetic".into(),
        select: |name| name.ends_with(".json"),
        framing: Framing::JsonArray("/Data Points"),
        part_bytes: budget,
    }
}
fn facts(length: usize) -> ObservationFacts {
    ObservationFacts {
        size: Some(length as u64),
        mtime_ns: Some(1000),
        inode: Some(10),
        device: Some(20),
        changed: true,
        ..Default::default()
    }
}
fn route(cursor: Value, pending: Option<Pending>, budget: usize) -> Route {
    Route {
        driver: driver(budget),
        cursor,
        pending,
    }
}
fn source(index: usize) -> crate::DeclaredSource<fit::FitParser> {
    let mut source = fit::source();
    source.spec.name = if index == 0 {
        "synthetic.fit-a"
    } else {
        "synthetic.fit-b"
    };
    source
}

#[derive(Default)]
struct Seen {
    cursors: BTreeMap<usize, Value>,
    carry: BTreeMap<usize, Value>,
    materials: BTreeMap<MaterialId, MaterialRef>,
    events: BTreeMap<usize, Vec<sinex_model::Event>>,
    originals: Vec<MaterialId>,
    errors: Vec<String>,
}
impl Seen {
    fn accept(
        &mut self,
        index: usize,
        acquired: Result<Acquired, DriverError>,
        sink: &Sink,
    ) -> bool {
        let acquired = match acquired {
            Ok(a) => a,
            Err(e) => {
                self.errors.push(e.to_string());
                return false;
            }
        };
        let source = source(index);
        let carry = self.carry.get(&index).cloned().unwrap_or(Value::Null);
        let prepared = drivers::prepare(
            &source,
            &format!("binding-{index}"),
            RunId::new(),
            acquired,
            carry,
            sink,
            crate::Limits::default(),
        )
        .unwrap();
        for batch in prepared.batches().unwrap() {
            let BatchKind::Source(progress) = batch.kind else {
                panic!("source progress")
            };
            if let Some(material) = progress.material {
                if material.manifest["role"] == "original-archive" {
                    assert_eq!(progress.cursor["done"], false);
                    self.originals.push(material.id);
                }
                self.materials.insert(material.id, material);
            }
            self.cursors.insert(index, progress.cursor);
            self.carry.insert(index, progress.carry);
            for record in batch.records {
                match record {
                    RecordOutcome::Events { events, .. } => {
                        self.events.entry(index).or_default().extend(events)
                    }
                    RecordOutcome::Failure { reason, .. } => self.errors.push(reason),
                }
            }
        }
        true
    }
}

#[test]
fn one_forward_decode_and_record_write_serve_distinct_logical_sources() {
    for gzip in [false, true] {
        let raw = tar(
            &[
                ("Takeout/Fit/All Data/a.json", points(400)),
                ("unused.txt", vec![b'x'; 300_000]),
            ],
            gzip,
        );
        let sink = Sink::default();
        let blob = sink.put(&raw).unwrap();
        let mut seen = Seen::default();
        let work = read(
            Box::new(std::io::Cursor::new(raw.clone())),
            blob.clone(),
            raw.len() as u64,
            facts(raw.len()),
            Arc::new(AtomicBool::new(false)),
            vec![route(Value::Null, None, 400), route(Value::Null, None, 400)],
            &sink,
            &mut |index, acquired| Ok(seen.accept(index, acquired, &sink)),
        )
        .unwrap();
        assert!(seen.errors.is_empty(), "{:?}", seen.errors);
        assert_eq!(seen.originals.len(), 2);
        assert_ne!(seen.originals[0], seen.originals[1]);
        assert_eq!(seen.materials[&seen.originals[0]].blob, blob);
        assert_eq!(seen.materials[&seen.originals[1]].blob, blob);
        for index in 0..2 {
            assert_eq!(seen.events[&index].len(), 400);
            assert_eq!(seen.cursors[&index]["done"], true);
        }
        assert!(
            seen.events[&0]
                .iter()
                .all(|event| event.source == "synthetic.fit-a")
        );
        assert!(
            seen.events[&1]
                .iter()
                .all(|event| event.source == "synthetic.fit-b")
        );
        let record_materials = seen.materials.len() - 2;
        assert_eq!(
            sink.writes.load(Ordering::Relaxed),
            1 + record_materials as u64 / 2,
            "shared physical record bodies are installed once, logical material IDs remain distinct"
        );
        let mut uncompressed = Vec::new();
        if gzip {
            flate2::read::MultiGzDecoder::new(raw.as_slice())
                .read_to_end(&mut uncompressed)
                .unwrap();
        } else {
            uncompressed = raw.clone();
        }
        // tar may stop before trailing zero padding. The useful physical-work property
        // is that adding a logical route does not add a decoding pass.
        assert!(work.decoded_bytes <= uncompressed.len() as u64);
        assert!(work.decoded_bytes >= 300_000 + points(400).len() as u64);
        let one = Sink::default();
        let blob = one.put(&raw).unwrap();
        let mut one_seen = Seen::default();
        let one_work = read(
            Box::new(std::io::Cursor::new(raw.clone())),
            blob,
            raw.len() as u64,
            facts(raw.len()),
            Arc::new(AtomicBool::new(false)),
            vec![route(Value::Null, None, 400)],
            &one,
            &mut |index, acquired| Ok(one_seen.accept(index, acquired, &one)),
        )
        .unwrap();
        assert_eq!(work.decoded_bytes, one_work.decoded_bytes);
        assert_eq!(work.member_staging_bytes, 0);
        assert_eq!(work.zip_staging_bytes, 0);
    }
}

#[test]
fn restart_finishes_a_part_with_its_original_material_id_and_anchors() {
    let raw = tar(&[("Takeout/Fit/All Data/a.json", points(40))], true);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    let mut stopped = false;
    let mut pending = None;
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob.clone(),
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![
            route(Value::Null, None, 1000),
            route(Value::Null, None, 1000),
        ],
        &sink,
        &mut |index, acquired| {
            let acquired = acquired.unwrap();
            if index == 0
                && !stopped
                && acquired
                    .material
                    .as_ref()
                    .is_some_and(|m| !m.records.is_empty())
            {
                let m = acquired.material.as_ref().unwrap();
                let first = m.records[0];
                seen.cursors.insert(0, (m.cursor_at)(first.1));
                let StoredBytes::Blob { name, length } = m.stored.as_ref().unwrap() else {
                    panic!("retained body")
                };
                let reference = MaterialRef {
                    id: m.id,
                    input: None,
                    blob: name.clone(),
                    length: *length,
                    part_index: m.part_index,
                    previous_part: m.previous_part,
                    parser: source(0).spec.parser.into(),
                    parser_version: source(0).spec.parser_version,
                    manifest: m.manifest.clone(),
                };
                pending = Some((reference, m.bytes.clone(), m.records.clone()));
                stopped = true;
                return Ok(false);
            }
            Ok(seen.accept(index, Ok(acquired), &sink))
        },
    )
    .unwrap();
    let (reference, bytes, records) = pending.unwrap();
    let id = reference.id;
    let saved = Pending::new(reference, bytes, &source(0).spec).unwrap();
    let mut resumed_id = None;
    let routes = vec![
        route(seen.cursors[&0].clone(), Some(saved), 31),
        route(seen.cursors[&1].clone(), None, 31),
    ];
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        routes,
        &sink,
        &mut |index, acquired| {
            if index == 0 && resumed_id.is_none() {
                let acquired = acquired.as_ref().unwrap();
                let material = acquired.material.as_ref().unwrap();
                assert_eq!(material.id, id);
                assert_eq!(material.records, records[1..]);
                resumed_id = Some(material.id);
            }
            Ok(seen.accept(index, acquired, &sink))
        },
    )
    .unwrap();
    assert_eq!(resumed_id, Some(id));
    assert!(seen.errors.is_empty(), "{:?}", seen.errors);
    assert_eq!(seen.events[&0].len(), 39);
    assert_eq!(seen.events[&1].len(), 40);
    assert_eq!(seen.cursors[&0]["done"], true);
}

#[test]
fn invalid_record_is_accounted_and_valid_suffix_is_still_admitted() {
    let good: Value = serde_json::from_slice(&points(2)).unwrap();
    let doc = serde_json::to_vec(&json!({"Data Points":[good["Data Points"][0], {"unknown":"shape"}, good["Data Points"][1]]})).unwrap();
    let raw = tar(&[("Takeout/Fit/All Data/a.json", doc)], true);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![route(Value::Null, None, 100)],
        &sink,
        &mut |index, acquired| Ok(seen.accept(index, acquired, &sink)),
    )
    .unwrap();
    assert_eq!(seen.events[&0].len(), 2);
    assert_eq!(seen.errors.len(), 1);
    assert_eq!(seen.cursors[&0]["done"], true);
    let orders: Vec<_> = seen.events[&0]
        .iter()
        .map(|event| event.reading_order)
        .collect();
    assert!(orders[0] < orders[1], "later parts keep source order");
}

#[test]
fn incompatible_framing_stages_one_member_and_failure_stays_with_its_route() {
    let mut document: Value = serde_json::from_slice(&points(3)).unwrap();
    document["Data Points"][1]["large"] = json!("x".repeat(1_000_000));
    let doc = serde_json::to_vec(&document).unwrap();
    let raw = tar(&[("Takeout/Fit/All Data/a.json", doc.clone())], true);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    let mut wrong = route(Value::Null, None, 100);
    wrong.driver.framing = Framing::JsonArray("/absent");
    let work = read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![route(Value::Null, None, 100), wrong],
        &sink,
        &mut |index, acquired| Ok(seen.accept(index, acquired, &sink)),
    )
    .unwrap();
    assert_eq!(seen.events[&0].len(), 3);
    assert_eq!(seen.cursors[&0]["done"], true);
    assert_eq!(seen.errors.len(), 1);
    assert_eq!(seen.cursors[&1]["done"], false);
    assert_eq!(work.member_staging_bytes, doc.len() as u64);
}

#[test]
fn cancellation_stops_decoding_before_an_unused_large_suffix() {
    let raw = tar(
        &[
            ("Takeout/Fit/All Data/a.json", points(400)),
            ("unused.txt", vec![b'x'; 4_000_000]),
        ],
        true,
    );
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    let stop = Arc::new(AtomicBool::new(false));
    let result = read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::clone(&stop),
        vec![route(Value::Null, None, 100)],
        &sink,
        &mut |index, acquired| {
            let part = acquired
                .as_ref()
                .is_ok_and(|a| a.material.as_ref().is_some_and(|m| !m.records.is_empty()));
            let keep = seen.accept(index, acquired, &sink);
            if part {
                stop.store(true, Ordering::Relaxed);
            }
            Ok(keep)
        },
    );
    assert!(matches!(result, Err(DriverError::Cancelled)));
    assert!(seen.events[&0].len() < 400);
    assert_ne!(seen.cursors[&0]["done"], true);
}

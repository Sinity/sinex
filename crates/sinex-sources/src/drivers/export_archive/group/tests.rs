use super::*;
use crate::drivers;
use crate::drivers::export_archive::Framing;
use crate::sources::google_takeout::fit;
use sinex_model::{BatchKind, MaterialRef, RecordOutcome, RunId};
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Default)]
struct Sink {
    bytes: Mutex<BTreeMap<String, Vec<u8>>>,
    writes: AtomicU64,
}
impl MaterialSink for Sink {
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }

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
        join: None,
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
fn route(cursor: Value, budget: usize) -> Route {
    Route {
        class: Class::Open,
        driver: driver(budget),
        cursor,
    }
}
fn source(index: usize) -> crate::DeclaredSource<fit::FitParser> {
    let mut source = fit::source(jiff::tz::TimeZone::UTC);
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
            let batch = batch.unwrap();
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

#[devkit::test]
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
            vec![route(Value::Null, 400), route(Value::Null, 400)],
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
            vec![route(Value::Null, 400)],
            &one,
            &mut |index, acquired| Ok(one_seen.accept(index, acquired, &one)),
        )
        .unwrap();
        assert_eq!(work.decoded_bytes, one_work.decoded_bytes);
        assert_eq!(work.member_staging_bytes, 0);
        assert_eq!(work.zip_staging_bytes, 0);
    }
}

#[devkit::test]
fn a_restart_reads_the_rest_of_a_partly_admitted_part_from_the_stored_archive() {
    let raw = tar(&[("Takeout/Fit/All Data/a.json", points(40))], true);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    let mut first_part = None;
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob.clone(),
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![route(Value::Null, 1000), route(Value::Null, 1000)],
        &sink,
        &mut |index, acquired| {
            let acquired = acquired.unwrap();
            if index == 0
                && first_part.is_none()
                && acquired
                    .material
                    .as_ref()
                    .is_some_and(|m| !m.records.is_empty())
            {
                // Only the part's first record is admitted before the stop.
                let m = acquired.material.as_ref().unwrap();
                let first = m.records[0];
                seen.cursors.insert(0, (m.cursor_at)(first.1).unwrap());
                first_part = Some(m.id);
                return Ok(false);
            }
            Ok(seen.accept(index, Ok(acquired), &sink))
        },
    )
    .unwrap();
    let first_part = first_part.unwrap();
    let mut rest = None;
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![
            route(seen.cursors[&0].clone(), 31),
            route(seen.cursors[&1].clone(), 31),
        ],
        &sink,
        &mut |index, acquired| {
            if index == 0 && rest.is_none() {
                let material = acquired.as_ref().unwrap().material.as_ref().unwrap();
                rest = Some((material.id, material.previous_part));
            }
            Ok(seen.accept(index, acquired, &sink))
        },
    )
    .unwrap();
    let (rest_id, previous) = rest.unwrap();
    assert_ne!(rest_id, first_part, "the rest is a new part");
    assert_eq!(previous, Some(first_part));
    assert!(seen.errors.is_empty(), "{:?}", seen.errors);
    assert_eq!(
        seen.events[&0].len(),
        39,
        "the first record was admitted before the stop"
    );
    assert_eq!(seen.events[&1].len(), 40);
    assert_eq!(seen.cursors[&0]["done"], true);
}

#[devkit::test]
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
        vec![route(Value::Null, 100)],
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

#[devkit::test]
fn incompatible_framing_stages_one_member_and_failure_stays_with_its_route() {
    let mut document: Value = serde_json::from_slice(&points(3)).unwrap();
    document["Data Points"][1]["large"] = json!("x".repeat(1_000_000));
    let doc = serde_json::to_vec(&document).unwrap();
    let raw = tar(&[("Takeout/Fit/All Data/a.json", doc.clone())], true);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    let mut wrong = route(Value::Null, 100);
    wrong.driver.framing = Framing::JsonArray("/absent");
    let work = read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![route(Value::Null, 100), wrong],
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

#[devkit::test]
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
        vec![route(Value::Null, 100)],
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

#[devkit::test]
fn sealed_zip_uses_supplied_seek_without_plaintext_staging() {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    zip.start_file("a.json", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(&points(7)).unwrap();
    let raw = zip.finish().unwrap().into_inner();
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut sealed_route = route(Value::Null, 400);
    sealed_route.class = Class::Sealed;
    let mut seen = Seen::default();
    let work = read_input(
        Input::Seekable(Box::new(std::io::Cursor::new(raw.clone()))),
        blob.clone(),
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![sealed_route],
        &sink,
        &mut |index, acquired| Ok(seen.accept(index, acquired, &sink)),
    )
    .unwrap();
    assert_eq!(work.zip_staging_bytes, 0);
    assert_eq!(work.member_staging_bytes, 0);
    assert_eq!(seen.events[&0].len(), 7);
    let mut sealed_route = route(Value::Null, 400);
    sealed_route.class = Class::Sealed;
    let error = read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![sealed_route],
        &sink,
        &mut |_, _| panic!("refuse before original admission"),
    )
    .unwrap_err();
    assert!(error.to_string().contains("plaintext is never staged"));
}

#[devkit::test]
fn zip_members_keep_exact_ranges_and_long_plain_records_stay_whole() {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    for i in 0..20 {
        zip.start_file(
            format!("Takeout/{i:02}.json"),
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        zip.write_all(format!("[{},{}]", i * 2, i * 2 + 1).as_bytes())
            .unwrap();
    }
    zip.start_file("unused.txt", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"not JSON").unwrap();
    let raw = zip.finish().unwrap().into_inner();
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut selected = route(Value::Null, 20);
    selected.driver.framing = Framing::JsonArray("");
    let mut records = Vec::new();
    let mut parts = 0;
    let mut original = 0;
    let mut cursor = Value::Null;
    let work = read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![selected],
        &sink,
        &mut |_, acquired| {
            let acquired = acquired?;
            if let Some(material) = acquired.material {
                if material.manifest["role"] == "original-archive" {
                    original += 1;
                } else {
                    parts += 1;
                    assert!(material.bytes.len() <= 20);
                    assert_eq!(material.manifest["source_container"], "zip");
                    let path = material.manifest["members"][0]["path"].as_str().unwrap();
                    assert!(path.ends_with(".json"));
                    for (start, end) in material.records {
                        records
                            .push(String::from_utf8(material.bytes[start..end].to_vec()).unwrap());
                    }
                }
            } else {
                cursor = acquired.cursor_unchanged;
            }
            Ok(true)
        },
    )
    .unwrap();
    assert_eq!(records, (0..40).map(|i| i.to_string()).collect::<Vec<_>>());
    assert_eq!(original, 1);
    assert!(parts > 5);
    assert_eq!(cursor["done"], true);
    assert_eq!(work.zip_staging_bytes, raw.len() as u64);
    let raw = format!("{}\nb\n", "a".repeat(50)).into_bytes();
    let blob = sink.put(&raw).unwrap();
    let mut selected = route(Value::Null, 8);
    selected.driver.framing = Framing::Lines;
    let mut records = Vec::new();
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![selected],
        &sink,
        &mut |_, acquired| {
            if let Some(material) = acquired?.material {
                for (start, end) in material.records {
                    records.push(material.bytes[start..end].to_vec());
                }
            }
            Ok(true)
        },
    )
    .unwrap();
    assert_eq!(
        records,
        vec![
            format!("{}\n", "a".repeat(50)).into_bytes(),
            b"b\n".to_vec()
        ]
    );
}

#[devkit::test]
fn identical_captured_content_keeps_completed_material_identity_when_file_facts_change() {
    let raw = points(4);
    let sink = Sink::default();
    let blob = sink.put(&raw).unwrap();
    let mut seen = Seen::default();
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob.clone(),
        raw.len() as u64,
        facts(raw.len()),
        Arc::new(AtomicBool::new(false)),
        vec![route(Value::Null, 400)],
        &sink,
        &mut |i, a| Ok(seen.accept(i, a, &sink)),
    )
    .unwrap();
    let prior = seen.cursors[&0].clone();
    let material = prior["material"].clone();
    let writes = sink.writes.load(Ordering::Relaxed);
    let mut touched = facts(raw.len());
    touched.mtime_ns = Some(2000);
    read(
        Box::new(std::io::Cursor::new(raw.clone())),
        blob,
        raw.len() as u64,
        touched,
        Arc::new(AtomicBool::new(false)),
        vec![route(prior, 400)],
        &sink,
        &mut |i, a| Ok(seen.accept(i, a, &sink)),
    )
    .unwrap();
    assert_eq!(seen.cursors[&0]["material"], material);
    assert_eq!(seen.cursors[&0]["mtime_ns"], 2000);
    assert_eq!(sink.writes.load(Ordering::Relaxed), writes);
    assert_eq!(seen.originals.len(), 1);
    assert_eq!(seen.events[&0].len(), 4);
}

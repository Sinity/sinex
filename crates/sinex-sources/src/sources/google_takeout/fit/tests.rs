use super::*;
use crate::drivers::MaterialSink;
use crate::drivers::export_archive::group;
use serde_json::json;
use sinex_model::ObservationFacts;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

struct Sink;
impl MaterialSink for Sink {
    fn put(&self, bytes: &[u8]) -> Result<String, String> {
        Ok(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }
    fn put_file(&self, path: &std::path::Path) -> Result<String, String> {
        self.put(&std::fs::read(path).map_err(|e| e.to_string())?)
    }
}

const HOUR: i64 = 3_600_000_000_000;
/// 2026-01-01T00:00:00Z.
const JAN_1: i64 = 1_767_225_600_000_000_000;

fn point(data_type: &str, start: i64, values: Value) -> Value {
    json!({"dataTypeName": data_type, "startTimeNanos": start, "endTimeNanos": start + HOUR / 60,
        "fitValue": values, "modifiedTimeMillis": 1, "originDataSourceId": "synthetic"})
}

/// Fit files, by name, read through the archive driver in `zone`: each
/// record's bytes and the event the parser makes of it, the carry passed from
/// record to record as the drain does.
fn read(files: &[(&str, &Value)], zone: &str, part_bytes: usize) -> Vec<(Vec<u8>, HealthRecord)> {
    let mut tar = tar::Builder::new(Vec::new());
    for (name, document) in files {
        let member = serde_json::to_vec(document).unwrap();
        let mut header = tar::Header::new_gnu();
        header.set_size(member.len() as u64);
        header.set_mode(0o600);
        header.set_cksum();
        tar.append_data(
            &mut header,
            format!("Takeout/Fit/All Data/{name}.json"),
            member.as_slice(),
        )
        .unwrap();
    }
    let raw = tar.into_inner().unwrap();
    let zone = TimeZone::get(zone).unwrap();
    let mut driver = driver(
        "/synthetic/takeout.tar".into(),
        "synthetic".into(),
        zone.clone(),
    );
    driver.part_bytes = part_bytes;
    let parser = FitParser { zone };
    let mut carry = Carry::default();
    let mut out = Vec::new();
    group::read(
        Box::new(std::io::Cursor::new(raw.clone())),
        Sink.put(&raw).unwrap(),
        raw.len() as u64,
        ObservationFacts {
            size: Some(raw.len() as u64),
            changed: true,
            ..Default::default()
        },
        Arc::new(AtomicBool::new(false)),
        vec![group::Route {
            class: Class::Open,
            driver,
            cursor: Value::Null,
        }],
        &Sink,
        &mut |_, acquired| {
            let Some(material) = acquired?.material else {
                return Ok(true);
            };
            let context = RecordContext {
                manifest: &material.manifest,
                member: material.manifest["members"].get(0),
            };
            for &(start, end) in &material.records {
                let bytes = &material.bytes[start..end];
                let (mut events, next) = parser.parse(&bytes, &carry, &context).unwrap();
                carry = next;
                assert_eq!(events.len(), 1);
                out.push((bytes.to_vec(), events.remove(0)));
            }
            Ok(true)
        },
    )
    .unwrap();
    out
}

#[devkit::test]
fn points_make_one_record_per_data_type_and_local_day() {
    let exported = vec![
        // 23:30 on 1 January in Warsaw, 00:30 on 2 January.
        point(
            "synthetic.steps",
            JAN_1 + 22 * HOUR + HOUR / 2,
            json!([{"intVal": 5}]),
        ),
        point(
            "synthetic.steps",
            JAN_1 + 23 * HOUR + HOUR / 2,
            json!([{"value": {"intVal": 9007199254740993_i64}}]),
        ),
        point(
            "synthetic.steps",
            JAN_1 + 30 * HOUR,
            json!([{"intVal": -3}]),
        ),
        point(
            "synthetic.steps",
            JAN_1 + 31 * HOUR,
            json!([{"fpVal": 2.5}]),
        ),
        point(
            "synthetic.location",
            JAN_1 + 32 * HOUR,
            json!([{"fpVal": 52.25}, {"stringVal": "synthetic"}, {"mapVal": [{"key": "k", "value": {"fpVal": 1.0}}]}]),
        ),
        point(
            "synthetic.location",
            JAN_1 + 33 * HOUR,
            json!([{"fpVal": 52.5}, {"stringVal": "synthetic"}]),
        ),
    ];
    let document = json!({"Data Source": "derived:synthetic", "Data Points": exported});
    // A small part size forces several parts; a day still stays one record.
    let records = read(
        &[("derived_steps_synthetic", &document)],
        "Europe/Warsaw",
        200,
    );
    let rows: Vec<Value> = records
        .iter()
        .map(|(_, record)| serde_json::to_value(record).unwrap())
        .collect();
    assert_eq!(
        rows,
        vec![
            json!({"provider": "google-fit", "category": "synthetic.steps",
                "native_id": "derived_steps_synthetic:2026-01-01:0",
                "start": "2026-01-01T22:30:00Z", "end": "2026-01-01T22:31:00Z", "offset_minutes": 60,
                "values": {"day": "2026-01-01", "zone": "Europe/Warsaw", "run": 0, "points": 1,
                    "fields": [{"count": 1, "sum": 5.0, "min": 5, "max": 5}]}}),
            json!({"provider": "google-fit", "category": "synthetic.steps",
                "native_id": "derived_steps_synthetic:2026-01-02:0",
                "start": "2026-01-01T23:30:00Z", "end": "2026-01-02T07:01:00Z", "offset_minutes": 60,
                "values": {"day": "2026-01-02", "zone": "Europe/Warsaw", "run": 0, "points": 3,
                    "fields": [{"count": 3, "sum": 9007199254740993_f64 - 3.0 + 2.5,
                        "min": -3, "max": 9007199254740993_i64}]}}),
            json!({"provider": "google-fit", "category": "synthetic.location",
                "native_id": "derived_steps_synthetic:2026-01-02:0",
                "start": "2026-01-02T08:00:00Z", "end": "2026-01-02T09:01:00Z", "offset_minutes": 60,
                "values": {"day": "2026-01-02", "zone": "Europe/Warsaw", "run": 0, "points": 2,
                    "fields": [
                        {"count": 2, "sum": 104.75, "min": 52.25, "max": 52.5},
                        {"count": 0, "sum": 0.0, "min": null, "max": null},
                        {"count": 0, "sum": 0.0, "min": null, "max": null}]}}),
        ]
    );
    // The same points in UTC fall on other days.
    assert_eq!(
        read_days(&document, "UTC"),
        ["2026-01-01", "2026-01-02", "2026-01-02"]
    );
    // Reading the records' points back gives every point exactly as exported.
    let back: Vec<Value> = records
        .iter()
        .flat_map(|(bytes, _)| points(bytes).map(|p| Value::Object(p.unwrap())))
        .collect();
    assert_eq!(back, document["Data Points"].as_array().unwrap().clone());
}

fn read_days(document: &Value, zone: &str) -> Vec<String> {
    read(&[("synthetic", document)], zone, 1 << 20)
        .into_iter()
        .map(|(_, record)| {
            serde_json::to_value(record).unwrap()["values"]["day"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[devkit::test]
fn a_point_that_cannot_be_read_fails_alone() {
    let document = json!({"Data Points": [
        point("synthetic.steps", JAN_1, json!([{"intVal": 1}])),
        point("synthetic.steps", JAN_1 + HOUR, json!([{"intVal": "2"}])),
        point("synthetic.steps", JAN_1 + 2 * HOUR, json!([{"intVal": 3}])),
    ]});
    let member = serde_json::to_vec(&document).unwrap();
    let ranges = crate::drivers::export_archive::Framing::JsonArray("/Data Points")
        .frame(&member, true)
        .0;
    let zone = TimeZone::UTC;
    let join = driver("/synthetic".into(), "synthetic".into(), zone.clone())
        .join
        .unwrap();
    let keys: Vec<Option<String>> = ranges
        .iter()
        .map(|&(s, e)| (join.0)(&member[s..e]))
        .collect();
    assert_eq!(
        keys,
        [
            Some("synthetic.steps\n2026-01-01".to_owned()),
            None,
            Some("synthetic.steps\n2026-01-01".to_owned())
        ]
    );
    let parser = FitParser { zone };
    let (s, e) = ranges[1];
    let error = parser
        .parse(&&member[s..e], &Carry::default(), &RecordContext::none())
        .unwrap_err();
    assert!(error.to_string().contains("not a number"), "{error}");
}

#[devkit::test]
fn points_without_a_start_are_undated_and_points_of_two_days_are_refused() {
    let zone = TimeZone::UTC;
    let parser = FitParser { zone };
    let undated = serde_json::to_vec(
        &json!({"dataTypeName": "synthetic.steps", "fitValue": [{"intVal": 4}]}),
    )
    .unwrap();
    let (events, _) = parser
        .parse(
            &undated.as_slice(),
            &Carry::default(),
            &RecordContext::none(),
        )
        .unwrap();
    let row = serde_json::to_value(&events[0]).unwrap();
    assert_eq!(row["native_id"], ":undated:0");
    assert_eq!(row["start"], Value::Null);
    assert_eq!(row["values"]["day"], Value::Null);
    let mut two_days = serde_json::to_vec(&point("synthetic.steps", JAN_1, json!([]))).unwrap();
    two_days.extend(
        serde_json::to_vec(&point("synthetic.steps", JAN_1 + 24 * HOUR, json!([]))).unwrap(),
    );
    assert!(
        parser
            .parse(
                &two_days.as_slice(),
                &Carry::default(),
                &RecordContext::none()
            )
            .is_err()
    );
}

#[devkit::test]
fn interleaved_and_unsorted_days_keep_every_point_in_numbered_runs() {
    // Two days of one data type, interleaved and out of time order, so each
    // day arrives in several runs; a second file repeats the days.
    let days = [JAN_1, JAN_1 + 24 * HOUR];
    let order = [1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0];
    let exported: Vec<Value> = order
        .iter()
        .enumerate()
        .map(|(i, &day)| {
            let at = days[day] + (20 - i as i64) * HOUR / 2;
            point("synthetic.steps", at, json!([{"intVal": i * 10 + day}]))
        })
        .collect();
    let first = json!({"Data Points": exported});
    let second = json!({"Data Points": exported[..3]});
    let records = read(&[("a", &first), ("b", &second)], "UTC", 300);
    let rows: Vec<Value> = records
        .iter()
        .map(|(_, record)| serde_json::to_value(record).unwrap())
        .collect();
    let ids: Vec<&str> = rows
        .iter()
        .map(|row| row["native_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        [
            "a:2026-01-02:0",
            "a:2026-01-01:0",
            "a:2026-01-02:1",
            "a:2026-01-01:1",
            "a:2026-01-02:2",
            "a:2026-01-01:2",
            "a:2026-01-02:3",
            "a:2026-01-01:3",
            "b:2026-01-02:0",
            "b:2026-01-01:0",
        ]
    );
    // Each day's runs add up to exactly its points.
    for (file, points_of) in [("a", &exported[..]), ("b", &exported[..3])] {
        for (day, name) in [(0, "2026-01-01"), (1, "2026-01-02")] {
            let values: Vec<i64> = points_of
                .iter()
                .filter(|p| p["fitValue"][0]["intVal"].as_i64().unwrap() % 10 == day)
                .map(|p| p["fitValue"][0]["intVal"].as_i64().unwrap())
                .collect();
            let runs: Vec<&Value> = rows
                .iter()
                .filter(|row| {
                    row["native_id"]
                        .as_str()
                        .unwrap()
                        .starts_with(&format!("{file}:{name}:"))
                })
                .map(|row| &row["values"])
                .collect();
            let count: u64 = runs.iter().map(|v| v["points"].as_u64().unwrap()).sum();
            let sum: f64 = runs
                .iter()
                .map(|v| v["fields"][0]["sum"].as_f64().unwrap())
                .sum();
            let min = runs
                .iter()
                .map(|v| v["fields"][0]["min"].as_i64().unwrap())
                .min();
            let max = runs
                .iter()
                .map(|v| v["fields"][0]["max"].as_i64().unwrap())
                .max();
            assert_eq!(count, values.len() as u64, "{file} {name}");
            assert_eq!(sum, values.iter().sum::<i64>() as f64, "{file} {name}");
            assert_eq!(min, values.iter().copied().min(), "{file} {name}");
            assert_eq!(max, values.iter().copied().max(), "{file} {name}");
        }
    }
    // A split day is the one with a run 1.
    let split: Vec<&str> = rows
        .iter()
        .filter(|row| row["values"]["run"] == 1)
        .map(|row| row["values"]["day"].as_str().unwrap())
        .collect();
    assert_eq!(split, ["2026-01-02", "2026-01-01"]);
    // Every point reads back, in file order, exactly as exported.
    let back: Vec<Value> = records
        .iter()
        .flat_map(|(bytes, _)| points(bytes).map(|p| Value::Object(p.unwrap())))
        .collect();
    let mut all = exported.clone();
    all.extend_from_slice(&exported[..3]);
    assert_eq!(back, all);
}

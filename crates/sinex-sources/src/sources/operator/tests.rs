use super::*;
use crate::sources::fields;
use serde_json::json;

const LOG: &[u8] = b"---\nid: logs.raw-log\n---\n\n- **2025-04-15 23:33:35** hello, world\n- **2025-04-15 23:34:10** another one\n  continued here\n- **2025-04-15 23:40:13** [mood] fine\n";

#[test]
fn entries_with_continuations() {
    let (records, covered) = frame_log(LOG);
    assert_eq!(records.len(), 3);
    assert_eq!(covered, LOG.len());
    let p = RawLogParser {
        zone: jiff::tz::TimeZone::get("Europe/Warsaw").unwrap(),
    };
    let (a, b) = records[1];
    let (out, ()) = p
        .parse(&&LOG[a..b], &(), &crate::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].text, "another one\n  continued here");
    assert_eq!(
        out[0].written_at,
        fields::rfc3339("2025-04-15T21:34:10Z").ok()
    );
    let (out, ()) = p
        .parse(
            &&b"- **SPECIAL:** [[note]].\n"[..],
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].written_at, None);
    assert_eq!(out[0].label.as_deref(), Some("SPECIAL"));
    assert_eq!(out[0].text, "[[note]].");
    let (out, ()) = p
        .parse(
            &&b"- **2025-08-18** a day\n"[..],
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        (out[0].written_at, out[0].label.as_deref()),
        (None, Some("2025-08-18"))
    );
    let (out, ()) = p
        .parse(
            &&b"- **2025-08-03:** a day\n"[..],
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(out[0].label.as_deref(), Some("2025-08-03"));
    let (out, ()) = p
        .parse(
            &&b"- **2025-07-30 21:57** minutes\n"[..],
            &(),
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(
        out[0].written_at,
        fields::rfc3339("2025-07-30T19:57:00Z").ok()
    );
}

#[test]
fn a_half_written_line_waits() {
    let cut = &LOG[..LOG.len() - 5];
    let (records, covered) = frame_log(cut);
    assert_eq!(records.len(), 2);
    assert!(covered < cut.len());
}

#[test]
fn steering_rows_take_their_tables_time() {
    let p = SteeringParser {
        table: "commitments",
    };
    let row = json!({"rowid": 1, "id": "c1", "text": "x", "created_at": "2026-10-12T08:00:00+02:00",
        "status": "open", "forecast_p": 0.7});
    let (out, ()) = p
        .parse(row.as_object().unwrap(), &(), &crate::RecordContext::none())
        .unwrap();
    use sinex_model::EventPayload;
    assert_eq!(
        out[0].ts_orig().at,
        fields::rfc3339("2026-10-12T06:00:00Z").ok()
    );
    let wire = serde_json::to_value(&out[0]).unwrap();
    assert!(!wire["values"].as_object().unwrap().contains_key("rowid"));
    let mut expected = row.clone();
    expected.as_object_mut().unwrap().remove("rowid");
    assert_eq!(wire["values"], expected);
    assert!(steering("reviews").is_some());
    assert!(steering("nope").is_none());
    assert!(steering("").is_none());
    assert_eq!(
        STEERING
            .iter()
            .map(|s| s.parser_version)
            .collect::<Vec<_>>(),
        vec![2; 3]
    );
}

#[test]
fn unknown_steering_shapes_are_accounted_without_losing_later_coordinates() {
    use crate::{DrainInput, Limits, Source};
    use sinex_model::{MaterialId, Origin, ReadingOrder, RecordLocation, RecordOutcome};
    let bytes = b"{\"rowid\":1,\"id\":\"c1\",\"status\":\"open\",\"text\":\"before\"}\n{\"rowid\":2,\"id\":\"unknown\",\"future_column\":true}\n{\"rowid\":3,\"id\":\"c3\",\"forecast_p\":null,\"text\":\"after\"}\n";
    let (records, covered) = crate::drivers::append_file::lines(bytes);
    assert_eq!(covered, bytes.len());
    let material = MaterialId::new();
    let outputs = steering("commitments")
        .unwrap()
        .drain(DrainInput {
            source: "operator.steering",
            class: Class::Open,
            material,
            bytes: bytes.as_slice().into(),
            records: &records,
            carry: Value::Null,
            reading_order: &|at| ReadingOrder(at as i64),
            limits: Limits::default(),
            manifest: &Value::Null,
        })
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        (outputs[0].events, outputs[0].failures, outputs[0].end),
        (2, 1, bytes.len())
    );
    for (i, outcome) in outputs[0].outcomes.iter().enumerate() {
        let location = RecordLocation {
            material,
            anchor: records[i].0 as u64,
        };
        match outcome {
            RecordOutcome::Failure {
                location: actual,
                reason,
            } => {
                assert_eq!(i, 1);
                assert_eq!(*actual, location);
                assert!(reason.contains("future_column"));
            }
            RecordOutcome::Events {
                location: actual,
                events,
            } => {
                assert_ne!(i, 1);
                assert_eq!(*actual, Some(location));
                assert_eq!(events.len(), 1);
                assert_eq!(
                    events[0].origin,
                    Origin::Record {
                        location,
                        ordinal: 0
                    }
                );
                assert_eq!(
                    events[0].record_hash,
                    *blake3::hash(&bytes[records[i].0..records[i].1]).as_bytes()
                );
                assert_eq!(events[0].reading_order, ReadingOrder(records[i].0 as i64));
                assert!(
                    !events[0].payload["values"]
                        .as_object()
                        .unwrap()
                        .contains_key("rowid")
                );
            }
        }
    }
}

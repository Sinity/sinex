use super::*;
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
    let (out, ()) = p.parse(&&LOG[a..b], &()).unwrap();
    assert_eq!(out[0].text, "another one\n  continued here");
    assert_eq!(
        out[0].written_at,
        fields::rfc3339("2025-04-15T21:34:10Z").ok()
    );
    let (out, ()) = p.parse(&&b"- **SPECIAL:** [[note]].\n"[..], &()).unwrap();
    assert_eq!(out[0].written_at, None);
    assert_eq!(out[0].label.as_deref(), Some("SPECIAL"));
    assert_eq!(out[0].text, "[[note]].");
    let (out, ()) = p.parse(&&b"- **2025-08-18** a day\n"[..], &()).unwrap();
    assert_eq!(
        (out[0].written_at, out[0].label.as_deref()),
        (None, Some("2025-08-18"))
    );
    let (out, ()) = p.parse(&&b"- **2025-08-03:** a day\n"[..], &()).unwrap();
    assert_eq!(out[0].label.as_deref(), Some("2025-08-03"));
    let (out, ()) = p
        .parse(&&b"- **2025-07-30 21:57** minutes\n"[..], &())
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
    let (out, ()) = p.parse(row.as_object().unwrap(), &()).unwrap();
    assert_eq!(out[0].at, fields::rfc3339("2026-10-12T06:00:00Z").ok());
    assert!(!out[0].values.contains_key("rowid"));
    assert!(steering("reviews").is_some());
    assert!(steering("nope").is_none());
}

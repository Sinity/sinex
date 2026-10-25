use super::*;
use serde_json::json;

/// Frame a cast with `cast_spans` and parse every record.
fn run(lines: &[&str]) -> Vec<CastEvent> {
    let bytes: String = lines.iter().map(|l| format!("{l}\n")).collect();
    let manifest = json!({"path": "/a/2026/10/04/sess-1/session.cast"});
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let (records, consumed) = cast_spans(bytes.as_bytes());
    assert_eq!(consumed, bytes.len());
    let mut carry = Carry::default();
    let mut out = Vec::new();
    for (s, e) in records {
        let record = &bytes.as_bytes()[s..e];
        let (events, next) = CastParser.parse(&record, &carry, &context).unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

#[devkit::test]
fn a_cast_is_one_session_and_spans_with_v3_intervals_accumulating() {
    let out = run(&[
        r#"{"version":3,"term":{"cols":80,"rows":24},"timestamp":1791129020,"command":"zsh"}"#,
        r#"[0.5, "i", "ls\r"]"#,
        r#"[0.25, "o", "\u001b[1;32mfile\u001b[0m\r\n"]"#,
        r#"[0.25, "r", "100x30"]"#,
        r#"[1.0, "o", "after"]"#,
    ]);
    assert_eq!(out.len(), 3, "header, a span ended by the resize, the rest");
    let CastEvent::Session(s) = &out[0] else {
        panic!()
    };
    assert_eq!(
        (s.session.as_str(), s.cols, s.rows),
        ("sess-1", Some(80), Some(24))
    );
    let CastEvent::Output(o) = &out[1] else {
        panic!()
    };
    assert_eq!(o.started_at.unix_ns(), 1_791_129_020_500_000_000);
    assert_eq!(o.ended_at.unix_ns(), 1_791_129_021_000_000_000);
    assert_eq!(
        (o.events, o.input.as_str(), o.output.as_str()),
        (3, "ls", "file\n")
    );
    assert_eq!(o.resize.as_deref(), Some("100x30"));
    let CastEvent::Output(o) = &out[2] else {
        panic!()
    };
    assert_eq!(o.started_at.unix_ns(), 1_791_129_022_000_000_000);
    assert_eq!(o.output, "after");
}

#[devkit::test]
fn v2_offsets_do_not_accumulate() {
    let out = run(&[
        r#"{"version":2,"width":80,"height":24,"timestamp":100,"env":{"SINNIX_CAPTURE_SESSION_ID":"x"}}"#,
        r#"[1.0, "o", "a"]"#,
        r#"[1.5, "o", "b"]"#,
    ]);
    let CastEvent::Output(o) = &out[1] else {
        panic!()
    };
    assert_eq!(
        (o.started_at.unix_ns(), o.ended_at.unix_ns()),
        (101_000_000_000, 101_500_000_000)
    );
    assert_eq!((o.session.as_str(), o.output.as_str()), ("x", "ab"));
}

#[devkit::test]
fn spans_are_bounded_and_cover_every_complete_line() {
    let line = format!("[0.1, \"o\", \"{}\"]\n", "x".repeat(1000));
    let mut cast = String::from("{\"version\":3,\"timestamp\":1}\n");
    for _ in 0..300 {
        cast.push_str(&line);
    }
    cast.push_str("[0.1, \"o\", \"partial");
    let (records, consumed) = cast_spans(cast.as_bytes());
    assert!(
        records
            .iter()
            .skip(1)
            .all(|(s, e)| e - s <= SPAN_BYTES + line.len())
    );
    assert!(records.len() > 300 * line.len() / SPAN_BYTES);
    assert_eq!(records.last().unwrap().1, consumed);
    assert!(consumed < cast.len(), "the partial last line waits");
    let covered: usize = records.iter().map(|(s, e)| e - s).sum();
    assert_eq!(covered, consumed);
}

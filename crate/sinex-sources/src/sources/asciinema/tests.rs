use super::*;
use serde_json::json;

fn run(lines: &[&str]) -> Vec<CastEvent> {
    let manifest = json!({"path": "/a/2026/10/04/sess-1/session.cast"});
    let context = RecordContext {
        manifest: &manifest,
        member: None,
    };
    let mut carry = Carry::default();
    let mut out = Vec::new();
    for line in lines {
        let (events, next) = CastParser
            .parse(&line.as_bytes(), &carry, &context)
            .unwrap();
        out.extend(events);
        carry = next;
    }
    out
}

#[test]
fn v3_intervals_accumulate_and_v2_offsets_do_not() {
    let out = run(&[
        r#"{"version":3,"term":{"cols":80,"rows":24},"timestamp":1791129020,"command":"zsh"}"#,
        r#"[0.5, "i", "z"]"#,
        r#"[0.25, "o", "z"]"#,
    ]);
    let CastEvent::Session(s) = &out[0] else {
        panic!()
    };
    assert_eq!(
        (s.session.as_str(), s.cols, s.rows),
        ("sess-1", Some(80), Some(24))
    );
    let CastEvent::Output(o) = &out[2] else {
        panic!()
    };
    assert_eq!(o.at.unix_ns(), 1_791_129_020_750_000_000);
    assert_eq!((o.code.as_str(), o.data.as_str()), ("o", "z"));
    let out = run(&[
        r#"{"version":2,"width":80,"height":24,"timestamp":100,"env":{"SINNIX_CAPTURE_SESSION_ID":"x"}}"#,
        r#"[1.0, "o", "a"]"#,
        r#"[1.5, "o", "b"]"#,
    ]);
    let CastEvent::Output(o) = &out[2] else {
        panic!()
    };
    assert_eq!(o.at.unix_ns(), 101_500_000_000);
    assert_eq!(o.session, "x");
}

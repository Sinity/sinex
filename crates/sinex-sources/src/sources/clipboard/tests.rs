use super::*;
use sinex_model::Timestamp;

fn parse(line: &[u8]) -> ClipboardCopied {
    let rec: Map<String, Value> = serde_json::from_slice(&live_stream::frame(
        Timestamp::from_unix_ns(1).unwrap(),
        line,
    ))
    .unwrap();
    ClipboardParser
        .parse(&rec, &(), &crate::RecordContext::none())
        .unwrap()
        .0
        .remove(0)
}

#[test]
fn text_is_recorded_with_its_hash() {
    let types = vec![
        "text/plain;charset=utf-8".to_owned(),
        "UTF8_STRING".to_owned(),
    ];
    let c = parse(&frame("clipboard", &types, "héllo".as_bytes()));
    assert_eq!(c.text.as_deref(), Some("héllo"));
    assert_eq!(c.bytes, 6);
    assert_eq!(c.mime_type.as_deref(), Some("text/plain;charset=utf-8"));
    assert_eq!(
        c.content_hash.unwrap(),
        blake3::hash("héllo".as_bytes()).to_hex().to_string()
    );
    assert!(!c.secret);
}

#[test]
fn binary_content_stays_out_of_the_event() {
    let c = parse(&frame(
        "primary",
        &["image/png".to_owned()],
        &[0x89, b'P', 0xff],
    ));
    assert!(c.text.is_none());
    assert_eq!(c.bytes, 3);
    assert_eq!(c.selection, "primary");
}

#[test]
fn secrets_are_not_read() {
    let line = frame(
        "clipboard",
        &["text/plain".to_owned(), SECRET_HINT.to_owned()],
        b"hunter2",
    );
    assert!(!String::from_utf8_lossy(&line).contains("hunter2"));
    let c = parse(&line);
    assert!(c.secret && c.text.is_none() && c.bytes == 0 && c.content_hash.is_none());
}

use super::*;

#[test]
fn a_message_the_stream_does_not_hold_is_gone() {
    assert!(missing(&stream::RawMessageError::new(
        stream::RawMessageErrorKind::NoMessageFound
    )));
}

#[test]
fn other_lookup_failures_are_failures() {
    // A server error that merely says "no message found" in its text is not
    // taken as absence: only the typed kind is.
    let worded: jetstream::Error = serde_json::from_value(serde_json::json!({
        "code": 500,
        "err_code": 10057,
        "description": "no message found",
    }))
    .unwrap();
    for kind in [
        stream::RawMessageErrorKind::JetStream(worded),
        stream::RawMessageErrorKind::Other,
        stream::RawMessageErrorKind::InvalidSubject,
    ] {
        assert!(!missing(&stream::RawMessageError::new(kind)));
    }
}

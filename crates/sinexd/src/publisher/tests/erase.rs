use super::*;

fn jetstream_error(code: u64, description: &str) -> stream::DeleteMessageError {
    let error: jetstream::Error = serde_json::from_value(serde_json::json!({
        "code": 404,
        "err_code": code,
        "description": description,
    }))
    .unwrap();
    stream::DeleteMessageError::new(stream::DeleteMessageErrorKind::JetStream(error))
}

#[test]
fn erasing_a_message_the_stream_no_longer_holds_is_done() {
    assert!(already_erased(&jetstream_error(10037, "no message found")));
    // The decision follows the server's error code, not its wording.
    assert!(already_erased(&jetstream_error(10037, "message is gone")));
}

#[test]
fn other_delete_failures_are_failures() {
    assert!(!already_erased(&jetstream_error(10059, "no message found")));
    assert!(!already_erased(&stream::DeleteMessageError::new(
        stream::DeleteMessageErrorKind::TimedOut
    )));
    assert!(!already_erased(&stream::DeleteMessageError::new(
        stream::DeleteMessageErrorKind::Request
    )));
}

use super::{Error, Writer};

#[test]
fn only_timeouts_make_session_work_retry() {
    assert!(Writer::session_retryable(&Error::Deadline));
    // The reason text of a timeout must not make a validation error retryable.
    for error in [
        Error::Invalid("session computation turn exceeded 3 seconds; dirty keys kept for retry"),
        Error::Invalid("session exact parent representation exceeded"),
        Error::ProjectionUnavailable("shell session reconciliation pending or failed"),
        Error::Stopped,
    ] {
        assert!(!Writer::session_retryable(&error));
    }
}

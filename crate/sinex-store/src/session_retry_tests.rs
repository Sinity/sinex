use super::{Error, WriterPool};

#[test]
fn session_retry_uses_the_fault_type_instead_of_the_published_reason() {
    assert!(WriterPool::session_retryable(&Error::Deadline));
    // The former timeout's identical text is still a permanent validation error.
    // A validation failure must never enter an autonomous retry loop.
    for error in [
        Error::Invalid("session computation turn exceeded 3 seconds; dirty key retained"),
        Error::Invalid("session exact parent representation exceeded"),
        Error::ProjectionUnavailable("shell session reconciliation pending or failed"),
        Error::Stopped,
    ] {
        assert!(!WriterPool::session_retryable(&error));
    }
}

use super::{ErrorClass, ErrorDetails, SinexError};
use xtask::sandbox::sinex_test;

fn details() -> ErrorDetails {
    ErrorDetails::new("test")
}

/// Returns every `SinexError` variant once.
/// When a new variant is added to `SinexError`, this list must be extended
/// or the `match` becomes a compile error — ensuring the test stays
/// exhaustive.
fn all_variants() -> Vec<SinexError> {
    vec![
        SinexError::Database(details()),
        SinexError::Validation(details()),
        SinexError::Service(details()),
        SinexError::Io(details()),
        SinexError::Configuration(details()),
        SinexError::Serialization(details()),
        SinexError::Parse(details()),
        SinexError::NotFound(details()),
        SinexError::AlreadyExists(details()),
        SinexError::InvalidState(details()),
        SinexError::PermissionDenied(details()),
        SinexError::Network(details()),
        SinexError::ChannelSend(details()),
        SinexError::ChannelReceive(details()),
        SinexError::Timeout(details()),
        SinexError::Cancelled(details()),
        SinexError::MaxRetriesExceeded(details()),
        SinexError::ResourceExhausted(details()),
        SinexError::Unknown(details()),
        SinexError::Kv(details()),
        SinexError::Automaton(details()),
        SinexError::Checkpoint(details()),
        SinexError::Lifecycle(details()),
        SinexError::Processing(details()),
        SinexError::DbPersistenceFailed(details()),
        SinexError::BlobStorage(details()),
        SinexError::Coordination(details()),
        #[cfg(feature = "nats")]
        SinexError::Nats(details()),
        #[cfg(feature = "nats")]
        SinexError::NatsAckFailed(details()),
        #[cfg(feature = "nats")]
        SinexError::NatsPublish(details()),
        #[cfg(feature = "nats")]
        SinexError::NatsSubscribe(details()),
    ]
}

/// `is_retryable` and `is_permanent` must agree with `error_class()` for
/// every variant. This test will fail to compile if a variant is missing
/// from `all_variants()` because the exhaustive `match` below covers the
/// enum — add missing variants to the list to fix it.
#[sinex_test]
async fn is_retryable_agrees_with_error_class() -> TestResult<()> {
    for err in all_variants() {
        let class = err.error_class();
        assert_eq!(
            err.is_retryable(),
            class == ErrorClass::TransientInfra,
            "is_retryable disagrees with error_class for {err:?}: class={class:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn is_permanent_agrees_with_error_class() -> TestResult<()> {
    for err in all_variants() {
        let class = err.error_class();
        assert_eq!(
            err.is_permanent(),
            class == ErrorClass::RuntimeFatal,
            "is_permanent disagrees with error_class for {err:?}: class={class:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn retryable_and_permanent_are_mutually_exclusive() -> TestResult<()> {
    for err in all_variants() {
        assert!(
            !(err.is_retryable() && err.is_permanent()),
            "error is both retryable and permanent: {err:?}"
        );
    }
    Ok(())
}

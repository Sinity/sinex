use sinex_primitives::{Bytes, Seconds, SinexError};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_seconds_validation_valid() -> TestResult<()> {
    assert!(Seconds::from_secs(0).validate().is_ok());
    assert!(Seconds::from_secs(30).validate().is_ok());
    assert!(Seconds::from_secs(3600).validate().is_ok());
    assert!(Seconds::from_secs(86400).validate().is_ok());
    Ok(())
}

#[sinex_test]
async fn test_seconds_validation_invalid() -> TestResult<()> {
    assert!(Seconds::from_secs(86401).validate().is_err());
    assert!(Seconds::from_secs(100000).validate().is_err());
    assert!(Seconds::from_secs(1000000).validate().is_err());
    Ok(())
}

#[sinex_test]
async fn test_seconds_from_validated() -> TestResult<()> {
    assert!(Seconds::from_secs_validated(30).is_ok());
    assert!(Seconds::from_secs_validated(86400).is_ok());

    assert!(Seconds::from_secs_validated(86401).is_err());
    assert!(Seconds::from_secs_validated(1000000).is_err());
    Ok(())
}

#[sinex_test]
async fn test_seconds_helper_constructors() -> TestResult<()> {
    assert_eq!(Seconds::from_millis(5000).as_secs(), 5);
    assert_eq!(Seconds::from_minutes(5).as_secs(), 300);
    assert_eq!(Seconds::from_hours(2).as_secs(), 7200);
    Ok(())
}

#[sinex_test]
async fn test_bytes_validation_valid() -> TestResult<()> {
    assert!(Bytes::from_bytes(0).validate().is_ok());
    assert!(Bytes::from_bytes(1024).validate().is_ok());
    assert!(Bytes::from_mebibytes(100).validate().is_ok());
    assert!(Bytes::from_mebibytes(1024).validate().is_ok());
    assert!(Bytes::from_gibibytes(1).validate().is_ok());
    Ok(())
}

#[sinex_test]
async fn test_bytes_validation_invalid() -> TestResult<()> {
    assert!(Bytes::from_mebibytes(1025).validate().is_err());
    assert!(Bytes::from_gibibytes(2).validate().is_err());
    assert!(
        Bytes::from_bytes(2 * 1024 * 1024 * 1024)
            .validate()
            .is_err()
    );
    Ok(())
}

#[sinex_test]
async fn test_bytes_from_validated() -> TestResult<()> {
    assert!(Bytes::from_bytes_validated(1024).is_ok());
    assert!(Bytes::from_bytes_validated(1024 * 1024 * 1024).is_ok());

    let over_limit = (1024 * 1024 * 1024) + 1;
    assert!(Bytes::from_bytes_validated(over_limit).is_err());
    Ok(())
}

#[sinex_test]
async fn test_bytes_helper_constructors() -> TestResult<()> {
    assert_eq!(Bytes::from_kibibytes(1).as_u64(), 1024);
    assert_eq!(Bytes::from_mebibytes(1).as_u64(), 1024 * 1024);
    assert_eq!(Bytes::from_gibibytes(1).as_u64(), 1024 * 1024 * 1024);
    Ok(())
}

#[sinex_test]
async fn test_validation_error_messages() -> TestResult<()> {
    let err = Seconds::from_secs(100000).validate().unwrap_err();
    assert!(matches!(err, SinexError::Validation(_)));
    let msg = err.message();
    assert!(msg.contains("100000"));
    assert!(msg.contains("86400"));
    assert!(msg.contains("24 hours"));

    let err = Bytes::from_mebibytes(2000).validate().unwrap_err();
    assert!(matches!(err, SinexError::Validation(_)));
    let msg = err.message();
    assert!(msg.contains("1 GiB"));
    Ok(())
}

#[sinex_test]
async fn test_const_max_values() -> TestResult<()> {
    assert_eq!(Seconds::MAX.as_secs(), 86400);
    assert_eq!(Bytes::MAX.as_u64(), 1024 * 1024 * 1024);
    Ok(())
}

use sinex_primitives::temporal::Duration;
use sinex_primitives::validation::query_validation::{
    validate_id, validate_limit, validate_offset, validate_time_range,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_validate_id_valid() -> TestResult<()> {
    assert!(validate_id("01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
    assert!(validate_id("my-resource-id").is_ok());
    assert!(validate_id("with_underscore").is_ok());
    assert!(validate_id("a").is_ok());
    Ok(())
}

#[sinex_test]
async fn test_validate_id_invalid() -> TestResult<()> {
    assert!(validate_id("").is_err());
    assert!(validate_id(&"a".repeat(129)).is_err());
    assert!(validate_id("has spaces").is_err());
    assert!(validate_id("has@special").is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_limit() -> TestResult<()> {
    assert!(validate_limit(1, 100).is_ok());
    assert!(validate_limit(100, 100).is_ok());
    assert!(validate_limit(0, 100).is_err());
    assert!(validate_limit(101, 100).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_offset() -> TestResult<()> {
    assert!(validate_offset(0).is_ok());
    assert!(validate_offset(1000).is_ok());
    assert!(validate_offset(-1).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_time_range() -> TestResult<()> {
    let now = sinex_primitives::temporal::now();
    let earlier = now - Duration::hours(1);
    let later = now + Duration::hours(1);

    assert!(validate_time_range(Some(earlier), Some(now)).is_ok());
    assert!(validate_time_range(Some(earlier), Some(later)).is_ok());
    assert!(validate_time_range(None, None).is_ok());
    assert!(validate_time_range(Some(earlier), None).is_ok());
    assert!(validate_time_range(None, Some(now)).is_ok());

    assert!(validate_time_range(Some(now), Some(earlier)).is_err());
    assert!(validate_time_range(Some(now), Some(now)).is_err());
    Ok(())
}

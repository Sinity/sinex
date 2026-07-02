use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_parse_pipeline_concurrency_accepts_positive_usize() -> TestResult<()> {
    assert_eq!(parse_pipeline_concurrency("12")?, 12);
    Ok(())
}

#[sinex_test]
async fn test_parse_pipeline_concurrency_rejects_invalid_number() -> TestResult<()> {
    let error = parse_pipeline_concurrency("abc").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("invalid SINEX_TEST_PIPELINE_CONCURRENCY value 'abc'"));
    Ok(())
}

#[sinex_test]
async fn test_parse_pipeline_concurrency_rejects_zero() -> TestResult<()> {
    let error = parse_pipeline_concurrency("0").unwrap_err();
    let rendered = format!("{error:#}");
    assert!(rendered.contains("must be greater than zero"));
    Ok(())
}

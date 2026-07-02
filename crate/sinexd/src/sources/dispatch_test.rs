use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn test_dispatch_returns_error_for_unknown_source() -> xtask::sandbox::TestResult<()> {
    let dispatch = default_parser_dispatch();
    let result = dispatch("completely-unknown-source-xyz", b"data", None);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("unknown source_id 'completely-unknown-source-xyz'"),
        "got: {err}"
    );
    Ok(())
}

#[sinex_test]
async fn test_parser_dispatch_records_calls() -> xtask::sandbox::TestResult<()> {
    let (dispatch, calls) = test_parser_dispatch();
    let result = dispatch("any-source", b"data", None);
    assert!(result.is_ok());
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "any-source");
    assert_eq!(calls[0].1, b"data");
    assert_eq!(calls[0].2, None);
    Ok(())
}

#[sinex_test]
async fn test_parser_dispatch_with_material_id() -> xtask::sandbox::TestResult<()> {
    let (dispatch, calls) = test_parser_dispatch();
    let material_id = Uuid::now_v7();
    let result = dispatch("weechat", b"some bytes", Some(material_id));
    assert!(result.is_ok());
    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].2, Some(material_id));
    Ok(())
}

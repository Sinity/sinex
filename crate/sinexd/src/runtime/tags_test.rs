use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_tag_name_construction() -> TestResult<()> {
    assert_eq!(
        tag_name("sys.mime", "text-markdown"),
        "sys.mime.text-markdown"
    );
    Ok(())
}

#[sinex_test]
async fn test_parent_prefix() -> TestResult<()> {
    assert_eq!(parent_prefix("sys.mime.text"), Some("sys.mime".to_string()));
    assert_eq!(parent_prefix("sys"), None);
    assert_eq!(parent_prefix("a.b.c.d"), Some("a.b.c".to_string()));
    Ok(())
}

#[sinex_test]
async fn test_prefix_pattern() -> TestResult<()> {
    assert_eq!(prefix_pattern("sys.mime"), "sys.mime.%");
    Ok(())
}

#[sinex_test]
async fn test_valid_tag_names() -> TestResult<()> {
    assert!(is_valid_tag_name("sys.source.screenshot"));
    assert!(is_valid_tag_name("inferred.file-type.rust"));
    assert!(!is_valid_tag_name(""));
    assert!(!is_valid_tag_name(".bad"));
    assert!(!is_valid_tag_name("bad."));
    Ok(())
}

use super::*;
use crate::sandbox::sinex_test;
use std::path::PathBuf;

#[sinex_test]
async fn test_should_trigger_rebuild() -> TestResult<()> {
    // Should trigger
    assert!(should_trigger_rebuild(&PathBuf::from("src/main.rs")));
    assert!(should_trigger_rebuild(&PathBuf::from("src/lib.rs")));
    assert!(should_trigger_rebuild(&PathBuf::from("src/foo/bar.rs")));
    assert!(should_trigger_rebuild(&PathBuf::from("Cargo.toml")));
    assert!(should_trigger_rebuild(&PathBuf::from("Cargo.lock")));

    // Should not trigger
    assert!(!should_trigger_rebuild(&PathBuf::from("target/debug/foo")));
    assert!(!should_trigger_rebuild(&PathBuf::from(".gitignore")));
    assert!(!should_trigger_rebuild(&PathBuf::from("README.md")));
    Ok(())
}

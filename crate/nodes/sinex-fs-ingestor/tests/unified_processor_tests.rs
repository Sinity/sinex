use sinex_core::types::Bytes;
use sinex_fs_ingestor::{FilesystemConfig, FilesystemProcessor};
use sinex_test_utils::prelude::*;

#[sinex_test]
async fn processor_initialization(ctx: TestContext) -> TestResult<()> {
    let config = FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(5),
        follow_symlinks: false,
        max_capture_bytes: Bytes::from_mebibytes(1),
    };

    let processor = FilesystemProcessor::with_config(config.clone());

    let configured = processor.config();
    assert_eq!(configured.watch_paths, config.watch_paths);
    assert_eq!(configured.max_depth, config.max_depth);
    assert_eq!(configured.follow_symlinks, config.follow_symlinks);

    Ok(())
}

#[sinex_test]
async fn config_validation(ctx: TestContext) -> TestResult<()> {
    let valid_config = FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(10),
        follow_symlinks: false,
        max_capture_bytes: Bytes::from_mebibytes(1),
    };
    assert!(valid_config.validate_config().is_ok());

    let invalid_config = FilesystemConfig {
        watch_paths: vec![],
        ..valid_config.clone()
    };
    assert!(invalid_config.validate_config().is_err());

    Ok(())
}

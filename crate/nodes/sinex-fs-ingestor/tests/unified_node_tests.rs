use sinex_fs_ingestor::{FilesystemConfig, FilesystemNode};
use sinex_primitives::Bytes;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn node_initialization(ctx: TestContext) -> TestResult<()> {
    let config = FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(5),
        follow_symlinks: false,
        max_capture_bytes: Bytes::from_mebibytes(1),
        ..Default::default()
    };

    let node = FilesystemNode::with_config(config.clone());

    let configured = node.config();
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
        ..Default::default()
    };
    assert!(valid_config.validate_config().is_ok());

    let invalid_config = FilesystemConfig {
        watch_paths: vec![],
        ..valid_config
    };
    assert!(invalid_config.validate_config().is_err());

    Ok(())
}

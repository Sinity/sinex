use sinex_fs_ingestor::FilesystemConfig;
use sinex_primitives::Bytes;
use xtask::sandbox::sinex_test;

fn base_config() -> FilesystemConfig {
    FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(10),
        follow_symlinks: false,
        max_capture_bytes: Bytes::from_mebibytes(8),
    }
}

#[sinex_test]
fn rejects_empty_watch_paths() -> TestResult<()> {
    let config = FilesystemConfig {
        watch_paths: vec![],
        ..base_config()
    };

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn enforces_max_depth_bounds() -> TestResult<()> {
    let zero_depth = FilesystemConfig {
        max_depth: Some(0),
        ..base_config()
    };
    assert!(zero_depth.validate_config().is_err());

    let too_large = FilesystemConfig {
        max_depth: Some(101),
        ..base_config()
    };
    assert!(too_large.validate_config().is_err());

    Ok(())
}

use sinex_fs_watcher::FilesystemConfig;
use sinex_test_utils::sinex_test;

fn base_config() -> FilesystemConfig {
    FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(10),
        follow_symlinks: false,
        max_capture_bytes: 8 * 1024 * 1024,
    }
}

#[sinex_test]
fn rejects_empty_watch_paths() -> color_eyre::eyre::Result<()> {
    let config = FilesystemConfig {
        watch_paths: vec![],
        ..base_config()
    };

    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn enforces_max_depth_bounds() -> color_eyre::eyre::Result<()> {
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

use sinex_fs_watcher::FilesystemConfig;
use sinex_test_utils::sinex_test;

fn base_config() -> FilesystemConfig {
    FilesystemConfig {
        watch_paths: vec!["/tmp/test".to_string()],
        max_depth: Some(10),
        follow_symlinks: false,
        batch_size: 100,
        processing_interval_ms: 1_000,
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

#[sinex_test]
fn enforces_batch_size_bounds() -> color_eyre::eyre::Result<()> {
    let too_small = FilesystemConfig {
        batch_size: 0,
        ..base_config()
    };
    assert!(too_small.validate_config().is_err());

    let too_large = FilesystemConfig {
        batch_size: 5_000,
        ..base_config()
    };
    assert!(too_large.validate_config().is_err());

    Ok(())
}

#[sinex_test]
fn enforces_processing_interval_bounds() -> color_eyre::eyre::Result<()> {
    let too_fast = FilesystemConfig {
        processing_interval_ms: 50,
        ..base_config()
    };
    assert!(too_fast.validate_config().is_err());

    let too_slow = FilesystemConfig {
        processing_interval_ms: 120_000,
        ..base_config()
    };
    assert!(too_slow.validate_config().is_err());

    Ok(())
}

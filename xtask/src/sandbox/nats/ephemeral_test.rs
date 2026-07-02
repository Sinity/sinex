use super::*;
use crate::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn test_log_tail_surfaces_read_failures() -> Result<()> {
    let store = TempDir::new()?;
    let missing = store.path().join("missing.log");
    let nats = EphemeralNats {
        process: Arc::new(AsyncMutex::new(None)),
        url: "127.0.0.1:4222".to_string(),
        _store: store,
        log_path: Some(missing.clone()),
        chaos: None,
        stream_prefix: None,
        tls: None,
        token: None,
    };

    let err = nats
        .log_tail(20)
        .expect_err("missing log should surface an error");
    assert!(
        err.to_string().contains("failed to read NATS log"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn test_poll_child_exit_status_surfaces_try_wait_failures() -> Result<()> {
    let err = EphemeralNats::poll_child_exit_status(
        Err(std::io::Error::other("probe exploded")),
        "startup readiness",
    )
    .expect_err("try_wait failures must not be flattened");
    assert!(
        err.to_string()
            .contains("failed to poll nats-server child status during startup readiness"),
        "unexpected error: {err:#}"
    );
    Ok(())
}

#[sinex_test]
async fn store_tempdir_uses_workspace_backed_root() -> Result<()> {
    let root = TempDir::new()?;
    let _guard = EnvGuard::set_single("SINEX_TEST_TMPDIR", root.path().as_os_str());

    let store = EphemeralNatsBuilder::store_tempdir()?;
    let expected_root = crate::config::workspace_root().join(".sinex/test-tmp/nats");
    assert!(
        store.path().starts_with(&expected_root),
        "store path {} should live under workspace-backed NATS root {}",
        store.path().display(),
        expected_root.display()
    );
    assert!(
        !store.path().starts_with(root.path()),
        "store path {} should ignore generic tmp root {}",
        store.path().display(),
        root.path().display()
    );
    Ok(())
}

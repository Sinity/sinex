use super::{resolve_database_url, resolve_nats_url};
use std::ffi::OsString;
use std::sync::LazyLock;
use xtask::sandbox::sinex_test;

static ENV_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn restore_var(key: &str, value: Option<OsString>) {
    match value {
        Some(value) => unsafe { std::env::set_var(key, value) },
        None => unsafe { std::env::remove_var(key) },
    }
}

#[sinex_test]
async fn resolve_nats_url_reports_missing_variable() -> xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let previous = std::env::var_os("SINEX_NATS_URL");
    unsafe { std::env::remove_var("SINEX_NATS_URL") };

    let error = resolve_nats_url().expect_err("missing NATS URL should surface");

    restore_var("SINEX_NATS_URL", previous);

    assert!(error.to_string().contains("SINEX_NATS_URL"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn resolve_nats_url_rejects_non_unicode_override() -> xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let _guard = ENV_LOCK.lock().await;
    let previous = std::env::var_os("SINEX_NATS_URL");
    unsafe { std::env::set_var("SINEX_NATS_URL", OsString::from_vec(vec![0x66, 0x6f, 0x80])) };

    let error = resolve_nats_url().expect_err("non-unicode NATS URL should surface");

    restore_var("SINEX_NATS_URL", previous);

    assert!(error.to_string().contains("not valid UTF-8"));
    Ok(())
}

#[sinex_test]
async fn resolve_database_url_reports_missing_variable() -> xtask::sandbox::TestResult<()> {
    let _guard = ENV_LOCK.lock().await;
    let previous = std::env::var_os("DATABASE_URL");
    unsafe { std::env::remove_var("DATABASE_URL") };

    let error = resolve_database_url().expect_err("missing DATABASE_URL should surface");

    restore_var("DATABASE_URL", previous);

    assert!(error.to_string().contains("DATABASE_URL"));
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn resolve_database_url_rejects_non_unicode_override() -> xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let _guard = ENV_LOCK.lock().await;
    let previous = std::env::var_os("DATABASE_URL");
    unsafe { std::env::set_var("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80])) };

    let error = resolve_database_url().expect_err("non-unicode DATABASE_URL should surface");

    restore_var("DATABASE_URL", previous);

    assert!(error.to_string().contains("DATABASE_URL"));
    assert!(error.to_string().contains("not valid UTF-8"));
    Ok(())
}

// Small inline tests are used here because the helpers are private
// synchronization internals that would otherwise need extra visibility.
use super::*;
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
async fn clone_environment_override_recovers_poisoned_lock() -> xtask::sandbox::TestResult<()> {
    let lock = std::sync::Arc::new(std::sync::RwLock::new(Some(SinexEnvironment::new("dev")?)));
    let poison_target = lock.clone();
    let _ = std::thread::spawn(move || {
        let _guard = poison_target
            .write()
            .expect("lock write succeeds before poisoning");
        panic!("intentional poison for test");
    })
    .join();

    let env =
        clone_environment_override(&lock).expect("poisoned lock should still yield override");
    assert_eq!(env.name, "dev");
    Ok(())
}

#[sinex_test]
async fn restore_environment_override_recovers_poisoned_lock() -> xtask::sandbox::TestResult<()>
{
    let lock = std::sync::Arc::new(std::sync::RwLock::new(Some(SinexEnvironment::new("dev")?)));
    let poison_target = lock.clone();
    let _ = std::thread::spawn(move || {
        let _guard = poison_target
            .write()
            .expect("lock write succeeds before poisoning");
        panic!("intentional poison for test");
    })
    .join();

    let mut previous = Some(SinexEnvironment::new("prod")?);
    restore_environment_override(&lock, &mut previous);

    let restored =
        clone_environment_override(&lock).expect("restored override should survive poison");
    assert_eq!(restored.name, "prod");
    assert!(
        previous.is_none(),
        "restore should consume previous override value"
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn current_environment_rejects_non_unicode_override() -> xtask::sandbox::TestResult<()> {
    use std::os::unix::ffi::OsStringExt;

    let _guard = ENV_LOCK.lock().await;
    let previous = std::env::var_os("SINEX_ENVIRONMENT");
    unsafe {
        std::env::set_var(
            "SINEX_ENVIRONMENT",
            OsString::from_vec(vec![0x64, 0x65, 0x80]),
        );
    };

    let error =
        SinexEnvironment::current().expect_err("non-unicode SINEX_ENVIRONMENT should surface");

    restore_var("SINEX_ENVIRONMENT", previous);

    assert!(error.to_string().contains("must be valid UTF-8"));
    Ok(())
}

// Inline because these helpers are local implementation detail and only exercised via env-driven call sites.
use super::{
    elapsed_seconds_with_warning, env_nonempty_string_optional,
    unix_timestamp_secs_with_warning,
};
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::time::SystemTime;
use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

#[sinex_serial_test]
async fn env_bool_with_default_uses_default_on_invalid_override()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_TEST_BOOL_OVERRIDE", "bogus");

    let value = sinex_primitives::env::bool_or("SINEX_TEST_BOOL_OVERRIDE", true, "test");
    assert!(value);
    Ok(())
}

#[sinex_serial_test]
async fn env_parse_with_default_uses_default_on_invalid_override()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_TEST_U64_OVERRIDE", "bogus");

    let value = sinex_primitives::env::parse_or("SINEX_TEST_U64_OVERRIDE", 42_u64, "test");
    assert_eq!(value, 42);
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn env_string_optional_ignores_non_utf8_override() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_TEST_STRING_OVERRIDE",
        OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
    );

    let value = sinex_primitives::env::var_optional("SINEX_TEST_STRING_OVERRIDE", "test");
    assert_eq!(value, None);
    Ok(())
}

#[sinex_serial_test]
async fn env_nonempty_string_optional_ignores_blank_override() -> xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.set("SINEX_TEST_STRING_OVERRIDE", "   ");

    let value = env_nonempty_string_optional("SINEX_TEST_STRING_OVERRIDE", "test");
    assert_eq!(value, None);
    Ok(())
}

#[sinex_test]
async fn test_elapsed_seconds_with_warning_uses_real_elapsed_time()
-> xtask::sandbox::TestResult<()> {
    let start_time = SystemTime::now()
        .checked_sub(std::time::Duration::from_secs(5))
        .expect("past timestamp");
    let elapsed = elapsed_seconds_with_warning(start_time, "test elapsed");
    assert!(elapsed >= 5);
    Ok(())
}

#[sinex_test]
async fn test_elapsed_seconds_with_warning_clamps_clock_rollback()
-> xtask::sandbox::TestResult<()> {
    let start_time = SystemTime::now()
        .checked_add(std::time::Duration::from_secs(5))
        .expect("future timestamp");
    assert_eq!(elapsed_seconds_with_warning(start_time, "test elapsed"), 0);
    Ok(())
}

#[sinex_test]
async fn test_unix_timestamp_secs_with_warning_preserves_valid_timestamps()
-> xtask::sandbox::TestResult<()> {
    let timestamp = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(42);
    assert_eq!(
        unix_timestamp_secs_with_warning(timestamp, "test timestamp"),
        42
    );
    Ok(())
}

#[sinex_test]
async fn test_unix_timestamp_secs_with_warning_clamps_pre_epoch_clock()
-> xtask::sandbox::TestResult<()> {
    let timestamp = SystemTime::UNIX_EPOCH
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("pre-epoch timestamp");
    assert_eq!(
        unix_timestamp_secs_with_warning(timestamp, "test timestamp"),
        0
    );
    Ok(())
}

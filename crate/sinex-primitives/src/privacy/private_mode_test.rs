use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn missing_private_mode_state_defaults_disabled() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let state = load_private_mode_state(dir.path())?;

    assert!(!state.enabled);
    assert_eq!(state.actor, "unknown");
    assert!(state.affected_source_classes.is_empty());
    Ok(())
}

#[sinex_test]
async fn private_mode_state_round_trips_json_file() -> xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let state = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string(), "clipboard".to_string()],
        Timestamp::UNIX_EPOCH,
    );

    save_private_mode_state(dir.path(), &state)?;
    let loaded = load_private_mode_state(dir.path())?;

    assert_eq!(loaded, state);
    assert!(private_mode_state_path(dir.path()).exists());
    Ok(())
}

#[sinex_test]
async fn expired_private_mode_state_is_not_active() -> xtask::sandbox::TestResult<()> {
    let active = RuntimePrivateModeState::enabled_by(
        "sinity",
        vec!["desktop".to_string()],
        Timestamp::UNIX_EPOCH,
    )
    .with_expires_at(Timestamp::from_unix_timestamp(10));
    let now = Timestamp::from_unix_timestamp(20).expect("valid timestamp expected");

    assert!(!active.is_active_at(now));
    let effective = active.effective_at(now);
    assert!(!effective.enabled);
    assert_eq!(effective.expires_at, Timestamp::from_unix_timestamp(10));
    Ok(())
}

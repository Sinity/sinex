use super::*;
use crate::sandbox::EnvGuard;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn resolve_state_dir_honors_worktree_relocation() -> TestResult<()> {
    let relocated = tempfile::tempdir()?;
    let checkout = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
    env.set("SINEX_DEV_STATE_DIR", relocated.path());
    assert_eq!(CheckoutState::resolve_state_dir(checkout.path()), relocated.path());
    Ok(())
}

#[sinex_test]
async fn resolve_state_dir_defaults_to_checkout_local_state() -> TestResult<()> {
    let checkout = tempfile::tempdir()?;
    let mut env = EnvGuard::with_keys(&["SINEX_DEV_STATE_DIR"]);
    env.clear("SINEX_DEV_STATE_DIR");
    assert_eq!(CheckoutState::resolve_state_dir(checkout.path()), checkout.path().join(".sinex"));
    Ok(())
}

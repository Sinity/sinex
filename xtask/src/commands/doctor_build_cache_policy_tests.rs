use super::*;
use crate::sandbox::{EnvGuard, sinex_test};

#[sinex_test]
async fn test_build_cache_policy_reports_sccache_forced_nonincremental()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::with_keys(&[
        "RUSTC_WRAPPER",
        "CARGO_INCREMENTAL",
        "SINEX_INCREMENTAL_KEEP_PER_CRATE",
    ]);
    env.set("RUSTC_WRAPPER", "/nix/store/hash/bin/sccache");
    env.clear("CARGO_INCREMENTAL");
    env.set("SINEX_INCREMENTAL_KEEP_PER_CRATE", "2");

    let policy = build_cache_policy_summary();

    assert_eq!(
        policy["xtask_cargo_incremental"].as_str(),
        Some("forced-off-for-sccache")
    );
    assert_eq!(policy["incremental_prune_keep_per_crate"].as_u64(), Some(2));
    Ok(())
}

#[sinex_test]
async fn test_build_cache_policy_reports_explicit_incremental_edit_loop()
-> ::xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::with_keys(&["RUSTC_WRAPPER", "CARGO_INCREMENTAL"]);
    env.clear("RUSTC_WRAPPER");
    env.set("CARGO_INCREMENTAL", "1");

    let policy = build_cache_policy_summary();

    assert_eq!(
        policy["xtask_cargo_incremental"].as_str(),
        Some("explicit-incremental-edit-loop")
    );
    Ok(())
}

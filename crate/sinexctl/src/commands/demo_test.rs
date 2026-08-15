use super::{DemoRng, require_confirmation};
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn demo_rng_is_seed_deterministic() -> xtask::sandbox::prelude::TestResult<()> {
    let mut left = DemoRng::new(42);
    let mut right = DemoRng::new(42);
    let mut different = DemoRng::new(43);

    assert_eq!(left.next_u64(), right.next_u64());
    assert_ne!(left.next_u64(), different.next_u64());
    Ok(())
}

#[sinex_test]
async fn demo_requires_explicit_confirmation_before_database_access()
-> xtask::sandbox::prelude::TestResult<()> {
    let error = require_confirmation(false).expect_err("demo writes must require confirmation");
    assert!(
        error.to_string().contains("--confirm"),
        "error should explain the confirmation flag: {error}"
    );
    require_confirmation(true).expect("explicit confirmation should authorize the write path");
    Ok(())
}

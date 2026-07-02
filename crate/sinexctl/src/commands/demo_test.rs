use super::DemoRng;
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

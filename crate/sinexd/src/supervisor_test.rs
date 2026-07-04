use super::automata_enabled_arg;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn automata_enabled_arg_distinguishes_unset_from_empty()
-> xtask::sandbox::TestResult<()> {
    assert_eq!(automata_enabled_arg(None), Some("all"));
    assert_eq!(automata_enabled_arg(Some("")), None);
    assert_eq!(automata_enabled_arg(Some("   ")), None);
    assert_eq!(automata_enabled_arg(Some("interval-lift")), Some("interval-lift"));
    assert_eq!(automata_enabled_arg(Some("all")), Some("all"));
    Ok(())
}

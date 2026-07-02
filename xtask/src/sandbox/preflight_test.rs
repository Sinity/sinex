use super::required_system_test_commands;
use crate::sandbox::sinex_test;

#[sinex_test]
async fn local_system_test_preflight_does_not_require_git_annex()
-> crate::sandbox::prelude::TestResult<()> {
    assert_eq!(required_system_test_commands(), &["git"]);
    assert!(
        !required_system_test_commands().contains(&"git-annex"),
        "ordinary system-test preflight must not require the optional legacy annex backend"
    );
    Ok(())
}

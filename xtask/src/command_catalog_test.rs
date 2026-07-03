use super::*;
use crate::sandbox::prelude::*;

#[sinex_test]
async fn command_catalog_exposes_core_public_surface() -> TestResult<()> {
    let commands = collect_command_catalog();

    assert!(
        commands.len() >= 15,
        "public command catalog unexpectedly shrank to {} entries",
        commands.len()
    );
    for command in ["check", "test", "build", "status", "docs", "schema"] {
        assert!(
            find_command(&commands, command).is_some(),
            "missing public xtask command `{command}`"
        );
    }
    assert!(
        find_command(&commands, "schema strict-diff").is_some(),
        "strict schema drift check must stay discoverable"
    );
    assert!(
        find_command(&commands, "schema backfill status").is_some(),
        "schema backfill status must stay discoverable"
    );
    assert!(
        find_command(&commands, "schema backfill run").is_some(),
        "schema backfill runner must stay discoverable"
    );
    let global_args = collect_global_args();
    for arg in ["json", "list_commands", "bg"] {
        assert!(
            global_args.iter().any(|candidate| candidate.name == arg),
            "missing global xtask arg `{arg}`"
        );
    }
    Ok(())
}

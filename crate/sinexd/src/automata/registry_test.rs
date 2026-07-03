use super::*;
use std::collections::HashSet;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn registered_automata_have_unique_names() -> TestResult<()> {
    let mut seen = HashSet::new();
    for spec in AUTOMATA {
        assert!(
            seen.insert(spec.name),
            "duplicate automaton registry name: {}",
            spec.name
        );
    }
    Ok(())
}

#[sinex_test]
async fn registered_automata_are_bridge_repairable() -> TestResult<()> {
    let mut checked = Vec::new();
    for spec in AUTOMATA {
        let contract = (spec.contract)();
        assert!(
            contract.supports_continuous,
            "{} must be a continuous runtime to use the confirmed-event bridge",
            spec.name
        );
        assert!(
            contract.supports_historical,
            "{} must support historical catch-up before consuming the confirmed-event tail",
            spec.name
        );
        assert!(
            !contract.manages_own_continuous_loop,
            "{} bypasses the generic bridge; add a dedicated loss-window proof before registering it here",
            spec.name
        );
        checked.push(spec.name);
    }

    assert_eq!(checked.len(), AUTOMATA.len());
    assert!(!checked.is_empty(), "automata registry must not be empty");
    Ok(())
}

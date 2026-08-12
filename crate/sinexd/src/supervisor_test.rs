use super::{automata_enabled_arg, jittered_runtime_backoff, runtime_startup_delay};
use std::time::Duration;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn automata_enabled_arg_distinguishes_unset_from_empty() -> xtask::sandbox::TestResult<()> {
    assert_eq!(automata_enabled_arg(None), Some("all"));
    assert_eq!(automata_enabled_arg(Some("")), None);
    assert_eq!(automata_enabled_arg(Some("   ")), None);
    assert_eq!(
        automata_enabled_arg(Some("interval-lift")),
        Some("interval-lift")
    );
    assert_eq!(automata_enabled_arg(Some("all")), Some("all"));
    Ok(())
}

#[sinex_test]
async fn runtime_startup_stagger_is_bounded_and_seeded() -> xtask::sandbox::TestResult<()> {
    let first = runtime_startup_delay(1);
    let second = runtime_startup_delay(2);
    assert!(first < Duration::from_secs(2));
    assert!(second < Duration::from_secs(2));
    assert_ne!(first, second);
    Ok(())
}

#[sinex_test]
async fn runtime_retry_backoff_has_bounded_jitter() -> xtask::sandbox::TestResult<()> {
    let base = Duration::from_secs(8);
    let first = jittered_runtime_backoff(base, 1);
    let second = jittered_runtime_backoff(base, u64::MAX);
    assert!(first >= base);
    assert!(second >= base);
    assert!(first <= Duration::from_secs(12));
    assert!(second <= Duration::from_secs(12));
    assert_ne!(first, second);
    Ok(())
}

/// sinex-ijz6: `SINEX_AUTOMATA_ENABLED` unset must select the 2026-07-08
/// ratified default-enabled set (canonicalizer, session-detector,
/// hourly-summarizer, daily-summarizer, health, attention-stream,
/// interval-lift -- 7 of 16), not "all". `automata_enabled_arg`'s own comment
/// still cites the superseded #1087 all-enabled default.
#[sinex_test]
#[ignore = "sinex-ijz6 open: SINEX_AUTOMATA_ENABLED unset still resolves to all 16 automata instead of the 2026-07-08 ratified 7-automaton default set"]
async fn unset_automata_enabled_selects_the_ratified_default_set_not_all()
-> xtask::sandbox::TestResult<()> {
    let effective = automata_enabled_arg(None);
    let selected = crate::automata::registry::parse_enabled(effective)
        .map_err(|e| color_eyre::eyre::eyre!("parse_enabled: {e}"))?;
    let mut names: Vec<&str> = selected.iter().map(|spec| spec.name).collect();
    names.sort_unstable();

    let mut ratified = vec![
        "canonicalizer",
        "session",
        "hourly",
        "daily",
        "health",
        "attention-stream",
        "interval-lift",
    ];
    ratified.sort_unstable();

    assert_eq!(
        names,
        ratified,
        "SINEX_AUTOMATA_ENABLED unset selected {} automata instead of the ratified \
         7-automaton default set -- entity-extractor/resolver/enricher, \
         relation-extractor, analytics, tag-applier, and embedding-producer must \
         stay default-off per the 2026-07-08 retire-until-needed ruling",
        names.len()
    );
    Ok(())
}
